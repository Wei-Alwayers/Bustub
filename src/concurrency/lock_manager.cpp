//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  txn->LockTxn();
  if(CanTxnTakeLock(txn, lock_mode)) {
    table_lock_map_latch_.lock();
    if (table_lock_map_.find(oid) == table_lock_map_.end()) {
      // table之前没有任何lock
      std::shared_ptr<LockRequest> lock_request =
          std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
      // 直接响应lock request
      lock_request->granted_ = true;
      auto lock_request_queue = std::make_shared<LockRequestQueue>();
      lock_request_queue->request_queue_.emplace_back(lock_request);
      InsertTxnTableLockSet(txn, lock_mode, oid);
      table_lock_map_[oid] = lock_request_queue;
      fmt::print("Txn {} locked table {} in {} mode successfully!\n", txn->GetTransactionId(), oid, lock_mode);
      table_lock_map_latch_.unlock();
      txn->UnlockTxn();
      return true;
    }
    // 该table正在被lock
    std::shared_ptr<LockRequestQueue> lock_request_queue;
    lock_request_queue = table_lock_map_[oid];
    table_lock_map_latch_.unlock();
    LockMode cur_lock_mode;

    std::unique_lock<std::mutex> lock(lock_request_queue->latch_);
    // 检查是否符合升级规则
    for (auto it = lock_request_queue->request_queue_.begin(); it != lock_request_queue->request_queue_.end(); ++it) {
      if ((*it)->txn_id_ == txn->GetTransactionId()) {
        cur_lock_mode = (*it)->lock_mode_;
        if (CanLockUpgrade(cur_lock_mode, lock_mode)) {
          // 升级锁
          if (cur_lock_mode != lock_mode) {
            if(lock_request_queue->upgrading_ == INVALID_TXN_ID){
              // upgrade
              lock_request_queue->upgrading_ = txn->GetTransactionId();
              // 释放之前的锁
              fmt::print("[upgrading...]Txn {} unlocked table {} cur mode {}!\n", txn->GetTransactionId(), oid, cur_lock_mode);
              DeleteTxnTableLockSet(txn, cur_lock_mode, oid);
              it = lock_request_queue->request_queue_.erase(it);
            }
            else{
              txn->SetState(TransactionState::ABORTED);
              throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
            }
          }
          else {
            // 和cur mode一致，不需要任何处理
            lock_request_queue->latch_.unlock();
            txn->UnlockTxn();
            return true;
          }
        }
        else{
          txn->SetState(TransactionState::ABORTED);
          txn->UnlockTxn();
          throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
        }
      }
    }
    // 增加一个请求
    std::shared_ptr<LockRequest> lock_request =
        std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
    lock_request_queue->request_queue_.push_back(lock_request);
    txn_id_t txn_id = txn->GetTransactionId();
    txn->UnlockTxn();
    while (!GrantLock(txn_id, lock_request_queue,lock_mode)){
      lock_request_queue->cv_.wait(lock);
    }
    txn->LockTxn();
    for (auto it = lock_request_queue->request_queue_.begin(); it != lock_request_queue->request_queue_.end(); ++it) {
      if ((*it)->txn_id_ == txn_id) {
        if(txn->GetState() == TransactionState::ABORTED){
          lock_request_queue->request_queue_.erase(it);
          txn->UnlockTxn();
          return false;
        }
        else{
          // 可以获得锁
          (*it)->granted_ = true;
          InsertTxnTableLockSet(txn, lock_mode, oid);
          fmt::print("Txn {} locked table {} in {} mode successfully!\n", txn->GetTransactionId(), oid, lock_mode);
          lock_request_queue->cv_.notify_all();
          break;
        }
      }
    }
    txn->UnlockTxn();
    return true;
  }
  else{
    if(txn->GetState() == TransactionState::SHRINKING){
      txn->SetState(TransactionState::ABORTED);
      txn->UnlockTxn();
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
    else {
      txn->SetState(TransactionState::ABORTED);
      txn->UnlockTxn();
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    }
  }
}

auto LockManager::GrantLock(txn_id_t txn_id, std::shared_ptr<LockRequestQueue> lock_request_queue, LockMode lock_mode) -> bool{
  //考虑是否和当前granted锁兼容
  bool is_not_compatible = false;
  for (const auto& request : lock_request_queue->request_queue_) {
    if (request->granted_ && !AreLocksCompatible(lock_mode, request->lock_mode_)) {
      is_not_compatible = true;
      break;
    }
  }
  if(is_not_compatible){
    return false;
  }
  // upgrading优先级最高
  if(lock_request_queue->upgrading_ != INVALID_TXN_ID){
    if(lock_request_queue->upgrading_ == txn_id){
      return true;
    }
    return false;
  }
  // FIFO
  bool is_first = false;
  for (const auto& request : lock_request_queue->request_queue_) {
    if (!request->granted_) {
      if(request->txn_id_ == txn_id){
        is_first = true;
      }
      break;
    }
  }
  return is_first;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  txn->LockTxn();
  table_lock_map_latch_.lock();
  // 检查txn是否lock该table
  if(!CheckTxnTableLockSet(txn, oid)){
    txn->SetState(TransactionState::ABORTED);
    table_lock_map_latch_.unlock();
    txn->UnlockTxn();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  if(CheckTxnRowLockSetTable(txn, oid)){
    txn->SetState(TransactionState::ABORTED);
    table_lock_map_latch_.unlock();
    txn->UnlockTxn();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }
  txn->UnlockTxn();
  std::shared_ptr<LockRequestQueue> lock_request_queue = table_lock_map_[oid];
  LockMode lock_mode;
  if(lock_request_queue->RemoveLockRequest(&lock_mode, txn->GetTransactionId())){
    table_lock_map_latch_.unlock();
    txn->LockTxn();
    DeleteTxnTableLockSet(txn, lock_mode, oid);
    fmt::print("Txn {} unlocked table {}!\n", txn->GetTransactionId(), oid);
    TransactionStateUpdate(txn, lock_mode);
    txn->UnlockTxn();
  }
  else {
    txn->SetState(TransactionState::ABORTED);
    table_lock_map_latch_.unlock();
    txn->UnlockTxn();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  return true;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  txn->LockTxn();
  if( lock_mode == LockMode::INTENTION_SHARED ||
      lock_mode ==LockMode::INTENTION_EXCLUSIVE ||
      lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    txn->SetState(TransactionState::ABORTED);
    txn->UnlockTxn();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }
  if(CanTxnTakeLock(txn, lock_mode)) {
    row_lock_map_latch_.lock();
    if (row_lock_map_.find(rid) == row_lock_map_.end()) {
      // row之前没有任何lock
      std::shared_ptr<LockRequest> lock_request =
          std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
      // 直接响应lock request
      lock_request->granted_ = true;
      auto lock_request_queue = std::make_shared<LockRequestQueue>();
      lock_request_queue->request_queue_.emplace_back(lock_request);
      InsertTxnRowLockSet(txn, lock_mode, oid, rid);
      row_lock_map_[rid] = lock_request_queue;
      fmt::print("Txn {} locked table {} RID {} in {} mode successfully!\n", txn->GetTransactionId(), oid, rid.ToString(), lock_mode);
      row_lock_map_latch_.unlock();
      txn->UnlockTxn();
      return true;
    }
    // 该table正在被lock
    std::shared_ptr<LockRequestQueue> lock_request_queue;
    lock_request_queue = row_lock_map_[rid];
    row_lock_map_latch_.unlock();
    LockMode cur_lock_mode;
    std::unique_lock<std::mutex> lock(lock_request_queue->latch_);
    // 检查是否符合升级规则
    for (auto it = lock_request_queue->request_queue_.begin(); it != lock_request_queue->request_queue_.end(); ++it) {
      if ((*it)->txn_id_ == txn->GetTransactionId()) {
        cur_lock_mode = (*it)->lock_mode_;
        if (CanLockUpgrade(cur_lock_mode, lock_mode)) {
          // 升级锁
          if (cur_lock_mode != lock_mode) {
            if(lock_request_queue->upgrading_ == INVALID_TXN_ID){
              // upgrade
              lock_request_queue->upgrading_ = txn->GetTransactionId();
              // 释放之前的锁
              fmt::print("[upgrading...]Txn {} unlocked table {} rid {} cur mode {}!\n", txn->GetTransactionId(), oid, rid.ToString(), cur_lock_mode);
              DeleteTxnRowLockSet(txn, cur_lock_mode, oid, rid);
              it = lock_request_queue->request_queue_.erase(it);
            }
            else{
              txn->SetState(TransactionState::ABORTED);
              txn->UnlockTxn();
              throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
            }
          }
          else {
            // 和cur mode一致，不需要任何处理
            lock_request_queue->latch_.unlock();
            txn->UnlockTxn();
            return true;
          }
        }
        else{
          txn->SetState(TransactionState::ABORTED);
          txn->UnlockTxn();
          throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
        }
      }
    }
    // 增加一个请求
    std::shared_ptr<LockRequest> lock_request =
        std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
    lock_request_queue->request_queue_.push_back(lock_request);
    txn_id_t txn_id = txn->GetTransactionId();
    txn->UnlockTxn();
    while (!GrantLock(txn_id, lock_request_queue,lock_mode)){
      lock_request_queue->cv_.wait(lock);
    }
    txn->LockTxn();
    for (auto it = lock_request_queue->request_queue_.begin(); it != lock_request_queue->request_queue_.end(); ++it) {
      if ((*it)->txn_id_ == txn_id) {
        if(txn->GetState() == TransactionState::ABORTED){
          lock_request_queue->request_queue_.erase(it);
          txn->UnlockTxn();
          return false;
        }
        else{
          // 可以获得锁
          (*it)->granted_ = true;
          InsertTxnRowLockSet(txn, lock_mode, oid, rid);
          fmt::print("Txn {} locked table {} rid {} in {} mode successfully!\n", txn->GetTransactionId(), oid, rid.ToString(), lock_mode);
          lock_request_queue->cv_.notify_all();
          break;
        }
      }
    }
    txn->UnlockTxn();
    return true;
  }
  else{
    if(txn->GetState() == TransactionState::SHRINKING){
      txn->SetState(TransactionState::ABORTED);
      txn->UnlockTxn();
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
    else {
      txn->SetState(TransactionState::ABORTED);
      txn->UnlockTxn();
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    }
  }
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid, bool force) -> bool {
  txn->LockTxn();
  row_lock_map_latch_.lock();
  // 检查txn是否lock该table
  if(!CheckTxnRowLockSetRow(txn, oid, rid)){
    txn->SetState(TransactionState::ABORTED);
    row_lock_map_latch_.unlock();
    txn->UnlockTxn();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  txn->UnlockTxn();
  std::shared_ptr<LockRequestQueue> lock_request_queue = row_lock_map_[rid];
  LockMode lock_mode;
  if(lock_request_queue->RemoveLockRequest(&lock_mode, txn->GetTransactionId())){
    row_lock_map_latch_.unlock();
    txn->LockTxn();
    DeleteTxnRowLockSet(txn, lock_mode, oid, rid);
    fmt::print("Txn {} unlocked table {} rid {}", txn->GetTransactionId(), oid, rid.ToString());
    if(!force){
      TransactionStateUpdate(txn, lock_mode);
    }
    txn->UnlockTxn();
  }
  else {
    txn->SetState(TransactionState::ABORTED);
    row_lock_map_latch_.unlock();
    txn->UnlockTxn();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  return true;
}

void LockManager::UnlockAll() {
  // You probably want to unlock all table and txn locks here.
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool { return false; }

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
    }
  }
}



auto LockManager::CanTxnTakeLock(Transaction *txn, LockMode lock_mode) ->bool {
  if(txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED){
    if(txn->GetState() == TransactionState::GROWING){
      if(lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::INTENTION_EXCLUSIVE){
        return true;
      }
    }
  }
  if(txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED){
    if(txn->GetState() == TransactionState::GROWING){
      return true;
    }
    if(txn->GetState() == TransactionState::SHRINKING){
      if(lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED){
        return true;
      }
    }
  }
  if(txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ){
    if(txn->GetState() == TransactionState::GROWING){
      return true;
    }
  }
  return false;
}

auto LockManager::CanLockUpgrade(LockMode curr_lock_mode, LockMode requested_lock_mode) -> bool{
  if(curr_lock_mode == requested_lock_mode){
    return true;
  }
  if(curr_lock_mode == LockMode::INTENTION_SHARED){
    if( requested_lock_mode == LockMode::SHARED ||
        requested_lock_mode == LockMode::EXCLUSIVE ||
        requested_lock_mode == LockMode::INTENTION_EXCLUSIVE ||
        requested_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE){
      return true;
    }
  }
  if(curr_lock_mode == LockMode::SHARED){
    if( requested_lock_mode == LockMode::EXCLUSIVE ||
        requested_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE){
      return true;
    }
  }
  if(curr_lock_mode == LockMode::INTENTION_EXCLUSIVE){
    if( requested_lock_mode == LockMode::EXCLUSIVE ||
        requested_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE){
      return true;
    }
  }
  if(curr_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE){
    if( requested_lock_mode == LockMode::EXCLUSIVE){
      return true;
    }
  }
  return false;
}

void LockManager::InsertTxnTableLockSet(Transaction *txn, LockMode lock_mode, const table_oid_t &oid){
  std::shared_ptr<std::unordered_set<table_oid_t>> table_lock_set;
  if(lock_mode == LockMode::SHARED){
    table_lock_set = txn->GetSharedTableLockSet();
  } else if(lock_mode == LockMode::EXCLUSIVE){
    table_lock_set = txn->GetExclusiveTableLockSet();
  } else if(lock_mode == LockMode::INTENTION_SHARED){
    table_lock_set = txn->GetIntentionSharedTableLockSet();
  } else if(lock_mode == LockMode::INTENTION_EXCLUSIVE){
    table_lock_set = txn->GetIntentionExclusiveTableLockSet();
  } else if(lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE){
    table_lock_set = txn->GetSharedIntentionExclusiveTableLockSet();
  }
  table_lock_set->insert(oid);
}

void LockManager::InsertTxnRowLockSet(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid){
  std::shared_ptr<std::unordered_map<table_oid_t, std::unordered_set<RID>>> row_lock_set;
  if(lock_mode == LockMode::SHARED){
    row_lock_set = txn->GetSharedRowLockSet();
  } else if(lock_mode == LockMode::EXCLUSIVE){
    row_lock_set = txn->GetExclusiveRowLockSet();
  }
  (*row_lock_set)[oid].insert(rid);
}

void LockManager::DeleteTxnTableLockSet(Transaction *txn, LockMode lock_mode, const table_oid_t &oid){
  std::shared_ptr<std::unordered_set<table_oid_t>> table_lock_set;
  if(lock_mode == LockMode::SHARED){
    table_lock_set = txn->GetSharedTableLockSet();
  } else if(lock_mode == LockMode::EXCLUSIVE){
    table_lock_set = txn->GetExclusiveTableLockSet();
  } else if(lock_mode == LockMode::INTENTION_SHARED){
    table_lock_set = txn->GetIntentionSharedTableLockSet();
  } else if(lock_mode == LockMode::INTENTION_EXCLUSIVE){
    table_lock_set = txn->GetIntentionExclusiveTableLockSet();
  } else if(lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE){
    table_lock_set = txn->GetSharedIntentionExclusiveTableLockSet();
  }
  table_lock_set->erase(oid);
}

void LockManager::DeleteTxnRowLockSet(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid){
  std::shared_ptr<std::unordered_map<table_oid_t, std::unordered_set<RID>>> row_lock_set;
  if(lock_mode == LockMode::SHARED){
    row_lock_set = txn->GetSharedRowLockSet();
  } else if(lock_mode == LockMode::EXCLUSIVE){
    row_lock_set = txn->GetExclusiveRowLockSet();
  }
  (*row_lock_set)[oid].erase(rid);
}

auto LockManager::CheckTxnTableLockSet(Transaction *txn, const table_oid_t &oid) -> bool{
  std::shared_ptr<std::unordered_set<table_oid_t>> table_lock_set;
  table_lock_set = txn->GetSharedTableLockSet();
  if(table_lock_set->find(oid) != table_lock_set->end()){
    return true;
  }
  table_lock_set = txn->GetExclusiveTableLockSet();
  if(table_lock_set->find(oid) != table_lock_set->end()){
    return true;
  }
  table_lock_set = txn->GetIntentionSharedTableLockSet();
  if(table_lock_set->find(oid) != table_lock_set->end()){
    return true;
  }
  table_lock_set = txn->GetIntentionExclusiveTableLockSet();
  if(table_lock_set->find(oid) != table_lock_set->end()){
    return true;
  }
  table_lock_set = txn->GetSharedIntentionExclusiveTableLockSet();
  if(table_lock_set->find(oid) != table_lock_set->end()){
    return true;
  }
  return false;
}

auto LockManager::CheckTxnRowLockSetTable(Transaction *txn, const table_oid_t &oid) -> bool{
  std::shared_ptr<std::unordered_map<table_oid_t, std::unordered_set<RID>>> row_lock_set;
  row_lock_set = txn->GetSharedRowLockSet();
  if(row_lock_set->find(oid) != row_lock_set->end()){
    return !row_lock_set->find(oid)->second.empty();
  }
  row_lock_set = txn->GetExclusiveRowLockSet();
  if(row_lock_set->find(oid) != row_lock_set->end()){
    return !row_lock_set->find(oid)->second.empty();
  }
  return false;
}

auto LockManager::CheckTxnRowLockSetRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool{
  std::shared_ptr<std::unordered_map<table_oid_t, std::unordered_set<RID>>> row_lock_set;
  row_lock_set = txn->GetSharedRowLockSet();
  if(row_lock_set->find(oid) != row_lock_set->end()){
    if(row_lock_set->find(oid)->second.find(rid) != row_lock_set->find(oid)->second.end()){
      return true;
    }
  }
  row_lock_set = txn->GetExclusiveRowLockSet();
  if(row_lock_set->find(oid) != row_lock_set->end()){
    if(row_lock_set->find(oid)->second.find(rid) != row_lock_set->find(oid)->second.end()){
      return true;
    }
  }
  return false;
}

void LockManager::TransactionStateUpdate(Transaction *txn, LockMode lock_mode){
  if(txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ){
    if( lock_mode == LockMode::SHARED ||
        lock_mode == LockMode::EXCLUSIVE){
      txn->SetState(TransactionState::SHRINKING);
    }
  } else if(txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED){
    if(lock_mode == LockMode::EXCLUSIVE){
      txn->SetState(TransactionState::SHRINKING);
    }
  } else if(txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED){
    if(lock_mode == LockMode::EXCLUSIVE){
      txn->SetState(TransactionState::SHRINKING);
    }
  }
}


bool LockManager::AreLocksCompatible(LockMode l1, LockMode l2) {
  switch (l1) {
    case LockMode::INTENTION_SHARED:
      return (l2 == LockMode::INTENTION_SHARED || l2 == LockMode::INTENTION_EXCLUSIVE ||
              l2 == LockMode::SHARED || l2 == LockMode::SHARED_INTENTION_EXCLUSIVE);
    case LockMode::INTENTION_EXCLUSIVE:
      return (l2 == LockMode::INTENTION_SHARED || l2 == LockMode::INTENTION_EXCLUSIVE);
    case LockMode::SHARED:
      return (l2 == LockMode::INTENTION_SHARED || l2 == LockMode::SHARED);
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      return (l2 == LockMode::INTENTION_SHARED);
    case LockMode::EXCLUSIVE:
      return false; // EXCLUSIVE is incompatible with all other modes
    default:
      return false; // Default case if invalid LockMode is provided
  }
}
}  // namespace bustub
