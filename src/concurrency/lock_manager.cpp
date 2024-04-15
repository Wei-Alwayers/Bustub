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
  if(CanTxnTakeLock(txn, lock_mode)) {
    std::lock_guard<std::mutex> lock(table_lock_map_latch_);
    if (table_lock_map_.find(oid) == table_lock_map_.end()) {
      // table之前没有任何lock
      std::shared_ptr<LockRequest> lock_request =
          std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
      // 直接响应lock request
      lock_request->granted_ = true;
      auto lock_request_queue = std::make_shared<LockRequestQueue>();
      lock_request_queue->AddLockRequest(lock_request);
      table_lock_map_[oid] = lock_request_queue;
      InsertTxnTableLockSet(txn, lock_mode, oid);
      return true;
    }
    // 该table正在被lock
    return UpgradeLockTable(txn, lock_mode, oid);
  }
  return false;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  std::lock_guard<std::mutex> lock(table_lock_map_latch_);
  // 检查txn是否lock该table
  if(table_lock_map_.find(oid) == table_lock_map_.end()){
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  // TODO: 检查该table的row有无lock
  std::shared_ptr<LockRequestQueue> lock_request_queue = table_lock_map_[oid];
  LockMode lock_mode;
  if(lock_request_queue->RemoveLockRequest(&lock_mode, txn->GetTransactionId())){
    // TODO：需要通知其他txn获取resource
    DeleteTxnTableLockSet(txn, lock_mode, oid);
    TransactionStateUpdate(txn, lock_mode);
  }
  else {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  return true;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid, bool force) -> bool {
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

auto LockManager::UpgradeLockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool{
  std::shared_ptr<LockRequestQueue> lock_request_queue;
  lock_request_queue = table_lock_map_[oid];
  // 检查当前事务是否拥有该锁
  LockMode cur_lock_mode;
  std::lock_guard<std::mutex> lock(lock_request_queue->latch_);
  for (auto it = lock_request_queue->request_queue_.begin(); it != lock_request_queue->request_queue_.end(); ++it) {
    if ((*it)->txn_id_ == txn->GetTransactionId() && (*it)->granted_) {
      cur_lock_mode = (*it)->lock_mode_;
      // 检查是否符合升级规则
      if (CanLockUpgrade(cur_lock_mode, lock_mode)) {
        // 升级锁
        if (cur_lock_mode != lock_mode) {
          // 检查该table有无其他txn在升级
          if(lock_request_queue->upgrading_ == INVALID_TXN_ID || lock_request_queue->upgrading_ == txn->GetTransactionId()){
            (*it)->lock_mode_ = lock_mode;
            lock_request_queue->upgrading_ = txn->GetTransactionId();
            DeleteTxnTableLockSet(txn, cur_lock_mode, oid);
            InsertTxnTableLockSet(txn, lock_mode, oid);
            return true;
          }
          txn->SetState(TransactionState::ABORTED);
          throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
        }
        return false;
      }
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
    }
  }
  // 没有锁，增加一个请求
  std::shared_ptr<LockRequest> lock_request =
      std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
  // TODO: 似乎不能立即granted吧
  lock_request->granted_ = true;
  lock_request_queue->AddLockRequest(lock_request);
  InsertTxnTableLockSet(txn, lock_mode, oid);
  table_lock_map_[oid] = lock_request_queue;
  return false;
}

auto LockManager::CanTxnTakeLock(Transaction *txn, LockMode lock_mode) ->bool {
  if(txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED){
    if(txn->GetState() == TransactionState::GROWING){
      if(lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::INTENTION_EXCLUSIVE){
        return true;
      }
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    }
    if(txn->GetState() == TransactionState::SHRINKING){
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
    return false;
  }
  if(txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED){
    if(txn->GetState() == TransactionState::GROWING){
      return true;
    }
    if(txn->GetState() == TransactionState::SHRINKING){
      if(lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED){
        return true;
      }
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
    return false;
  }
  if(txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ){
    if(txn->GetState() == TransactionState::GROWING){
      return true;
    }
    if(txn->GetState() == TransactionState::SHRINKING){
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
    return false;
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
}  // namespace bustub
