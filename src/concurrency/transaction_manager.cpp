//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <mutex>  // NOLINT
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "common/macros.h"
#include "storage/table/table_heap.h"
namespace bustub {

void TransactionManager::Commit(Transaction *txn) {
  // Release all the locks.
  ReleaseLocks(txn);

  txn->SetState(TransactionState::COMMITTED);
}

void TransactionManager::Abort(Transaction *txn) {
  fmt::print("[ABORT] {}\n", txn->GetTransactionId());
  auto table_write_set = txn->GetWriteSet();
  while (!table_write_set->empty()) {
    auto table_write_record = table_write_set->front();
    table_write_set->pop_front();
    TupleMeta meta = table_write_record.table_heap_->GetTupleMeta(table_write_record.rid_);
    meta.is_deleted_ = !meta.is_deleted_;
    table_write_record.table_heap_->UpdateTupleMeta(meta, table_write_record.rid_);
  }

  ReleaseLocks(txn);

  txn->SetState(TransactionState::ABORTED);
}

void TransactionManager::BlockAllTransactions() { UNIMPLEMENTED("block is not supported now!"); }

void TransactionManager::ResumeTransactions() { UNIMPLEMENTED("resume is not supported now!"); }

}  // namespace bustub
