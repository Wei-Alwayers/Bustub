//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include <utility>
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  // you may define your own constructor based on your member variables
  IndexIterator(ReadPageGuard guard, int index, BufferPoolManager *bpm) {
    index_ = index;
    guard_ = std::move(guard);
    bpm_ = bpm;
  }
  ~IndexIterator();  // NOLINT

  auto IsEnd() -> bool;

  auto operator*() -> const MappingType &;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool {
    return (this->guard_.PageId() == itr.guard_.PageId() && this->index_ == itr.index_);
  }

  auto operator!=(const IndexIterator &itr) const -> bool {
    return !(this->guard_.PageId() == itr.guard_.PageId() && this->index_ == itr.index_);
  }

 private:
  // add your own private member variables here
  ReadPageGuard guard_;
  BufferPoolManager *bpm_;
  int index_;
};

}  // namespace bustub
