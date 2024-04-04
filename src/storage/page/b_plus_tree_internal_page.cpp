//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, and set max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType { return array_[index].first; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const -> int {
  for (int i = 0; i < GetSize(); i++) {
    if (array_[i].second == value) {
      return i;
    }
  }
  return -1;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InternalFind(const KeyType &key, const KeyComparator &comparator) const
    -> page_id_t {
  // 二分查找
  int low = 1;  // 数组从索引1开始存储key
  int size = GetSize();
  int high = size - 1;
  if (comparator(key, array_[low].first) < 0) {
    return static_cast<page_id_t>(array_[0].second);  // key小于最小值
  }
  if (comparator(key, array_[high].first) >= 0) {
    return static_cast<page_id_t>(array_[high].second);  // key大于等于最大值，返回数组最后一个有效元素的索引
  }
  while (low <= high) {
    int mid = low + (high - low) / 2;
    if (comparator(key, array_[mid].first) < 0) {
      high = mid - 1;  // key比当前元素小，则往前查找
    } else {
      low = mid + 1;  // key比当前元素大或相等，则往后查找
    }
  }
  return static_cast<page_id_t>(array_[high].second);  // 返回找到的索引
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Add(const KeyType key, const page_id_t page_id, const KeyComparator comparator) {
  int size = GetSize();
  int i = size - 1;
  while (i >= 0 && comparator(key, array_[i].first) < 0) {
    array_[i + 1] = array_[i];
    i--;
  }
  array_[i + 1].first = key;
  array_[i + 1].second = page_id;
  SetSize(GetSize() + 1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::RedistributeWithInsert(BPlusTreeInternalPage *page,
                                                            BPlusTreeInternalPage *new_page, const KeyType key,
                                                            const page_id_t page_id, const KeyComparator comparator) {
  int half_size = page->GetMaxSize() / 2;
  bool is_not_balance = false;
  if (comparator(key, page->array_[half_size].first) > 0 && page->GetMaxSize() % 2 == 1) {
    // 可能会不balance
    half_size++;
    is_not_balance = true;
  }
  for (int i = 0; i < page->GetMaxSize() - half_size; i++) {
    new_page->array_[i] = page->array_[i + half_size];
  }
  page->SetSize(half_size);
  new_page->SetSize(page->GetMaxSize() - half_size);
  // 判断新的key插入哪
  if (is_not_balance || comparator(key, new_page->array_[0].first) > 0) {
    new_page->Add(key, page_id, comparator);
  } else {
    page->Add(key, page_id, comparator);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(KeyType key, KeyComparator comparator) {
  // 二分查找
  int low = 0;
  int high = GetSize() - 1;

  while (low <= high) {
    int mid = low + (high - low) / 2;
    int cmp = comparator(array_[mid].first, key);
    if (cmp < 0) {
      low = mid + 1;
    } else if (cmp > 0) {
      high = mid - 1;
    } else {
      // 如果找到key，删除对应元素
      for (int i = mid; i < GetSize(); i++) {
        array_[i] = array_[i + 1];
      }
      SetSize(GetSize() - 1);
      return;
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InternalMerge(BPlusTreeInternalPage *page, BPlusTreeInternalPage *sibling_page) {
  int i = page->GetSize();
  for (int j = 0; j < sibling_page->GetSize(); j++) {
    page->array_[i] = sibling_page->array_[j];
    i++;
  }
  page->SetSize(page->GetSize() + sibling_page->GetSize());
  sibling_page->SetSize(0);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveOneKey(BPlusTreeInternalPage *page, BPlusTreeInternalPage *sibling_page) {
  if (page->GetSize() < sibling_page->GetSize()) {
    page->array_[page->GetSize()] = sibling_page->array_[0];
    for (int i = 0; i < sibling_page->GetSize(); i++) {
      sibling_page->array_[i] = sibling_page->array_[i + 1];
    }
    page->SetSize(page->GetSize() + 1);
    sibling_page->SetSize(sibling_page->GetSize() - 1);
  } else {
    for (int i = sibling_page->GetSize(); i > 0; i--) {
      sibling_page->array_[i] = sibling_page->array_[i - 1];
    }
    sibling_page->array_[0] = page->array_[page->GetSize() - 1];
    page->SetSize(page->GetSize() - 1);
    sibling_page->SetSize(sibling_page->GetSize() + 1);
  }
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
