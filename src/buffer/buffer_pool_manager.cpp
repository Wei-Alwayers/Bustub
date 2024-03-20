//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  frame_id_t replacement_frame;
  if(!free_list_.empty()){
    // 优先从free list中获取replacement frame
    replacement_frame = free_list_.front();
    free_list_.pop_front();
  }
  else{
    if(replacer_->Evict(&replacement_frame)){
      //replacer找到可以替换的frame，检查是否为dirty
      if(pages_[replacement_frame].is_dirty_){
        disk_manager_->WritePage(pages_[replacement_frame].page_id_, pages_[replacement_frame].data_);
        pages_[replacement_frame].is_dirty_ = false;
      }
      pages_[replacement_frame].ResetMemory();
      page_table_.erase(pages_[replacement_frame].page_id_);
    }
    else{
      // 没有找到
      return nullptr;
    }
  }
  // 创建新的page
  page_id_t  new_page_id = AllocatePage();
  *page_id = new_page_id;
  pages_[replacement_frame].page_id_ = *page_id;
  pages_[replacement_frame].pin_count_++;
  page_table_.insert(std::make_pair(*page_id, replacement_frame));
  replacer_->RecordAccess(replacement_frame);
  replacer_->SetEvictable(replacement_frame, false);
  return &pages_[replacement_frame];
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  frame_id_t frame_id;
  if(page_table_.find(page_id) != page_table_.end()){
    // 已经在buffer pool中
    frame_id = page_table_.find(page_id)->second;
    replacer_->RecordAccess(frame_id);
    return &pages_[frame_id];
  }
  // 从disk中读区数据
  char* page_data = new char[BUSTUB_PAGE_SIZE];
  disk_manager_->ReadPage(page_id, page_data);

  if(!free_list_.empty()){
    // 优先从free list中获取replacement frame
    frame_id = free_list_.front();
    free_list_.pop_front();
  }
  else{
    if(replacer_->Evict(&frame_id)){
      //replacer找到可以替换的frame，检查是否为dirty
      if(pages_[frame_id].is_dirty_){
        disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].data_);
        pages_[frame_id].is_dirty_ = false;
      }
      pages_[frame_id].ResetMemory();
      page_table_.erase(pages_[frame_id].page_id_);
    }
    else{
      // 没有找到
      return nullptr;
    }
  }
  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].pin_count_++;
  pages_[frame_id].data_ = page_data;
  page_table_.insert(std::make_pair(page_id, frame_id));
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  return &pages_[frame_id];
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  if(page_table_.find(page_id) == page_table_.end()){
    // 没找到这个page
    return false;
  }
  frame_id_t frame_id = page_table_.find(page_id)->second;
  if(pages_[frame_id].GetPinCount() <= 0){
    // 已经unpin状态，不需要操作
    return false;
  }
  pages_[frame_id].pin_count_--;
  if(pages_[frame_id].pin_count_ == 0){
    replacer_->SetEvictable(frame_id, true);
  }
  pages_[frame_id].is_dirty_ = is_dirty;
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  if(page_table_.find(page_id) == page_table_.end()){
    // 没找到这个page
    return false;
  }
  frame_id_t frame_id = page_table_.find(page_id)->second;
  disk_manager_->WritePage(page_id, pages_[frame_id].data_);
  pages_[frame_id].is_dirty_ = false;
  return true;
}

void BufferPoolManager::FlushAllPages() {
  // 使用迭代器遍历 map
  for (auto it = page_table_.begin(); it != page_table_.end(); ++it) {
    page_id_t page_id = it->first;
    FlushPage(page_id);
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  if(page_table_.find(page_id) == page_table_.end()){
    // 没找到这个page
    return true;
  }
  frame_id_t frame_id = page_table_.find(page_id)->second;
  if(pages_[frame_id].pin_count_ > 0){
    // pin cannot be deleted
    return false;
  }
  replacer_->Remove(frame_id);
  page_table_.erase(page_id);
  free_list_.emplace_back(static_cast<int>(frame_id));
  pages_[frame_id].ResetMemory();
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

}  // namespace bustub
