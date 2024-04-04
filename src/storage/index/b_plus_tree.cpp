#include <sstream>
#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  ReadPageGuard guard = bpm_->FetchPageRead(header_page_id_);
  auto root_page = guard.template As<BPlusTreeHeaderPage>();
  return (root_page->root_page_id_ == INVALID_PAGE_ID);
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *txn) -> bool {
  ReadPageGuard guard = bpm_->FetchPageRead(header_page_id_);
  auto root_page = guard.template As<BPlusTreeHeaderPage>();
  if (root_page->root_page_id_ == INVALID_PAGE_ID) {
    // 树是空的
    return false;
  }
  guard = bpm_->FetchPageRead(root_page->root_page_id_);
  auto page = guard.template As<BPlusTreePage>();
  while (!page->IsLeafPage()) {
    // 顺着内部节点查询
    auto internal_page = guard.template As<InternalPage>();
    page_id_t page_id = internal_page->InternalFind(key, comparator_);
    guard = bpm_->FetchPageRead(page_id);
    page = guard.template As<BPlusTreePage>();
  }
  // 查找到叶节点
  auto leaf_page = guard.template As<LeafPage>();
  ValueType value;
  if (leaf_page->LeafFind(key, comparator_, &value)) {
    // 找到该key
    result->push_back(value);
    return true;
  }
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  if (root_page->root_page_id_ == INVALID_PAGE_ID) {
    // B+树是空的，插入第一条数据，创建一个新page
    bpm_->NewPageGuarded(&root_page->root_page_id_);
    // 创建leaf node
    WritePageGuard leaf_guard = bpm_->FetchPageWrite(root_page->root_page_id_);
    auto leaf_page = leaf_guard.template AsMut<LeafPage>();
    leaf_page->Init(leaf_max_size_);
    leaf_page->SetNextPageId(INVALID_PAGE_ID);
    leaf_page->Add(key, value, comparator_);
    return true;
  }
  // header page放到ctx中
  Context ctx;
  ctx.root_page_id_ = root_page->root_page_id_;
  ctx.header_page_ = std::move(guard);

  guard = bpm_->FetchPageWrite(root_page->root_page_id_);
  auto page = guard.AsMut<BPlusTreePage>();
  while (!page->IsLeafPage()) {
    // 顺着内部节点查询
    auto internal_page = guard.template As<InternalPage>();
    page_id_t page_id = internal_page->InternalFind(key, comparator_);
    ctx.write_set_.push_back(std::move(guard));  // guard更新之前把它internal page放到ctx中
    guard = bpm_->FetchPageWrite(page_id);
    page = guard.AsMut<BPlusTreePage>();
  }
  // 查找到叶节点
  auto leaf_page = guard.template AsMut<LeafPage>();
  ValueType find_value;
  if (leaf_page->LeafFind(key, comparator_, &find_value)) {
    // 该key已经存在
    return false;
  }
  leaf_page->Add(key, value, comparator_);
  if (leaf_page->GetSize() == leaf_page->GetMaxSize()) {
    SplitLeafNode(ctx, guard);
  }
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::SplitLeafNode(Context &ctx, WritePageGuard &guard) {
  auto leaf_page = guard.template AsMut<LeafPage>();
  // 新建leaf node
  page_id_t new_page_id;
  BasicPageGuard new_guard = bpm_->NewPageGuarded(&new_page_id);
  auto new_leaf_page = new_guard.template AsMut<LeafPage>();
  new_leaf_page->Init(leaf_max_size_);
  new_leaf_page->SetNextPageId(INVALID_PAGE_ID);
  // Redistribute leaf page
  LeafPage::Redistribute(leaf_page, new_leaf_page);
  // 设置next page id
  new_leaf_page->SetNextPageId(leaf_page->GetNextPageId());
  leaf_page->SetNextPageId(new_page_id);
  if (guard.PageId() == ctx.root_page_id_) {
    // 根叶节点满了
    // 新建parent node作为新的root node
    page_id_t new_root_id;
    BasicPageGuard new_root_guard = bpm_->NewPageGuarded(&new_root_id);
    auto new_root_page = new_root_guard.template AsMut<InternalPage>();
    new_root_page->Init(internal_max_size_);
    new_root_page->Add(leaf_page->KeyAt(0), guard.PageId(), comparator_);
    new_root_page->Add(new_leaf_page->KeyAt(0), new_page_id, comparator_);
    // 修改root值
    auto root_page = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
    root_page->root_page_id_ = new_root_id;
  } else {
    // 非根节点，说明有parent节点
    // 获取parent节点
    WritePageGuard parent_guard = std::move(ctx.write_set_.back());
    ctx.write_set_.pop_back();
    // 更新old_page_id的key
    auto parent_page = parent_guard.template AsMut<InternalPage>();
    int index = parent_page->ValueIndex(guard.PageId());
    parent_page->SetKeyAt(index, leaf_page->KeyAt(0));

    KeyType new_page_key = new_leaf_page->KeyAt(0);
    InsertIntoInternalNode(parent_guard, new_page_key, new_page_id, ctx);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoInternalNode(WritePageGuard &guard, const KeyType key, const page_id_t page_id,
                                            Context &ctx) {
  auto page = guard.template AsMut<InternalPage>();
  // 先检查是否已满
  if (page->GetSize() == page->GetMaxSize()) {
    // 新建internal node
    page_id_t new_internal_page_id;
    BasicPageGuard new_internal_guard = bpm_->NewPageGuarded(&new_internal_page_id);
    auto new_internal_page = new_internal_guard.template AsMut<InternalPage>();
    new_internal_page->Init(internal_max_size_);
    // Redistribute internal page
    InternalPage::RedistributeWithInsert(page, new_internal_page, key, page_id, comparator_);
    // 检查是否为根节点
    auto root_page = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
    if (guard.PageId() == root_page->root_page_id_) {
      // 新建parent node作为新的root node
      page_id_t new_root_id;
      BasicPageGuard new_root_guard = bpm_->NewPageGuarded(&new_root_id);
      auto new_root_page = new_root_guard.template AsMut<InternalPage>();
      new_root_page->Init(internal_max_size_);
      new_root_page->Add(page->KeyAt(0), guard.PageId(), comparator_);
      new_root_page->Add(new_internal_page->KeyAt(0), new_internal_guard.PageId(), comparator_);
      // 修改root值
      root_page->root_page_id_ = new_root_id;
    } else {
      // 获取parent节点
      WritePageGuard parent_guard = std::move(ctx.write_set_.back());
      ctx.write_set_.pop_back();
      // 更新old_page_id的key
      auto parent_page = parent_guard.template AsMut<InternalPage>();
      int index = parent_page->ValueIndex(guard.PageId());
      parent_page->SetKeyAt(index, page->KeyAt(0));
      // 向parent插入数据
      InsertIntoInternalNode(parent_guard, new_internal_page->KeyAt(0), new_internal_guard.PageId(), ctx);
    }
  } else {
    page->Add(key, page_id, comparator_);
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *txn) {
  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  if (root_page->root_page_id_ == INVALID_PAGE_ID) {
    // 树是空的
    return;
  }
  // header page放到ctx中
  Context ctx;
  ctx.root_page_id_ = root_page->root_page_id_;
  ctx.header_page_ = std::move(guard);

  guard = bpm_->FetchPageWrite(root_page->root_page_id_);
  auto page = guard.AsMut<BPlusTreePage>();
  while (!page->IsLeafPage()) {
    // 顺着内部节点查询
    auto internal_page = guard.template As<InternalPage>();
    page_id_t page_id = internal_page->InternalFind(key, comparator_);
    ctx.write_set_.push_back(std::move(guard));  // guard更新之前把它internal page放到ctx中
    guard = bpm_->FetchPageWrite(page_id);
    page = guard.AsMut<BPlusTreePage>();
  }
  // 查找到叶节点
  auto leaf_page = guard.template AsMut<LeafPage>();
  ValueType find_value;
  if (!leaf_page->LeafFind(key, comparator_, &find_value)) {
    // 该key不存在
    return;
  }
  leaf_page->Remove(key, comparator_);
  // 处理leaf root page
  if(guard.PageId() == ctx.root_page_id_){
    if(leaf_page->GetSize() == 0){
      // 删除page，将root page设置为invalid
      page_id_t page_id = guard.PageId();
      bpm_->DeletePage(page_id);
      root_page->root_page_id_ = INVALID_PAGE_ID;
      guard.Drop();
    }
    return;
  }

  // 不足min size，需要coalesce
  if(leaf_page->GetSize() < leaf_page->GetMinSize()){
    auto leaf_guard = std::move(guard);
    auto parent_guard = std::move(ctx.write_set_.back());
    ctx.write_set_.pop_back();
    auto parent_page = parent_guard.template AsMut<InternalPage>();
    // 确定internal page和internal sibling page
    int index = parent_page->ValueIndex(leaf_guard.PageId());
    WritePageGuard leaf_sibling_guard;
    page_id_t leaf_sibling_page_id;
    if(index < parent_page->GetSize() - 1){
      int sibling_index = index + 1;
      leaf_sibling_page_id = parent_page->ValueAt(sibling_index);
      leaf_sibling_guard = bpm_->FetchPageWrite(leaf_sibling_page_id);
    }
    else{
      int sibling_index = index - 1;
      leaf_sibling_guard = std::move(leaf_guard);
      page_id_t internal_page_id = parent_page->ValueAt(sibling_index);
      leaf_guard = bpm_->FetchPageWrite(internal_page_id);
      leaf_sibling_page_id = parent_page->ValueAt(index);
    }
    leaf_page = leaf_guard.template AsMut<LeafPage>();
    auto leaf_sibling_page = leaf_sibling_guard.template AsMut<LeafPage>();

    if (leaf_sibling_page->GetSize() + leaf_page->GetSize() < leaf_page->GetMaxSize()) {
      KeyType sibling_key = leaf_sibling_page->KeyAt(0);
      // merge
      LeafPage::LeafMerge(leaf_page, leaf_sibling_page);
      // 删除sibling page
      page_id_t sibling_page_id = leaf_sibling_guard.PageId();
      bpm_->DeletePage(sibling_page_id);
      leaf_sibling_guard.Drop();
      // 更新parent page
      parent_page->Remove(sibling_key, comparator_);
      // 检查internal page是否小于min size
      while (parent_page->GetSize() < parent_page->GetMinSize()){
        // 根节点不需要在意min size，大小为1时删除
        if(parent_guard.PageId() == ctx.root_page_id_){
          if(parent_page->GetSize() == 1){
            // 设置新的根节点
            root_page->root_page_id_ = parent_page->ValueAt(0);
            // 删除原有根节点
            page_id_t  old_page_id = parent_guard.PageId();
            bpm_->DeletePage(old_page_id);
            parent_guard.Drop();
          }
          return;
        }
        // 和leaf page一样，去找sibling page
        auto internal_guard = std::move(parent_guard);
        auto internal_page = internal_guard.template AsMut<InternalPage>();
        parent_guard = std::move(ctx.write_set_.back());
        ctx.write_set_.pop_back();
        parent_page = parent_guard.template AsMut<InternalPage>();
        // 确定internal page和internal sibling page
        index = parent_page->ValueIndex(internal_guard.PageId());
        WritePageGuard internal_sibling_guard;
        page_id_t internal_sibling_page_id;
        if(index < parent_page->GetSize() - 1){
          int sibling_index = index + 1;
          internal_sibling_page_id = parent_page->ValueAt(sibling_index);
          internal_sibling_guard = bpm_->FetchPageWrite(internal_sibling_page_id);
        }
        else{
          int sibling_index = index - 1;
          internal_sibling_guard = std::move(internal_guard);
          page_id_t internal_page_id = parent_page->ValueAt(sibling_index);
          internal_guard = bpm_->FetchPageWrite(internal_page_id);
          internal_sibling_page_id = parent_page->ValueAt(index);
        }
        internal_page = internal_guard.template AsMut<InternalPage>();
        auto internal_sibling_page = internal_sibling_guard.template AsMut<InternalPage>();

        if(internal_page->GetSize() + internal_sibling_page->GetSize() <= internal_page ->GetMaxSize()){
          // internal page merge
          KeyType internal_sibling_key = internal_sibling_page->KeyAt(0);
          InternalPage::InternalMerge(internal_page, internal_sibling_page);
          // 删除sibling page
          bpm_->DeletePage(internal_sibling_page_id);
          internal_sibling_guard.Drop();
          // 更新parent page
          parent_page->Remove(internal_sibling_key, comparator_);
        }
        else{
          if(internal_page->GetSize() < internal_sibling_page->GetSize()){
            InternalPage::MoveOneKey(internal_page, internal_sibling_page);
          }
          else{
            InternalPage::MoveOneKey(internal_sibling_page, internal_page);
          }
          // 更新parent
          int internal_sibling_index = parent_page->ValueIndex(internal_sibling_guard.PageId());
          parent_page->SetKeyAt(internal_sibling_index, internal_sibling_page->KeyAt(0));
          int internal_index = parent_page->ValueIndex(internal_guard.PageId());
          parent_page->SetKeyAt(internal_index, internal_page->KeyAt(0));
          // 递归更新
          if(parent_guard.PageId() != root_page->root_page_id_){
            UpdateInternalNode(parent_guard, ctx);
          }
          return;
        }
      }
      return;
    }
    else {
      // move 1 key from sibling page
      if(leaf_page->GetSize() < leaf_sibling_page->GetSize()){
        LeafPage::MoveOneKey(leaf_page, leaf_sibling_page);
      }
      else{
        LeafPage::MoveOneKey(leaf_sibling_page, leaf_page);
      }
      // 更新parent
      int sibling_index = parent_page->ValueIndex(leaf_sibling_guard.PageId());
      parent_page->SetKeyAt(sibling_index, leaf_sibling_page->KeyAt(0));
      index = parent_page->ValueIndex(guard.PageId());
      parent_page->SetKeyAt(index, leaf_page->KeyAt(0));
      // 递归更新
      UpdateInternalNode(parent_guard, ctx);
      return;
    }

  }
  else{
    // 获取parent节点
    WritePageGuard parent_guard = std::move(ctx.write_set_.back());
    ctx.write_set_.pop_back();
    // 更新old_page_id的key
    auto parent_page = parent_guard.template AsMut<InternalPage>();
    int index = parent_page->ValueIndex(guard.PageId());
    if(comparator_(parent_page->KeyAt(index), leaf_page->KeyAt(0)) != 0){
      parent_page->SetKeyAt(index, leaf_page->KeyAt(0));
      UpdateInternalNode(parent_guard, ctx);
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateInternalNode(WritePageGuard &child_guard, Context &ctx){
  auto child_page = child_guard.template AsMut<InternalPage>();
  auto parent_guard = std::move(ctx.write_set_.back());
  ctx.write_set_.pop_back();
  auto parent_page = parent_guard.template AsMut<InternalPage>();
  int index = parent_page->ValueIndex(child_guard.PageId());
  while (comparator_(parent_page->KeyAt(index), child_page->KeyAt(0)) != 0 && parent_guard.PageId() != ctx.root_page_id_){
    // 递归更新internal page的key
    parent_page->SetKeyAt(index, child_page->KeyAt(0));
    child_page = parent_page;
    child_guard = std::move(parent_guard);
    parent_guard = std::move(ctx.write_set_.back());
    ctx.write_set_.pop_back();
    parent_page = parent_guard.template AsMut<InternalPage>();
    index = parent_page->ValueIndex(child_guard.PageId());
  }
}
/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  ReadPageGuard guard = bpm_->FetchPageRead(header_page_id_);
  auto root_page = guard.template As<BPlusTreeHeaderPage>();
  if (root_page->root_page_id_ == INVALID_PAGE_ID) {
    // 树是空的
    throw Exception("The Tree is Empty!");
  }
  guard = bpm_->FetchPageRead(root_page->root_page_id_);
  auto page = guard.template As<BPlusTreePage>();
  while (!page->IsLeafPage()) {
    // 顺着内部节点查询
    auto internal_page = guard.template As<InternalPage>();
    page_id_t page_id = internal_page->ValueAt(0);
    guard = bpm_->FetchPageRead(page_id);
    page = guard.template As<BPlusTreePage>();
  }
  // 查找到叶节点
  return INDEXITERATOR_TYPE(std::move(guard), 0, bpm_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  ReadPageGuard guard = bpm_->FetchPageRead(header_page_id_);
  auto root_page = guard.template As<BPlusTreeHeaderPage>();
  if (root_page->root_page_id_ == INVALID_PAGE_ID) {
    // 树是空的
    throw Exception("The Tree is Empty!");
  }
  guard = bpm_->FetchPageRead(root_page->root_page_id_);
  auto page = guard.template As<BPlusTreePage>();
  while (!page->IsLeafPage()) {
    // 顺着内部节点查询
    auto internal_page = guard.template As<InternalPage>();
    page_id_t page_id = internal_page->InternalFind(key, comparator_);
    guard = bpm_->FetchPageRead(page_id);
    page = guard.template As<BPlusTreePage>();
  }
  // 查找到叶节点
//  auto leaf_page = guard.template As<LeafPage>();
  return INDEXITERATOR_TYPE(std::move(guard), 0, bpm_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  ReadPageGuard guard = bpm_->FetchPageRead(header_page_id_);
  auto root_page = guard.template As<BPlusTreeHeaderPage>();
  if (root_page->root_page_id_ == INVALID_PAGE_ID) {
    // 树是空的
    throw Exception("The Tree is Empty!");
  }
  guard = bpm_->FetchPageRead(root_page->root_page_id_);
  auto page = guard.template As<BPlusTreePage>();
  while (!page->IsLeafPage()) {
    // 顺着内部节点查询
    auto internal_page = guard.template As<InternalPage>();
    page_id_t page_id = internal_page->ValueAt(internal_page->GetSize() - 1);
    guard = bpm_->FetchPageRead(page_id);
    page = guard.template As<BPlusTreePage>();
  }
  // 查找到叶节点
  return INDEXITERATOR_TYPE(std::move(guard), -1, bpm_);
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  BasicPageGuard guard = bpm_->FetchPageBasic(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  return root_page->root_page_id_;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, txn);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, txn);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintTree(page_id_t page_id, const BPlusTreePage *page) {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    std::cout << "Leaf Page: " << page_id << "\tNext: " << leaf->GetNextPageId() << std::endl;

    // Print the contents of the leaf page.
    std::cout << "Contents: ";
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i);
      if ((i + 1) < leaf->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;

  } else {
    auto *internal = reinterpret_cast<const InternalPage *>(page);
    std::cout << "Internal Page: " << page_id << std::endl;

    // Print the contents of the internal page.
    std::cout << "Contents: ";
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i);
      if ((i + 1) < internal->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      auto guard = bpm_->FetchPageBasic(internal->ValueAt(i));
      PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
    }
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Drawing an empty tree");
    return;
  }

  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  ToGraph(guard.PageId(), guard.template As<BPlusTreePage>(), out);
  out << "}" << std::endl;
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out) {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    // Print node name
    out << leaf_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << page_id << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << page_id << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }
  } else {
    auto *inner = reinterpret_cast<const InternalPage *>(page);
    // Print node name
    out << internal_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_guard = bpm_->FetchPageBasic(inner->ValueAt(i));
      auto child_page = child_guard.template As<BPlusTreePage>();
      ToGraph(child_guard.PageId(), child_page, out);
      if (i > 0) {
        auto sibling_guard = bpm_->FetchPageBasic(inner->ValueAt(i - 1));
        auto sibling_page = sibling_guard.template As<BPlusTreePage>();
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_guard.PageId() << " " << internal_prefix
              << child_guard.PageId() << "};\n";
        }
      }
      out << internal_prefix << page_id << ":p" << child_guard.PageId() << " -> ";
      if (child_page->IsLeafPage()) {
        out << leaf_prefix << child_guard.PageId() << ";\n";
      } else {
        out << internal_prefix << child_guard.PageId() << ";\n";
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DrawBPlusTree() -> std::string {
  if (IsEmpty()) {
    return "()";
  }

  PrintableBPlusTree p_root = ToPrintableBPlusTree(GetRootPageId());
  std::ostringstream out_buf;
  p_root.Print(out_buf);

  return out_buf.str();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree {
  auto root_page_guard = bpm_->FetchPageBasic(root_id);
  auto root_page = root_page_guard.template As<BPlusTreePage>();
  PrintableBPlusTree proot;

  if (root_page->IsLeafPage()) {
    auto leaf_page = root_page_guard.template As<LeafPage>();
    proot.keys_ = leaf_page->ToString();
    proot.size_ = proot.keys_.size() + 4;  // 4 more spaces for indent

    return proot;
  }

  // draw internal page
  auto internal_page = root_page_guard.template As<InternalPage>();
  proot.keys_ = internal_page->ToString();
  proot.size_ = 0;
  for (int i = 0; i < internal_page->GetSize(); i++) {
    page_id_t child_id = internal_page->ValueAt(i);
    PrintableBPlusTree child_node = ToPrintableBPlusTree(child_id);
    proot.size_ += child_node.size_;
    proot.children_.push_back(child_node);
  }

  return proot;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
