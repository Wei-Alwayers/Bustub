#include "primer/trie.h"
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {

  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.

  std::shared_ptr<const TrieNode> current = root_;
  if(current == nullptr){
    return nullptr;
  }

  for(char c : key){
    auto childIt = current->children_.find(c);

    if (childIt != current->children_.end()) {
      // Child with character 'c' exists in the map
      current = childIt->second;
    } else {
      // Child with character 'c' does not exist in the map
      return nullptr;
    }
  }

  // Attempt to dynamic_cast to const TrieNodeWithValue<T>*
  std::shared_ptr<const TrieNodeWithValue<T>> currentWithValue = std::dynamic_pointer_cast<const TrieNodeWithValue<T>>(current);
  if(currentWithValue == nullptr){
    return nullptr;
  }else{
    return currentWithValue->value_.get();
  }

}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.


  std::shared_ptr<const TrieNode> current = root_;
  // 特殊情况：key == “”
  if(key.empty()){
    std::shared_ptr<const TrieNodeWithValue<T>> newRoot_ = std::make_shared<TrieNodeWithValue<T>>(current->children_, std::make_shared<T>(std::move(value)));
    Trie newTrie = Trie(newRoot_);
    return newTrie;
  }
  std::shared_ptr<const TrieNode> newRoot_;
  if(current != nullptr){
    newRoot_ = std::shared_ptr<const TrieNode>(root_->Clone());
  }else{
    newRoot_ = std::make_shared<const TrieNode>();
  }
  std::shared_ptr<TrieNode> newPre = std::const_pointer_cast<TrieNode>(newRoot_); // 来更新map，不能用const

  for (auto it = std::begin(key); it != std::end(key); ++it) {
    char c = *it;
    if (current != nullptr && current->children_.find(c) != current->children_.end()) {
        // Child with character 'c' exists in the map
        auto childIt = current->children_.find(c);
        current = childIt->second;
        if (std::next(it) == std::end(key)){
          std::shared_ptr<const TrieNodeWithValue<T>> newNode = std::make_shared<TrieNodeWithValue<T>>(current->children_, std::make_shared<T>(std::move(value)));
          newPre->children_[c] = newNode;
        }
        else{
          std::shared_ptr<const TrieNode> newNode = std::shared_ptr<const TrieNode>(current->Clone());
          newPre->children_[c] = newNode;
          newPre = std::const_pointer_cast<TrieNode>(newNode);
        }

    }
    else {
        // Child with character 'c' does not exist in the map
        if (std::next(it) == std::end(key)){
          // 最后一个nodeWithValue
          std::shared_ptr<const TrieNodeWithValue<T>> newNode = std::make_shared<TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)));
          newPre->children_[c] = newNode;
        }
        else{
          std::shared_ptr<const TrieNode> newNode = std::make_shared<TrieNode>();
          newPre->children_[c] = newNode;
          newPre = std::const_pointer_cast<TrieNode>(newNode);
          current = newNode;
        }
    }
  }

  Trie newTrie = Trie(newRoot_);
  return newTrie;

}

auto Trie::Remove(std::string_view key) const -> Trie {

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.

  std::shared_ptr<const TrieNode> current = root_;
  std::shared_ptr<const TrieNode> newRoot_;
  if(current != nullptr){
    newRoot_ = std::shared_ptr<const TrieNode>(root_->Clone());
  }else{
    newRoot_ = std::make_shared<const TrieNode>();
  }
  std::shared_ptr<TrieNode> newPre = std::const_pointer_cast<TrieNode>(newRoot_); // 来更新map，不能用const

  for (auto it = std::begin(key); it != std::end(key); ++it) {
    char c = *it;
    if (current != nullptr && current->children_.find(c) != current->children_.end()) {
      // Child with character 'c' exists in the map
      auto childIt = current->children_.find(c);
      current = childIt->second;

      if (std::next(it) == std::end(key)){
        // 最后一个，删除value
        std::shared_ptr<const TrieNode> newNode = std::shared_ptr<const TrieNode>(new TrieNode(current->children_));
        newPre->children_[c] = newNode;
      }
      else{
        std::shared_ptr<const TrieNode> newNode = std::shared_ptr<const TrieNode>(current->Clone());
        newPre->children_[c] = newNode;
        newPre = std::const_pointer_cast<TrieNode>(newNode);
      }
    }
    else {
      break;
    }
  }

  Trie newTrie = Trie(newRoot_);
  return newTrie;
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
