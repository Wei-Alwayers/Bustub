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
  if (current == nullptr) {
    return nullptr;
  }

  for (char c : key) {
    auto child_it = current->children_.find(c);

    if (child_it != current->children_.end()) {
      // Child with character 'c' exists in the map
      current = child_it->second;
    } else {
      // Child with character 'c' does not exist in the map
      return nullptr;
    }
  }

  // Attempt to dynamic_cast to const TrieNodeWithValue<T>*
  std::shared_ptr<const TrieNodeWithValue<T>> current_with_value =
      std::dynamic_pointer_cast<const TrieNodeWithValue<T>>(current);
  if (current_with_value == nullptr) {
    return nullptr;
  }
  return current_with_value->value_.get();
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.

  std::shared_ptr<const TrieNode> current = root_;
  // 特殊情况：key == “”
  if (key.empty()) {
    std::shared_ptr<const TrieNodeWithValue<T>> new_root =
        std::make_shared<TrieNodeWithValue<T>>(current->children_, std::make_shared<T>(std::move(value)));
    Trie new_trie = Trie(new_root);
    return new_trie;
  }
  std::shared_ptr<const TrieNode> new_root;
  if (current != nullptr) {
    new_root = std::shared_ptr<const TrieNode>(root_->Clone());
  } else {
    new_root = std::make_shared<const TrieNode>();
  }
  std::shared_ptr<TrieNode> new_pre = std::const_pointer_cast<TrieNode>(new_root);  // 来更新map，不能用const

  bool is_with_children = false;
  char last_c;
  for (auto it = std::begin(key); it != std::end(key); ++it) {
    char c = *it;
    if (current != nullptr && current->children_.find(c) != current->children_.end()) {
      // Child with character 'c' exists in the map
      auto child_it = current->children_.find(c);
      current = child_it->second;
      if (std::next(it) == std::end(key)) {
        is_with_children = true;
        last_c = c;
      } else {
        std::shared_ptr<const TrieNode> new_node = std::shared_ptr<const TrieNode>(current->Clone());
        new_pre->children_[c] = new_node;
        new_pre = std::const_pointer_cast<TrieNode>(new_node);
      }

    } else {
      // Child with character 'c' does not exist in the map
      if (std::next(it) == std::end(key)) {
        last_c = c;
      } else {
        std::shared_ptr<const TrieNode> new_node = std::make_shared<TrieNode>();
        new_pre->children_[c] = new_node;
        new_pre = std::const_pointer_cast<TrieNode>(new_node);
        current = new_node;
      }
    }
  }

  if (is_with_children) {
    std::shared_ptr<const TrieNodeWithValue<T>> new_node =
        std::make_shared<TrieNodeWithValue<T>>(current->children_, std::make_shared<T>(std::move(value)));
    new_pre->children_[last_c] = new_node;
  } else {
    // 最后一个nodeWithValue
    std::shared_ptr<const TrieNodeWithValue<T>> new_node =
        std::make_shared<TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)));
    new_pre->children_[last_c] = new_node;
  }
  Trie new_trie = Trie(new_root);
  return new_trie;
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.

  std::shared_ptr<const TrieNode> current = root_;
  std::shared_ptr<const TrieNode> new_root;
  if (current != nullptr) {
    new_root = std::shared_ptr<const TrieNode>(root_->Clone());
  } else {
    new_root = std::make_shared<const TrieNode>();
  }
  std::shared_ptr<TrieNode> new_pre = std::const_pointer_cast<TrieNode>(new_root);  // 来更新map，不能用const

  for (auto it = std::begin(key); it != std::end(key); ++it) {
    char c = *it;
    if (current != nullptr && current->children_.find(c) != current->children_.end()) {
      // Child with character 'c' exists in the map
      auto child_it = current->children_.find(c);
      current = child_it->second;

      if (std::next(it) == std::end(key)) {
        // 最后一个，删除value
        std::shared_ptr<const TrieNode> new_node = std::shared_ptr<const TrieNode>(new TrieNode(current->children_));
        new_pre->children_[c] = new_node;
      } else {
        std::shared_ptr<const TrieNode> new_node = std::shared_ptr<const TrieNode>(current->Clone());
        new_pre->children_[c] = new_node;
        new_pre = std::const_pointer_cast<TrieNode>(new_node);
      }
    } else {
      break;
    }
  }

  Trie new_trie = Trie(new_root);
  return new_trie;
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
