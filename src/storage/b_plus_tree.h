#pragma once

#include <cstdint>
#include <cstring>
#include <vector>
#include "page_v2.h"

namespace minidb {

// Forward declarations
class BufferPoolManager;
class LogManager;

// Record ID (RID) - points to a specific record
struct RID {
    uint32_t page_id;  // Page containing the record
    uint32_t slot_id;  // Slot within the page
    
    RID() : page_id(INVALID_PAGE_ID), slot_id(0) {}
    RID(uint32_t p, uint32_t s) : page_id(p), slot_id(s) {}
    
    bool IsValid() const { return page_id != INVALID_PAGE_ID; }
};

// B+ Tree node types
enum class NodeType : uint8_t {
    INTERNAL = 0,
    LEAF = 1
};

// B+ Tree node layout (fits in 4096-byte page)
// 
// HEADER (11 bytes):
//   Offset 0:         is_leaf (1 byte) - 0 for internal, 1 for leaf
//   Offset 1:         key_count (2 bytes) - number of keys in node
//   Offset 3:         parent_page_id (4 bytes) - parent node page_id
//   Offset 7:         next_leaf_page_id (4 bytes) - for leaf nodes only (linked list)
// 
// INTERNAL NODE LAYOUT:
//   Offset 11:        keys[key_count] - each key is 8 bytes (int64_t)
//   Offset 11 + (N*8): child_page_ids[key_count + 1] - each is 4 bytes
//                     Relationship: child0 < key0 <= child1 < key1 <= child2 ...
//                     N keys => N+1 children
//
// LEAF NODE LAYOUT:
//   Offset 11:        keys[key_count] - each key is 8 bytes (int64_t)
//   Offset 11 + (N*8): RIDs[key_count] - each RID is 8 bytes (page_id + slot_id)
//                     Keys and RIDs stored in sorted order

class BPlusTreeNode {
public:
    static constexpr uint32_t PAGE_SIZE = 4096;
    
    // Node header offsets
    static constexpr uint32_t OFFSET_IS_LEAF = 0;
    static constexpr uint32_t OFFSET_KEY_COUNT = 1;
    static constexpr uint32_t OFFSET_PARENT_PAGE_ID = 3;
    static constexpr uint32_t OFFSET_NEXT_LEAF = 7;
    static constexpr uint32_t HEADER_SIZE = 11;
    
    // Key configuration
    static constexpr uint32_t KEY_SIZE = 8;  // int64_t
    static constexpr uint32_t RID_SIZE = 8;   // 4 bytes page_id + 4 bytes slot_id
    static constexpr uint32_t CHILD_PAGE_ID_SIZE = 4;
    
    // Dynamic capacity calculation
    static constexpr uint32_t GetMaxLeafKeys() {
        return (PAGE_SIZE - HEADER_SIZE) / (KEY_SIZE + RID_SIZE);
    }
    
    static constexpr uint32_t GetMaxInternalKeys() {
        // For internal: N keys + (N+1) children
        // HEADER + N*8 + (N+1)*4 <= 4096
        // HEADER + 12N + 4 <= 4096
        // 12N <= 4085
        // N = 340
        return (PAGE_SIZE - HEADER_SIZE - CHILD_PAGE_ID_SIZE) / (KEY_SIZE + CHILD_PAGE_ID_SIZE);
    }
    
private:
    Page* page_;  // Page containing the node (not owned by node)
    
    // Helper methods to read/write header fields using offsets
    uint8_t ReadUint8(uint32_t offset) const {
        uint8_t value;
        std::memcpy(&value, page_->GetData() + offset, sizeof(uint8_t));
        return value;
    }
    
    void WriteUint8(uint32_t offset, uint8_t value) {
        std::memcpy(page_->GetData() + offset, &value, sizeof(uint8_t));
    }
    
    uint16_t ReadUint16(uint32_t offset) const {
        uint16_t value;
        std::memcpy(&value, page_->GetData() + offset, sizeof(uint16_t));
        return value;
    }
    
    void WriteUint16(uint32_t offset, uint16_t value) {
        std::memcpy(page_->GetData() + offset, &value, sizeof(uint16_t));
    }
    
    uint32_t ReadUint32(uint32_t offset) const {
        uint32_t value;
        std::memcpy(&value, page_->GetData() + offset, sizeof(uint32_t));
        return value;
    }
    
    void WriteUint32(uint32_t offset, uint32_t value) {
        std::memcpy(page_->GetData() + offset, &value, sizeof(uint32_t));
    }
    
    // Key access
    uint64_t ReadKey(uint32_t key_index) const {
        uint64_t key;
        uint32_t offset = HEADER_SIZE + key_index * KEY_SIZE;
        std::memcpy(&key, page_->GetData() + offset, sizeof(uint64_t));
        return key;
    }
    
    void WriteKey(uint32_t key_index, uint64_t key) {
        uint32_t offset = HEADER_SIZE + key_index * KEY_SIZE;
        std::memcpy(page_->GetData() + offset, &key, sizeof(uint64_t));
    }
    
    // RID access (leaf nodes)
    RID ReadRID(uint32_t rid_index) const {
        RID rid;
        uint32_t max_keys = GetMaxLeafKeys();
        uint32_t offset = HEADER_SIZE + max_keys * KEY_SIZE + rid_index * RID_SIZE;
        std::memcpy(&rid, page_->GetData() + offset, sizeof(RID));
        return rid;
    }
    
    void WriteRID(uint32_t rid_index, const RID& rid) {
        uint32_t max_keys = GetMaxLeafKeys();
        uint32_t offset = HEADER_SIZE + max_keys * KEY_SIZE + rid_index * RID_SIZE;
        std::memcpy(page_->GetData() + offset, &rid, sizeof(RID));
    }
    
    // Child page_id access (internal nodes)
    uint32_t ReadChildPageId(uint32_t child_index) const {
        uint32_t max_keys = GetMaxInternalKeys();
        uint32_t offset = HEADER_SIZE + max_keys * KEY_SIZE + child_index * CHILD_PAGE_ID_SIZE;
        return ReadUint32(offset);
    }
    
    void WriteChildPageId(uint32_t child_index, uint32_t page_id) {
        uint32_t max_keys = GetMaxInternalKeys();
        uint32_t offset = HEADER_SIZE + max_keys * KEY_SIZE + child_index * CHILD_PAGE_ID_SIZE;
        WriteUint32(offset, page_id);
    }

public:
    explicit BPlusTreeNode(Page* page) : page_(page) {
        if (page_) {
            // Initialize header if page is new
            if (page_->GetPageId() == INVALID_PAGE_ID) {
                SetIsLeaf(false);
                SetKeyCount(0);
                SetParentPageId(INVALID_PAGE_ID);
                SetNextLeafPageId(INVALID_PAGE_ID);
            }
        }
    }
    
    // Node type operations
    void SetIsLeaf(bool is_leaf) {
        WriteUint8(OFFSET_IS_LEAF, is_leaf ? 1 : 0);
    }
    
    bool IsLeaf() const {
        return ReadUint8(OFFSET_IS_LEAF) == 1;
    }
    
    // Key count operations
    void SetKeyCount(uint16_t count) {
        WriteUint16(OFFSET_KEY_COUNT, count);
    }
    
    uint16_t GetKeyCount() const {
        return ReadUint16(OFFSET_KEY_COUNT);
    }
    
    // Parent page_id operations
    void SetParentPageId(uint32_t page_id) {
        WriteUint32(OFFSET_PARENT_PAGE_ID, page_id);
    }
    
    uint32_t GetParentPageId() const {
        return ReadUint32(OFFSET_PARENT_PAGE_ID);
    }
    
    // Next leaf pointer (leaf nodes only)
    void SetNextLeafPageId(uint32_t page_id) {
        WriteUint32(OFFSET_NEXT_LEAF, page_id);
    }
    
    uint32_t GetNextLeafPageId() const {
        return ReadUint32(OFFSET_NEXT_LEAF);
    }
    
    // Key operations
    void SetKey(uint32_t key_index, uint64_t key) {
        uint32_t max_keys = IsLeaf() ? GetMaxLeafKeys() : GetMaxInternalKeys();
        if (key_index >= max_keys) return;
        WriteKey(key_index, key);
    }
    
    uint64_t GetKey(uint32_t key_index) const {
        uint32_t max_keys = IsLeaf() ? GetMaxLeafKeys() : GetMaxInternalKeys();
        if (key_index >= max_keys) return 0;
        return ReadKey(key_index);
    }
    
    // RID operations (leaf nodes)
    void SetRID(uint32_t rid_index, const RID& rid) {
        uint32_t max_keys = GetMaxLeafKeys();
        if (rid_index >= max_keys) return;
        WriteRID(rid_index, rid);
    }
    
    RID GetRID(uint32_t rid_index) const {
        uint32_t max_keys = GetMaxLeafKeys();
        if (rid_index >= max_keys) return RID();
        return ReadRID(rid_index);
    }
    
    // Child page_id operations (internal nodes)
    void SetChildPageId(uint32_t child_index, uint32_t page_id) {
        uint32_t max_children = GetMaxInternalKeys() + 1;
        if (child_index >= max_children) return;
        WriteChildPageId(child_index, page_id);
    }
    
    uint32_t GetChildPageId(uint32_t child_index) const {
        uint32_t max_children = GetMaxInternalKeys() + 1;
        if (child_index >= max_children) return INVALID_PAGE_ID;
        return ReadChildPageId(child_index);
    }
    
    // Utility
    Page* GetPage() { return page_; }
    const Page* GetPage() const { return page_; }
};

// Buffer pool interface for B+ Tree
template<typename BufferPoolType>
class BPlusTree {
public:
    BPlusTree(BufferPoolType* buffer_pool, uint32_t root_page_id, LogManager* log_manager = nullptr)
        : buffer_pool_(buffer_pool), root_page_id_(root_page_id), log_manager_(log_manager) {}
    
    // Get the root page ID (for testing/validation)
    uint32_t GetRootPageId() const { return root_page_id_; }
    
    // Search for a key in the B+ Tree
    // Returns true if found, and sets rid to the record location
    bool Search(uint64_t key, RID& rid) {
        if (root_page_id_ == INVALID_PAGE_ID) {
            return false;
        }
        
        uint32_t current_page_id = root_page_id_;
        
        while (true) {
            Page* page = buffer_pool_->FetchPage(current_page_id);
            if (!page) {
                return false;
            }
            
            BPlusTreeNode node(page);
            
            if (node.IsLeaf()) {
                bool found = SearchLeaf(node, key, rid);
                buffer_pool_->UnpinPage(current_page_id, false);
                return found;
            } else {
                uint32_t child_page_id = FindChild(node, key);
                buffer_pool_->UnpinPage(current_page_id, false);
                if (child_page_id == INVALID_PAGE_ID) {
                    return false;
                }
                current_page_id = child_page_id;
            }
        }
    }
    
    // Insert a key-value pair into the B+ Tree (with split handling)
    // Returns true if successful
    bool Insert(uint64_t key, const RID& rid) {
        if (root_page_id_ == INVALID_PAGE_ID) {
            return CreateRootLeaf(key, rid);
        }
        
        uint32_t current_page_id = root_page_id_;
        std::vector<uint32_t> path;
        
        while (true) {
            path.push_back(current_page_id);
            
            Page* page = buffer_pool_->FetchPage(current_page_id);
            if (!page) {
                std::cerr << "Insert failed: could not fetch page " << current_page_id << " for key " << key << std::endl;
                return false;
            }
            
            BPlusTreeNode node(page);
            
            if (node.IsLeaf()) {
                buffer_pool_->UnpinPage(current_page_id, false);
                
                if (InsertLeaf(node, key, rid)) {
                    buffer_pool_->UnpinPage(current_page_id, true);
                    return true;
                } else {
                    bool result = SplitLeafAndInsert(path, key, rid);
                    if (!result) {
                        std::cerr << "Insert failed: SplitLeafAndInsert returned false for key " << key << std::endl;
                    }
                    return result;
                }
            } else {
                uint32_t child_page_id = FindChild(node, key);
                buffer_pool_->UnpinPage(current_page_id, false);
                if (child_page_id == INVALID_PAGE_ID) {
                    std::cerr << "Insert failed: FindChild returned INVALID_PAGE_ID for key " << key << std::endl;
                    return false;
                }
                current_page_id = child_page_id;
            }
        }
    }
    
    // Set the root page ID (for testing)
    void SetRootPageId(uint32_t root_page_id) {
        root_page_id_ = root_page_id;
    }

private:
    BufferPoolType* buffer_pool_;
    uint32_t root_page_id_;
    LogManager* log_manager_;
    
    // WAL logging helper
    void LogPageModification(uint32_t page_id) {
        if (log_manager_) {
            // TODO: Log page modification for WAL recovery
        }
    }
    
    // Update page LSN after modification
    void UpdatePageLSN(Page* page) {
        if (log_manager_) {
            // TODO: Update page LSN after modification
        }
    }
    
    // Create the root as a leaf node with first key-value pair
    bool CreateRootLeaf(uint64_t key, const RID& rid) {
        uint32_t page_id;
        Page* page = buffer_pool_->NewPage(page_id);
        if (!page) {
            return false;
        }
        
        BPlusTreeNode node(page);
        node.SetIsLeaf(true);
        node.SetKeyCount(1);
        node.SetKey(0, key);
        node.SetRID(0, rid);
        node.SetParentPageId(INVALID_PAGE_ID);
        node.SetNextLeafPageId(INVALID_PAGE_ID);
        
        LogPageModification(page_id);
        UpdatePageLSN(page);
        
        buffer_pool_->UnpinPage(page_id, true);
        root_page_id_ = page_id;
        
        return true;
    }
    
    // Search within a leaf node for a specific key
    bool SearchLeaf(BPlusTreeNode& node, uint64_t key, RID& rid) {
        uint16_t key_count = node.GetKeyCount();
        
        for (uint16_t i = 0; i < key_count; i++) {
            if (node.GetKey(i) == key) {
                rid = node.GetRID(i);
                return true;
            }
        }
        
        return false;
    }
    
    // Insert into a leaf node (assumes space is available)
    bool InsertLeaf(BPlusTreeNode& node, uint64_t key, const RID& rid) {
        uint16_t key_count = node.GetKeyCount();
        uint32_t max_keys = BPlusTreeNode::GetMaxLeafKeys();
        
        if (key_count >= max_keys) {
            return false;
        }
        
        for (uint16_t i = 0; i < key_count; i++) {
            if (node.GetKey(i) == key) {
                return false;
            }
        }
        
        uint16_t insert_pos = 0;
        while (insert_pos < key_count && node.GetKey(insert_pos) < key) {
            insert_pos++;
        }
        
        for (uint16_t i = key_count; i > insert_pos; i--) {
            node.SetKey(i, node.GetKey(i - 1));
            node.SetRID(i, node.GetRID(i - 1));
        }
        
        node.SetKey(insert_pos, key);
        node.SetRID(insert_pos, rid);
        node.SetKeyCount(key_count + 1);
        
        return true;
    }
    
    // Find the correct child page_id in an internal node for a given key
    uint32_t FindChild(BPlusTreeNode& node, uint64_t key) {
        uint16_t key_count = node.GetKeyCount();
        
        for (uint16_t i = 0; i < key_count; i++) {
            if (key < node.GetKey(i)) {
                return node.GetChildPageId(i);
            }
        }
        
        return node.GetChildPageId(key_count);
    }
    
    // Split a full leaf node and insert the new key-value pair
    bool SplitLeafAndInsert(const std::vector<uint32_t>& path, uint64_t key, const RID& rid) {
        uint32_t leaf_page_id = path.back();
        Page* leaf_page = buffer_pool_->FetchPage(leaf_page_id);
        if (!leaf_page) {
            return false;
        }
        
        BPlusTreeNode leaf_node(leaf_page);
        
        uint32_t new_leaf_page_id;
        Page* new_leaf_page = buffer_pool_->NewPage(new_leaf_page_id);
        if (!new_leaf_page) {
            buffer_pool_->UnpinPage(leaf_page_id, false);
            return false;
        }
        
        BPlusTreeNode new_leaf_node(new_leaf_page);
        new_leaf_node.SetIsLeaf(true);
        new_leaf_node.SetKeyCount(0);
        new_leaf_node.SetParentPageId(INVALID_PAGE_ID);
        new_leaf_node.SetNextLeafPageId(INVALID_PAGE_ID);
        
        std::vector<uint64_t> all_keys;
        std::vector<RID> all_rids;
        
        uint16_t old_key_count = leaf_node.GetKeyCount();
        for (uint16_t i = 0; i < old_key_count; i++) {
            all_keys.push_back(leaf_node.GetKey(i));
            all_rids.push_back(leaf_node.GetRID(i));
        }
        
        bool inserted = false;
        for (size_t i = 0; i < all_keys.size(); i++) {
            if (!inserted && key < all_keys[i]) {
                all_keys.insert(all_keys.begin() + i, key);
                all_rids.insert(all_rids.begin() + i, rid);
                inserted = true;
            }
        }
        if (!inserted) {
            all_keys.push_back(key);
            all_rids.push_back(rid);
        }
        
        uint16_t split_point = all_keys.size() / 2;
        uint64_t split_key = all_keys[split_point];
        
        for (uint16_t i = 0; i < split_point; i++) {
            leaf_node.SetKey(i, all_keys[i]);
            leaf_node.SetRID(i, all_rids[i]);
        }
        leaf_node.SetKeyCount(split_point);
        
        for (uint16_t i = split_point; i < all_keys.size(); i++) {
            new_leaf_node.SetKey(i - split_point, all_keys[i]);
            new_leaf_node.SetRID(i - split_point, all_rids[i]);
        }
        new_leaf_node.SetKeyCount(all_keys.size() - split_point);
        
        new_leaf_node.SetNextLeafPageId(leaf_node.GetNextLeafPageId());
        leaf_node.SetNextLeafPageId(new_leaf_page_id);
        
        buffer_pool_->UnpinPage(leaf_page_id, true);
        buffer_pool_->UnpinPage(new_leaf_page_id, true);
        
        return InsertIntoParent(path, split_key, new_leaf_page_id);
    }
    
    // Insert a split key into the parent node
    bool InsertIntoParent(const std::vector<uint32_t>& path, uint64_t key, uint32_t child_page_id) {
        if (path.size() == 1) {
            return CreateNewRoot(key, path[0], child_page_id);
        }
        
        uint32_t parent_page_id = path[path.size() - 2];
        Page* parent_page = buffer_pool_->FetchPage(parent_page_id);
        if (!parent_page) {
            return false;
        }
        
        BPlusTreeNode parent_node(parent_page);
        
        Page* child_page = buffer_pool_->FetchPage(child_page_id);
        if (child_page) {
            BPlusTreeNode child_node(child_page);
            child_node.SetParentPageId(parent_page_id);
            buffer_pool_->UnpinPage(child_page_id, true);
        }
        
        uint16_t key_count = parent_node.GetKeyCount();
        uint32_t max_keys = BPlusTreeNode::GetMaxInternalKeys();
        
        if (key_count < max_keys) {
            uint16_t insert_pos = 0;
            while (insert_pos < key_count && parent_node.GetKey(insert_pos) < key) {
                insert_pos++;
            }
            
            for (uint16_t i = key_count; i > insert_pos; i--) {
                parent_node.SetKey(i, parent_node.GetKey(i - 1));
                parent_node.SetChildPageId(i + 1, parent_node.GetChildPageId(i));
            }
            
            parent_node.SetKey(insert_pos, key);
            parent_node.SetChildPageId(insert_pos + 1, child_page_id);
            parent_node.SetKeyCount(key_count + 1);
            
            buffer_pool_->UnpinPage(parent_page_id, true);
            return true;
        } else {
            buffer_pool_->UnpinPage(parent_page_id, false);
            return SplitInternalAndInsert(path, key, child_page_id);
        }
    }
    
    // Create a new root when the old root splits
    bool CreateNewRoot(uint64_t key, uint32_t left_child, uint32_t right_child) {
        uint32_t new_root_page_id;
        Page* new_root_page = buffer_pool_->NewPage(new_root_page_id);
        if (!new_root_page) {
            return false;
        }
        
        BPlusTreeNode new_root_node(new_root_page);
        new_root_node.SetIsLeaf(false);
        new_root_node.SetKeyCount(1);
        new_root_node.SetParentPageId(INVALID_PAGE_ID);
        new_root_node.SetKey(0, key);
        new_root_node.SetChildPageId(0, left_child);
        new_root_node.SetChildPageId(1, right_child);
        
        Page* left_page = buffer_pool_->FetchPage(left_child);
        if (left_page) {
            BPlusTreeNode left_node(left_page);
            left_node.SetParentPageId(new_root_page_id);
            buffer_pool_->UnpinPage(left_child, true);
        }
        
        Page* right_page = buffer_pool_->FetchPage(right_child);
        if (right_page) {
            BPlusTreeNode right_node(right_page);
            right_node.SetParentPageId(new_root_page_id);
            buffer_pool_->UnpinPage(right_child, true);
        }
        
        buffer_pool_->UnpinPage(new_root_page_id, true);
        root_page_id_ = new_root_page_id;
        
        return true;
    }
    
    // Split an internal node and insert the new key-child pair
    bool SplitInternalAndInsert(const std::vector<uint32_t>& path, uint64_t key, uint32_t child_page_id) {
        uint32_t internal_page_id = path[path.size() - 2];
        Page* internal_page = buffer_pool_->FetchPage(internal_page_id);
        if (!internal_page) {
            return false;
        }
        
        BPlusTreeNode internal_node(internal_page);
        
        uint32_t new_internal_page_id;
        Page* new_internal_page = buffer_pool_->NewPage(new_internal_page_id);
        if (!new_internal_page) {
            buffer_pool_->UnpinPage(internal_page_id, false);
            return false;
        }
        
        BPlusTreeNode new_internal_node(new_internal_page);
        new_internal_node.SetIsLeaf(false);
        new_internal_node.SetKeyCount(0);
        new_internal_node.SetParentPageId(INVALID_PAGE_ID);
        
        std::vector<uint64_t> all_keys;
        std::vector<uint32_t> all_children;
        
        uint16_t old_key_count = internal_node.GetKeyCount();
        
        for (uint16_t i = 0; i < old_key_count; i++) {
            all_keys.push_back(internal_node.GetKey(i));
            all_children.push_back(internal_node.GetChildPageId(i));
        }
        all_children.push_back(internal_node.GetChildPageId(old_key_count));
        
        bool inserted = false;
        for (size_t i = 0; i < all_keys.size(); i++) {
            if (!inserted && key < all_keys[i]) {
                all_keys.insert(all_keys.begin() + i, key);
                all_children.insert(all_children.begin() + i + 1, child_page_id);
                inserted = true;
            }
        }
        if (!inserted) {
            all_keys.push_back(key);
            all_children.push_back(child_page_id);
        }
        
        uint16_t split_point = all_keys.size() / 2;
        uint64_t split_key = all_keys[split_point];
        
        for (uint16_t i = 0; i < split_point; i++) {
            internal_node.SetKey(i, all_keys[i]);
            internal_node.SetChildPageId(i, all_children[i]);
        }
        internal_node.SetChildPageId(split_point, all_children[split_point]);
        internal_node.SetKeyCount(split_point);
        
        for (uint16_t i = split_point + 1; i < all_keys.size(); i++) {
            new_internal_node.SetKey(i - split_point - 1, all_keys[i]);
            new_internal_node.SetChildPageId(i - split_point - 1, all_children[i]);
        }
        new_internal_node.SetChildPageId(all_keys.size() - split_point - 1, all_children.back());
        new_internal_node.SetKeyCount(all_keys.size() - split_point - 1);
        
        // Update parent_page_id of children that moved to the new internal node
        for (uint16_t i = split_point + 1; i <= all_keys.size(); i++) {
            uint32_t child_id = all_children[i];
            Page* child_page = buffer_pool_->FetchPage(child_id);
            if (child_page) {
                BPlusTreeNode child_node(child_page);
                child_node.SetParentPageId(new_internal_page_id);
                buffer_pool_->UnpinPage(child_id, true);
            }
        }
        
        buffer_pool_->UnpinPage(internal_page_id, true);
        buffer_pool_->UnpinPage(new_internal_page_id, true);
        
        std::vector<uint32_t> parent_path(path.begin(), path.end() - 1);
        return InsertIntoParent(parent_path, split_key, new_internal_page_id);
    }
};

}
