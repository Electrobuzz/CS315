#pragma once

#include <cstdint>
#include <cstring>
#include <vector>
#include <set>
#include <iostream>
#include <cassert>
#include "page_v2.h"
#include "log_manager.h"

namespace minidb {

// Forward declarations
class BufferPoolManager;
class LogManager;

// B+ Tree node types
enum class NodeType : uint8_t {
    INTERNAL = 0,
    LEAF = 1
};

// B+ Tree node layout (fits in 4096-byte page)
// Page layout: [Node Data (0-4087) | LSN Trailer (4088-4095)]
// 
// HEADER (11 bytes):
//   Offset 0:         is_leaf (1 byte) - 0 for internal, 1 for leaf
//   Offset 1:         key_count (2 bytes) - number of keys in node
//   Offset 3:         parent_page_id (4 bytes) - parent node page_id
//   Offset 7:         next_leaf_page_id (4 bytes) - for leaf nodes only (linked list)
// 
// LEAF NODE LAYOUT:
//   Offset 11:        keys[key_count] - each key is 8 bytes (int64_t)
//   Offset 11 + (N*8): RIDs[key_count] - each RID is 8 bytes (page_id + slot_id)
//                     Keys and RIDs stored in sorted order
//
// INTERNAL NODE LAYOUT:
//   Offset 11:        keys[key_count] - each key is 8 bytes (int64_t)
//   Offset 11 + (N*8): child_page_ids[key_count + 1] - each is 4 bytes
//                     Relationship: child0 < key0 <= child1 < key1 <= child2 ...
//                     N keys => N+1 children

class BPlusTreeNode {
public:
    static constexpr uint32_t PAGE_SIZE = 4096;
    
    // Node header offsets (start after Page metadata to avoid overlap)
    static constexpr uint32_t NODE_HEADER_OFFSET = 24;  // After Page metadata (offsets 0-23)
    static constexpr uint32_t OFFSET_IS_LEAF = NODE_HEADER_OFFSET + 0;
    static constexpr uint32_t OFFSET_KEY_COUNT = NODE_HEADER_OFFSET + 1;
    static constexpr uint32_t OFFSET_PARENT_PAGE_ID = NODE_HEADER_OFFSET + 3;
    static constexpr uint32_t OFFSET_NEXT_LEAF = NODE_HEADER_OFFSET + 7;
    static constexpr uint32_t HEADER_SIZE = NODE_HEADER_OFFSET + 11;
    
    // Key configuration
    static constexpr uint32_t KEY_SIZE = 8;  // int64_t
    static constexpr uint32_t RID_SIZE = 8;   // 4 bytes page_id + 4 bytes slot_id
    static constexpr uint32_t CHILD_PAGE_ID_SIZE = 4;
    
    // Dynamic capacity calculation
    static constexpr uint32_t GetMaxLeafKeys() {
        return (Page::OFFSET_LAST_LSN - HEADER_SIZE) / (KEY_SIZE + RID_SIZE);
    }
    
    static constexpr uint32_t GetMaxInternalKeys() {
        return (Page::OFFSET_LAST_LSN - HEADER_SIZE - CHILD_PAGE_ID_SIZE) / (KEY_SIZE + CHILD_PAGE_ID_SIZE);
    }
    
    // Validate that node layout doesn't overlap with LSN trailer
    static bool ValidateNodeLayout() {
        // Ensure node header + max keys doesn't cross into LSN region
        uint32_t max_leaf_end = HEADER_SIZE + GetMaxLeafKeys() * (KEY_SIZE + RID_SIZE);
        uint32_t max_internal_end = HEADER_SIZE + GetMaxInternalKeys() * KEY_SIZE + (GetMaxInternalKeys() + 1) * CHILD_PAGE_ID_SIZE;
        
        return (max_leaf_end <= Page::OFFSET_LAST_LSN) && 
               (max_internal_end <= Page::OFFSET_LAST_LSN);
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
        assert(Page::ValidateWriteOffset(offset, sizeof(uint8_t)));
        std::memcpy(page_->GetData() + offset, &value, sizeof(uint8_t));
    }
    
    uint16_t ReadUint16(uint32_t offset) const {
        uint16_t value;
        std::memcpy(&value, page_->GetData() + offset, sizeof(uint16_t));
        return value;
    }
    
    void WriteUint16(uint32_t offset, uint16_t value) {
        assert(Page::ValidateWriteOffset(offset, sizeof(uint16_t)));
        std::memcpy(page_->GetData() + offset, &value, sizeof(uint16_t));
    }
    
    uint32_t ReadUint32(uint32_t offset) const {
        uint32_t value;
        std::memcpy(&value, page_->GetData() + offset, sizeof(uint32_t));
        return value;
    }
    
    void WriteUint32(uint32_t offset, uint32_t value) {
        assert(Page::ValidateWriteOffset(offset, sizeof(uint32_t)));
        std::memcpy(page_->GetData() + offset, &value, sizeof(uint32_t));
    }
    
    uint64_t ReadUint64(uint32_t offset) const {
        uint64_t value;
        std::memcpy(&value, page_->GetData() + offset, sizeof(uint64_t));
        return value;
    }
    
    void WriteUint64(uint32_t offset, uint64_t value) {
        assert(Page::ValidateWriteOffset(offset, sizeof(uint64_t)));
        std::memcpy(page_->GetData() + offset, &value, sizeof(uint64_t));
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
        assert(Page::ValidateWriteOffset(offset, KEY_SIZE));
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
        assert(Page::ValidateWriteOffset(offset, RID_SIZE));
        std::memcpy(page_->GetData() + offset, &rid, sizeof(RID));
    }
    
    // Child page_id access (internal nodes)
    uint32_t ReadChildPageId(uint32_t child_index) const {
        uint32_t max_keys = GetMaxInternalKeys();
        uint32_t offset = HEADER_SIZE + max_keys * KEY_SIZE + child_index * CHILD_PAGE_ID_SIZE;
        return ReadUint32(offset);
    }
    
    void WriteChildPageId(uint32_t child_index, uint32_t child_page_id) {
        uint32_t max_keys = GetMaxInternalKeys();
        uint32_t offset = HEADER_SIZE + max_keys * KEY_SIZE + child_index * CHILD_PAGE_ID_SIZE;
        assert(Page::ValidateWriteOffset(offset, CHILD_PAGE_ID_SIZE));
        WriteUint32(offset, child_page_id);
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
        : buffer_pool_(buffer_pool), root_page_id_(root_page_id), log_manager_(log_manager) {
    }
    
    // Get the root page ID (for testing/validation)
    uint32_t GetRootPageId() const { return root_page_id_; }
    
    // Public API for testing
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
                if (InsertLeaf(node, key, rid)) {
                    LogPageModification(page, LogRecordType::INSERT);
                    buffer_pool_->UnpinPage(current_page_id, true);
                    return true;
                } else {
                    buffer_pool_->UnpinPage(current_page_id, false);
                    return SplitLeafAndInsert(path, key, rid);
                }
            } else {
                // Internal node - find child
                uint32_t child_page_id = FindChild(node, key);
                buffer_pool_->UnpinPage(current_page_id, false);
                current_page_id = child_page_id;
            }
        }
    }
    
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
                // Internal node - find child
                uint32_t child_page_id = FindChild(node, key);
                buffer_pool_->UnpinPage(current_page_id, false);
                current_page_id = child_page_id;
            }
        }
    }
    
    void RangeScan(uint64_t start, uint64_t end, std::vector<std::pair<uint64_t, RID>>& results) {
        if (root_page_id_ == INVALID_PAGE_ID) {
            return;
        }
        
        uint32_t current_page_id = root_page_id_;
        
        // Traverse to the leftmost leaf node
        while (true) {
            Page* page = buffer_pool_->FetchPage(current_page_id);
            if (!page) {
                return;
            }
            
            BPlusTreeNode node(page);
            
            if (node.IsLeaf()) {
                buffer_pool_->UnpinPage(current_page_id, false);
                break;
            } else {
                // Internal node - go to leftmost child
                current_page_id = node.GetChildPageId(0);
                buffer_pool_->UnpinPage(current_page_id, false);
            }
        }
        
        // Scan leaf nodes from left to right
        while (current_page_id != INVALID_PAGE_ID) {
            Page* page = buffer_pool_->FetchPage(current_page_id);
            if (!page) {
                return;
            }
            
            BPlusTreeNode node(page);
            
            // Get all keys and RIDs in this leaf that are in range
            uint16_t key_count = node.GetKeyCount();
            for (uint16_t i = 0; i < key_count; i++) {
                uint64_t key = node.GetKey(i);
                if (key >= start && key <= end) {
                    results.push_back({key, node.GetRID(i)});
                } else if (key > end) {
                    // No more keys in range
                    break;
                }
            }
            
            // Move to next leaf
            current_page_id = node.GetNextLeafPageId();
            buffer_pool_->UnpinPage(page->GetPageId(), false);
        }
    }
    
private:
    BufferPoolType* buffer_pool_;
    uint32_t root_page_id_;
    LogManager* log_manager_;
    
    // Helper: Log page modification and update LSN
    // MUST be called AFTER page modification and BEFORE unpin
    void LogPageModification(Page* page, LogRecordType type) {
        if (!log_manager_) {
            return;
        }
        
        // Store previous LSN for WAL correctness enforcement
        lsn_t previous_lsn = page->GetLastLSN();
        
        uint32_t page_id = page->GetPageId();
        LogRecord record(type, INVALID_TXN_ID, page_id, nullptr, 0);
        lsn_t lsn = log_manager_->AppendLogRecord(record);
        log_manager_->FlushLog(lsn);
        
        // WAL correctness enforcement: LSN must be monotonically increasing
        assert(lsn > previous_lsn && "LSN must be monotonically increasing");
        
        page->SetLastLSN(lsn);
    }
    
    // Find the leaf node that would contain a given key
    // Returns the page_id of the leaf, or INVALID_PAGE_ID if tree is empty
    uint32_t FindLeafForKey(uint64_t key) {
        if (root_page_id_ == INVALID_PAGE_ID) {
            return INVALID_PAGE_ID;
        }
        
        uint32_t current_page_id = root_page_id_;
        
        while (true) {
            Page* page = buffer_pool_->FetchPage(current_page_id);
            if (!page) {
                return INVALID_PAGE_ID;
            }
            
            BPlusTreeNode node(page);
            
            if (node.IsLeaf()) {
                buffer_pool_->UnpinPage(current_page_id, false);
                return current_page_id;
            } else {
                uint32_t child_page_id = FindChild(node, key);
                buffer_pool_->UnpinPage(current_page_id, false);
                if (child_page_id == INVALID_PAGE_ID) {
                    return INVALID_PAGE_ID;
                }
                current_page_id = child_page_id;
            }
        }
    }
    
    // Delete a key from the B+ Tree (internal implementation)
    bool DeleteInternal(uint64_t key) {
        uint32_t current_page_id = root_page_id_;
        std::vector<uint32_t> path;
        
        // Traverse to the leaf node containing the key
        while (true) {
            path.push_back(current_page_id);
            
            Page* page = buffer_pool_->FetchPage(current_page_id);
            if (!page) {
                return false;
            }
            
            BPlusTreeNode node(page);
            
            if (node.IsLeaf()) {
                buffer_pool_->UnpinPage(current_page_id, false);
                
                // Delete from leaf with underflow handling
                return DeleteFromLeaf(current_page_id, key, path);
            } else {
                uint32_t child_page_id = FindChild(node, key);
                buffer_pool_->UnpinPage(current_page_id, false);
                if (child_page_id == INVALID_PAGE_ID) {
                    return false; // Key not found
                }
                current_page_id = child_page_id;
            }
        }
    }
    
    // Delete a key from a leaf node (basic implementation, no underflow handling)
    bool DeleteFromLeaf(uint32_t leaf_page_id, uint64_t key, const std::vector<uint32_t>& path) {
        Page* page = buffer_pool_->FetchPage(leaf_page_id);
        if (!page) {
            return false;
        }
        
        BPlusTreeNode node(page);
        
        uint16_t key_count = node.GetKeyCount();
        
        // Find the key
        uint16_t key_index = 0;
        bool found = false;
        for (uint16_t i = 0; i < key_count; i++) {
            if (node.GetKey(i) == key) {
                key_index = i;
                found = true;
                break;
            }
        }
        
        if (!found) {
            buffer_pool_->UnpinPage(leaf_page_id, false);
            return false; // Key not found
        }
        
        // Shift keys and RIDs to remove the deleted key
        for (uint16_t i = key_index; i < key_count - 1; i++) {
            node.SetKey(i, node.GetKey(i + 1));
            node.SetRID(i, node.GetRID(i + 1));
        }
        
        // Decrease key count
        node.SetKeyCount(key_count - 1);
        
        // Check for underflow and try to fix it
        if (path.size() > 1) { // Not the root
            if (IsNodeUnderflowed(node)) {
                uint32_t parent_page_id = path[path.size() - 2];
                if (!TryRedistribute(leaf_page_id, parent_page_id)) {
                    // Redistribution failed, try merge
                    if (!TryMerge(leaf_page_id, parent_page_id)) {
                        // Merge failed (shouldn't happen with proper implementation)
                        // For now, return true (delete succeeded, but underflow not fixed)
                    } else {
                        // Merge succeeded, check if parent underflows and fix recursively
                        FixParentUnderflow(parent_page_id, path);
                    }
                }
            }
        }
        
        // Log the delete operation after all modifications are complete
        LogPageModification(page, LogRecordType::DELETE);
        
        buffer_pool_->UnpinPage(leaf_page_id, true);
        
        return true;
    }
    
    // Check if a node is underflowed
    bool IsNodeUnderflowed(BPlusTreeNode& node) {
        uint16_t key_count = node.GetKeyCount();
        
        if (node.IsLeaf()) {
            uint32_t max_keys = BPlusTreeNode::GetMaxLeafKeys();
            uint32_t min_keys = (max_keys + 1) / 2; // ceil(max_keys / 2)
            return key_count < min_keys;
        } else {
            uint32_t max_keys = BPlusTreeNode::GetMaxInternalKeys();
            uint32_t min_keys = (max_keys + 1) / 2; // ceil(max_keys / 2)
            return key_count < min_keys;
        }
    }
    
    // Find the left sibling of a node
    // Returns page_id of left sibling, or INVALID_PAGE_ID if none exists
    uint32_t FindLeftSibling(uint32_t parent_page_id, uint32_t child_page_id) {
        Page* parent_page = buffer_pool_->FetchPage(parent_page_id);
        if (!parent_page) {
            return INVALID_PAGE_ID;
        }
        
        BPlusTreeNode parent_node(parent_page);
        uint16_t key_count = parent_node.GetKeyCount();
        
        // Find the index of child_page_id in parent's children
        uint16_t child_index = 0;
        for (uint16_t i = 0; i <= key_count; i++) {
            if (parent_node.GetChildPageId(i) == child_page_id) {
                child_index = i;
                break;
            }
        }
        
        uint32_t left_sibling_id = INVALID_PAGE_ID;
        if (child_index > 0) {
            left_sibling_id = parent_node.GetChildPageId(child_index - 1);
        }
        
        buffer_pool_->UnpinPage(parent_page_id, false);
        return left_sibling_id;
    }
    
    // Find the right sibling of a node
    // Returns page_id of right sibling, or INVALID_PAGE_ID if none exists
    uint32_t FindRightSibling(uint32_t parent_page_id, uint32_t child_page_id) {
        Page* parent_page = buffer_pool_->FetchPage(parent_page_id);
        if (!parent_page) {
            return INVALID_PAGE_ID;
        }
        
        BPlusTreeNode parent_node(parent_page);
        uint16_t key_count = parent_node.GetKeyCount();
        
        // Find the index of child_page_id in parent's children
        uint16_t child_index = 0;
        for (uint16_t i = 0; i <= key_count; i++) {
            if (parent_node.GetChildPageId(i) == child_page_id) {
                child_index = i;
                break;
            }
        }
        
        uint32_t right_sibling_id = INVALID_PAGE_ID;
        if (child_index < key_count) {
            right_sibling_id = parent_node.GetChildPageId(child_index + 1);
        }
        
        buffer_pool_->UnpinPage(parent_page_id, false);
        return right_sibling_id;
    }
    
    // Check if a sibling has enough keys to borrow (more than minimum)
    bool CanBorrowFromSibling(BPlusTreeNode& sibling) {
        uint16_t key_count = sibling.GetKeyCount();
        
        if (sibling.IsLeaf()) {
            uint32_t max_keys = BPlusTreeNode::GetMaxLeafKeys();
            uint32_t min_keys = (max_keys + 1) / 2;
            return key_count > min_keys;
        } else {
            uint32_t max_keys = BPlusTreeNode::GetMaxInternalKeys();
            uint32_t min_keys = (max_keys + 1) / 2;
            return key_count > min_keys;
        }
    }
    
    // Try to redistribute (borrow) from a sibling
    // Returns true if redistribution was successful
    bool TryRedistribute(uint32_t node_page_id, uint32_t parent_page_id) {
        Page* node_page = buffer_pool_->FetchPage(node_page_id);
        if (!node_page) {
            return false;
        }
        
        BPlusTreeNode node(node_page);
        
        // Try left sibling first
        uint32_t left_sibling_id = FindLeftSibling(parent_page_id, node_page_id);
        if (left_sibling_id != INVALID_PAGE_ID) {
            Page* left_sibling_page = buffer_pool_->FetchPage(left_sibling_id);
            if (left_sibling_page) {
                BPlusTreeNode left_sibling(left_sibling_page);
                if (CanBorrowFromSibling(left_sibling)) {
                    // Borrow from left sibling
                    if (node.IsLeaf()) {
                        RedistributeFromLeftLeaf(node, left_sibling, node_page_id, left_sibling_id, parent_page_id);
                        LogPageModification(left_sibling_page, LogRecordType::UPDATE);
                        LogPageModification(node_page, LogRecordType::UPDATE);
                        buffer_pool_->UnpinPage(left_sibling_id, true);
                        buffer_pool_->UnpinPage(node_page_id, true);
                        return true;
                    }
                    buffer_pool_->UnpinPage(left_sibling_id, false);
                }
                buffer_pool_->UnpinPage(left_sibling_id, false);
            }
        }
        
        // Try right sibling
        uint32_t right_sibling_id = FindRightSibling(parent_page_id, node_page_id);
        if (right_sibling_id != INVALID_PAGE_ID) {
            Page* right_sibling_page = buffer_pool_->FetchPage(right_sibling_id);
            if (right_sibling_page) {
                BPlusTreeNode right_sibling(right_sibling_page);
                if (CanBorrowFromSibling(right_sibling)) {
                    // Borrow from right sibling
                    if (node.IsLeaf()) {
                        RedistributeFromRightLeaf(node, right_sibling, node_page_id, right_sibling_id, parent_page_id);
                        LogPageModification(right_sibling_page, LogRecordType::UPDATE);
                        LogPageModification(node_page, LogRecordType::UPDATE);
                        buffer_pool_->UnpinPage(right_sibling_id, true);
                        buffer_pool_->UnpinPage(node_page_id, true);
                        return true;
                    }
                    buffer_pool_->UnpinPage(right_sibling_id, false);
                }
                buffer_pool_->UnpinPage(right_sibling_id, false);
            }
        }
        
        buffer_pool_->UnpinPage(node_page_id, false);
        return false;
    }
    
    // Redistribute from left sibling for leaf nodes
    void RedistributeFromLeftLeaf(BPlusTreeNode& node, BPlusTreeNode& left_sibling,
                                  uint32_t node_page_id, uint32_t left_sibling_id,
                                  uint32_t parent_page_id) {
        // Move last key from left sibling to beginning of node
        uint16_t left_key_count = left_sibling.GetKeyCount();
        uint16_t node_key_count = node.GetKeyCount();
        
        // Shift all keys in node to the right by 1
        for (uint16_t i = node_key_count; i > 0; i--) {
            node.SetKey(i, node.GetKey(i - 1));
            node.SetRID(i, node.GetRID(i - 1));
        }
        
        // Move last key from left sibling to node[0]
        node.SetKey(0, left_sibling.GetKey(left_key_count - 1));
        node.SetRID(0, left_sibling.GetRID(left_key_count - 1));
        
        // Update key counts
        node.SetKeyCount(node_key_count + 1);
        left_sibling.SetKeyCount(left_key_count - 1);
        
        // Update parent separator key (the key at index child_index in parent)
        Page* parent_page = buffer_pool_->FetchPage(parent_page_id);
        if (parent_page) {
            BPlusTreeNode parent(parent_page);
            uint16_t key_count = parent.GetKeyCount();
            for (uint16_t i = 0; i < key_count; i++) {
                if (parent.GetChildPageId(i + 1) == node_page_id) {
                    parent.SetKey(i, node.GetKey(0)); // Update separator to new first key
                    break;
                }
            }
            buffer_pool_->UnpinPage(parent_page_id, true);
        }
    }
    
    // Redistribute from right sibling for leaf nodes
    void RedistributeFromRightLeaf(BPlusTreeNode& node, BPlusTreeNode& right_sibling,
                                   uint32_t node_page_id, uint32_t right_sibling_id,
                                   uint32_t parent_page_id) {
        // Move first key from right sibling to end of node
        uint16_t node_key_count = node.GetKeyCount();
        
        // Move first key from right sibling to end of node
        node.SetKey(node_key_count, right_sibling.GetKey(0));
        node.SetRID(node_key_count, right_sibling.GetRID(0));
        
        // Shift all keys in right sibling to the left by 1
        uint16_t right_key_count = right_sibling.GetKeyCount();
        for (uint16_t i = 0; i < right_key_count - 1; i++) {
            right_sibling.SetKey(i, right_sibling.GetKey(i + 1));
            right_sibling.SetRID(i, right_sibling.GetRID(i + 1));
        }
        
        // Update key counts
        node.SetKeyCount(node_key_count + 1);
        right_sibling.SetKeyCount(right_key_count - 1);
        
        // Update parent separator key
        Page* parent_page = buffer_pool_->FetchPage(parent_page_id);
        if (parent_page) {
            BPlusTreeNode parent(parent_page);
            uint16_t key_count = parent.GetKeyCount();
            for (uint16_t i = 0; i < key_count; i++) {
                if (parent.GetChildPageId(i) == node_page_id) {
                    parent.SetKey(i, right_sibling.GetKey(0)); // Update separator to new first key of right sibling
                    break;
                }
            }
            buffer_pool_->UnpinPage(parent_page_id, true);
        }
    }
    
    // Merge with left sibling for leaf nodes
    bool MergeWithLeftLeaf(uint32_t node_page_id, uint32_t left_sibling_id, uint32_t parent_page_id) {
        Page* node_page = buffer_pool_->FetchPage(node_page_id);
        Page* left_sibling_page = buffer_pool_->FetchPage(left_sibling_id);
        if (!node_page || !left_sibling_page) {
            if (node_page) buffer_pool_->UnpinPage(node_page_id, false);
            if (left_sibling_page) buffer_pool_->UnpinPage(left_sibling_id, false);
            return false;
        }
        
        BPlusTreeNode node(node_page);
        BPlusTreeNode left_sibling(left_sibling_page);
        
        uint16_t node_key_count = node.GetKeyCount();
        uint16_t left_key_count = left_sibling.GetKeyCount();
        
        // Move all keys from node to left sibling
        for (uint16_t i = 0; i < node_key_count; i++) {
            left_sibling.SetKey(left_key_count + i, node.GetKey(i));
            left_sibling.SetRID(left_key_count + i, node.GetRID(i));
        }
        
        // Update left sibling key count
        left_sibling.SetKeyCount(left_key_count + node_key_count);
        
        // Update linked list: left_sibling.next = node.next
        left_sibling.SetNextLeafPageId(node.GetNextLeafPageId());
        
        // Update parent: remove separator key and child pointer
        Page* parent_page = buffer_pool_->FetchPage(parent_page_id);
        if (parent_page) {
            BPlusTreeNode parent(parent_page);
            uint16_t parent_key_count = parent.GetKeyCount();
            
            // Find the index of the child being removed
            // key[i] separates child[i] and child[i+1]
            // When merging child[i+1] (node) into child[i] (left_sibling)
            // We remove key[i] (separator) and child[i+1] (node)
            uint16_t child_index = 0;
            for (uint16_t i = 0; i <= parent_key_count; i++) {
                if (parent.GetChildPageId(i) == node_page_id) {
                    child_index = i;
                    break;
                }
            }
            
            // Remove separator key at child_index - 1
            // Shift keys to remove the separator
            for (uint16_t i = child_index - 1; i < parent_key_count - 1; i++) {
                parent.SetKey(i, parent.GetKey(i + 1));
            }
            
            // Remove child pointer at child_index
            // Shift children to remove the child
            for (uint16_t i = child_index; i <= parent_key_count; i++) {
                parent.SetChildPageId(i, parent.GetChildPageId(i + 1));
            }
            
            parent.SetKeyCount(parent_key_count - 1);
            buffer_pool_->UnpinPage(parent_page_id, true);
        }
        
        // Log the merge operation
        LogPageModification(left_sibling_page, LogRecordType::UPDATE);
        LogPageModification(node_page, LogRecordType::UPDATE);
        
        buffer_pool_->UnpinPage(left_sibling_id, true);
        buffer_pool_->UnpinPage(node_page_id, true);
        
        // TODO: Delete the merged node (node_page_id) from buffer pool
        // For now, we'll leave it as is (it's no longer reachable)
        
        return true;
    }
    
    // Merge with right sibling for leaf nodes
    bool MergeWithRightLeaf(uint32_t node_page_id, uint32_t right_sibling_id, uint32_t parent_page_id) {
        Page* node_page = buffer_pool_->FetchPage(node_page_id);
        Page* right_sibling_page = buffer_pool_->FetchPage(right_sibling_id);
        if (!node_page || !right_sibling_page) {
            if (node_page) buffer_pool_->UnpinPage(node_page_id, false);
            if (right_sibling_page) buffer_pool_->UnpinPage(right_sibling_id, false);
            return false;
        }
        
        BPlusTreeNode node(node_page);
        BPlusTreeNode right_sibling(right_sibling_page);
        
        uint16_t node_key_count = node.GetKeyCount();
        uint16_t right_key_count = right_sibling.GetKeyCount();
        
        // Move all keys from right sibling to node
        for (uint16_t i = 0; i < right_key_count; i++) {
            node.SetKey(node_key_count + i, right_sibling.GetKey(i));
            node.SetRID(node_key_count + i, right_sibling.GetRID(i));
        }
        
        // Update node key count
        node.SetKeyCount(node_key_count + right_key_count);
        
        // Update linked list: node.next = right_sibling.next
        node.SetNextLeafPageId(right_sibling.GetNextLeafPageId());
        
        // Update parent: remove separator key and child pointer
        Page* parent_page = buffer_pool_->FetchPage(parent_page_id);
        if (parent_page) {
            BPlusTreeNode parent(parent_page);
            uint16_t parent_key_count = parent.GetKeyCount();
            
            // Find the index of the child being removed
            // key[i] separates child[i] and child[i+1]
            // When merging child[i+1] (right_sibling) into child[i] (node)
            // We remove key[i] (separator) and child[i+1] (right_sibling)
            uint16_t child_index = 0;
            for (uint16_t i = 0; i <= parent_key_count; i++) {
                if (parent.GetChildPageId(i) == right_sibling_id) {
                    child_index = i;
                    break;
                }
            }
            
            // Remove separator key at child_index - 1
            // Shift keys to remove the separator
            for (uint16_t i = child_index - 1; i < parent_key_count - 1; i++) {
                parent.SetKey(i, parent.GetKey(i + 1));
            }
            
            // Remove child pointer at child_index
            // Shift children to remove the child
            for (uint16_t i = child_index; i <= parent_key_count; i++) {
                parent.SetChildPageId(i, parent.GetChildPageId(i + 1));
            }
            
            parent.SetKeyCount(parent_key_count - 1);
            buffer_pool_->UnpinPage(parent_page_id, true);
        }
        
        // Log the merge operations
        LogPageModification(node_page, LogRecordType::UPDATE);
        LogPageModification(right_sibling_page, LogRecordType::UPDATE);
        
        buffer_pool_->UnpinPage(node_page_id, true);
        buffer_pool_->UnpinPage(right_sibling_id, true);
        
        // TODO: Delete the merged node (right_sibling_id) from buffer pool
        // For now, we'll leave it as is (it's no longer reachable)
        
        return true;
    }
    
    // Merge with left sibling for internal nodes
    // Brings down separator key from parent
    bool MergeWithLeftInternal(uint32_t node_page_id, uint32_t left_sibling_id, uint32_t parent_page_id) {
        Page* node_page = buffer_pool_->FetchPage(node_page_id);
        Page* left_sibling_page = buffer_pool_->FetchPage(left_sibling_id);
        if (!node_page || !left_sibling_page) {
            if (node_page) buffer_pool_->UnpinPage(node_page_id, false);
            if (left_sibling_page) buffer_pool_->UnpinPage(left_sibling_id, false);
            return false;
        }
        
        BPlusTreeNode node(node_page);
        BPlusTreeNode left_sibling(left_sibling_page);
        
        // Verify both are internal nodes
        if (node.IsLeaf() || left_sibling.IsLeaf()) {
            buffer_pool_->UnpinPage(node_page_id, false);
            buffer_pool_->UnpinPage(left_sibling_id, false);
            return false;
        }
        
        uint16_t node_key_count = node.GetKeyCount();
        uint16_t left_key_count = left_sibling.GetKeyCount();
        
        // Get the separator key from parent (will be brought down)
        Page* parent_page = buffer_pool_->FetchPage(parent_page_id);
        if (!parent_page) {
            buffer_pool_->UnpinPage(node_page_id, false);
            buffer_pool_->UnpinPage(left_sibling_id, false);
            return false;
        }
        
        BPlusTreeNode parent(parent_page);
        uint16_t parent_key_count = parent.GetKeyCount();
        
        // Find the index of the separator key to bring down
        // When merging child[i+1] (node) into child[i] (left_sibling)
        // The separator is parent.keys[i]
        uint16_t child_index = 0;
        for (uint16_t i = 0; i <= parent_key_count; i++) {
            if (parent.GetChildPageId(i) == node_page_id) {
                child_index = i;
                break;
            }
        }
        
        uint64_t separator_key = parent.GetKey(child_index - 1);
        
        // Bring down separator key to left_sibling at position left_key_count
        left_sibling.SetKey(left_key_count, separator_key);
        
        // Move all keys from node to left_sibling (starting at left_key_count + 1)
        for (uint16_t i = 0; i < node_key_count; i++) {
            left_sibling.SetKey(left_key_count + 1 + i, node.GetKey(i));
        }
        
        // Move all children from node to left_sibling
        // LEFT.children = LEFT.children + RIGHT.children
        for (uint16_t i = 0; i <= node_key_count; i++) {
            uint32_t child_page_id = node.GetChildPageId(i);
            left_sibling.SetChildPageId(left_key_count + 1 + i, child_page_id);
            
            // Update parent pointer for the moved child (only if valid)
            if (child_page_id != INVALID_PAGE_ID) {
                Page* child_page = buffer_pool_->FetchPage(child_page_id);
                if (child_page) {
                    BPlusTreeNode child_node(child_page);
                    child_node.SetParentPageId(left_sibling_id);
                    buffer_pool_->UnpinPage(child_page_id, true);
                }
            }
        }
        
        // Update left_sibling key count
        left_sibling.SetKeyCount(left_key_count + 1 + node_key_count);
        
        // Log the merge operations
        LogPageModification(left_sibling_page, LogRecordType::UPDATE);
        LogPageModification(node_page, LogRecordType::UPDATE);
        
        buffer_pool_->UnpinPage(left_sibling_id, true);
        
        // Remove separator key from parent at index child_index - 1
        for (uint16_t i = child_index - 1; i < parent_key_count - 1; i++) {
            parent.SetKey(i, parent.GetKey(i + 1));
        }
        
        // Remove child pointer from parent at index child_index
        for (uint16_t i = child_index; i <= parent_key_count; i++) {
            parent.SetChildPageId(i, parent.GetChildPageId(i + 1));
        }
        
        parent.SetKeyCount(parent_key_count - 1);
        buffer_pool_->UnpinPage(parent_page_id, true);
        
        buffer_pool_->UnpinPage(node_page_id, false); // Node will be deleted
        
        return true;
    }
    
    // Merge with right sibling for internal nodes
    // Brings down separator key from parent
    bool MergeWithRightInternal(uint32_t node_page_id, uint32_t right_sibling_id, uint32_t parent_page_id) {
        Page* node_page = buffer_pool_->FetchPage(node_page_id);
        Page* right_sibling_page = buffer_pool_->FetchPage(right_sibling_id);
        if (!node_page || !right_sibling_page) {
            if (node_page) buffer_pool_->UnpinPage(node_page_id, false);
            if (right_sibling_page) buffer_pool_->UnpinPage(right_sibling_id, false);
            return false;
        }
        
        BPlusTreeNode node(node_page);
        BPlusTreeNode right_sibling(right_sibling_page);
        
        // Verify both are internal nodes
        if (node.IsLeaf() || right_sibling.IsLeaf()) {
            buffer_pool_->UnpinPage(node_page_id, false);
            buffer_pool_->UnpinPage(right_sibling_id, false);
            return false;
        }
        
        uint16_t node_key_count = node.GetKeyCount();
        uint16_t right_key_count = right_sibling.GetKeyCount();
        
        // Get the separator key from parent (will be brought down)
        Page* parent_page = buffer_pool_->FetchPage(parent_page_id);
        if (!parent_page) {
            buffer_pool_->UnpinPage(node_page_id, false);
            buffer_pool_->UnpinPage(right_sibling_id, false);
            return false;
        }
        
        BPlusTreeNode parent(parent_page);
        uint16_t parent_key_count = parent.GetKeyCount();
        
        // Find the index of the separator key to bring down
        // When merging child[i+1] (right_sibling) into child[i] (node)
        // The separator is parent.keys[i]
        uint16_t child_index = 0;
        for (uint16_t i = 0; i <= parent_key_count; i++) {
            if (parent.GetChildPageId(i) == right_sibling_id) {
                child_index = i;
                break;
            }
        }
        
        uint64_t separator_key = parent.GetKey(child_index - 1);
        
        // Bring down separator key to node at position node_key_count
        node.SetKey(node_key_count, separator_key);
        
        // Move all keys from right_sibling to node (starting at node_key_count + 1)
        for (uint16_t i = 0; i < right_key_count; i++) {
            node.SetKey(node_key_count + 1 + i, right_sibling.GetKey(i));
        }
        
        // Move all children from right_sibling to node
        // node.children = node.children + right_sibling.children
        for (uint16_t i = 0; i <= right_key_count; i++) {
            uint32_t child_page_id = right_sibling.GetChildPageId(i);
            node.SetChildPageId(node_key_count + 1 + i, child_page_id);
            
            // Update parent pointer for the moved child (only if valid)
            if (child_page_id != INVALID_PAGE_ID) {
                Page* child_page = buffer_pool_->FetchPage(child_page_id);
                if (child_page) {
                    BPlusTreeNode child_node(child_page);
                    child_node.SetParentPageId(node_page_id);
                    buffer_pool_->UnpinPage(child_page_id, true);
                }
            }
        }
        
        // Update node key count
        node.SetKeyCount(node_key_count + 1 + right_key_count);
        
        // Log the merge operations
        LogPageModification(node_page, LogRecordType::UPDATE);
        LogPageModification(right_sibling_page, LogRecordType::UPDATE);
        
        buffer_pool_->UnpinPage(node_page_id, true);
        
        // Remove separator key from parent at index child_index - 1
        for (uint16_t i = child_index - 1; i < parent_key_count - 1; i++) {
            parent.SetKey(i, parent.GetKey(i + 1));
        }
        
        // Remove child pointer from parent at index child_index
        for (uint16_t i = child_index; i <= parent_key_count; i++) {
            parent.SetChildPageId(i, parent.GetChildPageId(i + 1));
        }
        
        parent.SetKeyCount(parent_key_count - 1);
        buffer_pool_->UnpinPage(parent_page_id, true);
        
        buffer_pool_->UnpinPage(right_sibling_id, false); // right_sibling will be deleted
        
        return true;
    }
    
    // Try to merge with a sibling
    // Returns true if merge was successful
    // Ownership: Caller owns node_page fetch if passed, this function unpins all pages it fetches
    bool TryMerge(uint32_t node_page_id, uint32_t parent_page_id) {
        // Try left sibling first (preferred)
        uint32_t left_sibling_id = FindLeftSibling(parent_page_id, node_page_id);
        if (left_sibling_id != INVALID_PAGE_ID) {
            Page* left_sibling_page = buffer_pool_->FetchPage(left_sibling_id);
            if (left_sibling_page) {
                BPlusTreeNode left_sibling(left_sibling_page);
                Page* node_page = buffer_pool_->FetchPage(node_page_id);
                if (node_page) {
                    BPlusTreeNode node(node_page);
                    bool is_leaf = node.IsLeaf();
                    uint32_t max_keys = is_leaf ? BPlusTreeNode::GetMaxLeafKeys() : BPlusTreeNode::GetMaxInternalKeys();
                    uint16_t combined_keys = left_sibling.GetKeyCount() + node.GetKeyCount();
                    
                    buffer_pool_->UnpinPage(node_page_id, false);
                    buffer_pool_->UnpinPage(left_sibling_id, false);
                    
                    if (combined_keys <= max_keys) {
                        if (is_leaf) {
                            return MergeWithLeftLeaf(node_page_id, left_sibling_id, parent_page_id);
                        } else {
                            return MergeWithLeftInternal(node_page_id, left_sibling_id, parent_page_id);
                        }
                    }
                }
                buffer_pool_->UnpinPage(left_sibling_id, false);
            }
        }
        
        // Try right sibling
        uint32_t right_sibling_id = FindRightSibling(parent_page_id, node_page_id);
        if (right_sibling_id != INVALID_PAGE_ID) {
            Page* right_sibling_page = buffer_pool_->FetchPage(right_sibling_id);
            if (right_sibling_page) {
                BPlusTreeNode right_sibling(right_sibling_page);
                Page* node_page = buffer_pool_->FetchPage(node_page_id);
                if (node_page) {
                    BPlusTreeNode node(node_page);
                    bool is_leaf = node.IsLeaf();
                    uint32_t max_keys = is_leaf ? BPlusTreeNode::GetMaxLeafKeys() : BPlusTreeNode::GetMaxInternalKeys();
                    uint16_t combined_keys = node.GetKeyCount() + right_sibling.GetKeyCount();
                    
                    buffer_pool_->UnpinPage(node_page_id, false);
                    buffer_pool_->UnpinPage(right_sibling_id, false);
                    
                    if (combined_keys <= max_keys) {
                        if (is_leaf) {
                            return MergeWithRightLeaf(node_page_id, right_sibling_id, parent_page_id);
                        } else {
                            return MergeWithRightInternal(node_page_id, right_sibling_id, parent_page_id);
                        }
                    }
                }
                buffer_pool_->UnpinPage(right_sibling_id, false);
            }
        }
        
        return false;
    }
    
    // Fix parent underflow recursively after a merge
    void FixParentUnderflow(uint32_t parent_page_id, const std::vector<uint32_t>& path) {
        // Find the parent's parent
        size_t parent_index = 0;
        for (size_t i = 0; i < path.size(); i++) {
            if (path[i] == parent_page_id) {
                parent_index = i;
                break;
            }
        }
        
        // If parent is root, handle root reduction
        if (parent_index == 0) {
            HandleRootReduction(parent_page_id);
            return;
        }
        
        // Check if parent underflows
        Page* parent_page = buffer_pool_->FetchPage(parent_page_id);
        if (!parent_page) {
            return;
        }
        
        BPlusTreeNode parent_node(parent_page);
        if (!IsNodeUnderflowed(parent_node)) {
            buffer_pool_->UnpinPage(parent_page_id, false);
            return;
        }
        
        buffer_pool_->UnpinPage(parent_page_id, false);
        
        // Try redistribution or merge
        uint32_t grandparent_page_id = path[parent_index - 1];
        if (!TryRedistribute(parent_page_id, grandparent_page_id)) {
            if (TryMerge(parent_page_id, grandparent_page_id)) {
                // Merge succeeded, continue recursively
                FixParentUnderflow(grandparent_page_id, path);
            }
        }
    }
    
    // Handle root reduction when root has only one child
    void HandleRootReduction(uint32_t root_page_id) {
        Page* root_page = buffer_pool_->FetchPage(root_page_id);
        if (!root_page) {
            return;
        }
        
        BPlusTreeNode root_node(root_page);
        
        // If root is a leaf, nothing to do
        if (root_node.IsLeaf()) {
            buffer_pool_->UnpinPage(root_page_id, false);
            return;
        }
        
        // If root has more than one key, nothing to do
        uint16_t key_count = root_node.GetKeyCount();
        if (key_count > 0) {
            buffer_pool_->UnpinPage(root_page_id, false);
            return;
        }
        
        // Root has no keys but one child - replace root with its child
        uint32_t child_page_id = root_node.GetChildPageId(0);
        
        // Update child's parent to INVALID_PAGE_ID (it's now the root)
        Page* child_page = buffer_pool_->FetchPage(child_page_id);
        if (child_page) {
            BPlusTreeNode child_node(child_page);
            child_node.SetParentPageId(INVALID_PAGE_ID);
            buffer_pool_->UnpinPage(child_page_id, true);
        }
        
        buffer_pool_->UnpinPage(root_page_id, true);
        
        // Update root page ID
        root_page_id_ = child_page_id;
        
        // TODO: Delete the old root page from buffer pool
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
        
        LogPageModification(page, LogRecordType::INSERT);
        
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
        
        // Log the split operations
        LogPageModification(leaf_page, LogRecordType::UPDATE);
        LogPageModification(new_leaf_page, LogRecordType::UPDATE);
        
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
        
        // Log the new root creation
        LogPageModification(new_root_page, LogRecordType::UPDATE);
        
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
        
        // Log the split operations
        LogPageModification(internal_page, LogRecordType::UPDATE);
        LogPageModification(new_internal_page, LogRecordType::UPDATE);
        
        buffer_pool_->UnpinPage(internal_page_id, true);
        buffer_pool_->UnpinPage(new_internal_page_id, true);
        
        std::vector<uint32_t> parent_path(path.begin(), path.end() - 1);
        return InsertIntoParent(parent_path, split_key, new_internal_page_id);
    }
};

}
