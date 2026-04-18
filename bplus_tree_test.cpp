#include <iostream>
#include <vector>
#include <random>
#include <cassert>
#include <unordered_set>
#include <algorithm>
#include <unordered_map>
#include <fstream>

#include "src/storage/b_plus_tree.h"
#include "src/storage/disk_manager_v2.h"

using namespace minidb;

// Minimal buffer pool interface for testing (in-memory only)
class TestBufferPool {
public:
    TestBufferPool(const std::string& db_file) : next_page_id_(1) {
        (void)db_file; // Unused for in-memory version
    }
    
    ~TestBufferPool() {
        // Clean up all pages
        for (auto& pair : pages_) {
            delete pair.second;
        }
    }
    
    Page* NewPage(uint32_t& page_id) {
        page_id = next_page_id_++;
        Page* page = new Page();
        page->SetPageId(page_id);
        std::memset(page->GetData(), 0, PAGE_SIZE);
        pages_[page_id] = page;
        return page;
    }
    
    Page* FetchPage(uint32_t page_id) {
        auto it = pages_.find(page_id);
        if (it != pages_.end()) {
            return it->second;
        }
        
        // Page not found - this shouldn't happen in a correct B+ Tree
        std::cerr << "Error: Page " << page_id << " not found in buffer pool" << std::endl;
        return nullptr;
    }
    
    void UnpinPage(uint32_t page_id, bool dirty) {
        (void)page_id;
        (void)dirty; // In-memory version doesn't need to write to disk
    }
    
private:
    uint32_t next_page_id_;
    std::unordered_map<uint32_t, Page*> pages_;
};

// Test result tracking
struct TestStats {
    int total = 0;
    int passed = 0;
    int failed = 0;
    
    void RecordPass() { total++; passed++; }
    void RecordFail() { total++; failed++; }
    
    void PrintSummary() {
        std::cout << "\n=== Test Summary ===" << std::endl;
        std::cout << "Total: " << total << std::endl;
        std::cout << "Passed: " << passed << std::endl;
        std::cout << "Failed: " << failed << std::endl;
        std::cout << "Success Rate: " << (total > 0 ? (100.0 * passed / total) : 0) << "%" << std::endl;
    }
};

TestStats g_stats;

// Helper: Print node contents for debugging
void PrintNodeContents(BPlusTreeNode& node, uint32_t page_id) {
    std::cout << "    Page ID: " << page_id << std::endl;
    std::cout << "    Parent Page ID: " << node.GetParentPageId() << std::endl;
    std::cout << "    Is Leaf: " << (node.IsLeaf() ? "Yes" : "No") << std::endl;
    std::cout << "    Key Count: " << node.GetKeyCount() << std::endl;
    std::cout << "    Keys: ";
    for (uint16_t i = 0; i < node.GetKeyCount(); i++) {
        std::cout << node.GetKey(i);
        if (i < node.GetKeyCount() - 1) std::cout << ", ";
    }
    std::cout << std::endl;
}

// Helper: Check if keys in a node are sorted
bool ValidateNodeSorted(BPlusTreeNode& node, uint32_t page_id) {
    uint16_t key_count = node.GetKeyCount();
    for (uint16_t i = 1; i < key_count; i++) {
        if (node.GetKey(i) < node.GetKey(i - 1)) {
            std::cout << "  FAIL: Node " << page_id << " keys not sorted" << std::endl;
            PrintNodeContents(node, page_id);
            return false;
        }
    }
    return true;
}

// Helper: Check if node key count is within capacity
bool ValidateNodeCapacity(BPlusTreeNode& node, uint32_t page_id) {
    uint16_t key_count = node.GetKeyCount();
    uint32_t max_capacity = node.IsLeaf() ? BPlusTreeNode::GetMaxLeafKeys() : BPlusTreeNode::GetMaxInternalKeys();
    if (key_count > max_capacity) {
        std::cout << "  FAIL: Node " << page_id << " exceeds capacity (key_count=" << key_count << ", max_capacity=" << max_capacity << ")" << std::endl;
        PrintNodeContents(node, page_id);
        return false;
    }
    return true;
}

// Helper: Check if internal node has correct number of children
bool ValidateInternalNodeChildren(BPlusTreeNode& node, uint32_t page_id) {
    if (node.IsLeaf()) return true;  // Not applicable for leaves
    
    uint16_t key_count = node.GetKeyCount();
    // Internal nodes should have key_count + 1 children
    // We can't directly verify all children exist, but we can check the last one
    uint32_t last_child = node.GetChildPageId(key_count);
    if (last_child == INVALID_PAGE_ID && key_count > 0) {
        std::cout << "  FAIL: Node " << page_id << " missing last child (key_count=" << key_count << ")" << std::endl;
        PrintNodeContents(node, page_id);
        return false;
    }
    return true;
}

// Helper: Check parent-child consistency
bool ValidateParentConsistency(BPlusTreeNode& node, uint32_t page_id, TestBufferPool* bpm) {
    uint32_t parent_page_id = node.GetParentPageId();
    if (parent_page_id == INVALID_PAGE_ID) return true;  // Root or uninitialized
    
    Page* parent_page = bpm->FetchPage(parent_page_id);
    if (!parent_page) return false;
    
    BPlusTreeNode parent(parent_page);
    bool found = false;
    uint32_t current_page_id = page_id;
    
    // Check if current page is a child of parent
    uint16_t key_count = parent.GetKeyCount();
    for (uint16_t i = 0; i <= key_count; i++) {
        if (parent.GetChildPageId(i) == current_page_id) {
            found = true;
            break;
        }
    }
    
    bpm->UnpinPage(parent_page_id, false);
    
    if (!found) {
        std::cout << "  FAIL: Node " << page_id << " not found as child of parent " << parent_page_id << std::endl;
        PrintNodeContents(node, page_id);
        return false;
    }
    
    return true;
}

// Helper: Validate entire tree structure recursively
bool ValidateTreeStructure(BPlusTree<TestBufferPool>& tree, TestBufferPool* bpm) {
    uint32_t root_page_id = tree.GetRootPageId();
    if (root_page_id == INVALID_PAGE_ID) return true;
    
    // BFS traversal to validate all nodes
    std::vector<uint32_t> queue = {root_page_id};
    std::unordered_set<uint32_t> visited;
    
    while (!queue.empty()) {
        uint32_t current = queue.back();
        queue.pop_back();
        
        if (visited.count(current)) continue;
        visited.insert(current);
        
        Page* page = bpm->FetchPage(current);
        if (!page) return false;
        
        BPlusTreeNode node(page);
        
        // Validate node properties
        if (!ValidateNodeCapacity(node, current)) {
            bpm->UnpinPage(current, false);
            return false;
        }
        
        if (!ValidateNodeSorted(node, current)) {
            bpm->UnpinPage(current, false);
            return false;
        }
        
        if (!ValidateInternalNodeChildren(node, current)) {
            bpm->UnpinPage(current, false);
            return false;
        }
        
        if (!ValidateParentConsistency(node, current, bpm)) {
            bpm->UnpinPage(current, false);
            return false;
        }
        
        // Add children to queue for internal nodes
        if (!node.IsLeaf()) {
            uint16_t key_count = node.GetKeyCount();
            for (uint16_t i = 0; i <= key_count; i++) {
                uint32_t child_id = node.GetChildPageId(i);
                if (child_id != INVALID_PAGE_ID) {
                    queue.push_back(child_id);
                }
            }
        }
        
        bpm->UnpinPage(current, false);
    }
        
    return true;
}

// Helper: Verify all inserted keys are searchable
template <typename BufferPool>
bool VerifyAllKeysSearchable(BPlusTree<BufferPool>& tree, const std::vector<std::pair<uint64_t, RID>>& key_rid_pairs) {
    for (const auto& pair : key_rid_pairs) {
        RID found_rid;
        if (!tree.Search(pair.first, found_rid)) {
            std::cout << "  FAIL: Key " << pair.first << " not found" << std::endl;
            return false;
        }
        if (found_rid.page_id != pair.second.page_id || found_rid.slot_id != pair.second.slot_id) {
            std::cout << "  FAIL: Key " << pair.first << " has incorrect RID" << std::endl;
            return false;
        }
    }
    return true;
}

// Test 1: Ascending Insert Test
bool TestAscendingInserts() {
    std::cout << "\n=== Test: Ascending Inserts ===" << std::endl;
    
    const std::string db_file = "test_ascending.db";
    std::remove(db_file.c_str());
    
    TestBufferPool bpm(db_file);
    BPlusTree<TestBufferPool> tree(&bpm, INVALID_PAGE_ID);
    
    std::vector<std::pair<uint64_t, RID>> key_rid_pairs;
    
    // Insert 100 keys in ascending order
    for (uint64_t key = 1; key <= 100; key++) {
        RID rid(key, key * 2);
        key_rid_pairs.push_back({key, rid});
        
        if (!tree.Insert(key, rid)) {
            std::cout << "  FAIL: Failed to insert key " << key << std::endl;
            return false;
        }
    }
    
    std::cout << "  Inserted 100 keys in ascending order" << std::endl;
    
    // Verify all keys are searchable
    if (!VerifyAllKeysSearchable(tree, key_rid_pairs)) {
        std::cout << "  FAIL: Key search verification failed" << std::endl;
        return false;
    }
    
    std::cout << "  All keys verified searchable" << std::endl;
    
    // Validate tree structure
    if (!ValidateTreeStructure(tree, &bpm)) {
        std::cout << "  FAIL: Tree structure validation failed" << std::endl;
        return false;
    }
    
    std::cout << "  Tree structure validated" << std::endl;
    
    // Verify keys are not found when searching for non-existent keys
    RID dummy_rid;
    if (tree.Search(0, dummy_rid)) {
        std::cout << "  FAIL: Non-existent key 0 found" << std::endl;
        return false;
    }
    if (tree.Search(101, dummy_rid)) {
        std::cout << "  FAIL: Non-existent key 101 found" << std::endl;
        return false;
    }
    
    std::cout << "  Non-existent keys correctly not found" << std::endl;
    
    std::remove(db_file.c_str());
    return true;
}

// Test 2: Descending Insert Test
bool TestDescendingInserts() {
    std::cout << "\n=== Test: Descending Inserts ===" << std::endl;
    
    const std::string db_file = "test_descending.db";
    std::remove(db_file.c_str());
    
    TestBufferPool bpm(db_file);
    BPlusTree<TestBufferPool> tree(&bpm, INVALID_PAGE_ID);
    
    std::vector<std::pair<uint64_t, RID>> key_rid_pairs;
    
    // Insert 100 keys in descending order
    for (uint64_t key = 100; key >= 1; key--) {
        RID rid(key, key * 3);
        key_rid_pairs.push_back({key, rid});
        
        if (!tree.Insert(key, rid)) {
            std::cout << "  FAIL: Failed to insert key " << key << std::endl;
            return false;
        }
    }
    
    std::cout << "  Inserted 100 keys in descending order" << std::endl;
    
    // Verify all keys are searchable
    if (!VerifyAllKeysSearchable(tree, key_rid_pairs)) {
        std::cout << "  FAIL: Key search verification failed" << std::endl;
        return false;
    }
    
    std::cout << "  All keys verified searchable" << std::endl;
    
    // Validate tree structure
    if (!ValidateTreeStructure(tree, &bpm)) {
        std::cout << "  FAIL: Tree structure validation failed" << std::endl;
        return false;
    }
    
    std::cout << "  Tree structure validated" << std::endl;
    
    std::remove(db_file.c_str());
    return true;
}

// Test 3: Random Insert Test
bool TestRandomInserts() {
    std::cout << "\n=== Test: Random Inserts (1000 keys) ===" << std::endl;
    
    const std::string db_file = "test_random.db";
    std::remove(db_file.c_str());
    
    TestBufferPool bpm(db_file);
    BPlusTree<TestBufferPool> tree(&bpm, INVALID_PAGE_ID);
    
    const int num_keys = 1000;
    std::vector<std::pair<uint64_t, RID>> key_rid_pairs;
    std::unordered_set<uint64_t> inserted_keys;
    
    // Use random number generator
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<uint64_t> key_dist(1, 100000);
    
    int inserted = 0;
    int attempts = 0;
    
    while (inserted < num_keys && attempts < num_keys * 10) {
        uint64_t key = key_dist(gen);
        
        // Skip duplicates
        if (inserted_keys.count(key) > 0) {
            attempts++;
            continue;
        }
        
        RID rid(key, key * 4);
        
        if (!tree.Insert(key, rid)) {
            std::cout << "  FAIL: Failed to insert key " << key << " after inserting " << inserted << " keys" << std::endl;
            std::cout << "  Current tree root page_id: " << tree.GetRootPageId() << std::endl;
            return false;
        }
        
        inserted_keys.insert(key);
        key_rid_pairs.push_back({key, rid});
        inserted++;
    }
    
    if (inserted < num_keys) {
        std::cout << "  FAIL: Only inserted " << inserted << " keys (needed " << num_keys << ")" << std::endl;
        return false;
    }
    
    std::cout << "  Inserted " << inserted << " random keys" << std::endl;
    
    // Verify all keys are searchable
    if (!VerifyAllKeysSearchable(tree, key_rid_pairs)) {
        std::cout << "  FAIL: Key search verification failed" << std::endl;
        return false;
    }
    
    std::cout << "  All keys verified searchable" << std::endl;
    
    // Validate tree structure
    if (!ValidateTreeStructure(tree, &bpm)) {
        std::cout << "  FAIL: Tree structure validation failed" << std::endl;
        return false;
    }
    
    std::cout << "  Tree structure validated" << std::endl;
    
    std::remove(db_file.c_str());
    return true;
}

// Helper: Calculate tree height
int CalculateTreeHeight(BPlusTree<TestBufferPool>& tree, TestBufferPool* bpm) {
    uint32_t root_page_id = tree.GetRootPageId();
    if (root_page_id == INVALID_PAGE_ID) return 0;
    
    int height = 0;
    uint32_t current = root_page_id;
    
    while (current != INVALID_PAGE_ID) {
        height++;
        Page* page = bpm->FetchPage(current);
        if (!page) return height;
        
        BPlusTreeNode node(page);
        bpm->UnpinPage(current, false);
        
        if (node.IsLeaf()) {
            break;
        } else {
            current = node.GetChildPageId(0);
        }
    }
    
    return height;
}

// Test 4: Deep Tree Test
bool TestDeepTree() {
    std::cout << "\n=== Test: Deep Tree (100000 keys for height >= 3) ===" << std::endl;
    
    const std::string db_file = "test_deep.db";
    std::remove(db_file.c_str());
    
    TestBufferPool bpm(db_file);
    BPlusTree<TestBufferPool> tree(&bpm, INVALID_PAGE_ID);
    
    const int num_keys = 100000;
    std::vector<std::pair<uint64_t, RID>> key_rid_pairs;
    
    // Insert keys sequentially to trigger multiple splits
    for (uint64_t key = 1; key <= num_keys; key++) {
        RID rid(key, key * 5);
        key_rid_pairs.push_back({key, rid});
        
        if (!tree.Insert(key, rid)) {
            std::cout << "  FAIL: Failed to insert key " << key << std::endl;
            return false;
        }
    }
    
    std::cout << "  Inserted " << num_keys << " keys" << std::endl;
    
    // Calculate tree height
    int height = CalculateTreeHeight(tree, &bpm);
    std::cout << "  Tree height: " << height << std::endl;
    
    if (height < 3) {
        std::cout << "  FAIL: Tree height is " << height << " (expected >= 3)" << std::endl;
        return false;
    }
    
    std::cout << "  Tree height >= 3 verified" << std::endl;
    
    // Verify all keys are searchable
    if (!VerifyAllKeysSearchable(tree, key_rid_pairs)) {
        std::cout << "  FAIL: Key search verification failed" << std::endl;
        return false;
    }
    
    std::cout << "  All keys verified searchable" << std::endl;
    
    // Validate tree structure
    if (!ValidateTreeStructure(tree, &bpm)) {
        std::cout << "  FAIL: Tree structure validation failed" << std::endl;
        return false;
    }
    
    std::cout << "  Tree structure validated" << std::endl;
    
    std::remove(db_file.c_str());
    return true;
}

int main() {
    std::cout << "=== B+ Tree Comprehensive Test Suite ===" << std::endl;
    std::cout << "Starting test..." << std::endl;
    
    // Test 1: Ascending Inserts
    if (TestAscendingInserts()) {
        g_stats.RecordPass();
        std::cout << "  ✅ PASS" << std::endl;
    } else {
        g_stats.RecordFail();
        std::cout << "  ❌ FAIL" << std::endl;
    }
    
    // Test 2: Descending Inserts
    if (TestDescendingInserts()) {
        g_stats.RecordPass();
        std::cout << "  ✅ PASS" << std::endl;
    } else {
        g_stats.RecordFail();
        std::cout << "  ❌ FAIL" << std::endl;
    }
    
    // Test 3: Random Inserts
    if (TestRandomInserts()) {
        g_stats.RecordPass();
        std::cout << "  ✅ PASS" << std::endl;
    } else {
        g_stats.RecordFail();
        std::cout << "  ❌ FAIL" << std::endl;
    }
    
    // Test 4: Deep Tree
    if (TestDeepTree()) {
        g_stats.RecordPass();
        std::cout << "  ✅ PASS" << std::endl;
    } else {
        g_stats.RecordFail();
        std::cout << "  ❌ FAIL" << std::endl;
    }
    
    g_stats.PrintSummary();
    
    return (g_stats.failed == 0) ? 0 : 1;
}
