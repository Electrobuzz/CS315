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

// Helper function to verify results are sorted
bool VerifyResultsSorted(const std::vector<std::pair<uint64_t, RID>>& results) {
    for (size_t i = 1; i < results.size(); i++) {
        if (results[i-1].first > results[i].first) {
            std::cout << "  FAIL: Results not sorted at index " << i << ": " << results[i-1].first << " > " << results[i].first << std::endl;
            return false;
        }
    }
    return true;
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

// Test 5: Small Range Scan Test
bool TestSmallRangeScan() {
    std::cout << "\n=== Test: Small Range Scan ===" << std::endl;
    
    const std::string db_file = "test_small_range.db";
    std::remove(db_file.c_str());
    
    TestBufferPool bpm(db_file);
    BPlusTree<TestBufferPool> tree(&bpm, INVALID_PAGE_ID);
    
    // Insert 100 keys
    for (uint64_t key = 1; key <= 100; key++) {
        RID rid(key, key * 10);
        if (!tree.Insert(key, rid)) {
            std::cout << "  FAIL: Failed to insert key " << key << std::endl;
            return false;
        }
    }
    
    std::cout << "  Inserted 100 keys" << std::endl;
    
    // Scan range [25, 50]
    std::vector<std::pair<uint64_t, RID>> results;
    if (!tree.RangeScan(25, 50, results)) {
        std::cout << "  FAIL: RangeScan returned false" << std::endl;
        return false;
    }
    
    std::cout << "  Range scan returned " << results.size() << " results" << std::endl;
    
    // Verify we got exactly 26 keys (25 through 50 inclusive)
    if (results.size() != 26) {
        std::cout << "  FAIL: Expected 26 results, got " << results.size() << std::endl;
        return false;
    }
    
    // Verify results are sorted and correct
    for (size_t i = 0; i < results.size(); i++) {
        uint64_t expected_key = 25 + i;
        if (results[i].first != expected_key) {
            std::cout << "  FAIL: Expected key " << expected_key << " at index " << i << ", got " << results[i].first << std::endl;
            return false;
        }
        if (results[i].second.page_id != expected_key || results[i].second.slot_id != expected_key * 10) {
            std::cout << "  FAIL: Incorrect RID for key " << expected_key << std::endl;
            return false;
        }
    }
    
    // Verify results are sorted
    if (!VerifyResultsSorted(results)) {
        return false;
    }
    
    std::cout << "  Range scan verified: keys 25-50 with correct RIDs" << std::endl;
    
    std::remove(db_file.c_str());
    return true;
}

// Test 6: Full Range Scan Test
bool TestFullRangeScan() {
    std::cout << "\n=== Test: Full Range Scan ===" << std::endl;
    
    const std::string db_file = "test_full_range.db";
    std::remove(db_file.c_str());
    
    TestBufferPool bpm(db_file);
    BPlusTree<TestBufferPool> tree(&bpm, INVALID_PAGE_ID);
    
    // Insert 200 keys
    for (uint64_t key = 1; key <= 200; key++) {
        RID rid(key, key * 11);
        if (!tree.Insert(key, rid)) {
            std::cout << "  FAIL: Failed to insert key " << key << std::endl;
            return false;
        }
    }
    
    std::cout << "  Inserted 200 keys" << std::endl;
    
    // Scan entire range [1, 200]
    std::vector<std::pair<uint64_t, RID>> results;
    if (!tree.RangeScan(1, 200, results)) {
        std::cout << "  FAIL: RangeScan returned false" << std::endl;
        return false;
    }
    
    std::cout << "  Range scan returned " << results.size() << " results" << std::endl;
    
    // Verify we got exactly 200 keys
    if (results.size() != 200) {
        std::cout << "  FAIL: Expected 200 results, got " << results.size() << std::endl;
        return false;
    }
    
    // Verify results are sorted and correct
    for (size_t i = 0; i < results.size(); i++) {
        uint64_t expected_key = 1 + i;
        if (results[i].first != expected_key) {
            std::cout << "  FAIL: Expected key " << expected_key << " at index " << i << ", got " << results[i].first << std::endl;
            return false;
        }
    }
    
    // Verify results are sorted
    if (!VerifyResultsSorted(results)) {
        return false;
    }
    
    std::cout << "  Full range scan verified: all 200 keys in sorted order" << std::endl;
    
    std::remove(db_file.c_str());
    return true;
}

// Test 7: Cross-Leaf Range Scan Test
bool TestCrossLeafRangeScan() {
    std::cout << "\n=== Test: Cross-Leaf Range Scan ===" << std::endl;
    
    const std::string db_file = "test_cross_leaf.db";
    std::remove(db_file.c_str());
    
    TestBufferPool bpm(db_file);
    BPlusTree<TestBufferPool> tree(&bpm, INVALID_PAGE_ID);
    
    // Insert enough keys to cause multiple leaf splits
    const int num_keys = 500;
    for (uint64_t key = 1; key <= num_keys; key++) {
        RID rid(key, key * 12);
        if (!tree.Insert(key, rid)) {
            std::cout << "  FAIL: Failed to insert key " << key << std::endl;
            return false;
        }
    }
    
    std::cout << "  Inserted " << num_keys << " keys" << std::endl;
    
    // Scan range that spans multiple leaves [200, 400]
    std::vector<std::pair<uint64_t, RID>> results;
    if (!tree.RangeScan(200, 400, results)) {
        std::cout << "  FAIL: RangeScan returned false" << std::endl;
        return false;
    }
    
    std::cout << "  Range scan returned " << results.size() << " results" << std::endl;
    
    // Verify we got exactly 201 keys (200 through 400 inclusive)
    if (results.size() != 201) {
        std::cout << "  FAIL: Expected 201 results, got " << results.size() << std::endl;
        return false;
    }
    
    // Verify results are sorted and correct
    for (size_t i = 0; i < results.size(); i++) {
        uint64_t expected_key = 200 + i;
        if (results[i].first != expected_key) {
            std::cout << "  FAIL: Expected key " << expected_key << " at index " << i << ", got " << results[i].first << std::endl;
            return false;
        }
    }
    
    // Verify results are sorted
    if (!VerifyResultsSorted(results)) {
        return false;
    }
    
    std::cout << "  Cross-leaf range scan verified: 201 keys across multiple leaves" << std::endl;
    
    std::remove(db_file.c_str());
    return true;
}

// Test 8: Single-Key Range (Exact Match)
bool TestSingleKeyRange() {
    std::cout << "\n=== Test: Single-Key Range (Exact Match) ===" << std::endl;
    
    const std::string db_file = "test_single_key.db";
    std::remove(db_file.c_str());
    
    TestBufferPool bpm(db_file);
    BPlusTree<TestBufferPool> tree(&bpm, INVALID_PAGE_ID);
    
    // Insert 100 keys
    for (uint64_t key = 1; key <= 100; key++) {
        RID rid(key, key * 20);
        if (!tree.Insert(key, rid)) {
            std::cout << "  FAIL: Failed to insert key " << key << std::endl;
            return false;
        }
    }
    
    std::cout << "  Inserted 100 keys" << std::endl;
    
    // Scan range [50, 50]
    std::vector<std::pair<uint64_t, RID>> results;
    if (!tree.RangeScan(50, 50, results)) {
        std::cout << "  FAIL: RangeScan returned false" << std::endl;
        return false;
    }
    
    std::cout << "  Range scan returned " << results.size() << " results" << std::endl;
    
    // Verify we got exactly 1 result
    if (results.size() != 1) {
        std::cout << "  FAIL: Expected 1 result, got " << results.size() << std::endl;
        return false;
    }
    
    // Verify the result is key 50 with correct RID
    if (results[0].first != 50) {
        std::cout << "  FAIL: Expected key 50, got " << results[0].first << std::endl;
        return false;
    }
    if (results[0].second.page_id != 50 || results[0].second.slot_id != 50 * 20) {
        std::cout << "  FAIL: Incorrect RID for key 50" << std::endl;
        return false;
    }
    
    std::cout << "  Single-key range verified: key 50 with correct RID" << std::endl;
    
    std::remove(db_file.c_str());
    return true;
}

// Test 9: Empty Range (Outside Data)
bool TestEmptyRange() {
    std::cout << "\n=== Test: Empty Range (Outside Data) ===" << std::endl;
    
    const std::string db_file = "test_empty_range.db";
    std::remove(db_file.c_str());
    
    TestBufferPool bpm(db_file);
    BPlusTree<TestBufferPool> tree(&bpm, INVALID_PAGE_ID);
    
    // Insert 100 keys (max key = 100)
    for (uint64_t key = 1; key <= 100; key++) {
        RID rid(key, key * 21);
        if (!tree.Insert(key, rid)) {
            std::cout << "  FAIL: Failed to insert key " << key << std::endl;
            return false;
        }
    }
    
    std::cout << "  Inserted 100 keys (max key = 100)" << std::endl;
    
    // Scan range [1000, 2000] - should return empty
    std::vector<std::pair<uint64_t, RID>> results;
    if (!tree.RangeScan(1000, 2000, results)) {
        std::cout << "  FAIL: RangeScan returned false" << std::endl;
        return false;
    }
    
    std::cout << "  Range scan returned " << results.size() << " results" << std::endl;
    
    // Verify we got 0 results
    if (results.size() != 0) {
        std::cout << "  FAIL: Expected 0 results, got " << results.size() << std::endl;
        return false;
    }
    
    std::cout << "  Empty range verified: no garbage reads" << std::endl;
    
    std::remove(db_file.c_str());
    return true;
}

// Test 10: Reverse Range (Invalid Input)
bool TestReverseRange() {
    std::cout << "\n=== Test: Reverse Range (Invalid Input) ===" << std::endl;
    
    const std::string db_file = "test_reverse_range.db";
    std::remove(db_file.c_str());
    
    TestBufferPool bpm(db_file);
    BPlusTree<TestBufferPool> tree(&bpm, INVALID_PAGE_ID);
    
    // Insert 100 keys
    for (uint64_t key = 1; key <= 100; key++) {
        RID rid(key, key * 22);
        if (!tree.Insert(key, rid)) {
            std::cout << "  FAIL: Failed to insert key " << key << std::endl;
            return false;
        }
    }
    
    std::cout << "  Inserted 100 keys" << std::endl;
    
    // Scan range [100, 50] - should return empty
    std::vector<std::pair<uint64_t, RID>> results;
    if (!tree.RangeScan(100, 50, results)) {
        std::cout << "  FAIL: RangeScan returned false" << std::endl;
        return false;
    }
    
    std::cout << "  Range scan returned " << results.size() << " results" << std::endl;
    
    // Verify we got 0 results
    if (results.size() != 0) {
        std::cout << "  FAIL: Expected 0 results, got " << results.size() << std::endl;
        return false;
    }
    
    std::cout << "  Reverse range verified: no crash, empty result" << std::endl;
    
    std::remove(db_file.c_str());
    return true;
}

// Test 11: Boundary Split Case
bool TestBoundarySplit() {
    std::cout << "\n=== Test: Boundary Split Case ===" << std::endl;
    
    const std::string db_file = "test_boundary_split.db";
    std::remove(db_file.c_str());
    
    TestBufferPool bpm(db_file);
    BPlusTree<TestBufferPool> tree(&bpm, INVALID_PAGE_ID);
    
    // Insert keys that will cause a split
    // Leaf capacity is 255 keys, so insert 256 keys to force a split
    const int num_keys = 256;
    for (uint64_t key = 1; key <= num_keys; key++) {
        RID rid(key, key * 23);
        if (!tree.Insert(key, rid)) {
            std::cout << "  FAIL: Failed to insert key " << key << std::endl;
            return false;
        }
    }
    
    std::cout << "  Inserted " << num_keys << " keys (forces leaf split)" << std::endl;
    
    // The split point is at 128 (256/2)
    // Scan for the split key
    uint64_t split_key = 128;
    std::vector<std::pair<uint64_t, RID>> results;
    if (!tree.RangeScan(split_key, split_key, results)) {
        std::cout << "  FAIL: RangeScan returned false" << std::endl;
        return false;
    }
    
    std::cout << "  Range scan for split key " << split_key << " returned " << results.size() << " results" << std::endl;
    
    // Verify we got exactly 1 result (the split key should not be lost)
    if (results.size() != 1) {
        std::cout << "  FAIL: Expected 1 result, got " << results.size() << std::endl;
        return false;
    }
    
    // Verify the result is the split key
    if (results[0].first != split_key) {
        std::cout << "  FAIL: Expected key " << split_key << ", got " << results[0].first << std::endl;
        return false;
    }
    
    std::cout << "  Boundary split verified: split key " << split_key << " not lost during split" << std::endl;
    
    std::remove(db_file.c_str());
    return true;
}

// Test 12: First Leaf Edge
bool TestFirstLeafEdge() {
    std::cout << "\n=== Test: First Leaf Edge ===" << std::endl;
    
    const std::string db_file = "test_first_leaf.db";
    std::remove(db_file.c_str());
    
    TestBufferPool bpm(db_file);
    BPlusTree<TestBufferPool> tree(&bpm, INVALID_PAGE_ID);
    
    // Insert 100 keys
    for (uint64_t key = 1; key <= 100; key++) {
        RID rid(key, key * 24);
        if (!tree.Insert(key, rid)) {
            std::cout << "  FAIL: Failed to insert key " << key << std::endl;
            return false;
        }
    }
    
    std::cout << "  Inserted 100 keys" << std::endl;
    
    // Scan from first key
    std::vector<std::pair<uint64_t, RID>> results;
    if (!tree.RangeScan(1, 10, results)) {
        std::cout << "  FAIL: RangeScan returned false" << std::endl;
        return false;
    }
    
    std::cout << "  Range scan [1, 10] returned " << results.size() << " results" << std::endl;
    
    // Verify we got exactly 10 results
    if (results.size() != 10) {
        std::cout << "  FAIL: Expected 10 results, got " << results.size() << std::endl;
        return false;
    }
    
    // Verify results are sorted and correct
    for (size_t i = 0; i < results.size(); i++) {
        uint64_t expected_key = 1 + i;
        if (results[i].first != expected_key) {
            std::cout << "  FAIL: Expected key " << expected_key << " at index " << i << ", got " << results[i].first << std::endl;
            return false;
        }
    }
    
    std::cout << "  First leaf edge verified: keys 1-10 in sorted order" << std::endl;
    
    std::remove(db_file.c_str());
    return true;
}

// Test 13: Last Leaf Edge
bool TestLastLeafEdge() {
    std::cout << "\n=== Test: Last Leaf Edge ===" << std::endl;
    
    const std::string db_file = "test_last_leaf.db";
    std::remove(db_file.c_str());
    
    TestBufferPool bpm(db_file);
    BPlusTree<TestBufferPool> tree(&bpm, INVALID_PAGE_ID);
    
    // Insert 100 keys
    for (uint64_t key = 1; key <= 100; key++) {
        RID rid(key, key * 25);
        if (!tree.Insert(key, rid)) {
            std::cout << "  FAIL: Failed to insert key " << key << std::endl;
            return false;
        }
    }
    
    std::cout << "  Inserted 100 keys" << std::endl;
    
    // Scan to last key
    std::vector<std::pair<uint64_t, RID>> results;
    if (!tree.RangeScan(90, 100, results)) {
        std::cout << "  FAIL: RangeScan returned false" << std::endl;
        return false;
    }
    
    std::cout << "  Range scan [90, 100] returned " << results.size() << " results" << std::endl;
    
    // Verify we got exactly 11 results
    if (results.size() != 11) {
        std::cout << "  FAIL: Expected 11 results, got " << results.size() << std::endl;
        return false;
    }
    
    // Verify results are sorted and correct
    for (size_t i = 0; i < results.size(); i++) {
        uint64_t expected_key = 90 + i;
        if (results[i].first != expected_key) {
            std::cout << "  FAIL: Expected key " << expected_key << " at index " << i << ", got " << results[i].first << std::endl;
            return false;
        }
    }
    
    std::cout << "  Last leaf edge verified: keys 90-100 in sorted order" << std::endl;
    
    std::remove(db_file.c_str());
    return true;
}

// Test 14: Random Range Queries
bool TestRandomRangeQueries() {
    std::cout << "\n=== Test: Random Range Queries (100 iterations) ===" << std::endl;
    
    const std::string db_file = "test_random_range.db";
    std::remove(db_file.c_str());
    
    TestBufferPool bpm(db_file);
    BPlusTree<TestBufferPool> tree(&bpm, INVALID_PAGE_ID);
    
    // Insert 1000 keys
    const int num_keys = 1000;
    std::vector<uint64_t> inserted_keys;
    for (uint64_t key = 1; key <= num_keys; key++) {
        RID rid(key, key * 26);
        inserted_keys.push_back(key);
        if (!tree.Insert(key, rid)) {
            std::cout << "  FAIL: Failed to insert key " << key << std::endl;
            return false;
        }
    }
    
    std::cout << "  Inserted " << num_keys << " keys" << std::endl;
    
    // Run 100 random range queries
    srand(time(nullptr));
    int passed = 0;
    int failed = 0;
    
    for (int i = 0; i < 100; i++) {
        uint64_t l = (rand() % 1200) + 1; // Random from 1 to 1200
        uint64_t r = (rand() % 1200) + 1; // Random from 1 to 1200
        
        if (l > r) {
            std::swap(l, r);
        }
        
        std::vector<std::pair<uint64_t, RID>> results;
        if (!tree.RangeScan(l, r, results)) {
            std::cout << "  FAIL: RangeScan returned false for range [" << l << ", " << r << "]" << std::endl;
            failed++;
            continue;
        }
        
        // Verify results are sorted
        if (!VerifyResultsSorted(results)) {
            std::cout << "  FAIL: Results not sorted for range [" << l << ", " << r << "]" << std::endl;
            failed++;
            continue;
        }
        
        // Verify results are in range
        for (const auto& result : results) {
            if (result.first < l || result.first > r) {
                std::cout << "  FAIL: Key " << result.first << " outside range [" << l << ", " << r << "]" << std::endl;
                failed++;
                continue;
            }
        }
        
        // Compare with baseline (inserted keys)
        std::vector<uint64_t> expected;
        for (uint64_t key : inserted_keys) {
            if (key >= l && key <= r) {
                expected.push_back(key);
            }
        }
        
        if (results.size() != expected.size()) {
            std::cout << "  FAIL: Expected " << expected.size() << " results, got " << results.size() << " for range [" << l << ", " << r << "]" << std::endl;
            failed++;
            continue;
        }
        
        passed++;
    }
    
    std::cout << "  Random range queries: " << passed << " passed, " << failed << " failed" << std::endl;
    
    if (failed > 0) {
        std::cout << "  FAIL: Some random range queries failed" << std::endl;
        return false;
    }
    
    std::cout << "  All random range queries passed" << std::endl;
    
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
    
    // Test 5: Small Range Scan
    if (TestSmallRangeScan()) {
        g_stats.RecordPass();
        std::cout << "  ✅ PASS" << std::endl;
    } else {
        g_stats.RecordFail();
        std::cout << "  ❌ FAIL" << std::endl;
    }
    
    // Test 6: Full Range Scan
    if (TestFullRangeScan()) {
        g_stats.RecordPass();
        std::cout << "  ✅ PASS" << std::endl;
    } else {
        g_stats.RecordFail();
        std::cout << "  ❌ FAIL" << std::endl;
    }
    
    // Test 7: Cross-Leaf Range Scan
    if (TestCrossLeafRangeScan()) {
        g_stats.RecordPass();
        std::cout << "  ✅ PASS" << std::endl;
    } else {
        g_stats.RecordFail();
        std::cout << "  ❌ FAIL" << std::endl;
    }
    
    // Test 8: Single-Key Range
    if (TestSingleKeyRange()) {
        g_stats.RecordPass();
        std::cout << "  ✅ PASS" << std::endl;
    } else {
        g_stats.RecordFail();
        std::cout << "  ❌ FAIL" << std::endl;
    }
    
    // Test 9: Empty Range
    if (TestEmptyRange()) {
        g_stats.RecordPass();
        std::cout << "  ✅ PASS" << std::endl;
    } else {
        g_stats.RecordFail();
        std::cout << "  ❌ FAIL" << std::endl;
    }
    
    // Test 10: Reverse Range
    if (TestReverseRange()) {
        g_stats.RecordPass();
        std::cout << "  ✅ PASS" << std::endl;
    } else {
        g_stats.RecordFail();
        std::cout << "  ❌ FAIL" << std::endl;
    }
    
    // Test 11: Boundary Split
    if (TestBoundarySplit()) {
        g_stats.RecordPass();
        std::cout << "  ✅ PASS" << std::endl;
    } else {
        g_stats.RecordFail();
        std::cout << "  ❌ FAIL" << std::endl;
    }
    
    // Test 12: First Leaf Edge
    if (TestFirstLeafEdge()) {
        g_stats.RecordPass();
        std::cout << "  ✅ PASS" << std::endl;
    } else {
        g_stats.RecordFail();
        std::cout << "  ❌ FAIL" << std::endl;
    }
    
    // Test 13: Last Leaf Edge
    if (TestLastLeafEdge()) {
        g_stats.RecordPass();
        std::cout << "  ✅ PASS" << std::endl;
    } else {
        g_stats.RecordFail();
        std::cout << "  ❌ FAIL" << std::endl;
    }
    
    // Test 14: Random Range Queries
    if (TestRandomRangeQueries()) {
        g_stats.RecordPass();
        std::cout << "  ✅ PASS" << std::endl;
    } else {
        g_stats.RecordFail();
        std::cout << "  ❌ FAIL" << std::endl;
    }
    
    g_stats.PrintSummary();
    
    return (g_stats.failed == 0) ? 0 : 1;
}
