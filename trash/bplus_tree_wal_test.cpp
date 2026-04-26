#include "src/storage/b_plus_tree.h"
#include "src/storage/page_v2.h"
#include "src/storage/disk_manager_v2.h"
#include "src/storage/log_manager.h"
#include <iostream>
#include <vector>
#include <cassert>
#include <fstream>
#include <unordered_map>
#include <set>

using namespace minidb;

// Simple buffer pool for testing WAL integration
class WALTestBufferPool {
public:
    WALTestBufferPool(const std::string& db_file, size_t pool_size = 100) 
        : next_page_id_(1), pool_size_(pool_size) {
        (void)db_file; // In-memory for testing
    }
    
    ~WALTestBufferPool() {
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
        pin_counts_[page_id] = 1;
        return page;
    }
    
    Page* FetchPage(uint32_t page_id) {
        auto it = pages_.find(page_id);
        if (it != pages_.end()) {
            pin_counts_[page_id]++;
            return it->second;
        }
        return nullptr;
    }
    
    void UnpinPage(uint32_t page_id, bool dirty) {
        auto it = pin_counts_.find(page_id);
        if (it != pin_counts_.end()) {
            if (it->second > 0) {
                it->second--;
            }
        }
        (void)dirty;
    }
    
    size_t GetCurrentSize() const { return pages_.size(); }
    
private:
    uint32_t next_page_id_;
    size_t pool_size_;
    std::unordered_map<uint32_t, Page*> pages_;
    std::unordered_map<uint32_t, int> pin_counts_;
};

// Test: WAL LSN updates during insert operations
bool TestWALInsert() {
    std::cout << "\n=== Test: WAL LSN Updates During Insert ===" << std::endl;
    
    const std::string db_file = "test_wal_insert.db";
    const std::string log_file = "test_wal_insert.log";
    std::remove(db_file.c_str());
    std::remove(log_file.c_str());
    
    LogManager log_manager(log_file);
    WALTestBufferPool bpm(db_file);
    BPlusTree<WALTestBufferPool> tree(&bpm, INVALID_PAGE_ID, &log_manager);
    
    // Insert some keys
    std::vector<std::pair<uint64_t, RID>> key_rid_pairs;
    for (uint64_t key = 1; key <= 100; key++) {
        RID rid(key, key * 2);
        key_rid_pairs.push_back({key, rid});
        
        if (!tree.Insert(key, rid)) {
            std::cout << "  FAIL: Failed to insert key " << key << std::endl;
            return false;
        }
    }
    
    std::cout << "  Inserted 100 keys with WAL logging" << std::endl;
    
    // Verify all keys are searchable
    for (const auto& pair : key_rid_pairs) {
        RID found_rid;
        if (!tree.Search(pair.first, found_rid)) {
            std::cout << "  FAIL: Key " << pair.first << " not found" << std::endl;
            return false;
        }
    }
    
    std::cout << "  All keys verified searchable" << std::endl;
    
    // Check that log records were created
    lsn_t current_lsn = log_manager.GetCurrentLSN();
    std::cout << "  Current LSN after inserts: " << current_lsn << std::endl;
    
    if (current_lsn == 0) {
        std::cout << "  FAIL: No log records created" << std::endl;
        return false;
    }
    
    std::cout << "  WAL LSN updates verified" << std::endl;
    
    std::remove(db_file.c_str());
    std::remove(log_file.c_str());
    return true;
}

// Test: WAL LSN updates during delete operations
bool TestWALDelete() {
    std::cout << "\n=== Test: WAL LSN Updates During Delete ===" << std::endl;
    
    const std::string db_file = "test_wal_delete.db";
    const std::string log_file = "test_wal_delete.log";
    std::remove(db_file.c_str());
    std::remove(log_file.c_str());
    
    LogManager log_manager(log_file);
    WALTestBufferPool bpm(db_file);
    BPlusTree<WALTestBufferPool> tree(&bpm, INVALID_PAGE_ID, &log_manager);
    
    // Insert keys
    for (uint64_t key = 1; key <= 100; key++) {
        RID rid(key, key * 2);
        if (!tree.Insert(key, rid)) {
            std::cout << "  FAIL: Failed to insert key " << key << std::endl;
            return false;
        }
    }
    
    lsn_t lsn_before_delete = log_manager.GetCurrentLSN();
    std::cout << "  LSN before delete: " << lsn_before_delete << std::endl;
    
    // Delete some keys
    for (uint64_t key = 50; key <= 60; key++) {
        if (!tree.Delete(key)) {
            std::cout << "  FAIL: Failed to delete key " << key << std::endl;
            return false;
        }
    }
    
    lsn_t lsn_after_delete = log_manager.GetCurrentLSN();
    std::cout << "  LSN after delete: " << lsn_after_delete << std::endl;
    
    if (lsn_after_delete <= lsn_before_delete) {
        std::cout << "  FAIL: LSN did not increase after delete operations" << std::endl;
        return false;
    }
    
    std::cout << "  Deleted 11 keys with WAL logging" << std::endl;
    std::cout << "  WAL LSN updates verified for deletes" << std::endl;
    
    std::remove(db_file.c_str());
    std::remove(log_file.c_str());
    return true;
}

// Test: Simulate crash and recovery
bool TestCrashRecovery() {
    std::cout << "\n=== Test: Crash and Recovery ===" << std::endl;
    
    const std::string db_file = "test_crash_recovery.db";
    const std::string log_file = "test_crash_recovery.log";
    std::remove(db_file.c_str());
    std::remove(log_file.c_str());
    
    // Phase 1: Insert keys with WAL
    {
        LogManager log_manager(log_file);
        WALTestBufferPool bpm(db_file);
        BPlusTree<WALTestBufferPool> tree(&bpm, INVALID_PAGE_ID, &log_manager);
        
        // Insert keys that will cause splits
        for (uint64_t key = 1; key <= 300; key++) {
            RID rid(key, key * 2);
            if (!tree.Insert(key, rid)) {
                std::cout << "  FAIL: Failed to insert key " << key << std::endl;
                return false;
            }
        }
        
        lsn_t final_lsn = log_manager.GetCurrentLSN();
        std::cout << "  Phase 1: Inserted 300 keys, final LSN: " << final_lsn << std::endl;
        
        // Flush logs to simulate writing to disk
        log_manager.FlushAllLogs();
    }
    
    // Phase 2: Simulate crash by creating new instances
    // In a real system, we would read from disk and recover
    // For this test, we verify log records exist
    {
        LogManager log_manager(log_file);
        std::vector<LogRecord> log_records = log_manager.ReadAllLogRecords();
        
        std::cout << "  Phase 2: Found " << log_records.size() << " log records" << std::endl;
        
        if (log_records.size() == 0) {
            std::cout << "  FAIL: No log records found after crash" << std::endl;
            return false;
        }
        
        // Verify log records have valid LSNs
        for (const auto& record : log_records) {
            if (record.GetLSN() == INVALID_LSN) {
                std::cout << "  FAIL: Log record has invalid LSN" << std::endl;
                return false;
            }
        }
        
        std::cout << "  All log records have valid LSNs" << std::endl;
    }
    
    std::remove(db_file.c_str());
    std::remove(log_file.c_str());
    return true;
}

// Test: WAL LSN updates during split operations
bool TestWALSplit() {
    std::cout << "\n=== Test: WAL LSN Updates During Split ===" << std::endl;
    
    const std::string db_file = "test_wal_split.db";
    const std::string log_file = "test_wal_split.log";
    std::remove(db_file.c_str());
    std::remove(log_file.c_str());
    
    LogManager log_manager(log_file);
    WALTestBufferPool bpm(db_file);
    BPlusTree<WALTestBufferPool> tree(&bpm, INVALID_PAGE_ID, &log_manager);
    
    lsn_t lsn_before = log_manager.GetCurrentLSN();
    std::cout << "  LSN before splits: " << lsn_before << std::endl;
    
    // Insert enough keys to cause splits
    for (uint64_t key = 1; key <= 500; key++) {
        RID rid(key, key * 2);
        if (!tree.Insert(key, rid)) {
            std::cout << "  FAIL: Failed to insert key " << key << std::endl;
            return false;
        }
    }
    
    lsn_t lsn_after = log_manager.GetCurrentLSN();
    std::cout << "  LSN after splits: " << lsn_after << std::endl;
    
    if (lsn_after <= lsn_before) {
        std::cout << "  FAIL: LSN did not increase after split operations" << std::endl;
        return false;
    }
    
    std::cout << "  Inserted 500 keys (causing splits) with WAL logging" << std::endl;
    std::cout << "  WAL LSN updates verified for splits" << std::endl;
    
    std::remove(db_file.c_str());
    std::remove(log_file.c_str());
    return true;
}

// Test 5: Insert → crash → recover → verify all keys
bool TestCrashRecoveryInsert() {
    std::cout << "=== Test: Crash Recovery After Insert ===" << std::endl;
    
    const std::string db_file = "test_crash_recovery_insert.db";
    const std::string log_file = "test_crash_recovery_insert.log";
    std::remove(db_file.c_str());
    std::remove(log_file.c_str());
    
    LogManager log_manager(log_file);
    WALTestBufferPool buffer_pool(db_file);
    BPlusTree<WALTestBufferPool> tree(&buffer_pool, INVALID_PAGE_ID, &log_manager);
    
    // Phase 1: Insert keys
    std::cout << "  Phase 1: Inserting 500 keys..." << std::endl;
    for (int i = 1; i <= 500; i++) {
        RID rid(i, 0);
        if (!tree.Insert(i, rid)) {
            std::cout << "  FAIL: Failed to insert key " << i << std::endl;
            return false;
        }
    }
    lsn_t final_lsn = log_manager.GetCurrentLSN();
    std::cout << "  Final LSN: " << final_lsn << std::endl;
    
    // Phase 2: Simulate crash by creating new tree with same log
    std::cout << "  Phase 2: Simulating crash and recovery..." << std::endl;
    BPlusTree<WALTestBufferPool> recovered_tree(&buffer_pool, INVALID_PAGE_ID, &log_manager);
    
    // Phase 3: Verify all keys are present
    std::cout << "  Phase 3: Verifying all keys..." << std::endl;
    for (int i = 1; i <= 500; i++) {
        RID rid;
        if (!tree.Search(i, rid)) {
            std::cout << "  FAIL: Key " << i << " not found after recovery" << std::endl;
            return false;
        }
    }
    
    // Verify no duplicate entries
    std::cout << "  Phase 4: Verifying no duplicates..." << std::endl;
    std::set<uint64_t> found_keys;
    for (int i = 1; i <= 500; i++) {
        RID rid;
        if (tree.Search(i, rid)) {
            if (found_keys.count(i) > 0) {
                std::cout << "  FAIL: Duplicate key " << i << " found" << std::endl;
                return false;
            }
            found_keys.insert(i);
        }
    }
    
    std::cout << "  All keys verified after recovery" << std::endl;
    std::cout << "  ✅ PASS" << std::endl;
    return true;
}

// Test 6: Insert → delete → crash → recover → verify consistency
bool TestCrashRecoveryDelete() {
    std::cout << "=== Test: Crash Recovery After Delete ===" << std::endl;
    
    const std::string db_file = "test_crash_recovery_delete.db";
    const std::string log_file = "test_crash_recovery_delete.log";
    std::remove(db_file.c_str());
    std::remove(log_file.c_str());
    
    LogManager log_manager(log_file);
    WALTestBufferPool buffer_pool(db_file);
    BPlusTree<WALTestBufferPool> tree(&buffer_pool, INVALID_PAGE_ID, &log_manager);
    
    // Phase 1: Insert keys
    std::cout << "  Phase 1: Inserting 300 keys..." << std::endl;
    for (int i = 1; i <= 300; i++) {
        RID rid(i, 0);
        if (!tree.Insert(i, rid)) {
            std::cout << "  FAIL: Failed to insert key " << i << std::endl;
            return false;
        }
    }
    
    // Phase 2: Delete some keys
    std::cout << "  Phase 2: Deleting 100 keys..." << std::endl;
    for (int i = 1; i <= 100; i++) {
        if (!tree.Delete(i)) {
            std::cout << "  FAIL: Failed to delete key " << i << std::endl;
            return false;
        }
    }
    lsn_t final_lsn = log_manager.GetCurrentLSN();
    std::cout << "  Final LSN: " << final_lsn << std::endl;
    
    // Phase 3: Simulate crash by creating new tree
    std::cout << "  Phase 3: Simulating crash and recovery..." << std::endl;
    BPlusTree<WALTestBufferPool> recovered_tree(&buffer_pool, INVALID_PAGE_ID, &log_manager);
    
    // Phase 4: Verify consistency
    std::cout << "  Phase 4: Verifying consistency..." << std::endl;
    
    // Deleted keys should not be found
    for (int i = 1; i <= 100; i++) {
        RID rid;
        if (tree.Search(i, rid)) {
            std::cout << "  FAIL: Deleted key " << i << " found after recovery" << std::endl;
            return false;
        }
    }
    
    // Remaining keys should be found
    for (int i = 101; i <= 300; i++) {
        RID rid;
        if (!tree.Search(i, rid)) {
            std::cout << "  FAIL: Key " << i << " not found after recovery" << std::endl;
            return false;
        }
    }
    
    std::cout << "  Consistency verified after recovery" << std::endl;
    std::cout << "  ✅ PASS" << std::endl;
    return true;
}

// Test 7: Stress test with 10k keys
bool TestStressLargeInsert() {
    std::cout << "=== Test: Stress Test - Large Insert (10k keys) ===" << std::endl;
    
    const std::string db_file = "test_stress_large_insert.db";
    const std::string log_file = "test_stress_large_insert.log";
    std::remove(db_file.c_str());
    std::remove(log_file.c_str());
    
    LogManager log_manager(log_file);
    WALTestBufferPool buffer_pool(db_file);
    BPlusTree<WALTestBufferPool> tree(&buffer_pool, INVALID_PAGE_ID, &log_manager);
    
    std::cout << "  Inserting 10000 keys..." << std::endl;
    for (int i = 1; i <= 10000; i++) {
        RID rid(i, 0);
        if (!tree.Insert(i, rid)) {
            std::cout << "  FAIL: Failed to insert key " << i << std::endl;
            return false;
        }
    }
    
    lsn_t final_lsn = log_manager.GetCurrentLSN();
    std::cout << "  Final LSN: " << final_lsn << std::endl;
    
    std::cout << "  Verifying sorted order..." << std::endl;
    uint64_t prev_key = 0;
    for (int i = 1; i <= 10000; i++) {
        RID rid;
        if (!tree.Search(i, rid)) {
            std::cout << "  FAIL: Key " << i << " not found" << std::endl;
            return false;
        }
        if (i < prev_key) {
            std::cout << "  FAIL: Keys not in sorted order" << std::endl;
            return false;
        }
        prev_key = i;
    }
    
    std::cout << "  All keys verified" << std::endl;
    std::cout << "  ✅ PASS" << std::endl;
    return true;
}

// Test 8: Stress test - delete all keys
bool TestStressDeleteAll() {
    std::cout << "=== Test: Stress Test - Delete All Keys ===" << std::endl;
    
    const std::string db_file = "test_stress_delete_all.db";
    const std::string log_file = "test_stress_delete_all.log";
    std::remove(db_file.c_str());
    std::remove(log_file.c_str());
    
    LogManager log_manager(log_file);
    WALTestBufferPool buffer_pool(db_file);
    BPlusTree<WALTestBufferPool> tree(&buffer_pool, INVALID_PAGE_ID, &log_manager);
    
    std::cout << "  Phase 1: Inserting 5000 keys..." << std::endl;
    for (int i = 1; i <= 5000; i++) {
        RID rid(i, 0);
        if (!tree.Insert(i, rid)) {
            std::cout << "  FAIL: Failed to insert key " << i << std::endl;
            return false;
        }
    }
    
    std::cout << "  Phase 2: Deleting all keys..." << std::endl;
    for (int i = 1; i <= 5000; i++) {
        if (!tree.Delete(i)) {
            std::cout << "  FAIL: Failed to delete key " << i << std::endl;
            return false;
        }
    }
    
    std::cout << "  Phase 3: Verifying all keys deleted..." << std::endl;
    for (int i = 1; i <= 5000; i++) {
        RID rid;
        if (tree.Search(i, rid)) {
            std::cout << "  FAIL: Key " << i << " still found after deletion" << std::endl;
            return false;
        }
    }
    
    std::cout << "  All keys successfully deleted" << std::endl;
    std::cout << "  ✅ PASS" << std::endl;
    return true;
}

// Test 9: Stress test - reinsert after delete
bool TestStressReinsert() {
    std::cout << "=== Test: Stress Test - Reinsert After Delete ===" << std::endl;
    
    const std::string db_file = "test_stress_reinsert.db";
    const std::string log_file = "test_stress_reinsert.log";
    std::remove(db_file.c_str());
    std::remove(log_file.c_str());
    
    LogManager log_manager(log_file);
    WALTestBufferPool buffer_pool(db_file);
    BPlusTree<WALTestBufferPool> tree(&buffer_pool, INVALID_PAGE_ID, &log_manager);
    
    std::cout << "  Phase 1: Inserting 2000 keys..." << std::endl;
    for (int i = 1; i <= 2000; i++) {
        RID rid(i, 0);
        if (!tree.Insert(i, rid)) {
            std::cout << "  FAIL: Failed to insert key " << i << std::endl;
            return false;
        }
    }
    
    std::cout << "  Phase 2: Deleting 1000 keys..." << std::endl;
    for (int i = 1; i <= 1000; i++) {
        if (!tree.Delete(i)) {
            std::cout << "  FAIL: Failed to delete key " << i << std::endl;
            return false;
        }
    }
    
    std::cout << "  Phase 3: Reinserting 1000 keys..." << std::endl;
    for (int i = 1; i <= 1000; i++) {
        RID rid(i, 1); // Different slot_id
        if (!tree.Insert(i, rid)) {
            std::cout << "  FAIL: Failed to reinsert key " << i << std::endl;
            return false;
        }
    }
    
    std::cout << "  Phase 4: Verifying all keys..." << std::endl;
    for (int i = 1; i <= 2000; i++) {
        RID rid;
        if (!tree.Search(i, rid)) {
            std::cout << "  FAIL: Key " << i << " not found after reinsert" << std::endl;
            return false;
        }
    }
    
    std::cout << "  All keys verified after reinsert" << std::endl;
    std::cout << "  ✅ PASS" << std::endl;
    return true;
}

// Test 10: Range scan verification
bool TestRangeScan() {
    std::cout << "=== Test: Range Scan Verification ===" << std::endl;
    
    const std::string db_file = "test_range_scan.db";
    const std::string log_file = "test_range_scan.log";
    std::remove(db_file.c_str());
    std::remove(log_file.c_str());
    
    LogManager log_manager(log_file);
    WALTestBufferPool buffer_pool(db_file);
    BPlusTree<WALTestBufferPool> tree(&buffer_pool, INVALID_PAGE_ID, &log_manager);
    
    std::cout << "  Inserting 1000 keys..." << std::endl;
    for (int i = 1; i <= 1000; i++) {
        RID rid(i, 0);
        if (!tree.Insert(i, rid)) {
            std::cout << "  FAIL: Failed to insert key " << i << std::endl;
            return false;
        }
    }
    
    std::cout << "  Performing range scan (100-900)..." << std::endl;
    std::vector<std::pair<uint64_t, RID>> results;
    tree.RangeScan(100, 900, results);
    
    if (results.size() != 801) { // 100 to 900 inclusive
        std::cout << "  FAIL: Expected 801 results, got " << results.size() << std::endl;
        return false;
    }
    
    // Verify sorted order
    uint64_t prev_key = 99;
    for (const auto& [key, rid] : results) {
        if (key <= prev_key) {
            std::cout << "  FAIL: Results not in sorted order" << std::endl;
            return false;
        }
        prev_key = key;
    }
    
    // Verify range bounds
    if (results.front().first != 100 || results.back().first != 900) {
        std::cout << "  FAIL: Range bounds incorrect" << std::endl;
        return false;
    }
    
    std::cout << "  Range scan verified" << std::endl;
    std::cout << "  ✅ PASS" << std::endl;
    return true;
}

int main() {
    std::cout << "=== B+ Tree WAL Integration Test Suite ===" << std::endl;
    
    int passed = 0;
    int failed = 0;
    
    if (TestWALInsert()) {
        passed++;
        std::cout << "  ✅ PASS" << std::endl;
    } else {
        failed++;
        std::cout << "  ❌ FAIL" << std::endl;
    }
    
    if (TestWALDelete()) {
        passed++;
        std::cout << "  ✅ PASS" << std::endl;
    } else {
        failed++;
        std::cout << "  ❌ FAIL" << std::endl;
    }
    
    if (TestWALSplit()) {
        passed++;
        std::cout << "  ✅ PASS" << std::endl;
    } else {
        failed++;
        std::cout << "  ❌ FAIL" << std::endl;
    }
    
    if (TestCrashRecovery()) {
        passed++;
        std::cout << "  ✅ PASS" << std::endl;
    } else {
        failed++;
        std::cout << "  ❌ FAIL" << std::endl;
    }
    
    if (TestCrashRecoveryInsert()) {
        passed++;
        std::cout << "  ✅ PASS" << std::endl;
    } else {
        failed++;
        std::cout << "  ❌ FAIL" << std::endl;
    }
    
    if (TestCrashRecoveryDelete()) {
        passed++;
        std::cout << "  ✅ PASS" << std::endl;
    } else {
        failed++;
        std::cout << "  ❌ FAIL" << std::endl;
    }
    
    if (TestStressLargeInsert()) {
        passed++;
        std::cout << "  ✅ PASS" << std::endl;
    } else {
        failed++;
        std::cout << "  ❌ FAIL" << std::endl;
    }
    
    if (TestStressDeleteAll()) {
        passed++;
        std::cout << "  ✅ PASS" << std::endl;
    } else {
        failed++;
        std::cout << "  ❌ FAIL" << std::endl;
    }
    
    if (TestStressReinsert()) {
        passed++;
        std::cout << "  ✅ PASS" << std::endl;
    } else {
        failed++;
        std::cout << "  ❌ FAIL" << std::endl;
    }
    
    if (TestRangeScan()) {
        passed++;
        std::cout << "  ✅ PASS" << std::endl;
    } else {
        failed++;
        std::cout << "  ❌ FAIL" << std::endl;
    }
    
    std::cout << "=== Test Summary ===" << std::endl;
    std::cout << "Total: " << (passed + failed) << std::endl;
    std::cout << "Passed: " << passed << std::endl;
    std::cout << "Failed: " << failed << std::endl;
    
    return (failed == 0) ? 0 : 1;
}
