#include "../src/storage/page_v2.h"
#include "../src/storage/disk_manager_v2.h"
#include <iostream>
#include <cassert>
#include <string>

using namespace minidb;

void TestPageBasicOperations() {
    std::cout << "=== Testing Page Basic Operations ===" << std::endl;
    
    Page page;
    
    // Test initial state
    assert(page.GetPageId() == INVALID_PAGE_ID);
    assert(page.GetFreeSpace() == Page::SIZE - Page::Header::HEADER_SIZE);
    assert(page.GetSlotCount() == 0);
    
    // Test page ID
    page.SetPageId(42);
    assert(page.GetPageId() == 42);
    
    std::cout << "Page basic operations: PASSED" << std::endl;
}

void TestPageRecordOperations() {
    std::cout << "=== Testing Page Record Operations ===" << std::endl;
    
    Page page;
    page.SetPageId(1);
    
    // Test record insertion
    const char* record1 = "Hello, World!";
    uint32_t record1_size = std::strlen(record1) + 1;
    uint32_t slot_id1;
    
    bool insert_result = page.InsertRecord(record1, record1_size, slot_id1);
    assert(insert_result);
    assert(slot_id1 == 0);
    assert(page.GetSlotCount() == 1);
    assert(page.IsSlotUsed(slot_id1));
    
    // Test record retrieval
    char buffer[256];
    uint32_t actual_size;
    bool get_result = page.GetRecord(slot_id1, buffer, sizeof(buffer), actual_size);
    assert(get_result);
    assert(actual_size == record1_size);
    assert(std::strcmp(buffer, record1) == 0);
    
    // Test second record
    const char* record2 = "Second record";
    uint32_t record2_size = std::strlen(record2) + 1;
    uint32_t slot_id2;
    
    insert_result = page.InsertRecord(record2, record2_size, slot_id2);
    assert(insert_result);
    assert(slot_id2 == 1);
    assert(page.GetSlotCount() == 2);
    
    // Test record deletion
    bool delete_result = page.DeleteRecord(slot_id1);
    assert(delete_result);
    assert(!page.IsSlotUsed(slot_id1));
    assert(page.GetSlotCount() == 2);  // Slot count doesn't decrease
    
    // Test slot reuse
    const char* record3 = "Third record";
    uint32_t record3_size = std::strlen(record3) + 1;
    uint32_t slot_id3;
    
    insert_result = page.InsertRecord(record3, record3_size, slot_id3);
    assert(insert_result);
    assert(slot_id3 == 0);  // Should reuse deleted slot
    
    std::cout << "Page record operations: PASSED" << std::endl;
}

void TestPageSpaceManagement() {
    std::cout << "=== Testing Page Space Management ===" << std::endl;
    
    Page page;
    page.SetPageId(1);
    
    uint32_t initial_free_space = page.GetFreeSpace();
    
    // Insert a record and check space reduction
    const char* record = "Test record for space management";
    uint32_t record_size = std::strlen(record) + 1;
    uint32_t slot_id;
    
    bool insert_result = page.InsertRecord(record, record_size, slot_id);
    assert(insert_result);
    
    uint32_t new_free_space = page.GetFreeSpace();
    assert(new_free_space < initial_free_space);
    
    // Test space checking
    assert(page.HasSpaceFor(100));
    
    std::cout << "Page space management: PASSED" << std::endl;
}

void TestDiskManagerBasic() {
    std::cout << "=== Testing DiskManager Basic Operations ===" << std::endl;
    
    std::string test_file = "test_disk.db";
    
    // Remove existing test file
    std::remove(test_file.c_str());
    
    {
        DiskManager disk_manager(test_file);
        assert(disk_manager.IsOpen());
        assert(disk_manager.GetFilename() == test_file);
        assert(disk_manager.GetPageCount() == 0);
        
        // Test page allocation
        uint32_t page_id1 = disk_manager.AllocatePage();
        assert(page_id1 == 1);
        assert(disk_manager.GetPageCount() == 1);
        
        uint32_t page_id2 = disk_manager.AllocatePage();
        assert(page_id2 == 2);
        assert(disk_manager.GetPageCount() == 2);
        
        // Test page write
        Page page1;
        page1.SetPageId(page_id1);
        const char* data1 = "Page 1 data";
        uint32_t slot_id;
        page1.InsertRecord(data1, std::strlen(data1) + 1, slot_id);
        
        bool write_result = disk_manager.WritePage(page_id1, page1);
        assert(write_result);
        
        // Test page read
        Page page2;
        bool read_result = disk_manager.ReadPage(page_id1, page2);
        assert(read_result);
        assert(page2.GetPageId() == page_id1);
        
        // Verify data
        char buffer[256];
        uint32_t actual_size;
        bool get_result = page2.GetRecord(slot_id, buffer, sizeof(buffer), actual_size);
        assert(get_result);
        assert(std::strcmp(buffer, data1) == 0);
    }
    
    // Test reopening file
    {
        DiskManager disk_manager(test_file);
        assert(disk_manager.IsOpen());
        assert(disk_manager.GetPageCount() == 2);
        
        Page page;
        bool read_result = disk_manager.ReadPage(1, page);
        assert(read_result);
        assert(page.GetPageId() == 1);
    }
    
    // Cleanup
    std::remove(test_file.c_str());
    
    std::cout << "DiskManager basic operations: PASSED" << std::endl;
}

int main() {
    std::cout << "=== Storage Layer Tests ===" << std::endl;
    
    try {
        TestPageBasicOperations();
        TestPageRecordOperations();
        TestPageSpaceManagement();
        TestDiskManagerBasic();
        
        std::cout << "\n=== ALL TESTS PASSED ===" << std::endl;
        std::cout << "Page and DiskManager implementation is working correctly!" << std::endl;
        
    } catch (const std::exception& e) {
        std::cerr << "Test failed with exception: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}
