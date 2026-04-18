#include <iostream>
#include <fstream>
#include <cstdint>
#include <cstring>

#include "src/storage/page_v2.h"
#include "src/storage/disk_manager_v2.h"

using namespace minidb;

int main() {
    std::cout << "=== Testing LSN Persistence ===" << std::endl;
    
    // Clean up old test files
    std::remove("test_lsn_persistence.db");
    
    // Create DiskManager
    DiskManager disk_manager("test_lsn_persistence.db");
    
    // Create a page
    Page page;
    page.SetPageId(1);
    
    // Set LSN
    uint64_t test_lsn = 12345;
    page.SetLastLSN(test_lsn);
    
    std::cout << "Set page LSN to: " << test_lsn << std::endl;
    std::cout << "Page LSN before flush: " << page.GetLastLSN() << std::endl;
    
    // Write page to disk
    bool write_success = disk_manager.WritePage(1, page);
    std::cout << "Write page to disk: " << (write_success ? "SUCCESS" : "FAILED") << std::endl;
    
    // Create a new page to read back
    Page page_read;
    bool read_success = disk_manager.ReadPage(1, page_read);
    std::cout << "Read page from disk: " << (read_success ? "SUCCESS" : "FAILED") << std::endl;
    
    // Verify LSN
    uint64_t read_lsn = page_read.GetLastLSN();
    std::cout << "Page LSN after reload: " << read_lsn << std::endl;
    
    if (read_lsn == test_lsn) {
        std::cout << "✅ LSN PERSISTENCE TEST PASSED" << std::endl;
        std::cout << "LSN was correctly persisted to disk and restored" << std::endl;
    } else {
        std::cout << "❌ LSN PERSISTENCE TEST FAILED" << std::endl;
        std::cout << "Expected LSN: " << test_lsn << ", Got LSN: " << read_lsn << std::endl;
    }
    
    // Clean up
    std::remove("test_lsn_persistence.db");
    
    return (read_lsn == test_lsn) ? 0 : 1;
}
