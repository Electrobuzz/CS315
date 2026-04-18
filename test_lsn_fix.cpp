#include <iostream>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <string>

#include "src/storage/page_v2.h"
#include "src/storage/disk_manager_v2.h"

using namespace minidb;

int main() {
    std::cout << "=== Testing LSN Persistence Fix ===" << std::endl;
    
    // Clean up old test files
    std::remove("test_lsn_fix.db");
    
    try {
        // Create DiskManager
        DiskManager disk_manager("test_lsn_fix.db");
        
        // Allocate page first
        uint32_t page_id = disk_manager.AllocatePage();
        std::cout << "Allocated page ID: " << page_id << std::endl;
        
        // Create a page
        Page page;
        page.SetPageId(page_id);
        
        // Set LSN
        uint64_t test_lsn = 12345;
        page.SetLastLSN(test_lsn);
        
        std::cout << "Step 1: Set page LSN to: " << test_lsn << std::endl;
        std::cout << "Page LSN in memory: " << page.GetLastLSN() << std::endl;
        
        // Write page to disk
        bool write_success = disk_manager.WritePage(page_id, page);
        if (!write_success) {
            std::cerr << "ERROR: Failed to write page to disk" << std::endl;
            return 1;
        }
        std::cout << "Step 2: Successfully wrote page to disk" << std::endl;
        
        // Create a new page to read back
        Page page_read;
        bool read_success = disk_manager.ReadPage(page_id, page_read);
        if (!read_success) {
            std::cerr << "ERROR: Failed to read page from disk" << std::endl;
            return 1;
        }
        std::cout << "Step 3: Successfully read page from disk" << std::endl;
        
        // Verify LSN
        uint64_t read_lsn = page_read.GetLastLSN();
        std::cout << "Step 4: Page LSN after reload: " << read_lsn << std::endl;
        
        if (read_lsn == test_lsn) {
            std::cout << "✅ LSN PERSISTENCE TEST PASSED" << std::endl;
            std::cout << "LSN was correctly persisted to disk and restored using offset-based methods" << std::endl;
            
            // Clean up
            std::remove("test_lsn_fix.db");
            return 0;
        } else {
            std::cout << "❌ LSN PERSISTENCE TEST FAILED" << std::endl;
            std::cout << "Expected LSN: " << test_lsn << ", Got LSN: " << read_lsn << std::endl;
            std::cout << "This indicates the offset-based fix may not be working correctly" << std::endl;
            
            // Clean up
            std::remove("test_lsn_fix.db");
            return 1;
        }
        
    } catch (const std::exception& e) {
        std::cerr << "EXCEPTION: " << e.what() << std::endl;
        std::remove("test_lsn_fix.db");
        return 1;
    }
}
