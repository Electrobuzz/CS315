#include <iostream>
#include <fstream>
#include <cstdint>
#include <cstring>
#include <string>

#include "src/storage/page_v2.h"

using namespace minidb;

int main() {
    // Write results to file for verification
    std::ofstream result_file("disk_lsn_result.txt");
    
    result_file << "=== Testing LSN Disk Persistence (Minimal) ===" << std::endl;
    
    // Clean up old test file
    std::remove("test_disk_lsn.db");
    
    try {
        // Step 1: Create page and set LSN
        Page p;
        p.SetPageId(1);
        uint64_t test_lsn = 12345;
        p.SetLastLSN(test_lsn);
        
        result_file << "Step 1: Created page with LSN: " << test_lsn << std::endl;
        result_file << "Page LSN in memory: " << p.GetLastLSN() << std::endl;
        
        // Step 2: Write to disk using direct file I/O (bypass DiskManager)
        std::ofstream outfile("test_disk_lsn.db", std::ios::binary);
        if (!outfile) {
            result_file << "ERROR: Failed to open file for writing" << std::endl;
            result_file.close();
            return 1;
        }
        
        // Write raw page data to disk
        outfile.write(p.GetData(), Page::SIZE);
        if (outfile.fail()) {
            result_file << "ERROR: Failed to write page data" << std::endl;
            result_file.close();
            return 1;
        }
        outfile.close();
        result_file << "Step 2: Wrote page to disk (" << Page::SIZE << " bytes)" << std::endl;
        
        // Step 3: Read into NEW page object
        Page p2;
        std::ifstream infile("test_disk_lsn.db", std::ios::binary);
        if (!infile) {
            result_file << "ERROR: Failed to open file for reading" << std::endl;
            result_file.close();
            return 1;
        }
        
        // Read raw page data from disk
        infile.read(p2.GetData(), Page::SIZE);
        if (infile.fail() || infile.gcount() != Page::SIZE) {
            result_file << "ERROR: Failed to read page data" << std::endl;
            result_file.close();
            return 1;
        }
        infile.close();
        result_file << "Step 3: Read page from disk into new page object" << std::endl;
        
        // Step 4: Check LSN
        uint64_t read_lsn = p2.GetLastLSN();
        uint32_t read_page_id = p2.GetPageId();
        
        result_file << "Step 4: Verification" << std::endl;
        result_file << "  Page ID after reload: " << read_page_id << std::endl;
        result_file << "  LSN after reload: " << read_lsn << std::endl;
        
        if (read_lsn == test_lsn && read_page_id == 1) {
            result_file << "✅ LSN DISK PERSISTENCE TEST PASSED" << std::endl;
            result_file << "LSN was correctly persisted to disk and restored" << std::endl;
            result_file << "Offset-based layout is working correctly" << std::endl;
            result_file.close();
            
            // Clean up
            std::remove("test_disk_lsn.db");
            return 0;
        } else {
            result_file << "❌ LSN DISK PERSISTENCE TEST FAILED" << std::endl;
            result_file << "Expected LSN: " << test_lsn << ", Got: " << read_lsn << std::endl;
            result_file << "Expected Page ID: 1, Got: " << read_page_id << std::endl;
            result_file.close();
            
            // Clean up
            std::remove("test_disk_lsn.db");
            return 1;
        }
        
    } catch (const std::exception& e) {
        result_file << "EXCEPTION: " << e.what() << std::endl;
        result_file.close();
        std::remove("test_disk_lsn.db");
        return 1;
    }
}
