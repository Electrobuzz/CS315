#include <iostream>
#include <cstdint>
#include <cstring>

#include "src/storage/page_v2.h"

using namespace minidb;

int main() {
    std::cout << "=== Testing LSN Byte Layout ===" << std::endl;
    
    // Create a page
    Page page;
    page.SetPageId(1);
    
    // Set LSN using the setter
    uint64_t test_lsn = 12345;
    page.SetLastLSN(test_lsn);
    
    std::cout << "Step 1: Set page LSN to: " << test_lsn << std::endl;
    std::cout << "Page LSN using getter: " << page.GetLastLSN() << std::endl;
    
    // Verify the LSN is stored at the correct offset in raw memory
    const char* raw_data = page.GetData();
    
    // Read LSN directly from raw memory at offset 16
    uint64_t raw_lsn;
    std::memcpy(&raw_lsn, raw_data + 16, sizeof(uint64_t));
    
    std::cout << "Step 2: LSN read directly from raw memory at offset 16: " << raw_lsn << std::endl;
    
    // Verify page_id is at offset 0
    uint32_t raw_page_id;
    std::memcpy(&raw_page_id, raw_data + 0, sizeof(uint32_t));
    std::cout << "Step 3: Page ID from raw memory at offset 0: " << raw_page_id << std::endl;
    
    if (raw_lsn == test_lsn && raw_page_id == 1) {
        std::cout << "✅ LSN BYTE LAYOUT TEST PASSED" << std::endl;
        std::cout << "LSN is correctly stored at offset 16 in raw page memory" << std::endl;
        std::cout << "Page ID is correctly stored at offset 0 in raw page memory" << std::endl;
        return 0;
    } else {
        std::cout << "❌ LSN BYTE LAYOUT TEST FAILED" << std::endl;
        std::cout << "Expected LSN at offset 16: " << test_lsn << ", Got: " << raw_lsn << std::endl;
        std::cout << "Expected Page ID at offset 0: 1, Got: " << raw_page_id << std::endl;
        return 1;
    }
}
