#include <iostream>
#include <cstdint>
#include <cstring>

#include "src/storage/page_v2.h"

using namespace minidb;

int main() {
    std::cout << "=== Testing LSN Memory Storage ===" << std::endl;
    
    // Create a page
    Page page;
    page.SetPageId(1);
    
    // Set LSN
    uint64_t test_lsn = 12345;
    page.SetLastLSN(test_lsn);
    
    std::cout << "Set page LSN to: " << test_lsn << std::endl;
    std::cout << "Page LSN after set: " << page.GetLastLSN() << std::endl;
    
    // Verify LSN
    uint64_t read_lsn = page.GetLastLSN();
    
    if (read_lsn == test_lsn) {
        std::cout << "✅ LSN MEMORY TEST PASSED" << std::endl;
        std::cout << "LSN was correctly stored in page memory" << std::endl;
        return 0;
    } else {
        std::cout << "❌ LSN MEMORY TEST FAILED" << std::endl;
        std::cout << "Expected LSN: " << test_lsn << ", Got LSN: " << read_lsn << std::endl;
        return 1;
    }
}
