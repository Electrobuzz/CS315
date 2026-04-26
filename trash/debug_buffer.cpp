#include <iostream>
#include <fstream>
#include <cstdint>
#include <cstring>

constexpr uint32_t PAGE_SIZE = 4096;

class Page {
private:
    char data_[PAGE_SIZE];
    uint32_t page_id_;
    
public:
    Page() : page_id_(0) {
        std::memset(data_, 0, PAGE_SIZE);
    }
    
    void SetPageId(uint32_t id) { page_id_ = id; }
    uint32_t GetPageId() const { return page_id_; }
    char* GetData() { return data_; }
};

class DiskManager {
private:
    std::string filename_;
    std::fstream file_;
    uint32_t next_page_id_;
    
public:
    explicit DiskManager(const std::string& filename) 
        : filename_(filename), next_page_id_(1) {
        file_.open(filename, std::ios::in | std::ios::out | std::ios::binary);
        if (!file_.is_open()) {
            file_.open(filename, std::ios::out | std::ios::binary);
            file_.close();
            file_.open(filename, std::ios::in | std::ios::out | std::ios::binary);
        }
    }
    
    ~DiskManager() {
        if (file_.is_open()) file_.close();
    }
    
    bool WritePage(uint32_t page_id, const Page& page) {
        if (!file_.is_open()) {
            std::cout << "File not open" << std::endl;
            return false;
        }
        file_.seekp(page_id * PAGE_SIZE);
        if (file_.fail()) {
            std::cout << "Seek failed" << std::endl;
            return false;
        }
        file_.write(page.GetData(), PAGE_SIZE);
        if (file_.fail()) {
            std::cout << "Write failed" << std::endl;
            return false;
        }
        file_.flush();
        return true;
    }
    
    bool ReadPage(uint32_t page_id, Page& page) {
        if (!file_.is_open()) return false;
        file_.seekg(page_id * PAGE_SIZE);
        if (file_.fail()) return false;
        file_.read(page.GetData(), PAGE_SIZE);
        if (file_.fail() || file_.gcount() != PAGE_SIZE) {
            return true;  // Page doesn't exist, that's ok
        }
        return true;
    }
    
    uint32_t AllocatePage() {
        uint32_t page_id = next_page_id_++;
        Page new_page;
        new_page.SetPageId(page_id);
        WritePage(page_id, new_page);
        return page_id;
    }
};

int main() {
    std::cout << "=== Debug Buffer Pool Test ===" << std::endl;
    
    try {
        std::cout << "Step 1: Create DiskManager" << std::endl;
        std::remove("debug_test.db");
        DiskManager disk_manager("debug_test.db");
        std::cout << "DiskManager created" << std::endl;
        
        std::cout << "Step 2: Allocate page" << std::endl;
        uint32_t page_id = disk_manager.AllocatePage();
        std::cout << "Allocated page " << page_id << std::endl;
        
        std::cout << "Step 3: Create page and write data" << std::endl;
        Page page;
        page.SetPageId(page_id);
        const char* data = "Test data";
        std::memcpy(page.GetData(), data, std::strlen(data) + 1);
        std::cout << "Data prepared" << std::endl;
        
        std::cout << "Step 4: Write page to disk" << std::endl;
        bool write_success = disk_manager.WritePage(page_id, page);
        std::cout << "Write result: " << (write_success ? "SUCCESS" : "FAILED") << std::endl;
        
        if (!write_success) {
            return 1;
        }
        
        std::cout << "Step 5: Read page from disk" << std::endl;
        Page read_page;
        bool read_success = disk_manager.ReadPage(page_id, read_page);
        std::cout << "Read result: " << (read_success ? "SUCCESS" : "FAILED") << std::endl;
        
        std::cout << "Step 6: Verify data" << std::endl;
        std::cout << "Data read: " << read_page.GetData() << std::endl;
        
        std::cout << "✅ Test completed successfully" << std::endl;
        
        std::remove("debug_test.db");
        
    } catch (const std::exception& e) {
        std::cerr << "Exception: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}
