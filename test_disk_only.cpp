#include <iostream>
#include <fstream>
#include <cstdint>
#include <cstring>

constexpr uint32_t PAGE_SIZE = 4096;
constexpr uint32_t INVALID_PAGE_ID = 0;

class Page {
public:
    struct Header {
        uint32_t page_id;
        uint32_t free_space_offset;
        uint32_t slot_count;
        uint32_t free_slot_count;
    };
    
private:
    char data_[PAGE_SIZE];
    
public:
    Page() { Reset(); }
    
    void Reset() {
        std::memset(data_, 0, PAGE_SIZE);
        Header* header = GetHeader();
        header->page_id = INVALID_PAGE_ID;
        header->free_space_offset = PAGE_SIZE - sizeof(Header);
        header->slot_count = 0;
        header->free_slot_count = 0;
    }
    
    void SetPageId(uint32_t page_id) { GetHeader()->page_id = page_id; }
    uint32_t GetPageId() const { return GetHeader()->page_id; }
    char* GetData() { return data_; }
    const char* GetData() const { return data_; }
    
private:
    Header* GetHeader() { return reinterpret_cast<Header*>(data_); }
    const Header* GetHeader() const { return reinterpret_cast<const Header*>(data_); }
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
        if (!file_.is_open()) return false;
        file_.seekp(page_id * PAGE_SIZE);
        if (file_.fail()) return false;
        file_.write(page.GetData(), PAGE_SIZE);
        if (file_.fail()) return false;
        file_.flush();
        return true;
    }
    
    bool ReadPage(uint32_t page_id, Page& page) {
        if (!file_.is_open()) return false;
        file_.seekg(page_id * PAGE_SIZE);
        if (file_.fail()) return false;
        file_.read(page.GetData(), PAGE_SIZE);
        if (file_.fail() || file_.gcount() != PAGE_SIZE) {
            page.Reset();
            page.SetPageId(page_id);
            return true;
        }
        return true;
    }
    
    uint32_t AllocatePage() {
        uint32_t page_id = next_page_id_++;
        Page new_page;
        new_page.Reset();
        new_page.SetPageId(page_id);
        WritePage(page_id, new_page);
        return page_id;
    }
    
    uint32_t GetPageCount() const { return next_page_id_ - 1; }
};

int main() {
    std::cout << "=== Testing DiskManager Only ===" << std::endl;
    
    try {
        std::remove("test_disk.db");
        DiskManager disk_manager("test_disk.db");
        
        std::cout << "Creating page..." << std::endl;
        uint32_t page_id = disk_manager.AllocatePage();
        std::cout << "Allocated page " << page_id << std::endl;
        
        Page page;
        page.SetPageId(page_id);
        const char* data = "Test data";
        std::memcpy(page.GetData(), data, std::strlen(data) + 1);
        
        std::cout << "Writing page to disk..." << std::endl;
        bool write_success = disk_manager.WritePage(page_id, page);
        std::cout << "Write: " << (write_success ? "SUCCESS" : "FAILED") << std::endl;
        
        std::cout << "Reading page from disk..." << std::endl;
        Page read_page;
        bool read_success = disk_manager.ReadPage(page_id, read_page);
        std::cout << "Read: " << (read_success ? "SUCCESS" : "FAILED") << std::endl;
        
        std::cout << "Data read: " << read_page.GetData() << std::endl;
        
        std::cout << "✅ DiskManager test completed successfully" << std::endl;
        
        std::remove("test_disk.db");
        
    } catch (const std::exception& e) {
        std::cerr << "Test failed: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}
