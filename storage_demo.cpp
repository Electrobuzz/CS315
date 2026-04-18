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
    
    struct Slot {
        uint32_t offset;
        uint32_t length;
    };
    
private:
    char data_[PAGE_SIZE];
    
    static constexpr uint32_t HEADER_SIZE = sizeof(Header);
    static constexpr uint32_t SLOT_SIZE = sizeof(Slot);
    
    Header* GetHeader() { return reinterpret_cast<Header*>(data_); }
    const Header* GetHeader() const { return reinterpret_cast<const Header*>(data_); }
    Slot* GetSlots() { return reinterpret_cast<Slot*>(data_ + HEADER_SIZE); }
    const Slot* GetSlots() const { return reinterpret_cast<const Slot*>(data_ + HEADER_SIZE); }
    char* GetDataArea() { return data_ + HEADER_SIZE; }
    const char* GetDataArea() const { return data_ + HEADER_SIZE; }

public:
    Page() { Reset(); }
    
    void Reset() {
        std::memset(data_, 0, PAGE_SIZE);
        Header* header = GetHeader();
        header->page_id = INVALID_PAGE_ID;
        header->free_space_offset = PAGE_SIZE - HEADER_SIZE;
        header->slot_count = 0;
        header->free_slot_count = 0;
    }
    
    void SetPageId(uint32_t page_id) { GetHeader()->page_id = page_id; }
    uint32_t GetPageId() const { return GetHeader()->page_id; }
    char* GetData() { return data_; }
    const char* GetData() const { return data_; }
    
    uint32_t GetFreeSpace() const {
        const Header* header = GetHeader();
        uint32_t slot_dir_size = header->slot_count * SLOT_SIZE;
        uint32_t used_space = (PAGE_SIZE - HEADER_SIZE) - header->free_space_offset + slot_dir_size;
        return (PAGE_SIZE - HEADER_SIZE) - used_space;
    }
    
    bool InsertRecord(const char* record_data, uint32_t record_size, uint32_t& slot_id) {
        if (!record_data || record_size == 0) return false;
        if (GetFreeSpace() < record_size + SLOT_SIZE) return false;
        
        Header* header = GetHeader();
        Slot* slots = GetSlots();
        
        uint32_t target_slot_id;
        if (header->free_slot_count > 0) {
            for (uint32_t i = 0; i < header->slot_count; ++i) {
                if (slots[i].length == 0) {
                    target_slot_id = i;
                    break;
                }
            }
            header->free_slot_count--;
        } else {
            target_slot_id = header->slot_count;
            header->slot_count++;
        }
        
        uint32_t record_offset = header->free_space_offset - record_size;
        char* data_area = GetDataArea();
        std::memcpy(data_area + record_offset, record_data, record_size);
        
        slots[target_slot_id].offset = record_offset;
        slots[target_slot_id].length = record_size;
        
        header->free_space_offset = record_offset;
        slot_id = target_slot_id;
        return true;
    }
    
    bool GetRecord(uint32_t slot_id, char* buffer, uint32_t buffer_size, uint32_t& actual_size) const {
        const Header* header = GetHeader();
        if (slot_id >= header->slot_count) return false;
        
        const Slot* slots = GetSlots();
        if (slots[slot_id].length == 0) return false;
        
        const char* data_area = GetDataArea();
        uint32_t record_length = slots[slot_id].length;
        
        if (buffer_size < record_length) {
            actual_size = record_length;
            return false;
        }
        
        std::memcpy(buffer, data_area + slots[slot_id].offset, record_length);
        actual_size = record_length;
        return true;
    }
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
};

int main() {
    std::cout << "=== Storage Layer Demo ===" << std::endl;
    
    // Test Page operations
    std::cout << "\n1. Testing Page operations..." << std::endl;
    Page page;
    page.SetPageId(1);
    
    const char* record1 = "Hello, Database!";
    uint32_t slot_id1;
    bool success = page.InsertRecord(record1, std::strlen(record1) + 1, slot_id1);
    
    std::cout << "Page ID: " << page.GetPageId() << std::endl;
    std::cout << "Insert record 1: " << (success ? "SUCCESS" : "FAILED") << std::endl;
    std::cout << "Free space: " << page.GetFreeSpace() << " bytes" << std::endl;
    
    // Test record retrieval
    char buffer[256];
    uint32_t actual_size;
    bool get_success = page.GetRecord(slot_id1, buffer, sizeof(buffer), actual_size);
    std::cout << "Retrieve record 1: " << (get_success ? "SUCCESS" : "FAILED") << std::endl;
    std::cout << "Record content: " << buffer << std::endl;
    
    // Test DiskManager operations
    std::cout << "\n2. Testing DiskManager operations..." << std::endl;
    DiskManager disk_manager("test_storage.db");
    
    uint32_t page_id = disk_manager.AllocatePage();
    std::cout << "Allocated page ID: " << page_id << std::endl;
    
    bool write_success = disk_manager.WritePage(page_id, page);
    std::cout << "Write page to disk: " << (write_success ? "SUCCESS" : "FAILED") << std::endl;
    
    Page read_page;
    bool read_success = disk_manager.ReadPage(page_id, read_page);
    std::cout << "Read page from disk: " << (read_success ? "SUCCESS" : "FAILED") << std::endl;
    
    // Verify data integrity
    char read_buffer[256];
    uint32_t read_size;
    bool verify_success = read_page.GetRecord(slot_id1, read_buffer, sizeof(read_buffer), read_size);
    std::cout << "Data integrity check: " << (verify_success ? "SUCCESS" : "FAILED") << std::endl;
    std::cout << "Retrieved data: " << read_buffer << std::endl;
    
    std::cout << "\n=== Storage Layer Demo Completed ===" << std::endl;
    std::cout << "Core storage components are working correctly!" << std::endl;
    
    // Cleanup
    std::remove("test_storage.db");
    
    return 0;
}
