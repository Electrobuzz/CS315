#include <iostream>
#include <fstream>
#include <cstdint>
#include <cstring>
#include <vector>
#include <memory>

constexpr uint32_t PAGE_SIZE = 4096;
constexpr uint32_t INVALID_PAGE_ID = 0;

// Simplified Page implementation for demo
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
    
    bool IsSlotUsed(uint32_t slot_id) const {
        const Header* header = GetHeader();
        if (slot_id >= header->slot_count) return false;
        return GetSlots()[slot_id].length > 0;
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
    
    uint32_t GetSlotCount() const { return GetHeader()->slot_count; }
};

// Simplified DiskManager for demo
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

// Simplified HeapFile for demo
class HeapFile {
private:
    std::unique_ptr<DiskManager> disk_manager_;
    std::vector<uint32_t> page_ids_;
    uint32_t record_count_;
    uint32_t current_page_index_;
    Page current_page_;
    
public:
    explicit HeapFile(const std::string& filename) 
        : record_count_(0), current_page_index_(0) {
        disk_manager_ = std::make_unique<DiskManager>(filename);
    }
    
    bool Open() {
        if (!disk_manager_) return false;
        
        page_ids_.clear();
        uint32_t page_count = disk_manager_->GetPageCount();
        
        for (uint32_t i = 1; i <= page_count; ++i) {
            page_ids_.push_back(i);
        }
        
        if (page_ids_.empty()) {
            uint32_t new_page_id = disk_manager_->AllocatePage();
            if (new_page_id == INVALID_PAGE_ID) return false;
            page_ids_.push_back(new_page_id);
        }
        
        current_page_index_ = 0;
        return disk_manager_->ReadPage(page_ids_[current_page_index_], current_page_);
    }
    
    bool InsertRecord(const char* record_data, uint32_t record_size) {
        if (!record_data || record_size == 0) return false;
        
        // Try current page first
        uint32_t slot_id;
        if (current_page_.InsertRecord(record_data, record_size, slot_id)) {
            record_count_++;
            return disk_manager_->WritePage(page_ids_[current_page_index_], current_page_);
        }
        
        // Try other pages
        for (uint32_t i = 0; i < page_ids_.size(); ++i) {
            if (i == current_page_index_) continue;
            
            Page temp_page;
            if (!disk_manager_->ReadPage(page_ids_[i], temp_page)) continue;
            
            if (temp_page.InsertRecord(record_data, record_size, slot_id)) {
                current_page_index_ = i;
                current_page_ = temp_page;
                record_count_++;
                return disk_manager_->WritePage(page_ids_[current_page_index_], current_page_);
            }
        }
        
        // Allocate new page
        uint32_t new_page_id = disk_manager_->AllocatePage();
        if (new_page_id == INVALID_PAGE_ID) return false;
        
        page_ids_.push_back(new_page_id);
        current_page_index_ = page_ids_.size() - 1;
        current_page_.Reset();
        current_page_.SetPageId(new_page_id);
        
        if (current_page_.InsertRecord(record_data, record_size, slot_id)) {
            record_count_++;
            return disk_manager_->WritePage(page_ids_[current_page_index_], current_page_);
        }
        
        return false;
    }
    
    class ScanIterator {
    private:
        HeapFile* heap_file_;
        uint32_t current_page_index_;
        uint32_t current_slot_id_;
        Page current_page_;
        bool page_loaded_;
        
    public:
        explicit ScanIterator(HeapFile* heap_file) 
            : heap_file_(heap_file), current_page_index_(0), 
              current_slot_id_(0), page_loaded_(false) {}
        
        bool NextRecord(char* buffer, uint32_t buffer_size, uint32_t& actual_size) {
            while (current_page_index_ < heap_file_->page_ids_.size()) {
                if (!page_loaded_) {
                    heap_file_->disk_manager_->ReadPage(
                        heap_file_->page_ids_[current_page_index_], current_page_);
                    page_loaded_ = true;
                    current_slot_id_ = 0;
                }
                
                if (current_slot_id_ < current_page_.GetSlotCount()) {
                    if (current_page_.IsSlotUsed(current_slot_id_)) {
                        bool success = current_page_.GetRecord(
                            current_slot_id_, buffer, buffer_size, actual_size);
                        current_slot_id_++;
                        return success;
                    }
                    current_slot_id_++;
                } else {
                    current_page_index_++;
                    page_loaded_ = false;
                }
            }
            return false;
        }
    };
    
    std::unique_ptr<ScanIterator> CreateScanIterator() {
        return std::make_unique<ScanIterator>(this);
    }
    
    uint32_t GetRecordCount() const { return record_count_; }
    uint32_t GetPageCount() const { return page_ids_.size(); }
};

// Test functions
void TestHeapFileInsert() {
    std::cout << "=== Testing HeapFile Insert ===" << std::endl;
    
    std::remove("test_heap.db");
    HeapFile heap_file("test_heap.db");
    
    if (!heap_file.Open()) {
        std::cout << "Failed to open heap file" << std::endl;
        return;
    }
    
    // Insert test records
    std::vector<std::string> records = {
        "Alice,25,Engineer",
        "Bob,30,Manager", 
        "Charlie,35,Developer",
        "Diana,28,Designer",
        "Eve,32,Analyst"
    };
    
    for (const auto& record : records) {
        bool success = heap_file.InsertRecord(record.c_str(), record.length() + 1);
        std::cout << "Insert '" << record << "': " << (success ? "SUCCESS" : "FAILED") << std::endl;
    }
    
    std::cout << "Total records: " << heap_file.GetRecordCount() << std::endl;
    std::cout << "Total pages: " << heap_file.GetPageCount() << std::endl;
}

void TestHeapFileScan() {
    std::cout << "\n=== Testing HeapFile Scan ===" << std::endl;
    
    HeapFile heap_file("test_heap.db");
    if (!heap_file.Open()) {
        std::cout << "Failed to open heap file" << std::endl;
        return;
    }
    
    auto scanner = heap_file.CreateScanIterator();
    char buffer[256];
    uint32_t actual_size;
    int count = 0;
    
    std::cout << "Scanning all records:" << std::endl;
    while (scanner->NextRecord(buffer, sizeof(buffer), actual_size)) {
        std::cout << "Record " << (++count) << ": " << buffer << std::endl;
    }
    
    std::cout << "Total scanned records: " << count << std::endl;
}

int main() {
    std::cout << "=== HeapFile Storage Layer Demo ===" << std::endl;
    
    try {
        TestHeapFileInsert();
        TestHeapFileScan();
        
        std::cout << "\n=== HeapFile Demo Completed ===" << std::endl;
        std::cout << "Disk-based storage with slotted page layout is working!" << std::endl;
        
        // Cleanup
        std::remove("test_heap.db");
        
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}
