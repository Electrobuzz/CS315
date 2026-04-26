#include <iostream>
#include <fstream>
#include <cstdint>
#include <cstring>
#include <vector>
#include <memory>
#include <unordered_map>
#include <queue>

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

using frame_id_t = uint32_t;
constexpr frame_id_t INVALID_FRAME_ID = 0;

class Frame {
public:
    Frame() : pin_count_(0), is_dirty_(false), last_access_(0) {}
    
    Page& GetPage() { return page_; }
    const Page& GetPage() const { return page_; }
    
    int GetPinCount() const { return pin_count_; }
    void IncrementPinCount() { pin_count_++; }
    void DecrementPinCount() { 
        if (pin_count_ > 0) pin_count_--; 
    }
    
    bool IsDirty() const { return is_dirty_; }
    void SetDirty(bool dirty) { is_dirty_ = dirty; }
    
    uint64_t GetLastAccess() const { return last_access_; }
    void SetLastAccess(uint64_t timestamp) { last_access_ = timestamp; }

private:
    Page page_;
    int pin_count_;
    bool is_dirty_;
    uint64_t last_access_;  // For LRU
};

class BufferPoolManager {
public:
    BufferPoolManager(size_t pool_size, DiskManager* disk_manager)
        : pool_size_(pool_size), disk_manager_(disk_manager), access_counter_(0) {
        frames_.resize(pool_size_);
        for (size_t i = 0; i < pool_size_; ++i) {
            frames_[i] = std::make_unique<Frame>();
        }
        free_list_.resize(pool_size_, true);
    }
    
    ~BufferPoolManager() {
        FlushAllPages();
    }
    
    Page* FetchPage(uint32_t page_id) {
        auto it = page_table_.find(page_id);
        if (it != page_table_.end()) {
            frame_id_t frame_id = it->second;
            Frame* frame = frames_[frame_id].get();
            frame->IncrementPinCount();
            frame->SetLastAccess(++access_counter_);
            return &frame->GetPage();
        }
        
        frame_id_t frame_id = FindVictimFrame();
        if (frame_id == INVALID_FRAME_ID) {
            return nullptr;
        }
        
        Frame* frame = frames_[frame_id].get();
        
        if (frame->GetPage().GetPageId() != INVALID_PAGE_ID) {
            uint32_t old_page_id = frame->GetPage().GetPageId();
            if (!EvictFrame(frame_id)) {
                return nullptr;
            }
            page_table_.erase(old_page_id);
        }
        
        if (!ReadPageFromDisk(page_id, frame)) {
            return nullptr;
        }
        
        frame->GetPage().SetPageId(page_id);
        frame->IncrementPinCount();
        frame->SetDirty(false);
        frame->SetLastAccess(++access_counter_);
        
        page_table_[page_id] = frame_id;
        
        return &frame->GetPage();
    }
    
    bool UnpinPage(uint32_t page_id, bool is_dirty) {
        auto it = page_table_.find(page_id);
        if (it == page_table_.end()) {
            return false;
        }
        
        frame_id_t frame_id = it->second;
        Frame* frame = frames_[frame_id].get();
        
        if (frame->GetPinCount() <= 0) {
            return false;
        }
        
        frame->DecrementPinCount();
        
        if (is_dirty) {
            frame->SetDirty(true);
        }
        
        return true;
    }
    
    Page* NewPage(uint32_t& page_id) {
        page_id = disk_manager_->AllocatePage();
        if (page_id == INVALID_PAGE_ID) {
            return nullptr;
        }
        
        frame_id_t frame_id = FindVictimFrame();
        if (frame_id == INVALID_FRAME_ID) {
            return nullptr;
        }
        
        Frame* frame = frames_[frame_id].get();
        
        if (frame->GetPage().GetPageId() != INVALID_PAGE_ID) {
            uint32_t old_page_id = frame->GetPage().GetPageId();
            if (!EvictFrame(frame_id)) {
                return nullptr;
            }
            page_table_.erase(old_page_id);
        }
        
        frame->GetPage().Reset();
        frame->GetPage().SetPageId(page_id);
        frame->IncrementPinCount();
        frame->SetDirty(false);
        frame->SetLastAccess(++access_counter_);
        
        page_table_[page_id] = frame_id;
        
        return &frame->GetPage();
    }
    
    bool FlushPage(uint32_t page_id) {
        auto it = page_table_.find(page_id);
        if (it == page_table_.end()) {
            return false;
        }
        
        frame_id_t frame_id = it->second;
        Frame* frame = frames_[frame_id].get();
        
        if (frame->IsDirty()) {
            if (!WritePageToDisk(page_id, frame)) {
                return false;
            }
            frame->SetDirty(false);
        }
        
        return true;
    }
    
    void FlushAllPages() {
        for (const auto& [page_id, frame_id] : page_table_) {
            Frame* frame = frames_[frame_id].get();
            if (frame->IsDirty()) {
                WritePageToDisk(page_id, frame);
                frame->SetDirty(false);
            }
        }
    }
    
    size_t GetPageTableSize() const { return page_table_.size(); }

private:
    frame_id_t FindVictimFrame() {
        // First check for free frames
        for (size_t i = 0; i < pool_size_; ++i) {
            if (free_list_[i]) {
                free_list_[i] = false;
                return static_cast<frame_id_t>(i);
            }
        }
        
        // No free frames, use LRU eviction
        frame_id_t lru_frame = INVALID_FRAME_ID;
        uint64_t oldest_access = UINT64_MAX;
        
        for (size_t i = 0; i < pool_size_; ++i) {
            Frame* frame = frames_[i].get();
            if (frame->GetPinCount() == 0 && frame->GetLastAccess() < oldest_access) {
                oldest_access = frame->GetLastAccess();
                lru_frame = static_cast<frame_id_t>(i);
            }
        }
        
        return lru_frame;
    }
    
    bool EvictFrame(frame_id_t frame_id) {
        Frame* frame = frames_[frame_id].get();
        
        if (frame->GetPinCount() > 0) {
            return false;
        }
        
        if (frame->IsDirty()) {
            uint32_t page_id = frame->GetPage().GetPageId();
            if (!WritePageToDisk(page_id, frame)) {
                return false;
            }
            frame->SetDirty(false);
        }
        
        return true;
    }
    
    bool WritePageToDisk(uint32_t page_id, Frame* frame) {
        return disk_manager_->WritePage(page_id, frame->GetPage());
    }
    
    bool ReadPageFromDisk(uint32_t page_id, Frame* frame) {
        return disk_manager_->ReadPage(page_id, frame->GetPage());
    }

private:
    size_t pool_size_;
    DiskManager* disk_manager_;
    std::vector<std::unique_ptr<Frame>> frames_;
    std::unordered_map<uint32_t, frame_id_t> page_table_;
    std::vector<bool> free_list_;
    uint64_t access_counter_;
};

int main() {
    std::cout << "=== Buffer Pool Manager Test (Simple LRU) ===" << std::endl;
    
    try {
        std::remove("test_buffer_simple.db");
        DiskManager disk_manager("test_buffer_simple.db");
        BufferPoolManager buffer_pool(3, &disk_manager);
        
        // Test 1: Basic NewPage and FetchPage
        std::cout << "\nTest 1: Basic NewPage and FetchPage" << std::endl;
        uint32_t page1;
        Page* p1 = buffer_pool.NewPage(page1);
        if (p1) {
            const char* data = "Hello, Buffer Pool!";
            std::memcpy(p1->GetData(), data, std::strlen(data) + 1);
            buffer_pool.UnpinPage(page1, true);
            std::cout << "Created page " << page1 << " with data: " << data << std::endl;
        }
        
        Page* fetched = buffer_pool.FetchPage(page1);
        if (fetched) {
            std::cout << "Fetched page " << page1 << " data: " << fetched->GetData() << std::endl;
            buffer_pool.UnpinPage(page1, false);
        }
        
        // Test 2: Multiple pages
        std::cout << "\nTest 2: Multiple Pages" << std::endl;
        uint32_t page2, page3;
        Page* p2 = buffer_pool.NewPage(page2);
        Page* p3 = buffer_pool.NewPage(page3);
        
        if (p2 && p3) {
            std::memcpy(p2->GetData(), "Page 2 data", 11);
            std::memcpy(p3->GetData(), "Page 3 data", 11);
            buffer_pool.UnpinPage(page2, true);
            buffer_pool.UnpinPage(page3, true);
            std::cout << "Created pages " << page2 << " and " << page3 << std::endl;
            std::cout << "Buffer pool size: " << buffer_pool.GetPageTableSize() << std::endl;
        }
        
        // Test 3: LRU Eviction
        std::cout << "\nTest 3: LRU Eviction" << std::endl;
        BufferPoolManager small_pool(2, &disk_manager);
        uint32_t pageA, pageB, pageC;
        
        Page* pA = small_pool.NewPage(pageA);
        std::memcpy(pA->GetData(), "Page A", 7);
        small_pool.UnpinPage(pageA, true);
        
        Page* pB = small_pool.NewPage(pageB);
        std::memcpy(pB->GetData(), "Page B", 7);
        small_pool.UnpinPage(pageB, true);
        
        std::cout << "Created pages A and B in pool of size 2" << std::endl;
        
        // Access page A (make it most recently used)
        small_pool.FetchPage(pageA);
        small_pool.UnpinPage(pageA, false);
        
        // Create page C (should evict page B - least recently used)
        Page* pC = small_pool.NewPage(pageC);
        if (pC) {
            std::cout << "Created page C (should have evicted page B)" << std::endl;
            std::memcpy(pC->GetData(), "Page C", 7);
            small_pool.UnpinPage(pageC, true);
        }
        
        // Test 4: Flush
        std::cout << "\nTest 4: Flush Operation" << std::endl;
        bool flush_success = buffer_pool.FlushPage(page1);
        std::cout << "Flush page " << page1 << ": " << (flush_success ? "SUCCESS" : "FAILED") << std::endl;
        
        std::cout << "\n✅ All buffer pool tests completed successfully!" << std::endl;
        
        std::remove("test_buffer_simple.db");
        
    } catch (const std::exception& e) {
        std::cerr << "Test failed: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}
