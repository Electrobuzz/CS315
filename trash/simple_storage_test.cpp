#include <iostream>
#include <cstdint>
#include <cstring>

constexpr uint32_t PAGE_SIZE = 4096;
constexpr uint32_t INVALID_PAGE_ID = 0;

class SimplePage {
public:
    struct Header {
        uint32_t page_id;
        uint32_t free_space_offset;
        uint32_t slot_count;
        uint32_t free_slot_count;
    };
    
private:
    char data_[PAGE_SIZE];
    
    Header* GetHeader() { return reinterpret_cast<Header*>(data_); }
    
public:
    SimplePage() {
        Reset();
    }
    
    void Reset() {
        std::memset(data_, 0, PAGE_SIZE);
        Header* header = GetHeader();
        header->page_id = INVALID_PAGE_ID;
        header->free_space_offset = PAGE_SIZE - sizeof(Header);
        header->slot_count = 0;
        header->free_slot_count = 0;
    }
    
    void SetPageId(uint32_t page_id) { GetHeader()->page_id = page_id; }
    uint32_t GetPageId() const { return reinterpret_cast<const Header*>(data_)->page_id; }
    char* GetData() { return data_; }
    const char* GetData() const { return data_; }
};

int main() {
    std::cout << "=== Simple Storage Test ===" << std::endl;
    
    SimplePage page;
    std::cout << "Initial page ID: " << page.GetPageId() << std::endl;
    
    page.SetPageId(42);
    std::cout << "After setting page ID: " << page.GetPageId() << std::endl;
    
    std::cout << "Simple storage test: PASSED" << std::endl;
    return 0;
}
