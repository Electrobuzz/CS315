#pragma once

#include "../common/types.h"
#include <cstring>
#include <memory>

namespace minidb {

class Page {
public:
    static constexpr size_t SIZE = PAGE_SIZE;
    static constexpr size_t HEADER_SIZE = sizeof(uint32_t) * 4;
    static constexpr size_t DATA_SIZE = SIZE - HEADER_SIZE;

private:
    char data_[SIZE];
    
    struct Header {
        uint32_t page_id;
        uint32_t free_space_offset;
        uint32_t slot_count;
        uint32_t free_slot_count;
    };

    struct Slot {
        uint32_t offset;
        uint32_t length;
        bool in_use;
    };

    Header* GetHeader() { return reinterpret_cast<Header*>(data_); }
    const Header* GetHeader() const { return reinterpret_cast<const Header*>(data_); }
    Slot* GetSlots() { return reinterpret_cast<Slot*>(data_ + sizeof(Header)); }
    const Slot* GetSlots() const { return reinterpret_cast<const Slot*>(data_ + sizeof(Header)); }
    char* GetData() { return data_ + HEADER_SIZE; }
    const char* GetData() const { return data_ + HEADER_SIZE; }

public:
    Page();
    ~Page() = default;
    
    page_id_t GetPageId() const { return GetHeader()->page_id; }
    void SetPageId(page_id_t page_id) { GetHeader()->page_id = page_id; }
    
    bool InsertRecord(const Record& record, uint32_t& slot_id);
    bool DeleteRecord(uint32_t slot_id);
    bool GetRecord(uint32_t slot_id, Record& record) const;
    
    bool IsFull() const;
    void Reset();
    
    char* GetData() { return data_; }
    const char* GetData() const { return data_; }
};

}
