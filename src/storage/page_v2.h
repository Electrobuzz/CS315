#pragma once

#include <cstdint>
#include <cstring>

namespace minidb {

constexpr uint32_t PAGE_SIZE = 4096;
constexpr uint32_t INVALID_PAGE_ID = 0;

class Page {
public:
    static constexpr uint32_t SIZE = PAGE_SIZE;
    
    // Page layout: [Node Data (0-4087) | LSN Trailer (4088-4095)]
    // Node data region: [0, OFFSET_LAST_LSN)
    // LSN trailer region: [OFFSET_LAST_LSN, SIZE)
    static constexpr uint32_t PAGE_TRAILER_SIZE = sizeof(uint64_t);
    static constexpr uint32_t OFFSET_LAST_LSN = SIZE - PAGE_TRAILER_SIZE;
    
    // Page header layout (24 bytes total) - EXACT BYTE OFFSETS
    // Offset 0-3:   page_id (uint32_t)
    // Offset 4-7:   free_space_offset (uint32_t)
    // Offset 8-11:  slot_count (uint32_t)
    // Offset 12-15: free_slot_count (uint32_t)
    // Offset 16-23: (unused - reserved for future use)
    static constexpr uint32_t OFFSET_PAGE_ID = 0;
    static constexpr uint32_t OFFSET_FREE_SPACE_OFFSET = 4;
    static constexpr uint32_t OFFSET_SLOT_COUNT = 8;
    static constexpr uint32_t OFFSET_FREE_SLOT_COUNT = 12;
    static constexpr uint32_t HEADER_SIZE = 24;
    
    // Slot directory entry (8 bytes each)
    // Offset 0-3: offset (uint32_t)
    // Offset 4-7: length (uint32_t)
    static constexpr uint32_t SLOT_SIZE = 8;
    static constexpr uint32_t SLOT_OFFSET_OFFSET = 0;
    static constexpr uint32_t SLOT_LENGTH_OFFSET = 4;
    
    static constexpr uint32_t DATA_AREA_OFFSET = HEADER_SIZE;
    static constexpr uint32_t DATA_AREA_SIZE = SIZE - HEADER_SIZE;

private:
    char data_[SIZE];
    
    // Helper methods to read/write header fields using explicit offsets
    uint32_t ReadUint32(uint32_t offset) const {
        uint32_t value;
        std::memcpy(&value, data_ + offset, sizeof(uint32_t));
        return value;
    }
    
    void WriteUint32(uint32_t offset, uint32_t value) {
        std::memcpy(data_ + offset, &value, sizeof(uint32_t));
    }
    
    uint64_t ReadUint64(uint32_t offset) const {
        uint64_t value;
        std::memcpy(&value, data_ + offset, sizeof(uint64_t));
        return value;
    }
    
    void WriteUint64(uint32_t offset, uint64_t value) {
        std::memcpy(data_ + offset, &value, sizeof(uint64_t));
    }
    
    // Slot access methods
    uint32_t GetSlotOffset(uint32_t slot_id) const {
        return DATA_AREA_OFFSET + slot_id * SLOT_SIZE + SLOT_OFFSET_OFFSET;
    }
    
    uint32_t GetSlotLength(uint32_t slot_id) const {
        return ReadUint32(DATA_AREA_OFFSET + slot_id * SLOT_SIZE + SLOT_LENGTH_OFFSET);
    }
    
    void SetSlotOffset(uint32_t slot_id, uint32_t offset) {
        WriteUint32(DATA_AREA_OFFSET + slot_id * SLOT_SIZE + SLOT_OFFSET_OFFSET, offset);
    }
    
    void SetSlotLength(uint32_t slot_id, uint32_t length) {
        WriteUint32(DATA_AREA_OFFSET + slot_id * SLOT_SIZE + SLOT_LENGTH_OFFSET, length);
    }
    
    char* GetDataArea() { 
        return data_ + DATA_AREA_OFFSET; 
    }
    
    const char* GetDataArea() const { 
        return data_ + DATA_AREA_OFFSET; 
    }

public:
    Page();
    
    // Basic page operations
    void Reset();
    
    // Page ID operations
    void SetPageId(uint32_t page_id) { WriteUint32(OFFSET_PAGE_ID, page_id); }
    uint32_t GetPageId() const { return ReadUint32(OFFSET_PAGE_ID); }
    
    // Free space offset operations
    void SetFreeSpaceOffset(uint32_t offset) { WriteUint32(OFFSET_FREE_SPACE_OFFSET, offset); }
    uint32_t GetFreeSpaceOffset() const { return ReadUint32(OFFSET_FREE_SPACE_OFFSET); }
    
    // Slot count operations
    void SetSlotCount(uint32_t count) { WriteUint32(OFFSET_SLOT_COUNT, count); }
    uint32_t GetSlotCount() const { return ReadUint32(OFFSET_SLOT_COUNT); }
    
    // Free slot count operations
    void SetFreeSlotCount(uint32_t count) { WriteUint32(OFFSET_FREE_SLOT_COUNT, count); }
    uint32_t GetFreeSlotCount() const { return ReadUint32(OFFSET_FREE_SLOT_COUNT); }
    
    // LSN operations - THESE NOW READ/WRITE DIRECTLY TO RAW MEMORY
    uint64_t GetLastLSN() const { return ReadUint64(OFFSET_LAST_LSN); }
    void SetLastLSN(uint64_t lsn) { WriteUint64(OFFSET_LAST_LSN, lsn); }
    
    // Page layout validation
    static bool ValidateWriteOffset(uint32_t offset, uint32_t size) {
        return (offset + size) <= OFFSET_LAST_LSN;
    }
    
    static bool ValidatePageLayout() {
        static_assert(OFFSET_LAST_LSN == SIZE - PAGE_TRAILER_SIZE, 
                      "LSN offset must be at end of page");
        static_assert(PAGE_TRAILER_SIZE == sizeof(uint64_t), 
                      "Trailer size must be 8 bytes for uint64_t LSN");
        return true;
    }
    
    // Raw data access
    char* GetData() { return data_; }
    const char* GetData() const { return data_; }
    
    // Space management
    uint32_t GetFreeSpace() const;
    bool HasSpaceFor(uint32_t record_size) const;
    
    // Slot operations
    bool IsSlotUsed(uint32_t slot_id) const;
    
    // Record operations
    bool InsertRecord(const char* record_data, uint32_t record_size, uint32_t& slot_id);
    bool DeleteRecord(uint32_t slot_id);
    bool GetRecord(uint32_t slot_id, char* buffer, uint32_t buffer_size, uint32_t& actual_size) const;
};

}
