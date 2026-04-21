#include "page_v2.h"
#include <cstring>
#include <stdexcept>
#include <iostream>

namespace minidb {

Page::Page() {
    Reset();
}

void Page::Reset() {
    std::memset(data_, 0, SIZE);
    
    SetPageId(INVALID_PAGE_ID);
    SetFreeSpaceOffset(RECORD_AREA_SIZE);  // Start from end of record area
    SetSlotCount(0);
    SetFreeSlotCount(0);
    SetLastLSN(0);  // Initialize LSN to 0
}

uint32_t Page::GetFreeSpace() const {
    uint32_t used_record_space = RECORD_AREA_SIZE - GetFreeSpaceOffset();
    return RECORD_AREA_SIZE - used_record_space;
}

bool Page::HasSpaceFor(uint32_t record_size) const {
    // Need space for record + new slot
    uint32_t total_needed = record_size + SLOT_SIZE;
    return GetFreeSpace() >= total_needed;
}

bool Page::IsSlotUsed(uint32_t slot_id) const {
    if (slot_id >= GetSlotCount()) {
        return false;
    }
    
    return GetSlotLength(slot_id) > 0;  // Length 0 indicates unused slot
}

bool Page::InsertRecord(const char* record_data, uint32_t record_size, uint32_t& slot_id) {
    if (!record_data || record_size == 0) {
        return false;
    }
    
    // Check if we have space for a new slot
    if (GetSlotCount() >= MAX_SLOTS) {
        std::cerr << "[ERROR] Page::InsertRecord: maximum slots exceeded" << std::endl;
        return false;
    }
    
    if (!HasSpaceFor(record_size)) {
        return false;
    }
    
    // Find or allocate a slot
    uint32_t target_slot_id;
    if (GetFreeSlotCount() > 0) {
        // Find first free slot
        for (uint32_t i = 0; i < GetSlotCount(); ++i) {
            if (!IsSlotUsed(i)) {
                target_slot_id = i;
                break;
            }
        }
        SetFreeSlotCount(GetFreeSlotCount() - 1);
    } else {
        // Allocate new slot
        target_slot_id = GetSlotCount();
        SetSlotCount(GetSlotCount() + 1);
    }
    
    // Calculate offset for new record (grow from end of record area)
    uint32_t new_offset = GetFreeSpaceOffset() - record_size;
    
    // Write record data
    char* data_area = GetDataArea();
    std::memcpy(data_area + new_offset, record_data, record_size);
    
    // Update slot directory (offset is relative to RECORD_AREA_OFFSET)
    SetSlotOffset(target_slot_id, new_offset);
    SetSlotLength(target_slot_id, record_size);
    
    // Update free space pointer
    SetFreeSpaceOffset(new_offset);
    
    slot_id = target_slot_id;
    
    return true;
}

bool Page::DeleteRecord(uint32_t slot_id) {
    if (slot_id >= GetSlotCount()) {
        return false;
    }
    
    if (!IsSlotUsed(slot_id)) {
        return false;
    }
    
    // Mark slot as unused (set length to 0)
    SetSlotLength(slot_id, 0);
    SetFreeSlotCount(GetFreeSlotCount() + 1);
    
    // Note: We don't compact the page immediately - this is done during page compaction
    return true;
}

bool Page::GetRecord(uint32_t slot_id, char* buffer, uint32_t buffer_size, uint32_t& actual_size) const {
    if (slot_id >= GetSlotCount()) {
        return false;
    }
    
    if (!IsSlotUsed(slot_id)) {
        return false;
    }
    
    const char* data_area = GetDataArea();
    uint32_t record_offset = GetSlotOffset(slot_id);
    uint32_t record_length = GetSlotLength(slot_id);
    
    if (buffer_size < record_length) {
        actual_size = record_length;
        return false;
    }
    
    std::memcpy(buffer, data_area + record_offset, record_length);
    actual_size = record_length;
    
    return true;
}

}
