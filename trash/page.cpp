#include "page.h"
#include <cstring>
#include <stdexcept>

namespace minidb {

Page::Page() {
    Reset();
}

void Page::Reset() {
    std::memset(data_, 0, SIZE);
    Header* header = GetHeader();
    header->page_id = INVALID_PAGE_ID;
    header->free_space_offset = DATA_SIZE;
    header->slot_count = 0;
    header->free_slot_count = 0;
}

bool Page::InsertRecord(const Record& record, uint32_t& slot_id) {
    if (IsFull()) {
        return false;
    }
    
    Header* header = GetHeader();
    Slot* slots = GetSlots();
    
    uint32_t record_size = sizeof(uint32_t);
    for (const auto& value : record.values) {
        switch (value.type) {
            case DataType::INTEGER:
                record_size += sizeof(int32_t);
                break;
            case DataType::FLOAT:
                record_size += sizeof(float);
                break;
            case DataType::BOOLEAN:
                record_size += sizeof(bool);
                break;
            case DataType::VARCHAR:
                record_size += sizeof(uint32_t) + value.varchar_val.length();
                break;
        }
    }
    
    if (header->free_space_offset + record_size > DATA_SIZE) {
        return false;
    }
    
    uint32_t target_slot_id;
    if (header->free_slot_count > 0) {
        for (uint32_t i = 0; i < header->slot_count; ++i) {
            if (!slots[i].in_use) {
                target_slot_id = i;
                break;
            }
        }
        header->free_slot_count--;
    } else {
        target_slot_id = header->slot_count;
        header->slot_count++;
    }
    
    uint32_t offset = header->free_space_offset - record_size;
    
    slots[target_slot_id].offset = offset;
    slots[target_slot_id].length = record_size;
    slots[target_slot_id].in_use = true;
    
    char* data_ptr = GetData() + offset;
    uint32_t num_values = record.values.size();
    std::memcpy(data_ptr, &num_values, sizeof(uint32_t));
    data_ptr += sizeof(uint32_t);
    
    for (const auto& value : record.values) {
        switch (value.type) {
            case DataType::INTEGER:
                std::memcpy(data_ptr, &value.data.int_val, sizeof(int32_t));
                data_ptr += sizeof(int32_t);
                break;
            case DataType::FLOAT:
                std::memcpy(data_ptr, &value.data.float_val, sizeof(float));
                data_ptr += sizeof(float);
                break;
            case DataType::BOOLEAN:
                std::memcpy(data_ptr, &value.data.bool_val, sizeof(bool));
                data_ptr += sizeof(bool);
                break;
            case DataType::VARCHAR: {
                uint32_t str_len = value.varchar_val.length();
                std::memcpy(data_ptr, &str_len, sizeof(uint32_t));
                data_ptr += sizeof(uint32_t);
                std::memcpy(data_ptr, value.varchar_val.c_str(), str_len);
                data_ptr += str_len;
                break;
            }
        }
    }
    
    header->free_space_offset = offset;
    slot_id = target_slot_id;
    return true;
}

bool Page::DeleteRecord(uint32_t slot_id) {
    if (slot_id >= GetHeader()->slot_count) {
        return false;
    }
    
    Slot* slots = GetSlots();
    if (!slots[slot_id].in_use) {
        return false;
    }
    
    slots[slot_id].in_use = false;
    GetHeader()->free_slot_count++;
    return true;
}

bool Page::GetRecord(uint32_t slot_id, Record& record) const {
    if (slot_id >= GetHeader()->slot_count) {
        return false;
    }
    
    const Slot* slots = GetSlots();
    if (!slots[slot_id].in_use) {
        return false;
    }
    
    const char* data_ptr = GetData() + slots[slot_id].offset;
    
    uint32_t num_values;
    std::memcpy(&num_values, data_ptr, sizeof(uint32_t));
    data_ptr += sizeof(uint32_t);
    
    record.values.clear();
    record.values.reserve(num_values);
    
    for (uint32_t i = 0; i < num_values; ++i) {
        Value value;
        value.type = DataType::INTEGER;
        
        std::memcpy(&value.data.int_val, data_ptr, sizeof(int32_t));
        data_ptr += sizeof(int32_t);
        record.values.push_back(value);
    }
    
    return true;
}

bool Page::IsFull() const {
    const Header* header = GetHeader();
    return header->free_space_offset < sizeof(uint32_t);
}

}
