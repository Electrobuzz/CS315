#pragma once

#include "types.h"
#include <cstdint>
#include <cstring>
#include <iostream>

namespace minidb {

// Simple tuple format for our minimal SQL
// Each tuple has: id (INT), value (INT)
struct Tuple {
    int32_t id;
    int32_t value;
    
    Tuple() : id(0), value(0) {}
    Tuple(int32_t id_val, int32_t value_val) : id(id_val), value(value_val) {}
    
    // Serialize to buffer (for storage)
    void Serialize(char* buffer) const {
        std::memcpy(buffer, &id, sizeof(id));
        std::memcpy(buffer + sizeof(id), &value, sizeof(value));
    }
    
    // Deserialize from buffer (for storage)
    void Deserialize(const char* buffer) {
        std::memcpy(&id, buffer, sizeof(id));
        std::memcpy(&value, buffer + sizeof(id), sizeof(value));
    }
    
    // Get serialized size
    static constexpr size_t GetSize() {
        return sizeof(id) + sizeof(value);
    }
    
    // Convert to Record (for compatibility with existing execution layer)
    Record ToRecord() const {
        Record record;
        Value id_val;
        id_val.type = DataType::INTEGER;
        id_val.data.int_val = id;
        
        Value value_val;
        value_val.type = DataType::INTEGER;
        value_val.data.int_val = value;
        
        record.values.push_back(id_val);
        record.values.push_back(value_val);
        
        return record;
    }
    
    // Print tuple
    void Print() const {
        std::cout << "Tuple{id=" << id << ", value=" << value << "}" << std::endl;
    }
};

}
