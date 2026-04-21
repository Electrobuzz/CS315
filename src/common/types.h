#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <memory>
#include <limits>

namespace minidb {

using page_id_t = uint32_t;
using frame_id_t = uint32_t;
using table_id_t = uint32_t;
using column_id_t = uint32_t;

constexpr uint32_t PAGE_SIZE = 4096;
constexpr uint32_t INVALID_PAGE_ID = 0;
constexpr uint32_t INVALID_FRAME_ID = std::numeric_limits<uint32_t>::max();
constexpr uint32_t INVALID_TABLE_ID = 0;

enum class DataType {
    INTEGER,
    VARCHAR,
    BOOLEAN,
    FLOAT
};

enum class ComparisonType {
    EQ,
    NE,
    LT,
    LE,
    GT,
    GE
};

struct Value {
    DataType type;
    union {
        int32_t int_val;
        float float_val;
        bool bool_val;
    } data;
    std::string varchar_val;
    
    bool operator==(const Value& other) const;
    bool Compare(const Value& other, ComparisonType comp) const;
};

class Record {
public:
    std::vector<Value> values;
    
    Record() = default;
    explicit Record(const std::vector<Value>& vals) : values(vals) {}
};

struct RID {
    uint32_t page_id;
    uint32_t slot_id;
    
    RID() : page_id(INVALID_PAGE_ID), slot_id(0) {}
    RID(uint32_t p, uint32_t s) : page_id(p), slot_id(s) {}
    
    bool operator==(const RID& other) const {
        return page_id == other.page_id && slot_id == other.slot_id;
    }
};

}
