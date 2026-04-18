#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <memory>

namespace minidb {

using page_id_t = uint32_t;
using frame_id_t = uint32_t;
using table_id_t = uint32_t;
using column_id_t = uint32_t;

constexpr uint32_t PAGE_SIZE = 4096;
constexpr uint32_t INVALID_PAGE_ID = 0;
constexpr uint32_t INVALID_FRAME_ID = 0;
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

}
