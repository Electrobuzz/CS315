#include "types.h"
#include <stdexcept>

namespace minidb {

bool Value::operator==(const Value& other) const {
    if (type != other.type) {
        return false;
    }
    
    switch (type) {
        case DataType::INTEGER:
            return data.int_val == other.data.int_val;
        case DataType::FLOAT:
            return data.float_val == other.data.float_val;
        case DataType::BOOLEAN:
            return data.bool_val == other.data.bool_val;
        case DataType::VARCHAR:
            return varchar_val == other.varchar_val;
    }
    return false;
}

bool Value::Compare(const Value& other, ComparisonType comp) const {
    if (type != other.type) {
        throw std::runtime_error("Cannot compare values of different types");
    }
    
    bool result = false;
    switch (type) {
        case DataType::INTEGER:
            switch (comp) {
                case ComparisonType::EQ: result = data.int_val == other.data.int_val; break;
                case ComparisonType::NE: result = data.int_val != other.data.int_val; break;
                case ComparisonType::LT: result = data.int_val < other.data.int_val; break;
                case ComparisonType::LE: result = data.int_val <= other.data.int_val; break;
                case ComparisonType::GT: result = data.int_val > other.data.int_val; break;
                case ComparisonType::GE: result = data.int_val >= other.data.int_val; break;
            }
            break;
        case DataType::FLOAT:
            switch (comp) {
                case ComparisonType::EQ: result = data.float_val == other.data.float_val; break;
                case ComparisonType::NE: result = data.float_val != other.data.float_val; break;
                case ComparisonType::LT: result = data.float_val < other.data.float_val; break;
                case ComparisonType::LE: result = data.float_val <= other.data.float_val; break;
                case ComparisonType::GT: result = data.float_val > other.data.float_val; break;
                case ComparisonType::GE: result = data.float_val >= other.data.float_val; break;
            }
            break;
        case DataType::BOOLEAN:
            switch (comp) {
                case ComparisonType::EQ: result = data.bool_val == other.data.bool_val; break;
                case ComparisonType::NE: result = data.bool_val != other.data.bool_val; break;
                default: throw std::runtime_error("Invalid comparison for boolean type");
            }
            break;
        case DataType::VARCHAR:
            switch (comp) {
                case ComparisonType::EQ: result = varchar_val == other.varchar_val; break;
                case ComparisonType::NE: result = varchar_val != other.varchar_val; break;
                case ComparisonType::LT: result = varchar_val < other.varchar_val; break;
                case ComparisonType::LE: result = varchar_val <= other.varchar_val; break;
                case ComparisonType::GT: result = varchar_val > other.varchar_val; break;
                case ComparisonType::GE: result = varchar_val >= other.varchar_val; break;
            }
            break;
    }
    return result;
}

}
