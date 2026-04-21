#include "schema.h"
#include <stdexcept>

namespace minidb {

Schema::Schema(const std::vector<Column>& columns) : columns_(columns) {
    for (size_t i = 0; i < columns.size(); ++i) {
        name_to_id_[columns[i].name] = static_cast<column_id_t>(i);
    }
}

const Column& Schema::GetColumn(column_id_t col_id) const {
    if (col_id >= columns_.size()) {
        throw std::runtime_error("Column ID out of range");
    }
    return columns_[col_id];
}

column_id_t Schema::GetColumnId(const std::string& col_name) const {
    auto it = name_to_id_.find(col_name);
    if (it == name_to_id_.end()) {
        throw std::runtime_error("Column not found: " + col_name);
    }
    return it->second;
}

size_t Schema::GetSerializedSize() const {
    size_t size = sizeof(uint32_t);
    for (const auto& col : columns_) {
        size += sizeof(uint32_t) + col.name.length();
        size += sizeof(DataType);
        size += sizeof(size_t);
        size += sizeof(bool);
    }
    return size;
}

size_t Schema::GetRowSize() const {
    size_t size = 0;
    for (const auto& col : columns_) {
        switch (col.type) {
            case DataType::INTEGER:
                size += sizeof(int32_t);
                break;
            case DataType::FLOAT:
                size += sizeof(float);
                break;
            case DataType::BOOLEAN:
                size += sizeof(bool);
                break;
            case DataType::VARCHAR:
                size += sizeof(uint32_t) + col.length;
                break;
        }
    }
    return size;
}

TableInfo::TableInfo(table_id_t table_id, const std::string& table_name, 
                     std::shared_ptr<Schema> schema, page_id_t root_page_id)
    : table_id_(table_id), table_name_(table_name), 
      schema_(schema), root_page_id_(root_page_id), heap_file_(nullptr) {}

}
