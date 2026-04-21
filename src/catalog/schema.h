#pragma once

#include "../common/types.h"
#include <string>
#include <vector>
#include <unordered_map>

namespace minidb {

class HeapFile;

struct Column {
    std::string name;
    DataType type;
    size_t length;
    bool nullable;
    
    Column(const std::string& col_name, DataType col_type, 
           size_t col_length = 0, bool is_nullable = true)
        : name(col_name), type(col_type), length(col_length), nullable(is_nullable) {}
};

class Schema {
public:
    explicit Schema(const std::vector<Column>& columns);
    
    const Column& GetColumn(column_id_t col_id) const;
    column_id_t GetColumnId(const std::string& col_name) const;
    size_t GetColumnCount() const { return columns_.size(); }
    const std::vector<Column>& GetColumns() const { return columns_; }
    
    size_t GetSerializedSize() const;
    size_t GetRowSize() const;

private:
    std::vector<Column> columns_;
    std::unordered_map<std::string, column_id_t> name_to_id_;
};

class TableInfo {
public:
    TableInfo(table_id_t table_id, const std::string& table_name, 
              std::shared_ptr<Schema> schema, page_id_t root_page_id);
    
    table_id_t GetTableId() const { return table_id_; }
    const std::string& GetTableName() const { return table_name_; }
    std::shared_ptr<Schema> GetSchema() const { return schema_; }
    page_id_t GetRootPageId() const { return root_page_id_; }
    void SetRootPageId(page_id_t root_page_id) { root_page_id_ = root_page_id; }
    
    HeapFile* GetHeapFile() const { return heap_file_; }
    void SetHeapFile(HeapFile* heap_file) { heap_file_ = heap_file; }

private:
    table_id_t table_id_;
    std::string table_name_;
    std::shared_ptr<Schema> schema_;
    page_id_t root_page_id_;
    HeapFile* heap_file_;
};

}
