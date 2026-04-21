#pragma once

#include "schema.h"
#include "../common/types.h"
#include <string>
#include <unordered_map>
#include <memory>
#include <mutex>

namespace minidb {

class BufferPoolManager;

class Catalog {
public:
    static Catalog& GetInstance();
    
    // Table management
    bool CreateTable(const std::string& table_name,
                     std::shared_ptr<Schema> schema,
                     page_id_t root_page_id,
                     const std::string& heap_filename,
                     BufferPoolManager* buffer_pool);
    
    std::shared_ptr<TableInfo> GetTable(const std::string& table_name);
    bool TableExists(const std::string& table_name) const;
    
    // Get all tables
    std::vector<std::shared_ptr<TableInfo>> GetAllTables() const;
    
    // Clear all tables (for testing)
    void Clear();
    
private:
    Catalog() = default;
    ~Catalog() = default;
    Catalog(const Catalog&) = delete;
    Catalog& operator=(const Catalog&) = delete;
    
    std::unordered_map<std::string, std::shared_ptr<TableInfo>> tables_;
    table_id_t next_table_id_;
    mutable std::mutex mutex_;
};

}
