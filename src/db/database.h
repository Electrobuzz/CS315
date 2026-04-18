#pragma once

#include "../storage/buffer_pool_manager.h"
#include "../storage/disk_manager.h"
#include "../catalog/schema.h"
#include "../execution/executor.h"
#include <memory>
#include <unordered_map>
#include <string>

namespace minidb {

class Database {
public:
    explicit Database(const std::string& db_file);
    ~Database();
    
    bool CreateTable(const std::string& table_name, 
                    const std::vector<Column>& columns);
    
    bool InsertIntoTable(const std::string& table_name, 
                         const Record& record);
    
    std::unique_ptr<Executor> CreateSeqScanExecutor(const std::string& table_name);
    std::unique_ptr<Executor> CreateFilterExecutor(
        std::unique_ptr<Executor> child,
        const std::string& column_name,
        const Value& value,
        ComparisonType comp_type);
    
    bool Shutdown();

private:
    std::unique_ptr<DiskManager> disk_manager_;
    std::unique_ptr<BufferPoolManager> buffer_pool_manager_;
    std::unordered_map<std::string, std::shared_ptr<TableInfo>> tables_;
    table_id_t next_table_id_;
    
    bool LoadCatalog();
    bool SaveCatalog();
    page_id_t AllocateNewPage();
};

}
