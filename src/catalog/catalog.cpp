#include "catalog.h"
#include "../storage/heap_file.h"

namespace minidb {

Catalog& Catalog::GetInstance() {
    static Catalog instance;
    return instance;
}

bool Catalog::CreateTable(const std::string& table_name,
                          std::shared_ptr<Schema> schema,
                          page_id_t root_page_id,
                          const std::string& heap_filename,
                          BufferPoolManager* buffer_pool,
                          DiskManager* heap_disk_manager) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (tables_.find(table_name) != tables_.end()) {
        return false; // Table already exists
    }
    
    table_id_t table_id = next_table_id_++;
    auto table_info = std::make_shared<TableInfo>(table_id, table_name, schema, root_page_id);
    
    // Create shared HeapFile for this table
    HeapFile* heap_file;
    if (heap_disk_manager) {
        heap_file = new HeapFile(heap_disk_manager, buffer_pool);
    } else {
        heap_file = new HeapFile(heap_filename, buffer_pool);
    }
    if (!heap_file->Open()) {
        delete heap_file;
        return false;
    }
    
    table_info->SetHeapFile(heap_file);
    tables_[table_name] = table_info;
    
    return true;
}

std::shared_ptr<TableInfo> Catalog::GetTable(const std::string& table_name) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto it = tables_.find(table_name);
    if (it != tables_.end()) {
        return it->second;
    }
    return nullptr;
}

bool Catalog::TableExists(const std::string& table_name) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return tables_.find(table_name) != tables_.end();
}

std::vector<std::shared_ptr<TableInfo>> Catalog::GetAllTables() const {
    std::lock_guard<std::mutex> lock(mutex_);
    
    std::vector<std::shared_ptr<TableInfo>> result;
    for (const auto& pair : tables_) {
        result.push_back(pair.second);
    }
    return result;
}

void Catalog::Clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    // Reset root_page_id for all tables before clearing
    for (auto& pair : tables_) {
        pair.second->SetRootPageId(INVALID_PAGE_ID);
    }
    tables_.clear();
    next_table_id_ = 1;
}

}
