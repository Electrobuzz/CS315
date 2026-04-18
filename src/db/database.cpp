#include "database.h"
#include "../storage/buffer_pool_manager.h"
#include "../storage/disk_manager.h"
#include <iostream>

namespace minidb {

Database::Database(const std::string& db_file) 
    : next_table_id_(1) {
    disk_manager_ = std::make_unique<DiskManager>(db_file);
    buffer_pool_manager_ = std::make_unique<BufferPoolManager>(64, disk_manager_.get());
    LoadCatalog();
}

Database::~Database() {
    Shutdown();
}

bool Database::CreateTable(const std::string& table_name, 
                          const std::vector<Column>& columns) {
    if (tables_.find(table_name) != tables_.end()) {
        return false;
    }
    
    auto schema = std::make_shared<Schema>(columns);
    page_id_t root_page_id = AllocateNewPage();
    
    auto table_info = std::make_shared<TableInfo>(
        next_table_id_++, table_name, schema, root_page_id);
    
    tables_[table_name] = table_info;
    return SaveCatalog();
}

bool Database::InsertIntoTable(const std::string& table_name, 
                               const Record& record) {
    auto it = tables_.find(table_name);
    if (it == tables_.end()) {
        return false;
    }
    
    auto table_info = it->second;
    page_id_t current_page_id = table_info->GetRootPageId();
    
    while (current_page_id != INVALID_PAGE_ID) {
        Page* page = buffer_pool_manager_->FetchPage(current_page_id);
        if (!page) {
            return false;
        }
        
        uint32_t slot_id;
        if (page->InsertRecord(record, slot_id)) {
            buffer_pool_manager_->UnpinPage(current_page_id, true);
            return true;
        }
        
        buffer_pool_manager_->UnpinPage(current_page_id, false);
        current_page_id++;
    }
    
    page_id_t new_page_id;
    Page* new_page = buffer_pool_manager_->NewPage(new_page_id);
    if (!new_page) {
        return false;
    }
    
    uint32_t slot_id;
    bool success = new_page->InsertRecord(record, slot_id);
    buffer_pool_manager_->UnpinPage(new_page_id, true);
    
    return success;
}

std::unique_ptr<Executor> Database::CreateSeqScanExecutor(const std::string& table_name) {
    auto it = tables_.find(table_name);
    if (it == tables_.end()) {
        return nullptr;
    }
    
    return std::make_unique<SeqScanExecutor>(it->second, table_name);
}

std::unique_ptr<Executor> Database::CreateFilterExecutor(
    std::unique_ptr<Executor> child,
    const std::string& column_name,
    const Value& value,
    ComparisonType comp_type) {
    return std::make_unique<FilterExecutor>(
        std::move(child), column_name, value, comp_type);
}

bool Database::Shutdown() {
    return SaveCatalog();
}

bool Database::LoadCatalog() {
    return true;
}

bool Database::SaveCatalog() {
    return true;
}

page_id_t Database::AllocateNewPage() {
    page_id_t page_id;
    Page* page = buffer_pool_manager_->NewPage(page_id);
    if (page) {
        buffer_pool_manager_->UnpinPage(page_id, true);
        return page_id;
    }
    return INVALID_PAGE_ID;
}

}
