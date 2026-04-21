#include "sql_executor.h"
#include "../catalog/catalog.h"
#include <iostream>

namespace minidb {

// SeqScanExecutor implementation
SeqScanExecutor::SeqScanExecutor(std::shared_ptr<TableInfo> table_info,
                                   const std::string& heap_filename,
                                   BufferPoolManager* buffer_pool)
    : table_info_(table_info), initialized_(false) {
    heap_file_ = std::make_unique<HeapFile>(heap_filename, buffer_pool);
    if (!heap_file_->Open()) {
        std::cerr << "Failed to open heap file: " << heap_filename << std::endl;
    }
}

bool SeqScanExecutor::Next(Tuple& tuple) {
    if (!initialized_) {
        if (!heap_file_->IsOpen()) {
            return false;
        }
        scan_iterator_.reset(heap_file_->CreateScanIterator());
        initialized_ = true;
    }
    
    if (!scan_iterator_->HasNext()) {
        return false;
    }
    
    char buffer[Tuple::GetSize()];
    uint32_t actual_size = 0;
    
    if (!scan_iterator_->NextRecord(buffer, Tuple::GetSize(), actual_size)) {
        return false;
    }
    
    tuple.Deserialize(buffer);
    return true;
}

bool SeqScanExecutor::Execute() {
    // SeqScan doesn't need explicit Execute, Next() handles iteration
    return heap_file_->IsOpen();
}

// IndexScanExecutor implementation
IndexScanExecutor::IndexScanExecutor(std::shared_ptr<TableInfo> table_info,
                                     const std::string& column_name,
                                     const Value& value,
                                     const std::string& heap_filename,
                                     BufferPoolManager* buffer_pool)
    : table_info_(table_info), column_name_(column_name), value_(value),
      has_result_(false), executed_(false) {
    // Get HeapFile from TableInfo (created during CreateTable)
    heap_file_ = table_info_->GetHeapFile();
    
    // Create B+ Tree index
    index_ = std::make_unique<BPlusTree<BufferPoolManager>>(
        buffer_pool, table_info_->GetRootPageId());
}

bool IndexScanExecutor::Next(Tuple& tuple) {
    if (!executed_) {
        Execute();
    }
    
    if (!has_result_) {
        return false;
    }
    
    if (!heap_file_->GetTuple(result_rid_, tuple)) {
        std::cerr << "[ERROR] IndexScanExecutor::Next: GetTuple failed" << std::endl;
        has_result_ = false;
        return false;
    }
    
    has_result_ = false; // Only one result for point lookup
    
    return true;
}

bool IndexScanExecutor::Execute() {
    if (executed_) {
        return true;
    }
    
    uint64_t search_key = value_.data.int_val;
    
    RID rid;
    if (!index_->Search(search_key, rid)) {
        std::cerr << "Key not found in index: " << search_key << std::endl;
        has_result_ = false;
        executed_ = true;
        return true;
    }
    
    result_rid_ = rid;
    has_result_ = true;
    executed_ = true;
    
    return true;
}

// IndexRangeScanExecutor implementation
IndexRangeScanExecutor::IndexRangeScanExecutor(std::shared_ptr<TableInfo> table_info,
                                               const std::string& column_name,
                                               const Value& lower_bound,
                                               const Value& upper_bound,
                                               const std::string& heap_filename,
                                               BufferPoolManager* buffer_pool)
    : table_info_(table_info), column_name_(column_name),
      lower_bound_(lower_bound), upper_bound_(upper_bound),
      current_index_(0), executed_(false) {
    // Get HeapFile from TableInfo (created during CreateTable)
    heap_file_ = table_info_->GetHeapFile();
    
    // Create B+ Tree index
    index_ = std::make_unique<BPlusTree<BufferPoolManager>>(
        buffer_pool, table_info_->GetRootPageId());
}

bool IndexRangeScanExecutor::Next(Tuple& tuple) {
    if (!executed_) {
        Execute();
    }
    
    if (current_index_ >= scan_results_.size()) {
        return false;
    }
    
    // Fetch tuple from heap file using RID
    const auto& [key, rid] = scan_results_[current_index_];
    
    if (!heap_file_->GetTuple(rid, tuple)) {
        std::cerr << "[ERROR] IndexRangeScanExecutor::Next: GetTuple failed" << std::endl;
        current_index_++;
        return false;
    }
    
    current_index_++;
    return true;
}

bool IndexRangeScanExecutor::Execute() {
    if (executed_) {
        return true;
    }
    
    // Use B+ Tree range scan
    uint64_t lower = lower_bound_.data.int_val;
    uint64_t upper = upper_bound_.data.int_val;
    
    index_->RangeScan(lower, upper, scan_results_);
    executed_ = true;
    
    return !scan_results_.empty();
}

// InsertExecutor implementation
InsertExecutor::InsertExecutor(std::shared_ptr<TableInfo> table_info,
                               const std::vector<Value>& values,
                               const std::string& heap_filename,
                               BufferPoolManager* buffer_pool)
    : table_info_(table_info), values_(values), executed_(false) {
    // Get HeapFile from TableInfo (created during CreateTable)
    heap_file_ = table_info_->GetHeapFile();
    
    // Create B+ Tree index
    index_ = std::make_unique<BPlusTree<BufferPoolManager>>(
        buffer_pool, table_info_->GetRootPageId());
}

bool InsertExecutor::Next(Tuple& tuple) {
    // Insert doesn't produce output tuples
    return false;
}

bool InsertExecutor::Execute() {
    if (executed_) {
        return true;
    }
    
    // Create tuple from values
    if (values_.size() < 2) {
        std::cerr << "Insert requires at least 2 values (id, value)" << std::endl;
        return false;
    }
    
    Tuple tuple(values_[0].data.int_val, values_[1].data.int_val);
    
    // Serialize tuple
    char buffer[Tuple::GetSize()];
    tuple.Serialize(buffer);
    
    // Insert into heap file and get RID
    RID rid;
    if (!heap_file_->InsertRecord(buffer, Tuple::GetSize(), rid)) {
        std::cerr << "Failed to insert record into heap file" << std::endl;
        return false;
    }
    
    // Insert (key, RID) into B+ Tree index
    uint64_t key = tuple.id;
    if (!index_->Insert(key, rid)) {
        std::cerr << "Failed to insert into B+ Tree index" << std::endl;
        return false;
    }
    
    // Update catalog with new root_page_id
    uint32_t new_root_page_id = index_->GetRootPageId();
    table_info_->SetRootPageId(new_root_page_id);
    
    executed_ = true;
    return true;
}

// ExecutorFactory implementation
std::unique_ptr<SQLExecutor> ExecutorFactory::CreateExecutor(
    PlanNode* plan,
    const std::string& heap_filename,
    BufferPoolManager* buffer_pool) {
    
    if (!plan) {
        return nullptr;
    }
    
    switch (plan->GetType()) {
        case PlanType::INSERT: {
            auto insert_plan = static_cast<InsertPlan*>(plan);
            return std::make_unique<InsertExecutor>(
                insert_plan->GetTableInfo(),
                insert_plan->GetValues(),
                heap_filename,
                buffer_pool);
        }
        case PlanType::INDEX_SCAN: {
            auto index_scan_plan = static_cast<IndexScanPlan*>(plan);
            return std::make_unique<IndexScanExecutor>(
                index_scan_plan->GetTableInfo(),
                index_scan_plan->GetColumnName(),
                index_scan_plan->GetValue(),
                heap_filename,
                buffer_pool);
        }
        case PlanType::INDEX_RANGE_SCAN: {
            auto index_range_scan_plan = static_cast<IndexRangeScanPlan*>(plan);
            return std::make_unique<IndexRangeScanExecutor>(
                index_range_scan_plan->GetTableInfo(),
                index_range_scan_plan->GetColumnName(),
                index_range_scan_plan->GetLowerBound(),
                index_range_scan_plan->GetUpperBound(),
                heap_filename,
                buffer_pool);
        }
        case PlanType::SEQ_SCAN:
        default:
            return nullptr;
    }
}

}
