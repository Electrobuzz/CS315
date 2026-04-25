#pragma once

#include "../common/types.h"
#include "../common/tuple.h"
#include "../catalog/schema.h"
#include "../planner/plan.h"
#include "../storage/heap_file.h"
#include "../storage/b_plus_tree.h"
#include "../storage/buffer_pool_manager.h"
#include "../storage/disk_manager_v2.h"
#include <memory>
#include <string>

namespace minidb {

// Base SQL executor interface
class SQLExecutor {
public:
    virtual ~SQLExecutor() = default;
    virtual bool Next(Tuple& tuple) = 0;
    virtual bool Execute() = 0;
};

// Sequential scan executor - scans entire heap file
class SeqScanExecutor : public SQLExecutor {
public:
    SeqScanExecutor(std::shared_ptr<TableInfo> table_info,
                    BufferPoolManager* buffer_pool);
    
    bool Next(Tuple& tuple) override;
    bool Execute() override;
    
private:
    std::shared_ptr<TableInfo> table_info_;
    HeapFile* heap_file_;
    std::unique_ptr<HeapFile::ScanIterator> scan_iterator_;
    bool initialized_;
};

// Index scan executor - uses B+ Tree for point lookup (WHERE id = X)
class IndexScanExecutor : public SQLExecutor {
public:
    IndexScanExecutor(std::shared_ptr<TableInfo> table_info,
                      const std::string& column_name,
                      const Value& value,
                      BufferPoolManager* buffer_pool);
    
    bool Next(Tuple& tuple) override;
    bool Execute() override;
    
private:
    std::shared_ptr<TableInfo> table_info_;
    std::string column_name_;
    Value value_;
    HeapFile* heap_file_;
    std::unique_ptr<BPlusTree<BufferPoolManager>> index_;
    RID result_rid_;
    bool has_result_;
    bool executed_;
};

// Index range scan executor - uses B+ Tree range scan (WHERE id BETWEEN A AND B)
class IndexRangeScanExecutor : public SQLExecutor {
public:
    IndexRangeScanExecutor(std::shared_ptr<TableInfo> table_info,
                          const std::string& column_name,
                          const Value& lower_bound,
                          const Value& upper_bound,
                          BufferPoolManager* buffer_pool);
    
    bool Next(Tuple& tuple) override;
    bool Execute() override;
    
private:
    std::shared_ptr<TableInfo> table_info_;
    std::string column_name_;
    Value lower_bound_;
    Value upper_bound_;
    HeapFile* heap_file_;
    std::unique_ptr<BPlusTree<BufferPoolManager>> index_;
    std::vector<std::pair<uint64_t, RID>> scan_results_;
    size_t current_index_;
    bool executed_;
};

// Insert executor - inserts into heap file and B+ Tree index
class InsertExecutor : public SQLExecutor {
public:
    InsertExecutor(std::shared_ptr<TableInfo> table_info,
                   const std::vector<Value>& values,
                   BufferPoolManager* buffer_pool);
    
    bool Next(Tuple& tuple) override;
    bool Execute() override;
    
private:
    std::shared_ptr<TableInfo> table_info_;
    std::vector<Value> values_;
    HeapFile* heap_file_;
    std::unique_ptr<BPlusTree<BufferPoolManager>> index_;
    bool executed_;
};

// Executor factory
class ExecutorFactory {
public:
    static std::unique_ptr<SQLExecutor> CreateExecutor(
        PlanNode* plan,
        BufferPoolManager* buffer_pool);
};

}
