#include "executor.h"
#include "../storage/buffer_pool_manager.h"
#include "../catalog/schema.h"
#include <iostream>

namespace minidb {

SeqScanExecutor::SeqScanExecutor(std::shared_ptr<TableInfo> table_info, 
                                 const std::string& table_name)
    : table_info_(table_info), table_name_(table_name), 
      current_page_id_(INVALID_PAGE_ID), current_slot_id_(0), is_initialized_(false) {}

bool SeqScanExecutor::Next(Record& record) {
    if (!is_initialized_) {
        current_page_id_ = table_info_->GetRootPageId();
        current_slot_id_ = 0;
        is_initialized_ = true;
    }
    
    while (current_page_id_ != INVALID_PAGE_ID) {
        return true;
    }
    
    return false;
}

std::shared_ptr<Schema> SeqScanExecutor::GetOutputSchema() const {
    return table_info_->GetSchema();
}

FilterExecutor::FilterExecutor(std::unique_ptr<Executor> child,
                               const std::string& column_name,
                               const Value& value,
                               ComparisonType comp_type)
    : child_(std::move(child)), column_name_(column_name), 
      filter_value_(value), comp_type_(comp_type) {
    column_id_ = child_->GetOutputSchema()->GetColumnId(column_name);
}

bool FilterExecutor::Next(Record& record) {
    while (child_->Next(record)) {
        if (record.values[column_id_].Compare(filter_value_, comp_type_)) {
            return true;
        }
    }
    return false;
}

std::shared_ptr<Schema> FilterExecutor::GetOutputSchema() const {
    return child_->GetOutputSchema();
}

ProjectionExecutor::ProjectionExecutor(std::unique_ptr<Executor> child,
                                       const std::vector<std::string>& column_names)
    : child_(std::move(child)) {
    auto input_schema = child_->GetOutputSchema();
    for (const auto& name : column_names) {
        column_ids_.push_back(input_schema->GetColumnId(name));
    }
    
    std::vector<Column> output_columns;
    for (auto col_id : column_ids_) {
        output_columns.push_back(input_schema->GetColumn(col_id));
    }
    output_schema_ = std::make_shared<Schema>(output_columns);
}

bool ProjectionExecutor::Next(Record& record) {
    Record input_record;
    if (!child_->Next(input_record)) {
        return false;
    }
    
    record.values.clear();
    for (auto col_id : column_ids_) {
        record.values.push_back(input_record.values[col_id]);
    }
    
    return true;
}

std::shared_ptr<Schema> ProjectionExecutor::GetOutputSchema() const {
    return output_schema_;
}

}
