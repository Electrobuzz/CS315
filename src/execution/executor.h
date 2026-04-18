#pragma once

#include "../common/types.h"
#include "../catalog/schema.h"
#include <memory>

namespace minidb {

class Executor {
public:
    virtual ~Executor() = default;
    virtual bool Next(Record& record) = 0;
    virtual std::shared_ptr<Schema> GetOutputSchema() const = 0;
};

class SeqScanExecutor : public Executor {
public:
    SeqScanExecutor(std::shared_ptr<TableInfo> table_info, 
                    const std::string& table_name);
    
    bool Next(Record& record) override;
    std::shared_ptr<Schema> GetOutputSchema() const override;

private:
    std::shared_ptr<TableInfo> table_info_;
    std::string table_name_;
    page_id_t current_page_id_;
    uint32_t current_slot_id_;
    bool is_initialized_;
};

class FilterExecutor : public Executor {
public:
    FilterExecutor(std::unique_ptr<Executor> child,
                   const std::string& column_name,
                   const Value& value,
                   ComparisonType comp_type);
    
    bool Next(Record& record) override;
    std::shared_ptr<Schema> GetOutputSchema() const override;

private:
    std::unique_ptr<Executor> child_;
    std::string column_name_;
    Value filter_value_;
    ComparisonType comp_type_;
    column_id_t column_id_;
};

class ProjectionExecutor : public Executor {
public:
    ProjectionExecutor(std::unique_ptr<Executor> child,
                       const std::vector<std::string>& column_names);
    
    bool Next(Record& record) override;
    std::shared_ptr<Schema> GetOutputSchema() const override;

private:
    std::unique_ptr<Executor> child_;
    std::vector<column_id_t> column_ids_;
    std::shared_ptr<Schema> output_schema_;
};

}
