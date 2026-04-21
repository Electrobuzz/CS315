#pragma once

#include "../parser/parser.h"
#include "../catalog/schema.h"
#include "../common/types.h"
#include <memory>
#include <string>

namespace minidb {

// Plan node types
enum class PlanType {
    SEQ_SCAN,
    INDEX_SCAN,
    INDEX_RANGE_SCAN,
    INSERT
};

// Base plan node
class PlanNode {
public:
    virtual ~PlanNode() = default;
    virtual PlanType GetType() const = 0;
    virtual std::shared_ptr<Schema> GetOutputSchema() const = 0;
};

// Sequential scan plan (scan entire table)
class SeqScanPlan : public PlanNode {
public:
    SeqScanPlan(std::shared_ptr<TableInfo> table_info);
    
    PlanType GetType() const override { return PlanType::SEQ_SCAN; }
    std::shared_ptr<Schema> GetOutputSchema() const override;
    
    std::shared_ptr<TableInfo> GetTableInfo() const { return table_info_; }

private:
    std::shared_ptr<TableInfo> table_info_;
};

// Index scan plan (point lookup: WHERE id = X)
class IndexScanPlan : public PlanNode {
public:
    IndexScanPlan(std::shared_ptr<TableInfo> table_info,
                 const std::string& column_name,
                 const Value& value);
    
    PlanType GetType() const override { return PlanType::INDEX_SCAN; }
    std::shared_ptr<Schema> GetOutputSchema() const override;
    
    std::shared_ptr<TableInfo> GetTableInfo() const { return table_info_; }
    const std::string& GetColumnName() const { return column_name_; }
    const Value& GetValue() const { return value_; }

private:
    std::shared_ptr<TableInfo> table_info_;
    std::string column_name_;
    Value value_;
};

// Index range scan plan (WHERE id BETWEEN A AND B)
class IndexRangeScanPlan : public PlanNode {
public:
    IndexRangeScanPlan(std::shared_ptr<TableInfo> table_info,
                      const std::string& column_name,
                      const Value& lower_bound,
                      const Value& upper_bound);
    
    PlanType GetType() const override { return PlanType::INDEX_RANGE_SCAN; }
    std::shared_ptr<Schema> GetOutputSchema() const override;
    
    std::shared_ptr<TableInfo> GetTableInfo() const { return table_info_; }
    const std::string& GetColumnName() const { return column_name_; }
    const Value& GetLowerBound() const { return lower_bound_; }
    const Value& GetUpperBound() const { return upper_bound_; }

private:
    std::shared_ptr<TableInfo> table_info_;
    std::string column_name_;
    Value lower_bound_;
    Value upper_bound_;
};

// Insert plan
class InsertPlan : public PlanNode {
public:
    InsertPlan(std::shared_ptr<TableInfo> table_info,
              const std::vector<Value>& values);
    
    PlanType GetType() const override { return PlanType::INSERT; }
    std::shared_ptr<Schema> GetOutputSchema() const override;
    
    std::shared_ptr<TableInfo> GetTableInfo() const { return table_info_; }
    const std::vector<Value>& GetValues() const { return values_; }

private:
    std::shared_ptr<TableInfo> table_info_;
    std::vector<Value> values_;
};

// Planner class
class Planner {
public:
    Planner();
    
    // Convert AST to execution plan
    std::unique_ptr<PlanNode> Plan(std::unique_ptr<ASTNode> ast);

private:
    std::unique_ptr<PlanNode> PlanCreateTable(CreateTableStmt* stmt);
    std::unique_ptr<PlanNode> PlanInsert(InsertStmt* stmt);
    std::unique_ptr<PlanNode> PlanSelect(SelectStmt* stmt);
};

}
