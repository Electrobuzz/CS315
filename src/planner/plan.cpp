#include "plan.h"
#include "../catalog/catalog.h"
#include <iostream>

namespace minidb {

// SeqScanPlan implementation
SeqScanPlan::SeqScanPlan(std::shared_ptr<TableInfo> table_info)
    : table_info_(table_info) {
}

std::shared_ptr<Schema> SeqScanPlan::GetOutputSchema() const {
    return table_info_->GetSchema();
}

// IndexScanPlan implementation
IndexScanPlan::IndexScanPlan(std::shared_ptr<TableInfo> table_info,
                            const std::string& column_name,
                            const Value& value)
    : table_info_(table_info), column_name_(column_name), value_(value) {
}

std::shared_ptr<Schema> IndexScanPlan::GetOutputSchema() const {
    return table_info_->GetSchema();
}

// IndexRangeScanPlan implementation
IndexRangeScanPlan::IndexRangeScanPlan(std::shared_ptr<TableInfo> table_info,
                                       const std::string& column_name,
                                       const Value& lower_bound,
                                       const Value& upper_bound)
    : table_info_(table_info), column_name_(column_name),
      lower_bound_(lower_bound), upper_bound_(upper_bound) {
}

std::shared_ptr<Schema> IndexRangeScanPlan::GetOutputSchema() const {
    return table_info_->GetSchema();
}

// InsertPlan implementation
InsertPlan::InsertPlan(std::shared_ptr<TableInfo> table_info,
                       const std::vector<Value>& values)
    : table_info_(table_info), values_(values) {
}

std::shared_ptr<Schema> InsertPlan::GetOutputSchema() const {
    return nullptr; // Insert doesn't produce output
}

// Planner implementation
Planner::Planner() {
}

std::unique_ptr<PlanNode> Planner::Plan(std::unique_ptr<ASTNode> ast) {
    if (!ast) {
        return nullptr;
    }
    
    switch (ast->type) {
        case ASTNodeType::CREATE_TABLE:
            return PlanCreateTable(static_cast<CreateTableStmt*>(ast.get()));
        case ASTNodeType::INSERT:
            return PlanInsert(static_cast<InsertStmt*>(ast.get()));
        case ASTNodeType::SELECT:
            return PlanSelect(static_cast<SelectStmt*>(ast.get()));
        default:
            return nullptr;
    }
}

std::unique_ptr<PlanNode> Planner::PlanCreateTable(CreateTableStmt* stmt) {
    // CREATE TABLE is handled during execution, not as a plan node
    // Return nullptr to indicate this is a DDL statement
    return nullptr;
}

std::unique_ptr<PlanNode> Planner::PlanInsert(InsertStmt* stmt) {
    auto table_info = Catalog::GetInstance().GetTable(stmt->table_name);
    if (!table_info) {
        return nullptr;
    }
    
    return std::make_unique<InsertPlan>(table_info, stmt->values);
}

std::unique_ptr<PlanNode> Planner::PlanSelect(SelectStmt* stmt) {
    auto table_info = Catalog::GetInstance().GetTable(stmt->table_name);
    if (!table_info) {
        return nullptr;
    }
    
    // Check if there's a predicate
    if (!stmt->predicate) {
        // No predicate: use sequential scan
        return std::make_unique<SeqScanPlan>(table_info);
    }
    
    // Check predicate type and choose appropriate plan
    if (stmt->predicate->type == PredicateType::EQUALS) {
        auto* equals_pred = static_cast<EqualsPredicate*>(stmt->predicate.get());
        // WHERE id = X: use index scan
        return std::make_unique<IndexScanPlan>(table_info, 
                                                equals_pred->column_name,
                                                equals_pred->value);
    } else if (stmt->predicate->type == PredicateType::BETWEEN) {
        auto* between_pred = static_cast<BetweenPredicate*>(stmt->predicate.get());
        // WHERE id BETWEEN A AND B: use index range scan
        return std::make_unique<IndexRangeScanPlan>(table_info,
                                                     between_pred->column_name,
                                                     between_pred->lower_bound,
                                                     between_pred->upper_bound);
    }
    
    // Fallback to sequential scan
    return std::make_unique<SeqScanPlan>(table_info);
}

}
