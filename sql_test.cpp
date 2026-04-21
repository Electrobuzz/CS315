#include "src/catalog/catalog.h"
#include "src/parser/parser.h"
#include "src/planner/plan.h"
#include "src/execution/sql_executor.h"
#include "src/storage/buffer_pool_manager.h"
#include "src/storage/disk_manager_v2.h"
#include "src/storage/log_manager.h"
#include "src/storage/page_v2.h"
#include <iostream>
#include <memory>

using namespace minidb;

bool TestParser() {
    std::cout << "=== Test: Parser ===" << std::endl;
    
    // Test CREATE TABLE
    SQLParser parser1("CREATE TABLE users (id INT, value INT)");
    auto ast1 = parser1.Parse();
    if (!ast1 || ast1->type != ASTNodeType::CREATE_TABLE) {
        std::cout << "  FAIL: CREATE TABLE parsing failed" << std::endl;
        return false;
    }
    auto* create_stmt = static_cast<CreateTableStmt*>(ast1.get());
    if (create_stmt->table_name != "users" || create_stmt->columns.size() != 2) {
        std::cout << "  FAIL: CREATE TABLE schema incorrect" << std::endl;
        return false;
    }
    std::cout << "  CREATE TABLE parsed correctly" << std::endl;
    
    // Test INSERT
    SQLParser parser2("INSERT INTO users VALUES (10, 100)");
    auto ast2 = parser2.Parse();
    if (!ast2 || ast2->type != ASTNodeType::INSERT) {
        std::cout << "  FAIL: INSERT parsing failed" << std::endl;
        return false;
    }
    auto* insert_stmt = static_cast<InsertStmt*>(ast2.get());
    if (insert_stmt->table_name != "users" || insert_stmt->values.size() != 2) {
        std::cout << "  FAIL: INSERT values incorrect" << std::endl;
        return false;
    }
    std::cout << "  INSERT parsed correctly" << std::endl;
    
    // Test SELECT with WHERE id = X
    SQLParser parser3("SELECT * FROM users WHERE id = 10");
    auto ast3 = parser3.Parse();
    if (!ast3 || ast3->type != ASTNodeType::SELECT) {
        std::cout << "  FAIL: SELECT parsing failed" << std::endl;
        return false;
    }
    auto* select_stmt = static_cast<SelectStmt*>(ast3.get());
    if (select_stmt->table_name != "users" || !select_stmt->predicate) {
        std::cout << "  FAIL: SELECT predicate missing" << std::endl;
        return false;
    }
    std::cout << "  SELECT with WHERE id = X parsed correctly" << std::endl;
    
    // Test SELECT with WHERE id BETWEEN
    SQLParser parser4("SELECT * FROM users WHERE id BETWEEN 10 AND 100");
    auto ast4 = parser4.Parse();
    if (!ast4 || ast4->type != ASTNodeType::SELECT) {
        std::cout << "  FAIL: SELECT BETWEEN parsing failed" << std::endl;
        return false;
    }
    auto* select_stmt4 = static_cast<SelectStmt*>(ast4.get());
    if (select_stmt4->predicate->type != PredicateType::BETWEEN) {
        std::cout << "  FAIL: BETWEEN predicate type incorrect" << std::endl;
        return false;
    }
    std::cout << "  SELECT with WHERE id BETWEEN parsed correctly" << std::endl;
    
    std::cout << "  ✅ PASS" << std::endl;
    return true;
}

bool TestCatalog() {
    std::cout << "=== Test: Catalog ===" << std::endl;
    
    Catalog& catalog = Catalog::GetInstance();
    catalog.Clear();
    
    // Create table
    std::vector<Column> columns = {
        Column("id", DataType::INTEGER),
        Column("value", DataType::INTEGER)
    };
    auto schema = std::make_shared<Schema>(columns);
    
    // Setup storage for catalog test
    const std::string db_file = "test_catalog.db";
    const std::string log_file = "test_catalog.log";
    std::remove(db_file.c_str());
    std::remove(log_file.c_str());
    
    DiskManager disk_manager(db_file);
    LogManager log_manager(log_file);
    BufferPoolManager buffer_pool(10, &disk_manager);
    
    bool created = catalog.CreateTable("users", schema, 1, db_file, &buffer_pool);
    
    if (!created) {
        std::cout << "  FAIL: Failed to create table" << std::endl;
        return false;
    }
    
    // Get table
    auto table_info = catalog.GetTable("users");
    if (!table_info) {
        std::cout << "  FAIL: Failed to get table" << std::endl;
        return false;
    }
    
    if (table_info->GetTableName() != "users") {
        std::cout << "  FAIL: Table name mismatch" << std::endl;
        return false;
    }
    
    if (table_info->GetRootPageId() != 1) {
        std::cout << "  FAIL: Root page ID mismatch" << std::endl;
        return false;
    }
    
    std::cout << "  PASS" << std::endl;
    return true;
}

bool TestPlanner() {
    std::cout << "=== Test: Planner ===" << std::endl;
    
    Catalog& catalog = Catalog::GetInstance();
    catalog.Clear();
    
    // Create schema and table
    std::vector<Column> columns = {
        Column("id", DataType::INTEGER),
        Column("value", DataType::INTEGER)
    };
    auto schema = std::make_shared<Schema>(columns);
    
    // Setup storage for planner test
    const std::string db_file = "test_planner.db";
    const std::string log_file = "test_planner.log";
    std::remove(db_file.c_str());
    std::remove(log_file.c_str());
    
    DiskManager disk_manager(db_file);
    LogManager log_manager(log_file);
    BufferPoolManager buffer_pool(10, &disk_manager);
    
    catalog.CreateTable("users", schema, 1, db_file, &buffer_pool);
    
    Planner planner;
    
    // Test SELECT without predicate -> should use SeqScan
    SQLParser parser1("SELECT * FROM users");
    auto ast1 = parser1.Parse();
    auto plan1 = planner.Plan(std::move(ast1));
    if (!plan1 || plan1->GetType() != PlanType::SEQ_SCAN) {
        std::cout << "  FAIL: Expected SeqScanPlan" << std::endl;
        return false;
    }
    std::cout << "  SELECT without predicate -> SeqScanPlan" << std::endl;
    
    // Test SELECT with WHERE id = X -> should use IndexScan
    SQLParser parser2("SELECT * FROM users WHERE id = 100");
    auto ast2 = parser2.Parse();
    auto plan2 = planner.Plan(std::move(ast2));
    if (!plan2 || plan2->GetType() != PlanType::INDEX_SCAN) {
        std::cout << "  FAIL: Expected IndexScanPlan" << std::endl;
        return false;
    }
    std::cout << "  SELECT with WHERE id = X -> IndexScanPlan" << std::endl;
    
    // Test SELECT with WHERE id BETWEEN -> should use IndexRangeScan
    SQLParser parser3("SELECT * FROM users WHERE id BETWEEN 100 AND 200");
    auto ast3 = parser3.Parse();
    auto plan3 = planner.Plan(std::move(ast3));
    if (!plan3 || plan3->GetType() != PlanType::INDEX_RANGE_SCAN) {
        std::cout << "  FAIL: Expected IndexRangeScanPlan" << std::endl;
        return false;
    }
    std::cout << "  SELECT with WHERE id BETWEEN -> IndexRangeScanPlan" << std::endl;
    
    // Test INSERT -> should use InsertPlan
    SQLParser parser4("INSERT INTO users VALUES (1, 10)");
    auto ast4 = parser4.Parse();
    auto plan4 = planner.Plan(std::move(ast4));
    if (!plan4 || plan4->GetType() != PlanType::INSERT) {
        std::cout << "  FAIL: Expected InsertPlan" << std::endl;
        return false;
    }
    std::cout << "  INSERT -> InsertPlan" << std::endl;
    
    std::cout << "  PASS" << std::endl;
    return true;
}

bool TestEndToEnd() {
    std::cout << "=== Test: End-to-End Pipeline ===" << std::endl;
    
    // Clean up catalog
    Catalog& catalog = Catalog::GetInstance();
    catalog.Clear();
    
    // Setup storage
    const std::string db_file = "test_sql.db";
    const std::string log_file = "test_sql.log";
    std::remove(db_file.c_str());
    std::remove(log_file.c_str());
    
    DiskManager disk_manager(db_file);
    LogManager log_manager(log_file);
    BufferPoolManager buffer_pool(100, &disk_manager);
    
    // Create table
    std::vector<Column> columns = {
        Column("id", DataType::INTEGER),
        Column("value", DataType::INTEGER)
    };
    auto schema = std::make_shared<Schema>(columns);
    catalog.CreateTable("users", schema, INVALID_PAGE_ID, db_file, &buffer_pool);  // B+ Tree will create root on first insert
    
    // Parse and plan CREATE TABLE (DDL - no plan node)
    SQLParser parser1("CREATE TABLE users (id INT, value INT)");
    auto ast1 = parser1.Parse();
    Planner planner;
    auto plan1 = planner.Plan(std::move(ast1));
    if (plan1 != nullptr) {
        std::cout << "  FAIL: CREATE TABLE should not produce plan node" << std::endl;
        return false;
    }
    std::cout << "  CREATE TABLE: DDL handled correctly" << std::endl;
    
    // Insert 1000 rows
    std::cout << "  Inserting 1000 rows..." << std::endl;
    for (int i = 1; i <= 1000; i++) {
        std::string sql = "INSERT INTO users VALUES (" + std::to_string(i) + ", " + std::to_string(i * 10) + ")";
        SQLParser parser(sql);
        auto ast = parser.Parse();
        auto plan = planner.Plan(std::move(ast));
        
        auto executor = ExecutorFactory::CreateExecutor(plan.get(), db_file, &buffer_pool);
        if (!executor) {
            std::cout << "  FAIL: Failed to create executor" << std::endl;
            return false;
        }
        
        if (!executor->Execute()) {
            std::cout << "  FAIL: Insert execution failed for row " << i << std::endl;
            return false;
        }
    }
    std::cout << "  Inserted 1000 rows successfully" << std::endl;
    
    // Query by key (id = 500)
    std::cout << "  Query by key (id = 500)..." << std::endl;
    SQLParser parser2("SELECT * FROM users WHERE id = 500");
    auto ast2 = parser2.Parse();
    auto plan2 = planner.Plan(std::move(ast2));
    
    auto executor2 = ExecutorFactory::CreateExecutor(plan2.get(), db_file, &buffer_pool);
    if (!executor2) {
        std::cout << "  FAIL: Failed to create executor" << std::endl;
        return false;
    }
    
    if (!executor2->Execute()) {
        std::cout << "  FAIL: Query execution failed" << std::endl;
        return false;
    }
    
    Tuple tuple;
    if (!executor2->Next(tuple)) {
        std::cout << "  FAIL: Query returned no results" << std::endl;
        return false;
    }
    
    if (tuple.id != 500 || tuple.value != 5000) {
        std::cout << "  FAIL: Query returned wrong tuple: id=" << tuple.id << " value=" << tuple.value << std::endl;
        return false;
    }
    std::cout << "  Query by key successful" << std::endl;
    
    // Range query (id BETWEEN 100 AND 200)
    std::cout << "  Range query (id BETWEEN 100 AND 200)..." << std::endl;
    SQLParser parser3("SELECT * FROM users WHERE id BETWEEN 100 AND 200");
    auto ast3 = parser3.Parse();
    auto plan3 = planner.Plan(std::move(ast3));
    
    auto executor3 = ExecutorFactory::CreateExecutor(plan3.get(), db_file, &buffer_pool);
    if (!executor3) {
        std::cout << "  FAIL: Failed to create executor" << std::endl;
        return false;
    }
    
    if (!executor3->Execute()) {
        std::cout << "  FAIL: Range query execution failed" << std::endl;
        return false;
    }
    
    int count = 0;
    while (executor3->Next(tuple)) {
        count++;
    }
    
    if (count != 101) {
        std::cout << "  FAIL: Range query returned " << count << " tuples, expected 101" << std::endl;
        return false;
    }
    std::cout << "  Range query returned " << count << " tuples" << std::endl;
    std::cout << "  Range query successful" << std::endl;
    
    // Cleanup
    std::remove(db_file.c_str());
    std::remove(log_file.c_str());
    
    std::cout << "  PASS" << std::endl;
    return true;
}

int main() {
    std::cout << "=== SQL Layer Test Suite ===" << std::endl;
    
    int passed = 0;
    int failed = 0;
    
    if (TestParser()) {
        passed++;
    } else {
        failed++;
    }
    
    if (TestCatalog()) {
        passed++;
    } else {
        failed++;
    }
    
    if (TestPlanner()) {
        passed++;
    } else {
        failed++;
    }
    
    if (TestEndToEnd()) {
        passed++;
    } else {
        failed++;
    }
    
    std::cout << "=== Test Summary ===" << std::endl;
    std::cout << "Total: " << (passed + failed) << std::endl;
    std::cout << "Passed: " << passed << std::endl;
    std::cout << "Failed: " << failed << std::endl;
    
    return (failed == 0) ? 0 : 1;
}
