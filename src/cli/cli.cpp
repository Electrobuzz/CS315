#include "cli.h"
#include <iostream>
#include <sstream>
#include <iomanip>
#include <algorithm>

namespace minidb {

DatabaseCLI::DatabaseCLI(const std::string& db_file, const std::string& log_file, bool debug)
    : db_file_(db_file), log_file_(log_file), debug_(debug), running_(true) {
    
    disk_manager_ = std::make_unique<DiskManager>(db_file);
    log_manager_ = std::make_unique<LogManager>(log_file);
    buffer_pool_ = std::make_unique<BufferPoolManager>(BUFFER_POOL_SIZE, disk_manager_.get());
    
    // Use separate file for HeapFile data to avoid conflicts with B+ Tree index pages
    heap_file_ = db_file + "_heap";
    
    // Create separate DiskManager and BufferPoolManager for HeapFile
    heap_disk_manager_ = std::make_unique<DiskManager>(heap_file_);
    heap_buffer_pool_ = std::make_unique<BufferPoolManager>(BUFFER_POOL_SIZE, heap_disk_manager_.get());
    
    // Verify persistence on startup
    VerifyPersistence();
}

DatabaseCLI::~DatabaseCLI() {
    if (debug_) {
        std::cout << "[DEBUG] Shutting down CLI" << std::endl;
    }
}

void DatabaseCLI::Run() {
    std::cout << "MiniDB SQL Shell" << std::endl;
    std::cout.flush();
    std::cout << "Type 'help' for available commands, 'exit' to quit" << std::endl;
    std::cout.flush();
    std::cout << std::endl;
    std::cout.flush();
    
    while (running_) {
        std::string command = ReadLine();
        
        if (command.empty() && std::cin.eof()) {
            // EOF (Ctrl+D)
            std::cout << std::endl;
            break;
        }
        
        command = Trim(command);
        if (command.empty()) {
            continue;
        }
        
        ExecuteCommand(command);
    }
}

void DatabaseCLI::ExecuteCommand(const std::string& command) {
    // Check for special commands
    if (IsSpecialCommand(command)) {
        return;
    }
    
    // Parse SQL
    SQLParser parser(command);
    auto ast = parser.Parse();
    
    if (!ast) {
        PrintError("Failed to parse SQL command");
        return;
    }
    
    // Save AST type before moving
    ASTNodeType ast_type = ast->type;
    
    // Handle CREATE TABLE as DDL
    if (ast_type == ASTNodeType::CREATE_TABLE) {
        auto* create_stmt = static_cast<CreateTableStmt*>(ast.get());
        std::vector<Column> columns;
        for (const auto& col : create_stmt->columns) {
            columns.push_back(Column(col.name, col.type));
        }
        auto schema = std::make_shared<Schema>(columns);
        
        Catalog& catalog = Catalog::GetInstance();
        if (catalog.CreateTable(create_stmt->table_name, schema, INVALID_PAGE_ID, heap_file_, heap_buffer_pool_.get(), heap_disk_manager_.get())) {
            std::cout << "OK" << std::endl;
        } else {
            PrintError("Failed to create table");
        }
        return;
    }
    
    // Plan
    Planner planner;
    auto plan = planner.Plan(std::move(ast));
    
    if (!plan) {
        if (ast_type == ASTNodeType::INSERT) {
            PrintError("Table does not exist. Create it first with CREATE TABLE");
        } else if (ast_type == ASTNodeType::SELECT) {
            PrintError("Table does not exist or query failed");
        } else {
            PrintError("Failed to generate execution plan");
        }
        return;
    }
    
    if (debug_ && plan) {
        std::cout << "[DEBUG] Plan type: " << static_cast<int>(plan->GetType()) << std::endl;
    }
    
    // Execute
    Catalog& catalog = Catalog::GetInstance();
    // Use buffer_pool_ for B+ Tree operations
    auto executor = ExecutorFactory::CreateExecutor(plan.get(), buffer_pool_.get());
    if (!executor) {
        PrintError("Failed to create executor");
        return;
    }
    
    bool success = executor->Execute();
    
    if (!success) {
        PrintError("Failed to execute command");
        return;
    }
    
    // Update catalog root_page_id after INSERT operations
    // The InsertExecutor should have updated the table_info, but we need to ensure
    // the catalog has the latest root_page_id for subsequent queries
    if (plan && plan->GetType() == PlanType::INSERT) {
        auto insert_plan = static_cast<InsertPlan*>(plan.get());
        auto table_info = insert_plan->GetTableInfo();
        if (table_info) {
            // Force catalog to update by explicitly setting the root_page_id
            // This ensures the catalog's shared_ptr is updated for subsequent queries
            auto catalog_table_info = catalog.GetTable(table_info->GetTableName());
            if (catalog_table_info) {
                catalog_table_info->SetRootPageId(table_info->GetRootPageId());
            }
            if (debug_) {
                std::cout << "[DEBUG] After INSERT: root_page_id=" << table_info->GetRootPageId() << std::endl;
            }
        }
    }
    
    // Collect results for SELECT queries
    if (plan && (plan->GetType() == PlanType::SEQ_SCAN || 
                 plan->GetType() == PlanType::INDEX_SCAN || 
                 plan->GetType() == PlanType::INDEX_RANGE_SCAN)) {
        std::vector<Tuple> results;
        Tuple tuple;
        while (executor->Next(tuple)) {
            if (debug_) {
                std::cout << "[DEBUG] Fetched tuple: id=" << tuple.id << " value=" << tuple.value << std::endl;
            }
            results.push_back(tuple);
        }
        
        if (!results.empty()) {
            PrintResult(results);
        } else {
            std::cout << "(0 rows)" << std::endl;
        }
    } else {
        std::cout << "OK" << std::endl;
    }
}

void DatabaseCLI::PrintResult(const std::vector<Tuple>& tuples) {
    if (tuples.empty()) {
        return;
    }
    
    // Column widths
    const int id_width = 6;
    const int value_width = 7;
    
    // Print header
    std::cout << "+" << std::string(id_width, '-') << "+" << std::string(value_width, '-') << "+" << std::endl;
    std::cout << "|" << std::setw(id_width) << "id" << "|" << std::setw(value_width) << "value" << "|" << std::endl;
    std::cout << "+" << std::string(id_width, '-') << "+" << std::string(value_width, '-') << "+" << std::endl;
    
    // Print rows
    for (const auto& tuple : tuples) {
        std::cout << "|" << std::setw(id_width) << tuple.id 
                  << "|" << std::setw(value_width) << tuple.value << "|" << std::endl;
    }
    
    // Print footer
    std::cout << "+" << std::string(id_width, '-') << "+" << std::string(value_width, '-') << "+" << std::endl;
    std::cout << "(" << tuples.size() << " row" << (tuples.size() != 1 ? "s" : "") << ")" << std::endl;
}

void DatabaseCLI::PrintError(const std::string& message) {
    std::cout << "ERROR: " << message << std::endl;
}

void DatabaseCLI::PrintHelp() {
    std::cout << "Available commands:" << std::endl;
    std::cout << "  CREATE TABLE table_name (id INT, value INT)" << std::endl;
    std::cout << "  INSERT INTO table_name VALUES (id, value)" << std::endl;
    std::cout << "  SELECT * FROM table_name WHERE id = X" << std::endl;
    std::cout << "  SELECT * FROM table_name WHERE id BETWEEN A AND B" << std::endl;
    std::cout << "  SELECT * FROM table_name" << std::endl;
    std::cout << std::endl;
    std::cout << "Special commands:" << std::endl;
    std::cout << "  exit / quit - Exit the shell" << std::endl;
    std::cout << "  help       - Show this help message" << std::endl;
    std::cout << "  clear      - Clear the database (delete all tables)" << std::endl;
}

void DatabaseCLI::ClearDatabase() {
    std::cout << "Clearing database..." << std::endl;
    Catalog::GetInstance().Clear();
    
    // Delete database files
    std::remove(db_file_.c_str());
    std::remove(log_file_.c_str());
    
    // Reinitialize storage
    disk_manager_ = std::make_unique<DiskManager>(db_file_);
    log_manager_ = std::make_unique<LogManager>(log_file_);
    buffer_pool_ = std::make_unique<BufferPoolManager>(BUFFER_POOL_SIZE, disk_manager_.get());
    
    std::cout << "Database cleared" << std::endl;
}

void DatabaseCLI::VerifyPersistence() {
    Catalog& catalog = Catalog::GetInstance();
    auto tables = catalog.GetAllTables();
    
    if (!tables.empty()) {
        std::cout << "[Persistence Verified] Found " << tables.size() << " table(s) from previous session" << std::endl;
    }
}

std::string DatabaseCLI::ReadLine() {
    std::string line;
    std::cout << "db > ";
    std::getline(std::cin, line);
    return line;
}

std::string DatabaseCLI::Trim(const std::string& str) {
    size_t start = str.find_first_not_of(" \t\n\r");
    if (start == std::string::npos) {
        return "";
    }
    size_t end = str.find_last_not_of(" \t\n\r");
    return str.substr(start, end - start + 1);
}

bool DatabaseCLI::IsSpecialCommand(const std::string& command) {
    std::string lower = command;
    std::transform(lower.begin(), lower.end(), lower.begin(), ::tolower);
    
    if (lower == "exit" || lower == "quit") {
        running_ = false;
        return true;
    }
    
    if (lower == "help") {
        PrintHelp();
        return true;
    }
    
    if (lower == "clear") {
        ClearDatabase();
        return true;
    }
    
    return false;
}

} // namespace minidb
