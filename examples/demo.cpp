#include "../src/db/database.h"
#include "../src/common/types.h"
#include <iostream>
#include <memory>

using namespace minidb;

int main() {
    std::cout << "=== MiniDB Demo: Insert + Scan Milestone ===" << std::endl;
    
    Database db("test.db");
    
    std::cout << "\n1. Creating table 'users'..." << std::endl;
    std::vector<Column> columns = {
        Column("id", DataType::INTEGER),
        Column("name", DataType::VARCHAR),
        Column("age", DataType::INTEGER),
        Column("active", DataType::BOOLEAN)
    };
    
    if (!db.CreateTable("users", columns)) {
        std::cerr << "Failed to create table!" << std::endl;
        return 1;
    }
    std::cout << "Table created successfully!" << std::endl;
    
    std::cout << "\n2. Inserting records..." << std::endl;
    
    Record record1;
    record1.values = {
        Value{DataType::INTEGER, {.int_val = 1}},
        Value{DataType::VARCHAR, {.int_val = 0}, "Alice"},
        Value{DataType::INTEGER, {.int_val = 25}},
        Value{DataType::BOOLEAN, {.bool_val = true}}
    };
    
    Record record2;
    record2.values = {
        Value{DataType::INTEGER, {.int_val = 2}},
        Value{DataType::VARCHAR, {.int_val = 0}, "Bob"},
        Value{DataType::INTEGER, {.int_val = 30}},
        Value{DataType::BOOLEAN, {.bool_val = false}}
    };
    
    Record record3;
    record3.values = {
        Value{DataType::INTEGER, {.int_val = 3}},
        Value{DataType::VARCHAR, {.int_val = 0}, "Charlie"},
        Value{DataType::INTEGER, {.int_val = 35}},
        Value{DataType::BOOLEAN, {.bool_val = true}}
    };
    
    if (db.InsertIntoTable("users", record1) &&
        db.InsertIntoTable("users", record2) &&
        db.InsertIntoTable("users", record3)) {
        std::cout << "All records inserted successfully!" << std::endl;
    } else {
        std::cerr << "Failed to insert records!" << std::endl;
        return 1;
    }
    
    std::cout << "\n3. Scanning all records..." << std::endl;
    auto scanner = db.CreateSeqScanExecutor("users");
    if (!scanner) {
        std::cerr << "Failed to create scanner!" << std::endl;
        return 1;
    }
    
    Record result;
    int count = 0;
    std::cout << "ID | Name    | Age | Active" << std::endl;
    std::cout << "---|---------|-----|--------" << std::endl;
    
    while (scanner->Next(result)) {
        std::cout << result.values[0].data.int_val << "  | "
                  << result.values[1].varchar_val << " | "
                  << result.values[2].data.int_val << "   | "
                  << (result.values[3].data.bool_val ? "true" : "false") << std::endl;
        count++;
    }
    
    std::cout << "\nTotal records scanned: " << count << std::endl;
    
    std::cout << "\n4. Testing filter (age > 25)..." << std::endl;
    auto filter = db.CreateFilterExecutor(
        db.CreateSeqScanExecutor("users"),
        "age",
        Value{DataType::INTEGER, {.int_val = 25}},
        ComparisonType::GT
    );
    
    if (filter) {
        std::cout << "ID | Name    | Age | Active" << std::endl;
        std::cout << "---|---------|-----|--------" << std::endl;
        count = 0;
        while (filter->Next(result)) {
            std::cout << result.values[0].data.int_val << "  | "
                      << result.values[1].varchar_val << " | "
                      << result.values[2].data.int_val << "   | "
                      << (result.values[3].data.bool_val ? "true" : "false") << std::endl;
            count++;
        }
        std::cout << "\nRecords with age > 25: " << count << std::endl;
    }
    
    std::cout << "\n=== Demo completed successfully! ===" << std::endl;
    return 0;
}
