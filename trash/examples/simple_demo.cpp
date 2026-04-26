#include <iostream>
#include <vector>
#include <string>

// Simplified types for demo
enum class DataType { INTEGER, VARCHAR, BOOLEAN };

struct Value {
    DataType type;
    union {
        int int_val;
        bool bool_val;
    } data;
    std::string varchar_val;
};

struct Record {
    std::vector<Value> values;
};

// Simple table for demo
class SimpleTable {
public:
    void Insert(const Record& record) {
        records_.push_back(record);
    }
    
    void PrintAll() {
        std::cout << "Records in table:" << std::endl;
        for (size_t i = 0; i < records_.size(); ++i) {
            const auto& rec = records_[i];
            std::cout << "Record " << (i+1) << ": ";
            for (size_t j = 0; j < rec.values.size(); ++j) {
                const auto& val = rec.values[j];
                if (val.type == DataType::INTEGER) {
                    std::cout << val.data.int_val;
                } else if (val.type == DataType::VARCHAR) {
                    std::cout << val.varchar_val;
                } else if (val.type == DataType::BOOLEAN) {
                    std::cout << (val.data.bool_val ? "true" : "false");
                }
                if (j < rec.values.size() - 1) std::cout << ", ";
            }
            std::cout << std::endl;
        }
    }
    
    size_t Count() const { return records_.size(); }
    
    const std::vector<Record>& GetRecords() const { return records_; }

private:
    std::vector<Record> records_;
};

int main() {
    std::cout << "=== MiniDB Simple Demo: Insert + Scan Milestone ===" << std::endl;
    
    SimpleTable table;
    
    std::cout << "\n1. Inserting records..." << std::endl;
    
    Record record1;
    record1.values = {
        Value{DataType::INTEGER, {.int_val = 1}},
        Value{DataType::VARCHAR, {.int_val = 0}, "Alice"},
        Value{DataType::INTEGER, {.int_val = 25}},
        Value{DataType::BOOLEAN, {.bool_val = true}}
    };
    table.Insert(record1);
    
    Record record2;
    record2.values = {
        Value{DataType::INTEGER, {.int_val = 2}},
        Value{DataType::VARCHAR, {.int_val = 0}, "Bob"},
        Value{DataType::INTEGER, {.int_val = 30}},
        Value{DataType::BOOLEAN, {.bool_val = false}}
    };
    table.Insert(record2);
    
    Record record3;
    record3.values = {
        Value{DataType::INTEGER, {.int_val = 3}},
        Value{DataType::VARCHAR, {.int_val = 0}, "Charlie"},
        Value{DataType::INTEGER, {.int_val = 35}},
        Value{DataType::BOOLEAN, {.bool_val = true}}
    };
    table.Insert(record3);
    
    std::cout << "All records inserted successfully!" << std::endl;
    
    std::cout << "\n2. Scanning all records..." << std::endl;
    std::cout << "ID | Name    | Age | Active" << std::endl;
    std::cout << "---|---------|-----|--------" << std::endl;
    
    table.PrintAll();
    
    std::cout << "\nTotal records scanned: " << table.Count() << std::endl;
    
    std::cout << "\n3. Testing filter (age > 25)..." << std::endl;
    std::cout << "ID | Name    | Age | Active" << std::endl;
    std::cout << "---|---------|-----|--------" << std::endl;
    
    int filtered_count = 0;
    for (const auto& rec : table.GetRecords()) {
        if (rec.values[2].data.int_val > 25) {
            std::cout << rec.values[0].data.int_val << "  | "
                      << rec.values[1].varchar_val << " | "
                      << rec.values[2].data.int_val << "   | "
                      << (rec.values[3].data.bool_val ? "true" : "false") << std::endl;
            filtered_count++;
        }
    }
    
    std::cout << "\nRecords with age > 25: " << filtered_count << std::endl;
    
    std::cout << "\n=== Demo completed successfully! ===" << std::endl;
    std::cout << "\nThis demonstrates the core functionality:" << std::endl;
    std::cout << "- Record creation with multiple data types" << std::endl;
    std::cout << "- Insert operation" << std::endl;
    std::cout << "- Sequential scan operation" << std::endl;
    std::cout << "- Filter operation (WHERE clause equivalent)" << std::endl;
    
    return 0;
}
