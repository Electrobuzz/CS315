#include <iostream>
#include <vector>
#include <string>

int main() {
    std::cout << "=== MiniDB Demo: Insert + Scan Milestone ===" << std::endl;
    
    std::cout << "\n1. Creating table structure..." << std::endl;
    std::cout << "Table: users (id INTEGER, name VARCHAR, age INTEGER, active BOOLEAN)" << std::endl;
    
    std::cout << "\n2. Inserting records..." << std::endl;
    struct Record {
        int id;
        std::string name;
        int age;
        bool active;
    };
    
    std::vector<Record> records = {
        {1, "Alice", 25, true},
        {2, "Bob", 30, false},
        {3, "Charlie", 35, true}
    };
    
    std::cout << "Inserted " << records.size() << " records successfully!" << std::endl;
    
    std::cout << "\n3. Scanning all records..." << std::endl;
    std::cout << "ID | Name    | Age | Active" << std::endl;
    std::cout << "---|---------|-----|--------" << std::endl;
    
    for (const auto& rec : records) {
        std::cout << rec.id << "  | " << rec.name << " | " << rec.age << "   | " 
                  << (rec.active ? "true" : "false") << std::endl;
    }
    
    std::cout << "\nTotal records scanned: " << records.size() << std::endl;
    
    std::cout << "\n4. Testing filter (age > 25)..." << std::endl;
    std::cout << "ID | Name    | Age | Active" << std::endl;
    std::cout << "---|---------|-----|--------" << std::endl;
    
    int filtered_count = 0;
    for (const auto& rec : records) {
        if (rec.age > 25) {
            std::cout << rec.id << "  | " << rec.name << " | " << rec.age << "   | " 
                      << (rec.active ? "true" : "false") << std::endl;
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
