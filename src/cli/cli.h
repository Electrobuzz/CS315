#ifndef CLI_H
#define CLI_H

#include <string>
#include <vector>
#include <memory>
#include "../catalog/catalog.h"
#include "../parser/parser.h"
#include "../planner/plan.h"
#include "../execution/sql_executor.h"
#include "../storage/buffer_pool_manager.h"
#include "../storage/disk_manager_v2.h"
#include "../storage/log_manager.h"
#include "../catalog/schema.h"

namespace minidb {

class DatabaseCLI {
public:
    DatabaseCLI(const std::string& db_file, const std::string& log_file, bool debug = false);
    ~DatabaseCLI();
    
    void Run();
    
private:
    void ExecuteCommand(const std::string& command);
    void PrintResult(const std::vector<Tuple>& tuples);
    void PrintError(const std::string& message);
    void PrintHelp();
    void ClearDatabase();
    void VerifyPersistence();
    
    std::string ReadLine();
    std::string Trim(const std::string& str);
    bool IsSpecialCommand(const std::string& command);
    
    std::string db_file_;
    std::string heap_file_;
    std::string log_file_;
    bool debug_;
    bool running_;
    
    std::unique_ptr<DiskManager> disk_manager_;
    std::unique_ptr<LogManager> log_manager_;
    std::unique_ptr<BufferPoolManager> buffer_pool_;
    
    // Separate DiskManager and BufferPoolManager for HeapFile
    std::unique_ptr<DiskManager> heap_disk_manager_;
    std::unique_ptr<BufferPoolManager> heap_buffer_pool_;
    std::unique_ptr<HeapFile> heap_file_instance_;
    
    static constexpr uint32_t BUFFER_POOL_SIZE = 100;
};

} // namespace minidb

#endif // CLI_H
