#pragma once

#include "../common/types.h"
#include <string>
#include <fstream>

namespace minidb {

class DiskManager {
public:
    explicit DiskManager(const std::string& db_file);
    ~DiskManager();
    
    void ReadPage(page_id_t page_id, char* page_data);
    void WritePage(page_id_t page_id, const char* page_data);
    page_id_t AllocatePage();
    void DeallocatePage(page_id_t page_id);
    
    size_t GetNumPages() const;
    void ShutDown();

private:
    std::fstream db_io_;
    std::string file_name_;
    page_id_t next_page_id_;
    std::mutex io_latch_;
    
    void OpenFile();
    void CloseFile();
};

}
