#pragma once

#include "page_v2.h"
#include <string>
#include <fstream>
#include <mutex>

namespace minidb {

class DiskManager {
public:
    explicit DiskManager(const std::string& filename);
    ~DiskManager();
    
    // File operations
    bool Open();
    void Close();
    bool IsOpen() const { return file_stream_.is_open(); }
    
    // Page I/O operations
    bool ReadPage(uint32_t page_id, Page& page);
    bool WritePage(uint32_t page_id, const Page& page);
    
    // Page I/O operations with raw data (for BufferPoolManager compatibility)
    bool ReadPage(uint32_t page_id, char* data);
    bool WritePage(uint32_t page_id, const char* data);
    
    // Page allocation
    uint32_t AllocatePage();
    bool DeallocatePage(uint32_t page_id);
    
    // File metadata
    uint32_t GetPageCount() const;
    const std::string& GetFilename() const { return filename_; }

private:
    std::string filename_;
    std::fstream file_stream_;
    uint32_t next_page_id_;
    mutable std::mutex io_mutex_;
    
    // Internal helpers
    bool SeekToPage(uint32_t page_id);
    void InitializeNewFile();
    bool ValidatePageId(uint32_t page_id) const;
};

}
