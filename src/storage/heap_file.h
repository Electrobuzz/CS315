#pragma once

#include "page_v2.h"
#include "disk_manager_v2.h"
#include "buffer_pool_manager.h"
#include "../common/tuple.h"
#include "../common/types.h"
#include <vector>
#include <memory>

namespace minidb {

class HeapFile {
public:
    HeapFile(const std::string& filename, BufferPoolManager* buffer_pool = nullptr);
    ~HeapFile();
    
    // File operations
    bool Open();
    void Close();
    bool IsOpen() const { return disk_manager_ != nullptr && disk_manager_->IsOpen(); }
    
    // Record operations
    bool InsertRecord(const char* record_data, uint32_t record_size, RID& rid);
    bool GetTuple(RID rid, Tuple& tuple);
    
    // Scan operations
    class ScanIterator {
    public:
        ScanIterator(HeapFile* heap_file);
        ~ScanIterator();
        
        bool HasNext();
        bool NextRecord(char* buffer, uint32_t buffer_size, uint32_t& actual_size);
        void Reset();
        
    private:
        HeapFile* heap_file_;
        uint32_t current_page_id_;
        uint32_t current_slot_id_;
        Page current_page_;
        bool page_loaded_;
        
        bool LoadNextPage();
        bool FindNextValidSlot();
    };
    
    ScanIterator* CreateScanIterator();
    
    // File metadata
    uint32_t GetPageCount() const;
    uint32_t GetRecordCount() const;

private:
    std::unique_ptr<DiskManager> disk_manager_;
    BufferPoolManager* buffer_pool_;  // Not owned, for GetTuple operations
    uint32_t record_count_;
    
    // Internal helper methods
    bool FindSpaceInExistingPages(const char* record_data, uint32_t record_size, RID& rid);
    bool AllocateNewPage();
    bool WriteCurrentPage();
    
    // Page management
    std::vector<uint32_t> page_ids_;  // Track all pages in this heap file
    uint32_t current_page_index_;
    Page current_page_;
};

}
