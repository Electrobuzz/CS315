#pragma once

#include "page_v2.h"
#include "disk_manager_v2.h"
#include "log_manager.h"
#include <unordered_map>
#include <list>
#include <mutex>
#include <memory>
#include <vector>

namespace minidb {

using frame_id_t = uint32_t;
using page_id_t = uint32_t;

constexpr frame_id_t INVALID_FRAME_ID = 0;
constexpr size_t DEFAULT_BUFFER_POOL_SIZE = 10;

// WAL-enabled BufferPoolManager
// Ensures WAL protocol: log must be written BEFORE page is flushed
class BufferPoolManagerWAL {
public:
    BufferPoolManagerWAL(size_t pool_size, DiskManager* disk_manager, LogManager* log_manager);
    ~BufferPoolManagerWAL();
    
    // Core page operations (same as regular BufferPoolManager)
    Page* FetchPage(page_id_t page_id);
    bool UnpinPage(page_id_t page_id, bool is_dirty);
    Page* NewPage(page_id_t& page_id);
    bool FlushPage(page_id_t page_id);
    void FlushAllPages();
    bool DeletePage(page_id_t page_id);
    
    // WAL-specific operations
    lsn_t LogInsert(txn_id_t txn_id, page_id_t page_id, const char* record_data, uint32_t record_size);
    lsn_t LogDelete(txn_id_t txn_id, page_id_t page_id, const char* record_data, uint32_t record_size);
    lsn_t LogUpdate(txn_id_t txn_id, page_id_t page_id, const char* record_data, uint32_t record_size);
    lsn_t LogBegin(txn_id_t txn_id);
    lsn_t LogCommit(txn_id_t txn_id);
    lsn_t LogAbort(txn_id_t txn_id);
    
    // Utility functions
    size_t GetPoolSize() const { return pool_size_; }
    size_t GetPageTableSize() const { return page_table_.size(); }
    LogManager* GetLogManager() const { return log_manager_; }

private:
    // Helper methods (same as regular BufferPoolManager)
    frame_id_t FindVictimFrame();
    bool EvictFrame(frame_id_t frame_id);
    void UpdateLRU(frame_id_t frame_id);
    bool WritePageToDisk(page_id_t page_id, Frame* frame);
    bool ReadPageFromDisk(page_id_t page_id, Frame* frame);
    
    // WAL-specific helper
    bool EnsureLogFlushed(uint64_t page_lsn);

private:
    // Frame structure (same as regular BufferPoolManager)
    class Frame {
    public:
        Frame() : pin_count_(0), is_dirty_(0) {}
        
        Page& GetPage() { return page_; }
        const Page& GetPage() const { return page_; }
        
        int GetPinCount() const { return pin_count_; }
        void IncrementPinCount() { pin_count_++; }
        void DecrementPinCount() { 
            if (pin_count_ > 0) pin_count_--; 
        }
        
        bool IsDirty() const { return is_dirty_; }
        void SetDirty(bool dirty) { is_dirty_ = dirty; }
        
        std::list<frame_id_t>::iterator GetLRUIterator() { return lru_iter_; }
        void SetLRUIterator(std::list<frame_id_t>::iterator iter) { lru_iter_ = iter; }

    private:
        Page page_;
        int pin_count_;
        bool is_dirty_;
        std::list<frame_id_t>::iterator lru_iter_;
    };

private:
    size_t pool_size_;
    DiskManager* disk_manager_;
    LogManager* log_manager_;
    
    std::vector<std::unique_ptr<Frame>> frames_;
    std::unordered_map<page_id_t, frame_id_t> page_table_;
    std::list<frame_id_t> lru_list_;
    std::vector<bool> free_list_;
    
    std::mutex latch_;
};

}
