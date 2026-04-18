#pragma once

#include "page_v2.h"
#include "disk_manager_v2.h"
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

// Frame represents a single buffer pool frame containing a page
// Each frame tracks the page's state (pin count, dirty flag, etc.)
class Frame {
public:
    Frame() : pin_count_(0), is_dirty_(false) {}
    
    // Page access
    Page& GetPage() { return page_; }
    const Page& GetPage() const { return page_; }
    
    // Pin count management
    int GetPinCount() const { return pin_count_; }
    void IncrementPinCount() { pin_count_++; }
    void DecrementPinCount() { 
        if (pin_count_ > 0) pin_count_--; 
    }
    
    // Dirty flag management
    bool IsDirty() const { return is_dirty_; }
    void SetDirty(bool dirty) { is_dirty_ = dirty; }
    
    // LRU list iterator for replacement policy
    std::list<frame_id_t>::iterator GetLRUIterator() { return lru_iter_; }
    void SetLRUIterator(std::list<frame_id_t>::iterator iter) { lru_iter_ = iter; }

private:
    Page page_;                    // The actual page data
    int pin_count_;                // Number of users accessing this page
    bool is_dirty_;                // Whether page has been modified
    std::list<frame_id_t>::iterator lru_iter_;  // Position in LRU list
};

// BufferPoolManager manages in-memory page cache with LRU replacement
class BufferPoolManager {
public:
    BufferPoolManager(size_t pool_size, DiskManager* disk_manager);
    ~BufferPoolManager();
    
    // Core page operations
    
    // FetchPage: Load page into buffer pool, return pointer to page
    // If page is already in memory, just increment pin count
    // If not in memory, load from disk (evict if necessary)
    Page* FetchPage(page_id_t page_id);
    
    // UnpinPage: Decrease pin count, optionally mark as dirty
    // If pin count reaches 0, page becomes eligible for eviction
    bool UnpinPage(page_id_t page_id, bool is_dirty);
    
    // NewPage: Allocate a new page in buffer pool
    // Returns pointer to new page, sets output page_id
    Page* NewPage(page_id_t& page_id);
    
    // FlushPage: Write page back to disk if dirty
    bool FlushPage(page_id_t page_id);
    
    // FlushAllPages: Write all dirty pages back to disk
    void FlushAllPages();
    
    // DeletePage: Remove page from buffer pool and disk
    bool DeletePage(page_id_t page_id);
    
    // Utility functions
    size_t GetPoolSize() const { return pool_size_; }
    size_t GetPageTableSize() const { return page_table_.size(); }
    size_t GetFreeFrameCount() const;

private:
    // Helper methods
    
    // FindVictimFrame: Select a frame to evict using LRU policy
    // Returns INVALID_FRAME_ID if no evictable frame exists
    frame_id_t FindVictimFrame();
    
    // EvictFrame: Evict a frame from buffer pool
    // Writes dirty pages to disk before eviction
    bool EvictFrame(frame_id_t frame_id);
    
    // UpdateLRU: Move frame to most-recently-used position
    void UpdateLRU(frame_id_t frame_id);
    
    // WritePageToDisk: Write a page to disk
    bool WritePageToDisk(page_id_t page_id, Frame* frame);
    
    // ReadPageFromDisk: Read a page from disk
    bool ReadPageFromDisk(page_id_t page_id, Frame* frame);

private:
    size_t pool_size_;                              // Number of frames in buffer pool
    DiskManager* disk_manager_;                     // Disk manager for I/O operations
    
    std::vector<std::unique_ptr<Frame>> frames_;    // Array of frames
    std::unordered_map<page_id_t, frame_id_t> page_table_;  // Page ID → Frame ID mapping
    std::list<frame_id_t> lru_list_;                // LRU list (least recently used at front)
    std::vector<bool> free_list_;                   // Track free frames
    
    std::mutex latch_;                              // Mutex for thread safety
};

}
