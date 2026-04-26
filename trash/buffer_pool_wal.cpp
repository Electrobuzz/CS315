#include "buffer_pool_wal.h"
#include <iostream>
#include <stdexcept>

namespace minidb {

BufferPoolManagerWAL::BufferPoolManagerWAL(size_t pool_size, DiskManager* disk_manager, LogManager* log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
    
    if (pool_size == 0) {
        throw std::invalid_argument("Buffer pool size must be greater than 0");
    }
    
    if (!disk_manager) {
        throw std::invalid_argument("Disk manager cannot be null");
    }
    
    if (!log_manager) {
        throw std::invalid_argument("Log manager cannot be null");
    }
    
    // Initialize frames
    frames_.resize(pool_size_);
    for (size_t i = 0; i < pool_size_; ++i) {
        frames_[i] = std::make_unique<Frame>();
    }
    
    // Initialize free list
    free_list_.resize(pool_size_, true);
}

BufferPoolManagerWAL::~BufferPoolManagerWAL() {
    FlushAllPages();
}

Page* BufferPoolManagerWAL::FetchPage(page_id_t page_id) {
    std::lock_guard<std::mutex> guard(latch_);
    
    // Check if page is already in buffer pool
    auto it = page_table_.find(page_id);
    if (it != page_table_.end()) {
        frame_id_t frame_id = it->second;
        Frame* frame = frames_[frame_id].get();
        
        // Increment pin count
        frame->IncrementPinCount();
        
        // Update LRU (move to most recently used)
        UpdateLRU(frame_id);
        
        return &frame->GetPage();
    }
    
    // Page not in buffer pool, need to load from disk
    frame_id_t frame_id = FindVictimFrame();
    if (frame_id == INVALID_FRAME_ID) {
        return nullptr;  // No available frame
    }
    
    Frame* frame = frames_[frame_id].get();
    
    // Evict current page if frame is occupied
    if (frame->GetPage().GetPageId() != INVALID_PAGE_ID) {
        page_id_t old_page_id = frame->GetPage().GetPageId();
        if (!EvictFrame(frame_id)) {
            return nullptr;  // Eviction failed
        }
        page_table_.erase(old_page_id);
    }
    
    // Read page from disk
    if (!ReadPageFromDisk(page_id, frame)) {
        return nullptr;  // Read failed
    }
    
    frame->GetPage().SetPageId(page_id);
    frame->IncrementPinCount();
    frame->SetDirty(false);
    
    // Update page table and LRU
    page_table_[page_id] = frame_id;
    UpdateLRU(frame_id);
    
    return &frame->GetPage();
}

bool BufferPoolManagerWAL::UnpinPage(page_id_t page_id, bool is_dirty) {
    std::lock_guard<std::mutex> guard(latch_);
    
    auto it = page_table_.find(page_id);
    if (it == page_table_.end()) {
        return false;  // Page not in buffer pool
    }
    
    frame_id_t frame_id = it->second;
    Frame* frame = frames_[frame_id].get();
    
    if (frame->GetPinCount() <= 0) {
        return false;  // Page is already unpinned
    }
    
    frame->DecrementPinCount();
    
    if (is_dirty) {
        frame->SetDirty(true);
    }
    
    return true;
}

Page* BufferPoolManagerWAL::NewPage(page_id_t& page_id) {
    std::lock_guard<std::mutex> guard(latch_);
    
    // Allocate new page on disk
    page_id = disk_manager_->AllocatePage();
    if (page_id == INVALID_PAGE_ID) {
        return nullptr;
    }
    
    // Find a free frame
    frame_id_t frame_id = FindVictimFrame();
    if (frame_id == INVALID_FRAME_ID) {
        return nullptr;  // No available frame
    }
    
    Frame* frame = frames_[frame_id].get();
    
    // Evict current page if frame is occupied
    if (frame->GetPage().GetPageId() != INVALID_PAGE_ID) {
        page_id_t old_page_id = frame->GetPage().GetPageId();
        if (!EvictFrame(frame_id)) {
            return nullptr;
        }
        page_table_.erase(old_page_id);
    }
    
    // Initialize new page
    frame->GetPage().Reset();
    frame->GetPage().SetPageId(page_id);
    frame->IncrementPinCount();
    frame->SetDirty(false);
    frame->GetPage().SetLastLSN(0);
    
    // Update page table and LRU
    page_table_[page_id] = frame_id;
    UpdateLRU(frame_id);
    
    return &frame->GetPage();
}

bool BufferPoolManagerWAL::FlushPage(page_id_t page_id) {
    std::lock_guard<std::mutex> guard(latch_);
    
    auto it = page_table_.find(page_id);
    if (it == page_table_.end()) {
        return false;  // Page not in buffer pool
    }
    
    frame_id_t frame_id = it->second;
    Frame* frame = frames_[frame_id].get();
    
    if (frame->IsDirty()) {
        // WAL Protocol: Ensure log is flushed up to page's LSN before flushing page
        uint64_t page_lsn = frame->GetPage().GetLastLSN();
        if (!EnsureLogFlushed(page_lsn)) {
            return false;  // Log flush failed
        }
        
        if (!WritePageToDisk(page_id, frame)) {
            return false;
        }
        frame->SetDirty(false);
    }
    
    return true;
}

void BufferPoolManagerWAL::FlushAllPages() {
    std::lock_guard<std::mutex> guard(latch_);
    
    for (const auto& [page_id, frame_id] : page_table_) {
        Frame* frame = frames_[frame_id].get();
        if (frame->IsDirty()) {
            uint64_t page_lsn = frame->GetPage().GetLastLSN();
            EnsureLogFlushed(page_lsn);
            WritePageToDisk(page_id, frame);
            frame->SetDirty(false);
        }
    }
}

bool BufferPoolManagerWAL::DeletePage(page_id_t page_id) {
    std::lock_guard<std::mutex> guard(latch_);
    
    auto it = page_table_.find(page_id);
    if (it == page_table_.end()) {
        return true;  // Page not in buffer pool, nothing to delete
    }
    
    frame_id_t frame_id = it->second;
    Frame* frame = frames_[frame_id].get();
    
    if (frame->GetPinCount() > 0) {
        return false;  // Cannot delete pinned page
    }
    
    if (frame->IsDirty()) {
        uint64_t page_lsn = frame->GetPage().GetLastLSN();
        EnsureLogFlushed(page_lsn);
        WritePageToDisk(page_id, frame);
    }
    
    page_table_.erase(it);
    frame->GetPage().SetPageId(INVALID_PAGE_ID);
    frame->SetDirty(false);
    
    // Remove from LRU list
    lru_list_.erase(frame->GetLRUIterator());
    frame->SetLRUIterator(lru_list_.end());
    
    // Mark frame as free
    free_list_[frame_id] = true;
    
    return disk_manager_->DeallocatePage(page_id);
}

// WAL-specific logging operations
lsn_t BufferPoolManagerWAL::LogInsert(txn_id_t txn_id, page_id_t page_id, 
                                       const char* record_data, uint32_t record_size) {
    LogRecord record(LogRecordType::INSERT, txn_id, page_id, record_data, record_size);
    return log_manager_->AppendLogRecord(record);
}

lsn_t BufferPoolManagerWAL::LogDelete(txn_id_t txn_id, page_id_t page_id, 
                                       const char* record_data, uint32_t record_size) {
    LogRecord record(LogRecordType::DELETE, txn_id, page_id, record_data, record_size);
    return log_manager_->AppendLogRecord(record);
}

lsn_t BufferPoolManagerWAL::LogUpdate(txn_id_t txn_id, page_id_t page_id, 
                                       const char* record_data, uint32_t record_size) {
    LogRecord record(LogRecordType::UPDATE, txn_id, page_id, record_data, record_size);
    return log_manager_->AppendLogRecord(record);
}

lsn_t BufferPoolManagerWAL::LogBegin(txn_id_t txn_id) {
    LogRecord record(LogRecordType::BEGIN, txn_id, 0);
    return log_manager_->AppendLogRecord(record);
}

lsn_t BufferPoolManagerWAL::LogCommit(txn_id_t txn_id) {
    LogRecord record(LogRecordType::COMMIT, txn_id, 0);
    lsn_t lsn = log_manager_->AppendLogRecord(record);
    // Flush log immediately for commit
    if (lsn != INVALID_LSN) {
        log_manager_->FlushLog(lsn);
    }
    return lsn;
}

lsn_t BufferPoolManagerWAL::LogAbort(txn_id_t txn_id) {
    LogRecord record(LogRecordType::ABORT, txn_id, 0);
    return log_manager_->AppendLogRecord(record);
}

// Helper methods
frame_id_t BufferPoolManagerWAL::FindVictimFrame() {
    // First check for completely free frames
    for (size_t i = 0; i < pool_size_; ++i) {
        if (free_list_[i]) {
            free_list_[i] = false;
            return static_cast<frame_id_t>(i);
        }
    }
    
    // No free frames, use LRU eviction
    if (!lru_list_.empty()) {
        frame_id_t frame_id = lru_list_.front();
        Frame* frame = frames_[frame_id].get();
        
        // Can only evict unpinned pages
        if (frame->GetPinCount() == 0) {
            lru_list_.pop_front();
            frame->SetLRUIterator(lru_list_.end());
            return frame_id;
        }
    }
    
    return INVALID_FRAME_ID;  // No evictable frame
}

bool BufferPoolManagerWAL::EvictFrame(frame_id_t frame_id) {
    Frame* frame = frames_[frame_id].get();
    
    if (frame->GetPinCount() > 0) {
        return false;  // Cannot evict pinned page
    }
    
    if (frame->IsDirty()) {
        page_id_t page_id = frame->GetPage().GetPageId();
        
        // WAL Protocol: Ensure log is flushed before eviction
        uint64_t page_lsn = frame->GetPage().GetLastLSN();
        if (!EnsureLogFlushed(page_lsn)) {
            return false;
        }
        
        if (!WritePageToDisk(page_id, frame)) {
            return false;
        }
        frame->SetDirty(false);
    }
    
    // Remove from LRU list
    lru_list_.erase(frame->GetLRUIterator());
    frame->SetLRUIterator(lru_list_.end());
    
    return true;
}

void BufferPoolManagerWAL::UpdateLRU(frame_id_t frame_id) {
    Frame* frame = frames_[frame_id].get();
    
    // Remove from current position if in list
    if (frame->GetLRUIterator() != lru_list_.end()) {
        lru_list_.erase(frame->GetLRUIterator());
    }
    
    // Add to end (most recently used)
    lru_list_.push_back(frame_id);
    frame->SetLRUIterator(std::prev(lru_list_.end()));
}

bool BufferPoolManagerWAL::WritePageToDisk(page_id_t page_id, Frame* frame) {
    return disk_manager_->WritePage(page_id, frame->GetPage());
}

bool BufferPoolManagerWAL::ReadPageFromDisk(page_id_t page_id, Frame* frame) {
    return disk_manager_->ReadPage(page_id, frame->GetPage());
}

bool BufferPoolManagerWAL::EnsureLogFlushed(uint64_t page_lsn) {
    if (page_lsn == 0) {
        return true;  // No log record associated with this page
    }
    
    // Ensure log is flushed up to the page's LSN
    return log_manager_->FlushLog(page_lsn);
}

}
