#include "buffer_pool.h"
#include <iostream>
#include <stdexcept>

namespace minidb {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager* disk_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager) {
    
    if (pool_size == 0) {
        throw std::invalid_argument("Buffer pool size must be greater than 0");
    }
    
    if (!disk_manager) {
        throw std::invalid_argument("Disk manager cannot be null");
    }
    
    // Initialize frames
    frames_.resize(pool_size_);
    for (size_t i = 0; i < pool_size_; ++i) {
        frames_[i] = std::make_unique<Frame>();
    }
    
    // Initialize free list (all frames are initially free)
    free_list_.resize(pool_size_, true);
}

BufferPoolManager::~BufferPoolManager() {
    FlushAllPages();
}

Page* BufferPoolManager::FetchPage(page_id_t page_id) {
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

bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
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

Page* BufferPoolManager::NewPage(page_id_t& page_id) {
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
    
    // Update page table and LRU
    page_table_[page_id] = frame_id;
    UpdateLRU(frame_id);
    
    return &frame->GetPage();
}

bool BufferPoolManager::FlushPage(page_id_t page_id) {
    std::lock_guard<std::mutex> guard(latch_);
    
    auto it = page_table_.find(page_id);
    if (it == page_table_.end()) {
        return false;  // Page not in buffer pool
    }
    
    frame_id_t frame_id = it->second;
    Frame* frame = frames_[frame_id].get();
    
    if (frame->IsDirty()) {
        if (!WritePageToDisk(page_id, frame)) {
            return false;
        }
        frame->SetDirty(false);
    }
    
    return true;
}

void BufferPoolManager::FlushAllPages() {
    std::lock_guard<std::mutex> guard(latch_);
    
    for (const auto& [page_id, frame_id] : page_table_) {
        Frame* frame = frames_[frame_id].get();
        if (frame->IsDirty()) {
            WritePageToDisk(page_id, frame);
            frame->SetDirty(false);
        }
    }
}

bool BufferPoolManager::DeletePage(page_id_t page_id) {
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

size_t BufferPoolManager::GetFreeFrameCount() const {
    std::lock_guard<std::mutex> guard(latch_);
    
    size_t count = 0;
    for (bool is_free : free_list_) {
        if (is_free) count++;
    }
    return count;
}

frame_id_t BufferPoolManager::FindVictimFrame() {
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

bool BufferPoolManager::EvictFrame(frame_id_t frame_id) {
    Frame* frame = frames_[frame_id].get();
    
    if (frame->GetPinCount() > 0) {
        return false;  // Cannot evict pinned page
    }
    
    if (frame->IsDirty()) {
        page_id_t page_id = frame->GetPage().GetPageId();
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

void BufferPoolManager::UpdateLRU(frame_id_t frame_id) {
    Frame* frame = frames_[frame_id].get();
    
    // Remove from current position if in list
    if (frame->GetLRUIterator() != lru_list_.end()) {
        lru_list_.erase(frame->GetLRUIterator());
    }
    
    // Add to end (most recently used)
    lru_list_.push_back(frame_id);
    frame->SetLRUIterator(std::prev(lru_list_.end()));
}

bool BufferPoolManager::WritePageToDisk(page_id_t page_id, Frame* frame) {
    return disk_manager_->WritePage(page_id, frame->GetPage());
}

bool BufferPoolManager::ReadPageFromDisk(page_id_t page_id, Frame* frame) {
    return disk_manager_->ReadPage(page_id, frame->GetPage());
}

}
