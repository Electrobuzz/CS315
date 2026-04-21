#include "buffer_pool_manager.h"
#include "disk_manager.h"
#include <stdexcept>
#include <iostream>

namespace minidb {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager* disk_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager) {
    frames_.resize(pool_size_);
    free_list_.resize(pool_size_, true);
    
    for (size_t i = 0; i < pool_size_; ++i) {
        frames_[i] = std::make_unique<Frame>();
        frames_[i]->is_dirty = false;
        frames_[i]->pin_count = 0;
        frames_[i]->lru_iter = lru_list_.end();
    }
}

BufferPoolManager::~BufferPoolManager() {
    FlushAllPages();
}

Page* BufferPoolManager::FetchPage(page_id_t page_id) {
    std::lock_guard<std::mutex> guard(latch_);
    
    auto it = page_table_.find(page_id);
    if (it != page_table_.end()) {
        frame_id_t frame_id = it->second;
        frames_[frame_id]->pin_count++;
        UpdateLRU(frame_id);
        return &frames_[frame_id]->page;
    }
    
    frame_id_t frame_id = FindVictimFrame();
    if (frame_id == INVALID_FRAME_ID) {
        return nullptr;
    }
    
    Frame* frame = frames_[frame_id].get();
    
    if (frame->is_dirty) {
        WritePageToDisk(frame->page.GetPageId(), frame);
    }
    
    if (frame->page.GetPageId() != INVALID_PAGE_ID) {
        page_table_.erase(frame->page.GetPageId());
    }
    
    if (!TryToReadPage(page_id, frame)) {
        return nullptr;
    }
    
    frame->page.SetPageId(page_id);
    frame->is_dirty = false;
    frame->pin_count = 1;
    
    page_table_[page_id] = frame_id;
    UpdateLRU(frame_id);
    
    return &frame->page;
}

bool BufferPoolManager::FlushPage(page_id_t page_id) {
    std::lock_guard<std::mutex> guard(latch_);
    
    auto it = page_table_.find(page_id);
    if (it == page_table_.end()) {
        return false;
    }
    
    frame_id_t frame_id = it->second;
    Frame* frame = frames_[frame_id].get();
    
    if (frame->is_dirty) {
        WritePageToDisk(page_id, frame);
        frame->is_dirty = false;
    }
    
    return true;
}

Page* BufferPoolManager::NewPage(page_id_t& page_id) {
    std::lock_guard<std::mutex> guard(latch_);
    
    frame_id_t frame_id = FindVictimFrame();
    if (frame_id == INVALID_FRAME_ID) {
        return nullptr;
    }
    
    Frame* frame = frames_[frame_id].get();
    
    if (frame->is_dirty) {
        WritePageToDisk(frame->page.GetPageId(), frame);
    }
    
    if (frame->page.GetPageId() != INVALID_PAGE_ID) {
        page_table_.erase(frame->page.GetPageId());
    }
    
    page_id = disk_manager_->AllocatePage();
    if (page_id == INVALID_PAGE_ID) {
        return nullptr;
    }
    
    // Initialize the page with the correct layout
    frame->page.Reset();
    frame->page.SetPageId(page_id);
    frame->is_dirty = true;
    frame->pin_count = 1;
    
    page_table_[page_id] = frame_id;
    
    return &frame->page;
}

bool BufferPoolManager::DeletePage(page_id_t page_id) {
    std::lock_guard<std::mutex> guard(latch_);
    
    auto it = page_table_.find(page_id);
    if (it == page_table_.end()) {
        return true;
    }
    
    frame_id_t frame_id = it->second;
    Frame* frame = frames_[frame_id].get();
    
    if (frame->pin_count > 0) {
        return false;
    }
    
    if (frame->is_dirty) {
        WritePageToDisk(page_id, frame);
    }
    
    page_table_.erase(it);
    frame->page.SetPageId(INVALID_PAGE_ID);
    frame->is_dirty = false;
    
    disk_manager_->DeallocatePage(page_id);
    return true;
}

bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
    std::lock_guard<std::mutex> guard(latch_);
    
    auto it = page_table_.find(page_id);
    if (it == page_table_.end()) {
        return false;
    }
    
    frame_id_t frame_id = it->second;
    Frame* frame = frames_[frame_id].get();
    
    if (frame->pin_count == 0) {
        return false;
    }
    
    frame->pin_count--;
    if (is_dirty) {
        frame->is_dirty = true;
    }
    
    return true;
}

void BufferPoolManager::FlushAllPages() {
    std::lock_guard<std::mutex> guard(latch_);
    
    for (const auto& [page_id, frame_id] : page_table_) {
        Frame* frame = frames_[frame_id].get();
        if (frame->is_dirty) {
            WritePageToDisk(page_id, frame);
            frame->is_dirty = false;
        }
    }
}

frame_id_t BufferPoolManager::FindVictimFrame() {
    for (size_t i = 0; i < pool_size_; ++i) {
        if (free_list_[i]) {
            free_list_[i] = false;
            return i;
        }
    }
    
    // No free frame, use LRU
    if (!lru_list_.empty()) {
        frame_id_t frame_id = lru_list_.back();
        lru_list_.pop_back();
        return frame_id;
    }
    
    return INVALID_FRAME_ID;
}

void BufferPoolManager::UpdateLRU(frame_id_t frame_id) {
    Frame* frame = frames_[frame_id].get();
    
    if (frame->lru_iter != lru_list_.end()) {
        lru_list_.erase(frame->lru_iter);
    }
    
    lru_list_.push_back(frame_id);
    frame->lru_iter = std::prev(lru_list_.end());
}

bool BufferPoolManager::TryToReadPage(page_id_t page_id, Frame* frame) {
    try {
        disk_manager_->ReadPage(page_id, frame->page.GetData());
        return true;
    } catch (const std::exception& e) {
        std::cerr << "Failed to read page " << page_id << ": " << e.what() << std::endl;
        return false;
    }
}

void BufferPoolManager::WritePageToDisk(page_id_t page_id, Frame* frame) {
    try {
        disk_manager_->WritePage(page_id, frame->page.GetData());
    } catch (const std::exception& e) {
        std::cerr << "Failed to write page " << page_id << ": " << e.what() << std::endl;
    }
}

}
