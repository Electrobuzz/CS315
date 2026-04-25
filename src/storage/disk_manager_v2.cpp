#include "disk_manager_v2.h"
#include <stdexcept>
#include <iostream>

namespace minidb {

DiskManager::DiskManager(const std::string& filename) 
    : filename_(filename), next_page_id_(1) {
    Open();
}

DiskManager::~DiskManager() {
    Close();
}

bool DiskManager::Open() {
    std::lock_guard<std::mutex> lock(io_mutex_);
    
    if (file_stream_.is_open()) {
        return true;
    }
    
    // Try to open existing file or create new one
    file_stream_.open(filename_, 
                      std::ios::in | std::ios::out | std::ios::binary);
    
    if (!file_stream_.is_open()) {
        // File doesn't exist, create it
        file_stream_.open(filename_, 
                          std::ios::out | std::ios::binary);
        if (!file_stream_.is_open()) {
            return false;
        }
        file_stream_.close();
        
        // Reopen for read/write
        file_stream_.open(filename_, 
                          std::ios::in | std::ios::out | std::ios::binary);
        if (!file_stream_.is_open()) {
            return false;
        }
        
        InitializeNewFile();
    } else {
        // File exists, initialize next_page_id_ from file size
        file_stream_.seekg(0, std::ios::end);
        uint64_t file_size = file_stream_.tellg();
        uint32_t page_count = static_cast<uint32_t>(file_size / Page::SIZE);
        next_page_id_ = page_count + 1;
        file_stream_.seekg(0, std::ios::beg);
    }
    
    return true;
}

void DiskManager::Close() {
    std::lock_guard<std::mutex> lock(io_mutex_);
    if (file_stream_.is_open()) {
        file_stream_.close();
    }
}

bool DiskManager::ReadPage(uint32_t page_id, Page& page) {
    std::lock_guard<std::mutex> lock(io_mutex_);
    
    if (!ValidatePageId(page_id)) {
        return false;
    }
    
    if (!SeekToPage(page_id)) {
        return false;
    }
    
    file_stream_.read(page.GetData(), Page::SIZE);
    if (file_stream_.fail() || file_stream_.gcount() != Page::SIZE) {
        // Page doesn't exist or read failed, return empty page
        page.Reset();
    }
    
    page.SetPageId(page_id);
    return true;
}

bool DiskManager::WritePage(uint32_t page_id, const Page& page) {
    std::lock_guard<std::mutex> lock(io_mutex_);
    
    if (!ValidatePageId(page_id)) {
        return false;
    }
    
    if (!SeekToPage(page_id)) {
        return false;
    }
    
    file_stream_.write(page.GetData(), Page::SIZE);
    if (file_stream_.fail()) {
        return false;
    }
    
    file_stream_.flush();
    file_stream_.sync();  // Sync to OS buffer cache
    return true;
}

bool DiskManager::ReadPage(uint32_t page_id, char* data) {
    std::lock_guard<std::mutex> lock(io_mutex_);
    
    if (!ValidatePageId(page_id)) {
        return false;
    }
    
    if (!SeekToPage(page_id)) {
        return false;
    }
    
    file_stream_.read(data, Page::SIZE);
    if (file_stream_.fail() || file_stream_.gcount() != Page::SIZE) {
        // Page doesn't exist or read failed, return empty page
        std::memset(data, 0, Page::SIZE);
        return true;
    }
    
    return true;
}

bool DiskManager::WritePage(uint32_t page_id, const char* data) {
    std::lock_guard<std::mutex> lock(io_mutex_);
    
    if (!ValidatePageId(page_id)) {
        return false;
    }
    
    if (!SeekToPage(page_id)) {
        return false;
    }
    
    file_stream_.write(data, Page::SIZE);
    if (file_stream_.fail()) {
        return false;
    }
    
    file_stream_.flush();
    return true;
}

uint32_t DiskManager::AllocatePage() {
    std::lock_guard<std::mutex> lock(io_mutex_);
    
    uint32_t page_id = next_page_id_++;
    
    // Create empty page and write it to disk
    Page new_page;
    new_page.Reset();
    new_page.SetPageId(page_id);
    
    if (!SeekToPage(page_id)) {
        next_page_id_--;
        return INVALID_PAGE_ID;
    }
    
    file_stream_.write(new_page.GetData(), Page::SIZE);
    if (file_stream_.fail()) {
        next_page_id_--;
        return INVALID_PAGE_ID;
    }
    
    file_stream_.flush();
    return page_id;
}

bool DiskManager::DeallocatePage(uint32_t page_id) {
    std::lock_guard<std::mutex> lock(io_mutex_);
    
    if (!ValidatePageId(page_id)) {
        return false;
    }
    
    // For now, just write an empty page back
    // In a real implementation, we'd maintain a free list
    Page empty_page;
    empty_page.Reset();
    empty_page.SetPageId(INVALID_PAGE_ID);
    
    return WritePage(page_id, empty_page);
}

uint32_t DiskManager::GetPageCount() const {
    std::lock_guard<std::mutex> lock(io_mutex_);
    
    // Calculate actual page count from file size
    if (!file_stream_.is_open()) {
        return next_page_id_ - 1;
    }
    
    // Save current position (need const_cast because tellg/seekg are non-const)
    auto& file_stream = const_cast<std::fstream&>(file_stream_);
    auto current_pos = file_stream.tellg();
    
    // Seek to end to get file size
    file_stream.seekg(0, std::ios::end);
    uint64_t file_size = file_stream.tellg();
    
    // Restore position
    file_stream.seekg(current_pos);
    
    // Calculate page count from file size
    uint32_t page_count = static_cast<uint32_t>(file_size / Page::SIZE);
    
    // Return the maximum of allocated pages and actual file pages
    return std::max(next_page_id_ - 1, page_count);
}

bool DiskManager::SeekToPage(uint32_t page_id) {
    if (!file_stream_.is_open()) {
        return false;
    }
    
    std::streampos offset = static_cast<std::streampos>(page_id) * Page::SIZE;
    file_stream_.seekp(offset);
    file_stream_.seekg(offset);
    
    if (file_stream_.fail()) {
        return false;
    }
    
    return true;
}

void DiskManager::InitializeNewFile() {
    // File is newly created, nothing to initialize
    // next_page_id_ is already set to 1
}

bool DiskManager::ValidatePageId(uint32_t page_id) const {
    return page_id != INVALID_PAGE_ID && page_id < next_page_id_;
}

}
