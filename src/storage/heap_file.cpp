#include "heap_file.h"
#include <stdexcept>

namespace minidb {

HeapFile::HeapFile(const std::string& filename, BufferPoolManager* buffer_pool) 
    : buffer_pool_(buffer_pool), record_count_(0), current_page_index_(0) {
    disk_manager_ = std::make_unique<DiskManager>(filename);
}

HeapFile::~HeapFile() {
    Close();
}

bool HeapFile::Open() {
    if (!disk_manager_) {
        return false;
    }
    
    // Read page count from disk
    uint32_t page_count = disk_manager_->GetPageCount();
    if (page_count == 0) {
        // New file - start with empty page list
        page_ids_.clear();
        current_page_index_ = 0;
        return true;
    }
    
    // Load page IDs from disk (assume sequential for now)
    page_ids_.clear();
    for (uint32_t i = 0; i < page_count; i++) {
        page_ids_.push_back(i);
    }
    
    if (page_ids_.empty()) {
        return true;
    }
    
    current_page_index_ = 0;
    
    // Load first page using DiskManager
    if (!disk_manager_->ReadPage(page_ids_[current_page_index_], current_page_)) {
        return false;
    }
    
    return true;
}

void HeapFile::Close() {
    if (IsOpen() && current_page_index_ < page_ids_.size()) {
        WriteCurrentPage();
    }
}

bool HeapFile::InsertRecord(const char* record_data, uint32_t record_size, RID& rid) {
    if (!IsOpen() || !record_data || record_size == 0) {
        return false;
    }
    
    // Try to find space in existing pages
    for (size_t page_index = 0; page_index < page_ids_.size(); ++page_index) {
        uint32_t page_id = page_ids_[page_index];
        
        if (!disk_manager_->ReadPage(page_id, current_page_)) {
            continue;
        }
        
        uint32_t slot_id;
        if (current_page_.InsertRecord(record_data, record_size, slot_id)) {
            rid = RID(page_id, slot_id);
            current_page_index_ = page_index;
            // Write the modified page back to disk
            if (!disk_manager_->WritePage(page_id, current_page_)) {
                std::cerr << "[ERROR] HeapFile::InsertRecord: failed to write page " << page_id << std::endl;
                return false;
            }
            record_count_++;
            return true;
        }
    }
    
    // If no space found, allocate new page
    if (!AllocateNewPage()) {
        return false;
    }
    
    // Try inserting into the new page
    uint32_t slot_id;
    if (current_page_.InsertRecord(record_data, record_size, slot_id)) {
        record_count_++;
        rid = RID(page_ids_[current_page_index_], slot_id);
        return WriteCurrentPage();
    }
    
    return false;
}

bool HeapFile::AllocateNewPage() {
    if (!IsOpen()) {
        return false;
    }
    
    // Write current page if valid
    if (current_page_index_ < page_ids_.size()) {
        WriteCurrentPage();
    }
    
    // Allocate new page
    uint32_t new_page_id = disk_manager_->AllocatePage();
    if (new_page_id == INVALID_PAGE_ID) {
        return false;
    }
    
    // Add to page list
    page_ids_.push_back(new_page_id);
    current_page_index_ = page_ids_.size() - 1;
    
    // Reset and initialize new page
    current_page_.Reset();
    current_page_.SetPageId(new_page_id);
    
    return true;
}

bool HeapFile::WriteCurrentPage() {
    if (!disk_manager_ || current_page_index_ >= page_ids_.size()) {
        return false;
    }
    
    uint32_t page_id = page_ids_[current_page_index_];
    
    return disk_manager_->WritePage(page_id, current_page_);
}

uint32_t HeapFile::GetPageCount() const {
    return page_ids_.size();
}

uint32_t HeapFile::GetRecordCount() const {
    return record_count_;
}

bool HeapFile::GetTuple(RID rid, Tuple& tuple) {
    if (!disk_manager_ || rid.page_id == INVALID_PAGE_ID) {
        return false;
    }
    
    
    // Read page using DiskManager
    if (!disk_manager_->ReadPage(rid.page_id, current_page_)) {
        std::cerr << "[ERROR] HeapFile::GetTuple: failed to read page " << rid.page_id << std::endl;
        return false;
    }
    
    // Read record from page
    char buffer[Tuple::GetSize()];
    uint32_t actual_size = 0;
    
    bool success = current_page_.GetRecord(rid.slot_id, buffer, Tuple::GetSize(), actual_size);
    
    if (!success || actual_size != Tuple::GetSize()) {
        std::cerr << "[ERROR] HeapFile::GetTuple: GetRecord failed, success=" << success 
                  << " actual_size=" << actual_size << " expected=" << Tuple::GetSize() << std::endl;
        return false;
    }
    
    // Deserialize into tuple
    tuple.Deserialize(buffer);
    return true;
}

// ScanIterator implementation
HeapFile::ScanIterator::ScanIterator(HeapFile* heap_file) 
    : heap_file_(heap_file), current_page_id_(INVALID_PAGE_ID), 
      current_slot_id_(0), page_loaded_(false) {
    Reset();
}

HeapFile::ScanIterator::~ScanIterator() = default;

void HeapFile::ScanIterator::Reset() {
    current_page_id_ = INVALID_PAGE_ID;
    current_slot_id_ = 0;
    page_loaded_ = false;
}

bool HeapFile::ScanIterator::HasNext() {
    if (!heap_file_ || !heap_file_->IsOpen()) {
        return false;
    }
    
    // If page is loaded, check remaining slots
    if (page_loaded_) {
        if (current_slot_id_ < current_page_.GetSlotCount()) {
            return FindNextValidSlot();
        }
    }
    
    // Try to load next page
    return LoadNextPage();
}

bool HeapFile::ScanIterator::NextRecord(char* buffer, uint32_t buffer_size, uint32_t& actual_size) {
    if (!HasNext()) {
        return false;
    }
    
    return current_page_.GetRecord(current_slot_id_, buffer, buffer_size, actual_size);
}

bool HeapFile::ScanIterator::LoadNextPage() {
    if (!heap_file_ || !heap_file_->IsOpen()) {
        return false;
    }
    
    uint32_t page_count = heap_file_->GetPageCount();
    if (page_count == 0) {
        return false;
    }
    
    // Determine next page to load
    uint32_t next_page_index;
    if (current_page_id_ == INVALID_PAGE_ID) {
        next_page_index = 0;  // Start with first page
    } else {
        // Find current page index and move to next
        next_page_index = 0;  // Simplified: just start from beginning
        for (uint32_t i = 0; i < page_count; ++i) {
            if (heap_file_->page_ids_[i] == current_page_id_) {
                next_page_index = i + 1;
                break;
            }
        }
        
        if (next_page_index >= page_count) {
            return false;  // No more pages
        }
    }
    
    // Load the page
    if (!heap_file_->disk_manager_->ReadPage(heap_file_->page_ids_[next_page_index], current_page_)) {
        return false;
    }
    
    current_page_id_ = heap_file_->page_ids_[next_page_index];
    current_slot_id_ = 0;
    page_loaded_ = true;
    
    return FindNextValidSlot();
}

bool HeapFile::ScanIterator::FindNextValidSlot() {
    while (current_slot_id_ < current_page_.GetSlotCount()) {
        if (current_page_.IsSlotUsed(current_slot_id_)) {
            return true;
        }
        current_slot_id_++;
    }
    
    // No more valid slots in this page
    page_loaded_ = false;
    return false;
}

HeapFile::ScanIterator* HeapFile::CreateScanIterator() {
    return new ScanIterator(this);
}

}
