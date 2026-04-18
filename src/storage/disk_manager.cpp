#include "disk_manager.h"
#include <stdexcept>
#include <iostream>

namespace minidb {

DiskManager::DiskManager(const std::string& db_file) 
    : file_name_(db_file), next_page_id_(1) {
    OpenFile();
}

DiskManager::~DiskManager() {
    CloseFile();
}

void DiskManager::OpenFile() {
    db_io_.open(file_name_, std::ios::in | std::ios::out | std::ios::binary);
    
    if (!db_io_.is_open()) {
        db_io_.open(file_name_, std::ios::out | std::ios::binary);
        db_io_.close();
        db_io_.open(file_name_, std::ios::in | std::ios::out | std::ios::binary);
    }
    
    if (!db_io_.is_open()) {
        throw std::runtime_error("Failed to open database file: " + file_name_);
    }
}

void DiskManager::CloseFile() {
    if (db_io_.is_open()) {
        db_io_.close();
    }
}

void DiskManager::ReadPage(page_id_t page_id, char* page_data) {
    std::lock_guard<std::mutex> guard(io_latch_);
    
    if (page_id == INVALID_PAGE_ID) {
        throw std::runtime_error("Cannot read invalid page ID");
    }
    
    db_io_.seekg(page_id * PAGE_SIZE);
    if (db_io_.fail()) {
        throw std::runtime_error("Failed to seek to page " + std::to_string(page_id));
    }
    
    db_io_.read(page_data, PAGE_SIZE);
    if (db_io_.gcount() != PAGE_SIZE) {
        std::memset(page_data, 0, PAGE_SIZE);
    }
}

void DiskManager::WritePage(page_id_t page_id, const char* page_data) {
    std::lock_guard<std::mutex> guard(io_latch_);
    
    if (page_id == INVALID_PAGE_ID) {
        throw std::runtime_error("Cannot write to invalid page ID");
    }
    
    db_io_.seekp(page_id * PAGE_SIZE);
    if (db_io_.fail()) {
        throw std::runtime_error("Failed to seek to page " + std::to_string(page_id));
    }
    
    db_io_.write(page_data, PAGE_SIZE);
    if (db_io_.fail()) {
        throw std::runtime_error("Failed to write page " + std::to_string(page_id));
    }
    
    db_io_.flush();
}

page_id_t DiskManager::AllocatePage() {
    std::lock_guard<std::mutex> guard(io_latch_);
    return next_page_id_++;
}

void DiskManager::DeallocatePage(page_id_t page_id) {
    std::lock_guard<std::mutex> guard(io_latch_);
}

size_t DiskManager::GetNumPages() const {
    std::lock_guard<std::mutex> guard(io_latch_);
    return next_page_id_ - 1;
}

void DiskManager::ShutDown() {
    CloseFile();
}

}
