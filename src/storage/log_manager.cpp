#include "log_manager.h"
#include <cstring>
#include <stdexcept>

namespace minidb {

// LogRecord Implementation
LogRecord::LogRecord() 
    : lsn_(INVALID_LSN), type_(LogRecordType::BEGIN), 
      txn_id_(INVALID_TXN_ID), page_id_(0), data_size_(0) {
}

LogRecord::LogRecord(LogRecordType type, txn_id_t txn_id, uint32_t page_id, 
                     const char* data, uint32_t data_size)
    : lsn_(INVALID_LSN), type_(type), txn_id_(txn_id), 
      page_id_(page_id), data_size_(data_size) {
    
    if (data && data_size > 0) {
        data_.resize(data_size);
        std::memcpy(data_.data(), data, data_size);
    }
}

uint32_t LogRecord::GetSerializedSize() const {
    // Format: [LSN(8)][Type(4)][TxnID(8)][PageID(4)][DataSize(4)][Data(variable)]
    return sizeof(lsn_t) + sizeof(LogRecordType) + sizeof(txn_id_t) + 
           sizeof(uint32_t) + sizeof(uint32_t) + data_size_;
}

bool LogRecord::Serialize(char* buffer, uint32_t buffer_size) const {
    if (buffer_size < GetSerializedSize()) {
        return false;
    }
    
    char* ptr = buffer;
    
    // Write LSN
    std::memcpy(ptr, &lsn_, sizeof(lsn_t));
    ptr += sizeof(lsn_t);
    
    // Write Type
    LogRecordType type = type_;
    std::memcpy(ptr, &type, sizeof(LogRecordType));
    ptr += sizeof(LogRecordType);
    
    // Write TxnID
    std::memcpy(ptr, &txn_id_, sizeof(txn_id_t));
    ptr += sizeof(txn_id_t);
    
    // Write PageID
    std::memcpy(ptr, &page_id_, sizeof(uint32_t));
    ptr += sizeof(uint32_t);
    
    // Write DataSize
    std::memcpy(ptr, &data_size_, sizeof(uint32_t));
    ptr += sizeof(uint32_t);
    
    // Write Data
    if (data_size_ > 0 && !data_.empty()) {
        std::memcpy(ptr, data_.data(), data_size_);
    }
    
    return true;
}

bool LogRecord::Deserialize(const char* buffer, uint32_t buffer_size) {
    const char* ptr = buffer;
    
    // Check minimum size
    uint32_t min_size = sizeof(lsn_t) + sizeof(LogRecordType) + sizeof(txn_id_t) + 
                       sizeof(uint32_t) + sizeof(uint32_t);
    if (buffer_size < min_size) {
        return false;
    }
    
    // Read LSN
    std::memcpy(&lsn_, ptr, sizeof(lsn_t));
    ptr += sizeof(lsn_t);
    
    // Read Type
    std::memcpy(&type_, ptr, sizeof(LogRecordType));
    ptr += sizeof(LogRecordType);
    
    // Read TxnID
    std::memcpy(&txn_id_, ptr, sizeof(txn_id_t));
    ptr += sizeof(txn_id_t);
    
    // Read PageID
    std::memcpy(&page_id_, ptr, sizeof(uint32_t));
    ptr += sizeof(uint32_t);
    
    // Read DataSize
    std::memcpy(&data_size_, ptr, sizeof(uint32_t));
    ptr += sizeof(uint32_t);
    
    // Check if we have enough buffer for data
    if (buffer_size < min_size + data_size_) {
        return false;
    }
    
    // Read Data
    if (data_size_ > 0) {
        data_.resize(data_size_);
        std::memcpy(data_.data(), ptr, data_size_);
    }
    
    return true;
}

std::string LogRecord::GetTypeString() const {
    switch (type_) {
        case LogRecordType::BEGIN: return "BEGIN";
        case LogRecordType::COMMIT: return "COMMIT";
        case LogRecordType::ABORT: return "ABORT";
        case LogRecordType::INSERT: return "INSERT";
        case LogRecordType::DELETE: return "DELETE";
        case LogRecordType::UPDATE: return "UPDATE";
        default: return "UNKNOWN";
    }
}

bool LogRecord::IsTransactionControl() const {
    return type_ == LogRecordType::BEGIN || 
           type_ == LogRecordType::COMMIT || 
           type_ == LogRecordType::ABORT;
}

// LogManager Implementation
LogManager::LogManager(const std::string& log_file)
    : log_file_path_(log_file), next_lsn_(1), last_flushed_lsn_(0) {
    
    // Open log file in append mode
    log_file_.open(log_file_path_, std::ios::in | std::ios::out | std::ios::binary);
    
    if (!log_file_.is_open()) {
        // Create new log file
        log_file_.open(log_file_path_, std::ios::out | std::ios::binary);
        log_file_.close();
        log_file_.open(log_file_path_, std::ios::in | std::ios::out | std::ios::binary);
    }
    
    if (!log_file_.is_open()) {
        throw std::runtime_error("Failed to open log file: " + log_file_path_);
    }
    
    // Scan existing log file to determine next LSN
    // For simplicity, we'll start from the end of the file
    log_file_.seekg(0, std::ios::end);
    uint64_t file_size = log_file_.tellg();
    
    if (file_size > 0) {
        // Try to read the last record to determine next LSN
        // This is a simplified approach - real implementation would be more robust
        LogRecord record;
        uint64_t offset = 0;
        while (offset < file_size) {
            log_file_.seekg(offset);
            if (record.DeserializeFromFile(log_file_)) {
                next_lsn_ = record.GetLSN() + 1;
                offset += record.GetSerializedSize();
            } else {
                break;
            }
        }
    }
}

LogManager::~LogManager() {
    FlushAllLogs();
    if (log_file_.is_open()) {
        log_file_.close();
    }
}

lsn_t LogManager::AppendLogRecord(const LogRecord& record) {
    std::lock_guard<std::mutex> guard(log_mutex_);
    
    // Assign LSN to the record
    lsn_t assigned_lsn = next_lsn_++;
    
    // Create a copy of the record with the assigned LSN
    LogRecord record_with_lsn = record;
    record_with_lsn.SetLSN(assigned_lsn);
    
    // Write to log file
    if (!WriteLogRecordToFile(assigned_lsn, record_with_lsn)) {
        // Rollback LSN assignment on failure
        next_lsn_--;
        return INVALID_LSN;
    }
    
    return assigned_lsn;
}

bool LogManager::FlushLog(lsn_t lsn) {
    std::lock_guard<std::mutex> guard(log_mutex_);
    
    if (lsn <= last_flushed_lsn_) {
        return true;  // Already flushed
    }
    
    // Flush the log file to ensure durability
    log_file_.flush();
    last_flushed_lsn_ = lsn;
    
    return true;
}

bool LogManager::FlushAllLogs() {
    std::lock_guard<std::mutex> guard(log_mutex_);
    
    if (next_lsn_ > 1) {
        log_file_.flush();
        last_flushed_lsn_ = next_lsn_ - 1;
    }
    
    return true;
}

std::vector<LogRecord> LogManager::ReadAllLogRecords() {
    std::lock_guard<std::mutex> guard(log_mutex_);
    
    std::vector<LogRecord> records;
    
    // Seek to beginning of file
    log_file_.seekg(0, std::ios::beg);
    
    while (log_file_.good()) {
        LogRecord record;
        uint64_t offset = log_file_.tellg();
        
        // Read record header first to get size
        uint32_t header_size = sizeof(lsn_t) + sizeof(LogRecordType) + 
                               sizeof(txn_id_t) + sizeof(uint32_t) + sizeof(uint32_t);
        
        char header_buffer[header_size];
        log_file_.read(header_buffer, header_size);
        
        if (log_file_.gcount() != header_size) {
            break;  // End of file or error
        }
        
        // Parse header to get data size
        const char* ptr = header_buffer;
        ptr += sizeof(lsn_t) + sizeof(LogRecordType) + sizeof(txn_id_t) + sizeof(uint32_t);
        uint32_t data_size;
        std::memcpy(&data_size, ptr, sizeof(uint32_t));
        
        // Calculate total record size
        uint32_t total_size = header_size + data_size;
        
        // Read full record
        log_file_.seekg(offset);
        std::vector<char> buffer(total_size);
        log_file_.read(buffer.data(), total_size);
        
        if (log_file_.gcount() != total_size) {
            break;  // End of file or error
        }
        
        // Deserialize record
        if (record.Deserialize(buffer.data(), total_size)) {
            records.push_back(record);
        }
    }
    
    return records;
}

bool LogManager::WriteLogRecordToFile(lsn_t lsn, const LogRecord& record) {
    uint64_t offset = CalculateOffset(lsn);
    
    log_file_.seekp(offset);
    if (log_file_.fail()) {
        return false;
    }
    
    uint32_t record_size = record.GetSerializedSize();
    std::vector<char> buffer(record_size);
    
    if (!record.Serialize(buffer.data(), record_size)) {
        return false;
    }
    
    log_file_.write(buffer.data(), record_size);
    if (log_file_.fail()) {
        return false;
    }
    
    return true;
}

bool LogManager::ReadLogRecordFromFile(lsn_t lsn, LogRecord& record) {
    uint64_t offset = CalculateOffset(lsn);
    
    log_file_.seekg(offset);
    if (log_file_.fail()) {
        return false;
    }
    
    // Read header first
    uint32_t header_size = sizeof(lsn_t) + sizeof(LogRecordType) + 
                           sizeof(txn_id_t) + sizeof(uint32_t) + sizeof(uint32_t);
    
    char header_buffer[header_size];
    log_file_.read(header_buffer, header_size);
    
    if (log_file_.gcount() != header_size) {
        return false;
    }
    
    // Parse header to get data size
    const char* ptr = header_buffer;
    ptr += sizeof(lsn_t) + sizeof(LogRecordType) + sizeof(txn_id_t) + sizeof(uint32_t);
    uint32_t data_size;
    std::memcpy(&data_size, ptr, sizeof(uint32_t));
    
    // Calculate total record size
    uint32_t total_size = header_size + data_size;
    
    // Read full record
    log_file_.seekg(offset);
    std::vector<char> buffer(total_size);
    log_file_.read(buffer.data(), total_size);
    
    if (log_file_.gcount() != total_size) {
        return false;
    }
    
    return record.Deserialize(buffer.data(), total_size);
}

uint64_t LogManager::CalculateOffset(lsn_t lsn) const {
    // For simplicity, we use a fixed-size mapping
    // In a real implementation, this would be more sophisticated
    // For now, we assume each record starts at offset = (LSN - 1) * MAX_RECORD_SIZE
    // But we'll implement a simpler approach: append-only with seeking
    
    // This is a placeholder - real implementation would maintain an index
    return 0;  // We'll handle this differently in the actual implementation
}

}
