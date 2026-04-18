#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <fstream>
#include <mutex>

namespace minidb {

// Log Sequence Number - unique identifier for each log record
using lsn_t = uint64_t;
constexpr lsn_t INVALID_LSN = 0;

// Transaction ID - unique identifier for each transaction
using txn_id_t = uint64_t;
constexpr txn_id_t INVALID_TXN_ID = 0;

// Log Record Types
enum class LogRecordType {
    BEGIN,          // Transaction start
    COMMIT,         // Transaction commit
    ABORT,          // Transaction abort
    INSERT,         // Insert operation
    DELETE,         // Delete operation
    UPDATE          // Update operation
};

// Log Record Structure
// Format: [LSN][Type][TxnID][PageID][DataSize][Data]
class LogRecord {
public:
    LogRecord();
    LogRecord(LogRecordType type, txn_id_t txn_id, uint32_t page_id, 
              const char* data = nullptr, uint32_t data_size = 0);
    
    // Getters
    lsn_t GetLSN() const { return lsn_; }
    LogRecordType GetType() const { return type_; }
    txn_id_t GetTxnID() const { return txn_id_; }
    uint32_t GetPageID() const { return page_id_; }
    const char* GetData() const { return data_.data(); }
    uint32_t GetDataSize() const { return data_size_; }
    
    // Setters
    void SetLSN(lsn_t lsn) { lsn_ = lsn; }
    
    // Serialization
    uint32_t GetSerializedSize() const;
    bool Serialize(char* buffer, uint32_t buffer_size) const;
    bool Deserialize(const char* buffer, uint32_t buffer_size);
    bool DeserializeFromFile(std::fstream& file);
    
    // Utility
    std::string GetTypeString() const;
    bool IsTransactionControl() const;

private:
    lsn_t lsn_;
    LogRecordType type_;
    txn_id_t txn_id_;
    uint32_t page_id_;
    uint32_t data_size_;
    std::vector<char> data_;  // Variable-length data
};

// LogManager manages the write-ahead log
// Responsibilities:
// - Append log records to log file
// - Maintain increasing LSN
// - Flush logs to disk
// - Support recovery
class LogManager {
public:
    explicit LogManager(const std::string& log_file);
    ~LogManager();
    
    // Core logging operations
    
    // Append a log record to the log buffer
    // Returns the LSN assigned to this record
    lsn_t AppendLogRecord(const LogRecord& record);
    
    // Flush all log records up to (and including) the given LSN to disk
    bool FlushLog(lsn_t lsn);
    
    // Flush all buffered log records to disk
    bool FlushAllLogs();
    
    // Get the current LSN (next LSN to be assigned)
    lsn_t GetCurrentLSN() const { return next_lsn_; }
    
    // Get the last flushed LSN
    lsn_t GetLastFlushedLSN() const { return last_flushed_lsn_; }
    
    // Recovery operations
    
    // Read all log records from the log file
    std::vector<LogRecord> ReadAllLogRecords();
    
    // Get the log file path
    const std::string& GetLogFilePath() const { return log_file_path_; }

private:
    // Internal helper methods
    
    // Write log record to file at specific offset
    bool WriteLogRecordToFile(lsn_t lsn, const LogRecord& record);
    
    // Read log record from file at specific offset
    bool ReadLogRecordFromFile(lsn_t lsn, LogRecord& record);
    
    // Calculate file offset for a given LSN
    uint64_t CalculateOffset(lsn_t lsn) const;

private:
    std::string log_file_path_;
    std::fstream log_file_;
    
    lsn_t next_lsn_;              // Next LSN to assign
    lsn_t last_flushed_lsn_;      // Last LSN flushed to disk
    
    std::mutex log_mutex_;        // Mutex for thread safety
};

}
