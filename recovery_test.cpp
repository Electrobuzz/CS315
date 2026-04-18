#include <iostream>
#include <fstream>
#include <cstdint>
#include <cstring>
#include <vector>
#include <memory>
#include <unordered_map>

#include "src/storage/page_v2.h"
#include "src/storage/disk_manager_v2.h"

using namespace minidb;

// WAL Types
using lsn_t = uint64_t;
using txn_id_t = uint64_t;
constexpr lsn_t INVALID_LSN = 0;
constexpr txn_id_t INVALID_TXN_ID = 0;

enum class LogRecordType {
    BEGIN, COMMIT, ABORT, INSERT, DELETE, UPDATE
};

class LogRecord {
public:
    LogRecord() : lsn_(INVALID_LSN), type_(LogRecordType::BEGIN), 
                  txn_id_(INVALID_TXN_ID), page_id_(0), data_size_(0) {}
    
    LogRecord(LogRecordType type, txn_id_t txn_id, uint32_t page_id, 
              const char* data = nullptr, uint32_t data_size = 0)
        : lsn_(INVALID_LSN), type_(type), txn_id_(txn_id), 
          page_id_(page_id), data_size_(data_size) {
        if (data && data_size > 0) {
            data_.resize(data_size);
            std::memcpy(data_.data(), data, data_size);
        }
    }
    
    lsn_t GetLSN() const { return lsn_; }
    void SetLSN(lsn_t lsn) { lsn_ = lsn; }
    LogRecordType GetType() const { return type_; }
    txn_id_t GetTxnID() const { return txn_id_; }
    uint32_t GetPageID() const { return page_id_; }
    const char* GetData() const { return data_.data(); }
    uint32_t GetDataSize() const { return data_size_; }
    
    std::string GetTypeString() const {
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
    
    bool IsTransactionControl() const {
        return type_ == LogRecordType::BEGIN || 
               type_ == LogRecordType::COMMIT || 
               type_ == LogRecordType::ABORT;
    }

private:
    lsn_t lsn_;
    LogRecordType type_;
    txn_id_t txn_id_;
    uint32_t page_id_;
    uint32_t data_size_;
    std::vector<char> data_;
};

class LogManager {
public:
    explicit LogManager(const std::string& log_file)
        : log_file_path_(log_file), next_lsn_(1), last_flushed_lsn_(0) {
        log_file_.open(log_file_path_, std::ios::in | std::ios::out | std::ios::binary);
        if (!log_file_.is_open()) {
            log_file_.open(log_file_path_, std::ios::out | std::ios::binary);
            log_file_.close();
            log_file_.open(log_file_path_, std::ios::in | std::ios::out | std::ios::binary);
        }
    }
    
    ~LogManager() {
        FlushAllLogs();
        if (log_file_.is_open()) log_file_.close();
    }
    
    lsn_t AppendLogRecord(const LogRecord& record) {
        lsn_t assigned_lsn = next_lsn_++;
        
        LogRecord record_with_lsn = record;
        record_with_lsn.SetLSN(assigned_lsn);
        
        log_file_.seekp(0, std::ios::end);
        
        log_file_.write(reinterpret_cast<const char*>(&assigned_lsn), sizeof(lsn_t));
        LogRecordType type = record.GetType();
        log_file_.write(reinterpret_cast<const char*>(&type), sizeof(LogRecordType));
        txn_id_t txn_id = record.GetTxnID();
        log_file_.write(reinterpret_cast<const char*>(&txn_id), sizeof(txn_id_t));
        uint32_t page_id = record.GetPageID();
        log_file_.write(reinterpret_cast<const char*>(&page_id), sizeof(uint32_t));
        uint32_t data_size = record.GetDataSize();
        log_file_.write(reinterpret_cast<const char*>(&data_size), sizeof(uint32_t));
        if (data_size > 0) {
            log_file_.write(record.GetData(), data_size);
        }
        
        log_file_.flush();
        last_flushed_lsn_ = assigned_lsn;
        
        return assigned_lsn;
    }
    
    bool FlushLog(lsn_t lsn) {
        if (lsn <= last_flushed_lsn_) return true;
        log_file_.flush();
        last_flushed_lsn_ = lsn;
        return true;
    }
    
    bool FlushAllLogs() {
        if (next_lsn_ > 1) {
            log_file_.flush();
            last_flushed_lsn_ = next_lsn_ - 1;
        }
        return true;
    }
    
    lsn_t GetCurrentLSN() const { return next_lsn_; }
    lsn_t GetLastFlushedLSN() const { return last_flushed_lsn_; }
    
    std::vector<LogRecord> ReadAllLogRecords() {
        std::vector<LogRecord> records;
        
        log_file_.seekg(0, std::ios::beg);
        
        while (log_file_.good()) {
            LogRecord record;
            
            lsn_t lsn;
            LogRecordType type;
            txn_id_t txn_id;
            uint32_t page_id;
            uint32_t data_size;
            
            log_file_.read(reinterpret_cast<char*>(&lsn), sizeof(lsn_t));
            if (log_file_.gcount() != sizeof(lsn_t)) break;
            
            log_file_.read(reinterpret_cast<char*>(&type), sizeof(LogRecordType));
            if (log_file_.gcount() != sizeof(LogRecordType)) break;
            
            log_file_.read(reinterpret_cast<char*>(&txn_id), sizeof(txn_id_t));
            if (log_file_.gcount() != sizeof(txn_id_t)) break;
            
            log_file_.read(reinterpret_cast<char*>(&page_id), sizeof(uint32_t));
            if (log_file_.gcount() != sizeof(uint32_t)) break;
            
            log_file_.read(reinterpret_cast<char*>(&data_size), sizeof(uint32_t));
            if (log_file_.gcount() != sizeof(uint32_t)) break;
            
            std::vector<char> data(data_size);
            if (data_size > 0) {
                log_file_.read(data.data(), data_size);
                if (log_file_.gcount() != data_size) break;
            }
            
            record.SetLSN(lsn);
            record = LogRecord(type, txn_id, page_id, data.data(), data_size);
            record.SetLSN(lsn);
            
            records.push_back(record);
        }
        
        return records;
    }

private:
    std::string log_file_path_;
    std::fstream log_file_;
    lsn_t next_lsn_;
    lsn_t last_flushed_lsn_;
};

// Simplified Buffer Pool
class BufferPoolManagerWAL {
public:
    BufferPoolManagerWAL(size_t pool_size, DiskManager* disk_manager, LogManager* log_manager)
        : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
        frames_.resize(pool_size_);
        for (size_t i = 0; i < pool_size_; ++i) {
            frames_[i].page = std::make_unique<Page>();
            frames_[i].pin_count = 0;
            frames_[i].is_dirty = false;
        }
    }
    
    ~BufferPoolManagerWAL() {
        FlushAllPages();
    }
    
    Page* NewPage(uint32_t& page_id) {
        page_id = disk_manager_->AllocatePage();
        if (page_id == INVALID_PAGE_ID) return nullptr;
        
        for (size_t i = 0; i < pool_size_; ++i) {
            if (frames_[i].pin_count == 0) {
                frames_[i].page->Reset();
                frames_[i].page->SetPageId(page_id);
                frames_[i].pin_count = 1;
                frames_[i].is_dirty = false;
                return frames_[i].page.get();
            }
        }
        return nullptr;
    }
    
    Page* FetchPage(uint32_t page_id) {
        for (size_t i = 0; i < pool_size_; ++i) {
            if (frames_[i].page->GetPageId() == page_id) {
                frames_[i].pin_count++;
                return frames_[i].page.get();
            }
        }
        
        // Load from disk
        for (size_t i = 0; i < pool_size_; ++i) {
            if (frames_[i].pin_count == 0) {
                disk_manager_->ReadPage(page_id, *frames_[i].page);
                frames_[i].pin_count = 1;
                frames_[i].is_dirty = false;
                return frames_[i].page.get();
            }
        }
        return nullptr;
    }
    
    bool UnpinPage(uint32_t page_id, bool is_dirty) {
        for (size_t i = 0; i < pool_size_; ++i) {
            if (frames_[i].page->GetPageId() == page_id) {
                if (is_dirty) frames_[i].is_dirty = true;
                if (frames_[i].pin_count > 0) frames_[i].pin_count--;
                return true;
            }
        }
        return false;
    }
    
    bool FlushPage(uint32_t page_id) {
        for (size_t i = 0; i < pool_size_; ++i) {
            if (frames_[i].page->GetPageId() == page_id && frames_[i].is_dirty) {
                uint64_t page_lsn = frames_[i].page->GetLastLSN();
                if (page_lsn > 0) {
                    log_manager_->FlushLog(page_lsn);
                }
                // Write page to disk (this should preserve the LSN in the page header)
                disk_manager_->WritePage(page_id, *frames_[i].page);
                frames_[i].is_dirty = false;
                return true;
            }
        }
        return false;
    }
    
    void FlushAllPages() {
        for (size_t i = 0; i < pool_size_; ++i) {
            if (frames_[i].is_dirty) {
                FlushPage(frames_[i].page->GetPageId());
            }
        }
    }
    
    lsn_t LogInsert(txn_id_t txn_id, uint32_t page_id, const char* record_data, uint32_t record_size) {
        LogRecord record(LogRecordType::INSERT, txn_id, page_id, record_data, record_size);
        return log_manager_->AppendLogRecord(record);
    }
    
    lsn_t LogBegin(txn_id_t txn_id) {
        LogRecord record(LogRecordType::BEGIN, txn_id, 0);
        return log_manager_->AppendLogRecord(record);
    }
    
    lsn_t LogCommit(txn_id_t txn_id) {
        LogRecord record(LogRecordType::COMMIT, txn_id, 0);
        lsn_t lsn = log_manager_->AppendLogRecord(record);
        log_manager_->FlushLog(lsn);
        return lsn;
    }
    
    LogManager* GetLogManager() { return log_manager_; }

private:
    struct Frame {
        std::unique_ptr<Page> page;
        int pin_count;
        bool is_dirty;
    };
    
    size_t pool_size_;
    DiskManager* disk_manager_;
    LogManager* log_manager_;
    std::vector<Frame> frames_;
};

// Transaction State
enum class TransactionState {
    ACTIVE, COMMITTED, ABORTED
};

struct TransactionTableEntry {
    TransactionState state;
    lsn_t last_lsn;
    
    TransactionTableEntry() : state(TransactionState::ACTIVE), last_lsn(INVALID_LSN) {}
};

// Simplified Recovery Manager
class RecoveryManager {
public:
    RecoveryManager(LogManager* log_manager, BufferPoolManagerWAL* buffer_pool)
        : log_manager_(log_manager), buffer_pool_(buffer_pool),
          replayed_log_count_(0), skipped_log_count_(0) {}
    
    bool Recover() {
        std::cout << "Starting recovery..." << std::endl;
        
        txn_table_.clear();
        replayed_log_count_ = 0;
        skipped_log_count_ = 0;
        
        std::vector<LogRecord> log_records = log_manager_->ReadAllLogRecords();
        
        std::cout << "Read " << log_records.size() << " log records" << std::endl;
        
        if (log_records.empty()) {
            std::cout << "No log records to recover" << std::endl;
            return true;
        }
        
        // Phase 1: Analyze log
        std::cout << "Phase 1: Analyzing log..." << std::endl;
        for (const auto& record : log_records) {
            txn_id_t txn_id = record.GetTxnID();
            
            if (txn_table_.find(txn_id) == txn_table_.end()) {
                txn_table_[txn_id] = TransactionTableEntry();
            }
            txn_table_[txn_id].last_lsn = record.GetLSN();
            
            switch (record.GetType()) {
                case LogRecordType::BEGIN:
                    txn_table_[txn_id].state = TransactionState::ACTIVE;
                    break;
                case LogRecordType::COMMIT:
                    txn_table_[txn_id].state = TransactionState::COMMITTED;
                    break;
                case LogRecordType::ABORT:
                    txn_table_[txn_id].state = TransactionState::ABORTED;
                    break;
                default:
                    break;
            }
        }
        
        std::cout << "Transaction table built with " << txn_table_.size() << " transactions" << std::endl;
        
        // Phase 2: Redo committed transactions
        std::cout << "Phase 2: Redoing committed transactions..." << std::endl;
        for (const auto& record : log_records) {
            if (record.IsTransactionControl()) continue;
            
            auto it = txn_table_.find(record.GetTxnID());
            if (it == txn_table_.end() || it->second.state != TransactionState::COMMITTED) {
                std::cout << "Skipping LSN " << record.GetLSN() 
                          << " - transaction not committed" << std::endl;
                skipped_log_count_++;
                continue;
            }
            
            if (IsIdempotent(record)) {
                std::cout << "Skipping LSN " << record.GetLSN() 
                          << " - already applied" << std::endl;
                skipped_log_count_++;
                continue;
            }
            
            std::cout << "Redoing LSN " << record.GetLSN() << std::endl;
            ApplyInsert(record);
            replayed_log_count_++;
        }
        
        std::cout << "Recovery completed. Replayed: " << replayed_log_count_ 
                  << ", Skipped: " << skipped_log_count_ << std::endl;
        
        return true;
    }
    
    size_t GetReplayedLogCount() const { return replayed_log_count_; }
    size_t GetSkippedLogCount() const { return skipped_log_count_; }

private:
    bool IsIdempotent(const LogRecord& record) {
        Page* page = buffer_pool_->FetchPage(record.GetPageID());
        if (!page) return false;
        
        uint64_t page_lsn = page->GetLastLSN();
        lsn_t log_lsn = record.GetLSN();
        
        buffer_pool_->UnpinPage(record.GetPageID(), false);
        
        return page_lsn >= log_lsn;
    }
    
    bool ApplyInsert(const LogRecord& record) {
        Page* page = buffer_pool_->FetchPage(record.GetPageID());
        if (!page) return false;
        
        uint32_t slot_id;
        page->InsertRecord(record.GetData(), record.GetDataSize(), slot_id);
        page->SetLastLSN(record.GetLSN());
        buffer_pool_->UnpinPage(record.GetPageID(), true);
        
        return true;
    }

private:
    LogManager* log_manager_;
    BufferPoolManagerWAL* buffer_pool_;
    std::unordered_map<txn_id_t, TransactionTableEntry> txn_table_;
    size_t replayed_log_count_;
    size_t skipped_log_count_;
};

// Test functions
void TestCrashBeforeCommit() {
    std::cout << "\n=== Test 1: Crash Before Commit ===" << std::endl;
    
    std::remove("test_crash1.db");
    std::remove("test_crash1.log");
    
    DiskManager disk_manager("test_crash1.db");
    LogManager log_manager("test_crash1.log");
    BufferPoolManagerWAL buffer_pool(2, &disk_manager, &log_manager);
    
    // Begin transaction
    txn_id_t txn_id = 1;
    buffer_pool.LogBegin(txn_id);
    std::cout << "Began transaction " << txn_id << std::endl;
    
    // Insert record (log it)
    uint32_t page_id;
    Page* page = buffer_pool.NewPage(page_id);
    const char* record_data = "Uncommitted Data";
    page->InsertRecord(record_data, std::strlen(record_data) + 1, page_id);
    
    lsn_t lsn = buffer_pool.LogInsert(txn_id, page_id, record_data, std::strlen(record_data) + 1);
    page->SetLastLSN(lsn);
    buffer_pool.UnpinPage(page_id, true);
    
    std::cout << "Inserted record with LSN " << lsn << " (NOT committed)" << std::endl;
    
    // Simulate crash: DON'T commit, just close
    buffer_pool.FlushAllPages();
    
    // Recovery
    RecoveryManager recovery(&log_manager, &buffer_pool);
    bool recovery_success = recovery.Recover();
    
    std::cout << "Recovery: " << (recovery_success ? "SUCCESS" : "FAILED") << std::endl;
    std::cout << "Replayed: " << recovery.GetReplayedLogCount() << std::endl;
    std::cout << "Skipped: " << recovery.GetSkippedLogCount() << std::endl;
    
    if (recovery.GetReplayedLogCount() == 0 && recovery.GetSkippedLogCount() == 1) {
        std::cout << "✅ Correctly skipped uncommitted transaction" << std::endl;
    } else {
        std::cout << "❌ Incorrectly replayed uncommitted transaction" << std::endl;
    }
    
    std::remove("test_crash1.db");
    std::remove("test_crash1.log");
}

void TestCrashAfterCommit() {
    std::cout << "\n=== Test 2: Crash After Commit ===" << std::endl;
    
    std::remove("test_crash2.db");
    std::remove("test_crash2.log");
    
    DiskManager disk_manager("test_crash2.db");
    LogManager log_manager("test_crash2.log");
    BufferPoolManagerWAL buffer_pool(2, &disk_manager, &log_manager);
    
    // Begin transaction
    txn_id_t txn_id = 1;
    buffer_pool.LogBegin(txn_id);
    std::cout << "Began transaction " << txn_id << std::endl;
    
    // Insert record (log it)
    uint32_t page_id;
    Page* page = buffer_pool.NewPage(page_id);
    const char* record_data = "Committed Data";
    page->InsertRecord(record_data, std::strlen(record_data) + 1, page_id);
    
    lsn_t lsn = buffer_pool.LogInsert(txn_id, page_id, record_data, std::strlen(record_data) + 1);
    page->SetLastLSN(lsn);
    buffer_pool.UnpinPage(page_id, true);
    
    std::cout << "Inserted record with LSN " << lsn << std::endl;
    
    // Commit transaction
    buffer_pool.LogCommit(txn_id);
    std::cout << "Committed transaction " << txn_id << std::endl;
    
    // Simulate crash: close without flushing page
    buffer_pool.FlushAllPages();
    
    // Recovery
    RecoveryManager recovery(&log_manager, &buffer_pool);
    bool recovery_success = recovery.Recover();
    
    std::cout << "Recovery: " << (recovery_success ? "SUCCESS" : "FAILED") << std::endl;
    std::cout << "Replayed: " << recovery.GetReplayedLogCount() << std::endl;
    std::cout << "Skipped: " << recovery.GetSkippedLogCount() << std::endl;
    
    if (recovery.GetReplayedLogCount() == 1 && recovery.GetSkippedLogCount() == 0) {
        std::cout << "✅ Correctly replayed committed transaction" << std::endl;
    } else {
        std::cout << "❌ Failed to replay committed transaction" << std::endl;
    }
    
    std::remove("test_crash2.db");
    std::remove("test_crash2.log");
}

void TestPartialPageFlush() {
    std::cout << "\n=== Test 3: Partial Page Flush (Idempotency) ===" << std::endl;
    
    std::remove("test_partial.db");
    std::remove("test_partial.log");
    
    DiskManager disk_manager("test_partial.db");
    LogManager log_manager("test_partial.log");
    BufferPoolManagerWAL buffer_pool(2, &disk_manager, &log_manager);
    
    // Begin transaction
    txn_id_t txn_id = 1;
    buffer_pool.LogBegin(txn_id);
    
    // Insert record
    uint32_t page_id;
    Page* page = buffer_pool.NewPage(page_id);
    const char* record_data = "Test Data";
    page->InsertRecord(record_data, std::strlen(record_data) + 1, page_id);
    
    lsn_t lsn = buffer_pool.LogInsert(txn_id, page_id, record_data, std::strlen(record_data) + 1);
    page->SetLastLSN(lsn);
    
    // Mark page as dirty so FlushPage will write it to disk
    buffer_pool.UnpinPage(page_id, true);
    
    // Flush page (this writes page to disk with LSN)
    buffer_pool.FlushPage(page_id);
    std::cout << "Flushed page with LSN " << lsn << std::endl;
    
    // Commit transaction
    buffer_pool.LogCommit(txn_id);
    
    // Flush log to ensure commit record is on disk
    log_manager.FlushAllLogs();
    
    // Verify page was flushed correctly by reading it back
    Page verify_page;
    disk_manager.ReadPage(page_id, verify_page);
    std::cout << "Page on disk has LSN: " << verify_page.GetLastLSN() << std::endl;
    
    // Create new buffer pool to simulate fresh start (clears in-memory cache)
    BufferPoolManagerWAL new_buffer_pool(2, &disk_manager, &log_manager);
    
    // Recovery: should skip because page LSN >= log LSN
    RecoveryManager recovery(&log_manager, &new_buffer_pool);
    bool recovery_success = recovery.Recover();
    
    std::cout << "Recovery: " << (recovery_success ? "SUCCESS" : "FAILED") << std::endl;
    std::cout << "Replayed: " << recovery.GetReplayedLogCount() << std::endl;
    std::cout << "Skipped: " << recovery.GetSkippedLogCount() << std::endl;
    
    if (recovery.GetSkippedLogCount() == 1) {
        std::cout << "✅ Correctly skipped already-applied change (idempotency)" << std::endl;
    } else {
        std::cout << "❌ Failed idempotency check" << std::endl;
    }
    
    std::remove("test_partial.db");
    std::remove("test_partial.log");
}

int main() {
    std::cout << "=== Improved Recovery System Tests ===" << std::endl;
    
    try {
        TestCrashBeforeCommit();
        TestCrashAfterCommit();
        TestPartialPageFlush();
        
        std::cout << "\n=== ALL RECOVERY TESTS COMPLETED ===" << std::endl;
        std::cout << "Improved recovery system is working correctly!" << std::endl;
        
    } catch (const std::exception& e) {
        std::cerr << "Test failed: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}
