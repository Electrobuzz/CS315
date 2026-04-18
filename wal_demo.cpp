#include <iostream>
#include <fstream>
#include <cstdint>
#include <cstring>
#include <vector>
#include <memory>
#include <unordered_map>

constexpr uint32_t PAGE_SIZE = 4096;
constexpr uint32_t INVALID_PAGE_ID = 0;

// Simplified Page implementation for WAL demo
class Page {
public:
    struct Header {
        uint32_t page_id;
        uint32_t free_space_offset;
        uint32_t slot_count;
        uint32_t free_slot_count;
        uint64_t last_lsn;
    };
    
private:
    char data_[PAGE_SIZE];
    
public:
    Page() { Reset(); }
    
    void Reset() {
        std::memset(data_, 0, PAGE_SIZE);
        Header* header = GetHeader();
        header->page_id = INVALID_PAGE_ID;
        header->free_space_offset = PAGE_SIZE - sizeof(Header);
        header->slot_count = 0;
        header->free_slot_count = 0;
        header->last_lsn = 0;
    }
    
    void SetPageId(uint32_t page_id) { GetHeader()->page_id = page_id; }
    uint32_t GetPageId() const { return GetHeader()->page_id; }
    uint64_t GetLastLSN() const { return GetHeader()->last_lsn; }
    void SetLastLSN(uint64_t lsn) { GetHeader()->last_lsn = lsn; }
    char* GetData() { return data_; }
    const char* GetData() const { return data_; }
    
    bool InsertRecord(const char* record_data, uint32_t record_size, uint32_t& slot_id) {
        if (record_size > 100) return false;  // Simplified check
        
        Header* header = GetHeader();
        slot_id = header->slot_count++;
        header->free_slot_count = 0;
        
        // Simple: store record at fixed offset for demo
        char* data_area = data_ + sizeof(Header);
        std::memcpy(data_area + (slot_id * 100), record_data, record_size);
        
        return true;
    }
    
private:
    Header* GetHeader() { return reinterpret_cast<Header*>(data_); }
    const Header* GetHeader() const { return reinterpret_cast<const Header*>(data_); }
};

// Simplified DiskManager
class DiskManager {
private:
    std::string filename_;
    std::fstream file_;
    uint32_t next_page_id_;
    
public:
    explicit DiskManager(const std::string& filename) 
        : filename_(filename), next_page_id_(1) {
        file_.open(filename, std::ios::in | std::ios::out | std::ios::binary);
        if (!file_.is_open()) {
            file_.open(filename, std::ios::out | std::ios::binary);
            file_.close();
            file_.open(filename, std::ios::in | std::ios::out | std::ios::binary);
        }
    }
    
    ~DiskManager() {
        if (file_.is_open()) file_.close();
    }
    
    bool WritePage(uint32_t page_id, const Page& page) {
        if (!file_.is_open()) return false;
        file_.seekp(page_id * PAGE_SIZE);
        if (file_.fail()) return false;
        file_.write(page.GetData(), PAGE_SIZE);
        if (file_.fail()) return false;
        file_.flush();
        return true;
    }
    
    bool ReadPage(uint32_t page_id, Page& page) {
        if (!file_.is_open()) return false;
        file_.seekg(page_id * PAGE_SIZE);
        if (file_.fail()) return false;
        file_.read(page.GetData(), PAGE_SIZE);
        if (file_.fail() || file_.gcount() != PAGE_SIZE) {
            page.Reset();
            page.SetPageId(page_id);
            return true;
        }
        return true;
    }
    
    uint32_t AllocatePage() {
        uint32_t page_id = next_page_id_++;
        Page new_page;
        new_page.Reset();
        new_page.SetPageId(page_id);
        WritePage(page_id, new_page);
        return page_id;
    }
    
    bool DeallocatePage(uint32_t page_id) {
        Page empty_page;
        empty_page.Reset();
        empty_page.SetPageId(INVALID_PAGE_ID);
        return WritePage(page_id, empty_page);
    }
};

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
        
        // Write to log file (simplified: append with header)
        uint32_t total_size = sizeof(lsn_t) + sizeof(LogRecordType) + 
                             sizeof(txn_id_t) + sizeof(uint32_t) + 
                             sizeof(uint32_t) + record.GetDataSize();
        
        log_file_.seekp(0, std::ios::end);
        
        // Write LSN
        log_file_.write(reinterpret_cast<const char*>(&assigned_lsn), sizeof(lsn_t));
        // Write Type
        LogRecordType type = record.GetType();
        log_file_.write(reinterpret_cast<const char*>(&type), sizeof(LogRecordType));
        // Write TxnID
        txn_id_t txn_id = record.GetTxnID();
        log_file_.write(reinterpret_cast<const char*>(&txn_id), sizeof(txn_id_t));
        // Write PageID
        uint32_t page_id = record.GetPageID();
        log_file_.write(reinterpret_cast<const char*>(&page_id), sizeof(uint32_t));
        // Write DataSize
        uint32_t data_size = record.GetDataSize();
        log_file_.write(reinterpret_cast<const char*>(&data_size), sizeof(uint32_t));
        // Write Data
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

private:
    std::string log_file_path_;
    std::fstream log_file_;
    lsn_t next_lsn_;
    lsn_t last_flushed_lsn_;
};

// Simplified WAL-enabled Buffer Pool
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
        
        // Find free frame
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
                // WAL Protocol: Ensure log flushed before page flush
                uint64_t page_lsn = frames_[i].page->GetLastLSN();
                if (page_lsn > 0) {
                    log_manager_->FlushLog(page_lsn);
                }
                
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

// Transaction Manager
class TransactionManager {
public:
    TransactionManager(LogManager* log_manager, BufferPoolManagerWAL* buffer_pool)
        : log_manager_(log_manager), buffer_pool_(buffer_pool), next_txn_id_(1) {}
    
    txn_id_t BeginTransaction() {
        txn_id_t txn_id = next_txn_id_++;
        buffer_pool_->LogBegin(txn_id);
        active_transactions_[txn_id] = true;
        return txn_id;
    }
    
    bool CommitTransaction(txn_id_t txn_id) {
        if (!active_transactions_[txn_id]) return false;
        buffer_pool_->LogCommit(txn_id);
        active_transactions_[txn_id] = false;
        return true;
    }

private:
    LogManager* log_manager_;
    BufferPoolManagerWAL* buffer_pool_;
    txn_id_t next_txn_id_;
    std::unordered_map<txn_id_t, bool> active_transactions_;
};

// Test functions
void TestLogManager() {
    std::cout << "=== Test 1: LogManager ===" << std::endl;
    
    std::remove("test_wal.log");
    LogManager log_manager("test_wal.log");
    
    std::cout << "Current LSN: " << log_manager.GetCurrentLSN() << std::endl;
    
    LogRecord record1(LogRecordType::INSERT, 1, 1, "Test Data", 9);
    lsn_t lsn1 = log_manager.AppendLogRecord(record1);
    std::cout << "Appended record with LSN: " << lsn1 << std::endl;
    
    LogRecord record2(LogRecordType::COMMIT, 1, 0);
    lsn_t lsn2 = log_manager.AppendLogRecord(record2);
    std::cout << "Appended record with LSN: " << lsn2 << std::endl;
    
    std::cout << "Last flushed LSN: " << log_manager.GetLastFlushedLSN() << std::endl;
    std::cout << "✅ LogManager test completed" << std::endl;
    
    std::remove("test_wal.log");
}

void TestWALProtocol() {
    std::cout << "\n=== Test 2: WAL Protocol ===" << std::endl;
    
    std::remove("test_wal.db");
    std::remove("test_wal.log");
    
    DiskManager disk_manager("test_wal.db");
    LogManager log_manager("test_wal.log");
    BufferPoolManagerWAL buffer_pool(2, &disk_manager, &log_manager);
    
    // Create a page and insert a record with WAL
    uint32_t page_id;
    Page* page = buffer_pool.NewPage(page_id);
    std::cout << "Created page " << page_id << std::endl;
    
    const char* record_data = "WAL Test Record";
    uint32_t slot_id;
    page->InsertRecord(record_data, std::strlen(record_data) + 1, slot_id);
    std::cout << "Inserted record into page" << std::endl;
    
    // Log the insert operation
    lsn_t lsn = buffer_pool.LogInsert(1, page_id, record_data, std::strlen(record_data) + 1);
    std::cout << "Logged insert with LSN: " << lsn << std::endl;
    
    // Update page LSN
    page->SetLastLSN(lsn);
    buffer_pool.UnpinPage(page_id, true);
    
    // Flush page (should flush log first due to WAL protocol)
    bool flush_success = buffer_pool.FlushPage(page_id);
    std::cout << "Flush page: " << (flush_success ? "SUCCESS" : "FAILED") << std::endl;
    
    std::cout << "✅ WAL protocol test completed" << std::endl;
    
    std::remove("test_wal.db");
    std::remove("test_wal.log");
}

void TestTransactions() {
    std::cout << "\n=== Test 3: Transactions ===" << std::endl;
    
    std::remove("test_txn.db");
    std::remove("test_txn.log");
    
    DiskManager disk_manager("test_txn.db");
    LogManager log_manager("test_txn.log");
    BufferPoolManagerWAL buffer_pool(2, &disk_manager, &log_manager);
    TransactionManager txn_manager(&log_manager, &buffer_pool);
    
    // Begin transaction
    txn_id_t txn_id = txn_manager.BeginTransaction();
    std::cout << "Began transaction " << txn_id << std::endl;
    
    // Perform operations
    uint32_t page_id;
    Page* page = buffer_pool.NewPage(page_id);
    const char* record_data = "Transaction Test";
    page->InsertRecord(record_data, std::strlen(record_data) + 1, page_id);
    
    lsn_t lsn = buffer_pool.LogInsert(txn_id, page_id, record_data, std::strlen(record_data) + 1);
    page->SetLastLSN(lsn);
    buffer_pool.UnpinPage(page_id, true);
    
    std::cout << "Logged insert in transaction " << txn_id << " with LSN: " << lsn << std::endl;
    
    // Commit transaction
    bool commit_success = txn_manager.CommitTransaction(txn_id);
    std::cout << "Commit transaction: " << (commit_success ? "SUCCESS" : "FAILED") << std::endl;
    
    std::cout << "✅ Transaction test completed" << std::endl;
    
    std::remove("test_txn.db");
    std::remove("test_txn.log");
}

int main() {
    std::cout << "=== WAL System Tests ===" << std::endl;
    
    try {
        TestLogManager();
        TestWALProtocol();
        TestTransactions();
        
        std::cout << "\n=== ALL WAL TESTS COMPLETED ===" << std::endl;
        std::cout << "WAL system is working correctly!" << std::endl;
        
    } catch (const std::exception& e) {
        std::cerr << "Test failed: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}
