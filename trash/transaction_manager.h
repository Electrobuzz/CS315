#pragma once

#include "log_manager.h"
#include "buffer_pool_wal.h"
#include <unordered_map>
#include <mutex>

namespace minidb {

// Transaction state for recovery
enum class TransactionState {
    ACTIVE,      // Transaction is in progress
    COMMITTED,   // Transaction committed
    ABORTED      // Transaction aborted
};

// Transaction table entry for recovery
struct TransactionTableEntry {
    TransactionState state;
    lsn_t last_lsn;  // Last LSN for this transaction
    
    TransactionTableEntry() : state(TransactionState::ACTIVE), last_lsn(INVALID_LSN) {}
    TransactionTableEntry(TransactionState s, lsn_t l) : state(s), last_lsn(l) {}
};

// Transaction Manager handles transaction lifecycle
// Responsibilities:
// - Begin transactions
// - Commit transactions (with WAL logging)
// - Abort transactions
// - Track active transactions
class TransactionManager {
public:
    explicit TransactionManager(LogManager* log_manager, BufferPoolManagerWAL* buffer_pool);
    ~TransactionManager() = default;
    
    // Transaction operations
    txn_id_t BeginTransaction();
    bool CommitTransaction(txn_id_t txn_id);
    bool AbortTransaction(txn_id_t txn_id);
    
    // Transaction state queries
    bool IsTransactionActive(txn_id_t txn_id) const;
    size_t GetActiveTransactionCount() const;

private:
    LogManager* log_manager_;
    BufferPoolManagerWAL* buffer_pool_;
    
    std::unordered_map<txn_id_t, bool> active_transactions_;
    txn_id_t next_txn_id_;
    std::mutex txn_mutex_;
};

// Recovery Manager handles crash recovery
// Responsibilities:
// - Replay logs to restore database state
// - Ensure durability after crash
class RecoveryManager {
public:
    explicit RecoveryManager(LogManager* log_manager, BufferPoolManagerWAL* buffer_pool);
    ~RecoveryManager() = default;
    
    // Recovery operations
    bool Recover();  // Perform redo-only recovery with transaction state tracking
    
    // Utility
    size_t GetReplayedLogCount() const { return replayed_log_count_; }
    size_t GetSkippedLogCount() const { return skipped_log_count_; }

private:
    // Recovery helper methods
    bool AnalyzeLog(const std::vector<LogRecord>& log_records);
    bool RedoPhase(const std::vector<LogRecord>& log_records);
    bool RedoLogRecord(const LogRecord& record);
    bool ApplyInsert(const LogRecord& record);
    bool ApplyDelete(const LogRecord& record);
    bool ApplyUpdate(const LogRecord& record);
    bool IsTransactionCommitted(txn_id_t txn_id) const;
    bool IsIdempotent(const LogRecord& record);

private:
    LogManager* log_manager_;
    BufferPoolManagerWAL* buffer_pool_;
    
    // Transaction table for recovery
    std::unordered_map<txn_id_t, TransactionTableEntry> txn_table_;
    
    // Statistics
    size_t replayed_log_count_;
    size_t skipped_log_count_;
};

}
