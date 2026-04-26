#include "transaction_manager.h"
#include <iostream>

namespace minidb {

// TransactionManager Implementation
TransactionManager::TransactionManager(LogManager* log_manager, BufferPoolManagerWAL* buffer_pool)
    : log_manager_(log_manager), buffer_pool_(buffer_pool), next_txn_id_(1) {
}

txn_id_t TransactionManager::BeginTransaction() {
    std::lock_guard<std::mutex> guard(txn_mutex_);
    
    txn_id_t txn_id = next_txn_id_++;
    
    // Log BEGIN record
    lsn_t lsn = buffer_pool_->LogBegin(txn_id);
    if (lsn == INVALID_LSN) {
        // Rollback transaction ID on failure
        next_txn_id_--;
        return INVALID_TXN_ID;
    }
    
    // Mark transaction as active
    active_transactions_[txn_id] = true;
    
    return txn_id;
}

bool TransactionManager::CommitTransaction(txn_id_t txn_id) {
    std::lock_guard<std::mutex> guard(txn_mutex_);
    
    // Check if transaction is active
    auto it = active_transactions_.find(txn_id);
    if (it == active_transactions_.end() || !it->second) {
        return false;  // Transaction not active
    }
    
    // Log COMMIT record
    lsn_t lsn = buffer_pool_->LogCommit(txn_id);
    if (lsn == INVALID_LSN) {
        return false;  // Log commit failed
    }
    
    // Mark transaction as inactive
    active_transactions_[txn_id] = false;
    
    return true;
}

bool TransactionManager::AbortTransaction(txn_id_t txn_id) {
    std::lock_guard<std::mutex> guard(txn_mutex_);
    
    // Check if transaction is active
    auto it = active_transactions_.find(txn_id);
    if (it == active_transactions_.end() || !it->second) {
        return false;  // Transaction not active
    }
    
    // Log ABORT record
    lsn_t lsn = buffer_pool_->LogAbort(txn_id);
    if (lsn == INVALID_LSN) {
        return false;  // Log abort failed
    }
    
    // Mark transaction as inactive
    active_transactions_[txn_id] = false;
    
    return true;
}

bool TransactionManager::IsTransactionActive(txn_id_t txn_id) const {
    auto it = active_transactions_.find(txn_id);
    return it != active_transactions_.end() && it->second;
}

size_t TransactionManager::GetActiveTransactionCount() const {
    size_t count = 0;
    for (const auto& [txn_id, is_active] : active_transactions_) {
        if (is_active) count++;
    }
    return count;
}

// RecoveryManager Implementation
RecoveryManager::RecoveryManager(LogManager* log_manager, BufferPoolManagerWAL* buffer_pool)
    : log_manager_(log_manager), buffer_pool_(buffer_pool), 
      replayed_log_count_(0), skipped_log_count_(0) {
}

bool RecoveryManager::Recover() {
    std::cout << "Starting recovery..." << std::endl;
    
    // Clear transaction table
    txn_table_.clear();
    replayed_log_count_ = 0;
    skipped_log_count_ = 0;
    
    // Read all log records
    std::vector<LogRecord> log_records = log_manager_->ReadAllLogRecords();
    
    std::cout << "Read " << log_records.size() << " log records" << std::endl;
    
    if (log_records.empty()) {
        std::cout << "No log records to recover" << std::endl;
        return true;
    }
    
    // Phase 1: Analyze log to build transaction table
    std::cout << "Phase 1: Analyzing log to build transaction table..." << std::endl;
    if (!AnalyzeLog(log_records)) {
        std::cerr << "Log analysis failed" << std::endl;
        return false;
    }
    
    std::cout << "Transaction table built with " << txn_table_.size() << " transactions" << std::endl;
    
    // Phase 2: Redo committed transactions
    std::cout << "Phase 2: Redoing committed transactions..." << std::endl;
    if (!RedoPhase(log_records)) {
        std::cerr << "Redo phase failed" << std::endl;
        return false;
    }
    
    std::cout << "Recovery completed. Replayed: " << replayed_log_count_ 
              << ", Skipped: " << skipped_log_count_ << std::endl;
    
    return true;
}

bool RecoveryManager::AnalyzeLog(const std::vector<LogRecord>& log_records) {
    for (const auto& record : log_records) {
        txn_id_t txn_id = record.GetTxnID();
        
        // Initialize transaction entry if not exists
        if (txn_table_.find(txn_id) == txn_table_.end()) {
            txn_table_[txn_id] = TransactionTableEntry(TransactionState::ACTIVE, record.GetLSN());
        } else {
            // Update last LSN for this transaction
            txn_table_[txn_id].last_lsn = record.GetLSN();
        }
        
        // Update transaction state based on log record type
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
                // INSERT, DELETE, UPDATE don't change transaction state
                break;
        }
    }
    
    return true;
}

bool RecoveryManager::RedoPhase(const std::vector<LogRecord>& log_records) {
    for (const auto& record : log_records) {
        // Skip transaction control records
        if (record.IsTransactionControl()) {
            continue;
        }
        
        // Check if transaction is committed
        if (!IsTransactionCommitted(record.GetTxnID())) {
            std::cout << "Skipping log record LSN " << record.GetLSN() 
                      << " - transaction " << record.GetTxnID() << " not committed" << std::endl;
            skipped_log_count_++;
            continue;
        }
        
        // Check idempotency: skip if already applied
        if (IsIdempotent(record)) {
            std::cout << "Skipping log record LSN " << record.GetLSN() 
                      << " - already applied (page LSN >= log LSN)" << std::endl;
            skipped_log_count_++;
            continue;
        }
        
        // Redo the operation
        std::cout << "Redoing log record LSN " << record.GetLSN() 
                  << " - " << record.GetTypeString() << " on page " << record.GetPageID() << std::endl;
        
        if (RedoLogRecord(record)) {
            replayed_log_count_++;
        }
    }
    
    return true;
}

bool RecoveryManager::RedoLogRecord(const LogRecord& record) {
    switch (record.GetType()) {
        case LogRecordType::INSERT:
            return ApplyInsert(record);
        case LogRecordType::DELETE:
            return ApplyDelete(record);
        case LogRecordType::UPDATE:
            return ApplyUpdate(record);
        default:
            return false;
    }
}

bool RecoveryManager::ApplyInsert(const LogRecord& record) {
    // Fetch the page
    Page* page = buffer_pool_->FetchPage(record.GetPageID());
    if (!page) {
        return false;
    }
    
    // Insert the record
    uint32_t slot_id;
    bool success = page->InsertRecord(record.GetData(), record.GetDataSize(), slot_id);
    
    // Update page LSN
    if (success) {
        page->SetLastLSN(record.GetLSN());
        buffer_pool_->UnpinPage(record.GetPageID(), true);
    } else {
        buffer_pool_->UnpinPage(record.GetPageID(), false);
    }
    
    return success;
}

bool RecoveryManager::ApplyDelete(const LogRecord& record) {
    // Fetch the page
    Page* page = buffer_pool_->FetchPage(record.GetPageID());
    if (!page) {
        return false;
    }
    
    // For simplicity, we'll just log the delete operation
    // In a real implementation, we would find and delete the specific record
    // For now, we'll just mark the page as dirty
    
    page->SetLastLSN(record.GetLSN());
    buffer_pool_->UnpinPage(record.GetPageID(), true);
    
    return true;
}

bool RecoveryManager::ApplyUpdate(const LogRecord& record) {
    // Fetch the page
    Page* page = buffer_pool_->FetchPage(record.GetPageID());
    if (!page) {
        return false;
    }
    
    // For simplicity, we'll just log the update operation
    // In a real implementation, we would find and update the specific record
    
    page->SetLastLSN(record.GetLSN());
    buffer_pool_->UnpinPage(record.GetPageID(), true);
    
    return true;
}

bool RecoveryManager::IsTransactionCommitted(txn_id_t txn_id) const {
    auto it = txn_table_.find(txn_id);
    if (it == txn_table_.end()) {
        return false;  // Transaction not found
    }
    return it->second.state == TransactionState::COMMITTED;
}

bool RecoveryManager::IsIdempotent(const LogRecord& record) {
    // Fetch the page to check its current LSN
    Page* page = buffer_pool_->FetchPage(record.GetPageID());
    if (!page) {
        return false;  // Page not found, need to redo
    }
    
    uint64_t page_lsn = page->GetLastLSN();
    lsn_t log_lsn = record.GetLSN();
    
    buffer_pool_->UnpinPage(record.GetPageID(), false);
    
    // If page LSN is >= log LSN, the change has already been applied
    return page_lsn >= log_lsn;
}

}
