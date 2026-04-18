#pragma once

#include "page.h"
#include "../common/types.h"
#include <unordered_map>
#include <list>
#include <mutex>
#include <string>
#include <memory>
#include <vector>

namespace minidb {

class DiskManager;
class Page;

class BufferPoolManager {
public:
    BufferPoolManager(size_t pool_size, DiskManager* disk_manager);
    ~BufferPoolManager();
    
    Page* FetchPage(page_id_t page_id);
    bool FlushPage(page_id_t page_id);
    Page* NewPage(page_id_t& page_id);
    bool DeletePage(page_id_t page_id);
    bool UnpinPage(page_id_t page_id, bool is_dirty);
    
    void FlushAllPages();

private:
    struct Frame {
        Page page;
        bool is_dirty;
        int pin_count;
        std::list<frame_id_t>::iterator lru_iter;
    };
    
    size_t pool_size_;
    DiskManager* disk_manager_;
    std::vector<std::unique_ptr<Frame>> frames_;
    std::unordered_map<page_id_t, frame_id_t> page_table_;
    std::list<frame_id_t> lru_list_;
    std::vector<bool> free_list_;
    std::mutex latch_;
    
    frame_id_t FindVictimFrame();
    void UpdateLRU(frame_id_t frame_id);
    bool TryToReadPage(page_id_t page_id, Frame* frame);
    void WritePageToDisk(page_id_t page_id, Frame* frame);
};

}
