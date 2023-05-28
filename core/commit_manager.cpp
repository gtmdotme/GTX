//
// Created by zhou822 on 5/28/23.
//
#include "commit_manager.hpp"
namespace bwgraph{
#if USING_WAL
#else
    //wal can easily be supported as part of the txn_entry
    void CommitManager::txn_commit(entry_ptr txn_entry) {
        latch.lock();
        double_buffer_queue[offset].emplace(txn_entry);
        latch.unlock();
        return;
    }
    void CommitManager::server_loop() {
        while(true){
            latch.lock();
            uint8_t current_offset = offset;
            offset=1-offset;
            latch.unlock();
            //skip empty buffer because there is no one to commit
            if(double_buffer_queue[current_offset].empty()){
                continue;
            }
            global_write_epoch++;
            while(!double_buffer_queue[current_offset].empty()){
                auto current_entry = double_buffer_queue[current_offset].front();
                //wal version adds a persist log step
                current_entry->status.store(global_write_epoch);
                double_buffer_queue[current_offset].pop();
            }
            global_read_epoch.fetch_add(1);
        }
    }
#endif
}