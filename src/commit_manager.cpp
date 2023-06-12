//
// Created by zhou822 on 5/28/23.
//
#include "commit_manager.hpp"
namespace bwgraph{
#if USING_WAL
#else
    void CommitManager::server_loop() {
        while(running.load()){
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
        for(int i=0; i<=1; i++){
            if(double_buffer_queue[i].empty()){
                continue;
            }
            global_write_epoch++;
            while(!double_buffer_queue[i].empty()){
                auto current_entry = double_buffer_queue[i].front();
                //wal version adds a persist log step
                current_entry->status.store(global_write_epoch);
                double_buffer_queue[i].pop();
            }
            global_read_epoch.fetch_add(1);
        }
    }
#endif
}