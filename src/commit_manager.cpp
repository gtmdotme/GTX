//
// Created by zhou822 on 5/28/23.
//
#include "commit_manager.hpp"
namespace bwgraph{
#if USING_WAL
#else
    /*
     * the latch is held when the function is invoked, so the latch must be released here
     */
    void CommitManager::collaborative_commit(){
        commit_latch.lock();//synchronize commit groups
        uint8_t current_offset = offset;
        offset=1-offset;
        latch.unlock();
        //present the other queue to the worker threads
#if COMMIT_TEST
        if(double_buffer_queue[current_offset].queue_buffer.empty()){
            throw CommitException();
        }
#endif
        std::fill(std::begin(double_buffer_queue[current_offset].already_committed), std::end(double_buffer_queue[current_offset].already_committed), false);
        global_write_epoch++;
        while(!double_buffer_queue[current_offset].queue_buffer.empty()){
            auto current_entry = double_buffer_queue[current_offset].queue_buffer.front();
            //wal version adds a persist log step
            current_entry->status.store(global_write_epoch);
            double_buffer_queue[current_offset].queue_buffer.pop();
        }
        global_read_epoch.fetch_add(1);
        commit_latch.unlock();
    }

    void CommitManager::server_loop() {
        while(running.load()){
            latch.lock();
            commit_latch.lock();
            uint8_t current_offset = offset;
            offset=1-offset;
            latch.unlock();
            //skip empty buffer because there is no one to commit
            if(double_buffer_queue[current_offset].queue_buffer.empty()){
                commit_latch.unlock();
                continue;
            }
            std::fill(std::begin(double_buffer_queue[current_offset].already_committed), std::end(double_buffer_queue[current_offset].already_committed), false);
            global_write_epoch++;
            while(!double_buffer_queue[current_offset].queue_buffer.empty()){
                auto current_entry = double_buffer_queue[current_offset].queue_buffer.front();
                //wal version adds a persist log step
                current_entry->status.store(global_write_epoch);
                double_buffer_queue[current_offset].queue_buffer.pop();
            }
            global_read_epoch.fetch_add(1);
            commit_latch.unlock();
        }
        for(int i=0; i<=1; i++){
            if(double_buffer_queue[i].queue_buffer.empty()){
                continue;
            }
            global_write_epoch++;
            while(!double_buffer_queue[i].queue_buffer.empty()){
                auto current_entry = double_buffer_queue[i].queue_buffer.front();
                //wal version adds a persist log step
                current_entry->status.store(global_write_epoch);
                double_buffer_queue[i].queue_buffer.pop();
            }
            global_read_epoch.fetch_add(1);
        }
    }
#endif
}