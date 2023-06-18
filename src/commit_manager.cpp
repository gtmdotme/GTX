//
// Created by zhou822 on 5/28/23.
//
#include "core/commit_manager.hpp"
#include <random>
namespace bwgraph{
#if USING_WAL
#else
    /*
     * the latch is held when the function is invoked, so the latch must be released here
     */
    void LatchDoubleBufferCommitManager::collaborative_commit(){
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

    void LatchDoubleBufferCommitManager::server_loop() {
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
    //Concurrent Array Commit Manager Server Loop
    void ConcurrentArrayCommitManager::server_loop() {
        std::uniform_int_distribution<> offset_distribution(0,worker_thread_num-1);
        std::random_device rd; // obtain a random number from hardware
        std::mt19937 gen(rd());
        while(running.load()){
            size_t commit_count =0;
            offset = offset_distribution(gen);
            uint32_t current_offset = offset;
            global_write_epoch++;
            do{
                auto current_txn_entry = commit_array[current_offset].txn_ptr.load();
                if(current_txn_entry){
                    current_txn_entry->status.store(global_write_epoch);
                    commit_array[current_offset].txn_ptr.store(nullptr);
                    commit_count++;
                }
                current_offset = (current_offset+1)%worker_thread_num;
            }while(current_offset!=offset);
            if(commit_count){
                global_read_epoch.fetch_add(1);
            }else{
                global_write_epoch--;
            }
        }
        //now the stop running signal is sent
        global_write_epoch++;
        for(uint32_t i=0; i<worker_thread_num;i++){
            auto current_txn_entry = commit_array[i].txn_ptr.load();
            if(current_txn_entry){
                current_txn_entry->status.store(global_write_epoch);
                commit_array[i].txn_ptr.store(nullptr);
            }
        }
        global_read_epoch.fetch_add(1);
    }
#endif
}