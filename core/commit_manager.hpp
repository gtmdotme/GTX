//
// Created by zhou822 on 5/28/23.
//

#ifndef BWGRAPH_V2_COMMIT_MANAGER_HPP
#define BWGRAPH_V2_COMMIT_MANAGER_HPP
#include "spin_latch.hpp"
#include <queue>
#include "graph_global.hpp"
#include <array>
#include "transaction_tables.hpp"
namespace bwgraph{
#if USING_WAL

#else
    //using CommitQueue = std::queue<entry_ptr>;
    struct CommitQueue{
        inline void emplace(entry_ptr txn_entry){
            queue_buffer.emplace(txn_entry);
        }
        std::queue<entry_ptr> queue_buffer;
        std::array<bool,worker_thread_num> already_committed;
    };
#endif
    //todo: examine no server thread solution
    class LatchDoubleBufferCommitManager{
    public:
#if USING_ARRAY_TABLE
        LatchDoubleBufferCommitManager(/*ArrayTransactionTables& graph_txn_table*/):latch()/*,txn_tables(graph_txn_table)*/{
            for(int i=0; i<2; i++){
                for(uint32_t j=0; j<worker_thread_num;j++){
                    double_buffer_queue[i].already_committed[j]=false;
                }
            }
        }
#else
        CommitManager(ConcurrentTransactionTables& graph_txn_table):latch(),txn_tables(graph_txn_table){}
#endif
#if USING_WAL
//todo: implement this
        void txn_commit(){

        }
#else
        inline uint64_t get_current_read_ts(){return global_read_epoch.load();}
        inline void txn_commit(uint8_t thread_id,entry_ptr txn_entry){
            while(true){
                latch.lock();
                if(double_buffer_queue[offset].already_committed[thread_id]){
                    //do collaborative commit
                    collaborative_commit();
                    continue;
                }
                double_buffer_queue[offset].emplace(txn_entry);
                double_buffer_queue[offset].already_committed[thread_id]=true;
                latch.unlock();
                return;
            }
        }
        //with latch held
        void collaborative_commit();
        void server_loop();//server loops to commit
        inline void shutdown_signal(){
            running.store(false);
        }
#endif
    private:
        std::atomic_bool running = true;
        std::atomic_uint64_t global_read_epoch = 0;
        uint64_t global_write_epoch = 0;
       // void batch_commit();
        uint8_t offset = 0 ;
        spinlock latch;
        spinlock commit_latch;
        std::array<CommitQueue,2>double_buffer_queue;
#if USING_ARRAY_TABLE
       // ArrayTransactionTables& txn_tables;
#else
        ConcurrentTransactionTables& txn_tables;
#endif
       // size_t current_entries = 0;
    };
    struct alignas(64) padded_txn_entry_ptr{
        padded_txn_entry_ptr(): txn_ptr(nullptr){
            for(int i=0; i<56;i++){
                padding[i]=0;
            }
        }
        std::atomic<entry_ptr> txn_ptr;
        char padding[56];
    };
    class ConcurrentArrayCommitManager{
    public:
        ConcurrentArrayCommitManager(){

        }
        inline bool txn_commit(uint8_t thread_id,entry_ptr txn_entry, bool willing_to_wait){
            entry_ptr null_ptr = nullptr;
            if(willing_to_wait){
                //todo:: check memory order more
                while(!commit_array[thread_id].txn_ptr.compare_exchange_strong(null_ptr, txn_entry /*,std::memory_order_acquire*/)){
                    null_ptr = nullptr;
                    //do something?
                }
                return true;
            }else{
                return commit_array[thread_id].txn_ptr.compare_exchange_strong(null_ptr, txn_entry /*,std::memory_order_acquire*/);
            }
        }
        void server_loop();
        inline uint64_t get_current_read_ts(){return global_read_epoch.load();}
        inline void shutdown_signal(){running.store(false);}
    private:
        std::array<padded_txn_entry_ptr, worker_thread_num> commit_array;
        std::atomic_bool running = true;
        std::atomic_uint64_t global_read_epoch = 0;
        uint64_t global_write_epoch = 0;
        uint32_t offset = 0 ;
    };
    using CommitManager = ConcurrentArrayCommitManager;
}
#endif //BWGRAPH_V2_COMMIT_MANAGER_HPP
