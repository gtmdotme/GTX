//
// Created by zhou822 on 5/23/23.
//

#ifndef BWGRAPH_V2_TRANSACTION_TABLES_HPP
#define BWGRAPH_V2_TRANSACTION_TABLES_HPP

#include <cstdint>
#include <atomic>
#include <vector>
#include "graph_global.hpp"
#include "exceptions.hpp"
#include "../Libraries/parallel_hashmap/phmap.h"
#include "utils.hpp"

namespace bwgraph {
#define TXN_TABLE_TEST true

    struct txn_table_entry {
        txn_table_entry() {
            status.store(IN_PROGRESS);
        };

        txn_table_entry(uint64_t new_txn_id) : txn_id(new_txn_id) {
            status.store(IN_PROGRESS);
        }

        txn_table_entry(const txn_table_entry &other) {
            txn_id = other.txn_id;
            status.store(other.status);
            op_count.store(other.op_count);
        }

        txn_table_entry &operator=(const txn_table_entry &other) {
            txn_id = other.txn_id;
            status.store(other.status);
            op_count.store(other.op_count);
            return *this;
        }

        inline bool reduce_op_count(int64_t num) {
            if (op_count.fetch_sub(num) == num) {
                return true;
            }
#if TXN_TABLE_TEST
            if (op_count.load() < 0) {
                throw TransactionTableOpCountException();
            }
#endif
            return false;
        }

        inline void commit(uint64_t ts) {
            status.store(ts);
        }

        inline void abort() {
            status.store(ABORT);
        }

        uint64_t txn_id;
        std::atomic_uint64_t status;
        std::atomic_int64_t op_count;
#if USING_ARRAY_TABLE
        std::vector<uint64_t> touched_blocks;
        char padding[16];
#else
        char padding[40];
#endif
    };

    using entry_ptr = txn_table_entry *;
    static_assert(sizeof(txn_table_entry) == 64);
    using Map = phmap::parallel_flat_hash_map<
            uint64_t,
            entry_ptr ,
            phmap::priv::hash_default_hash<uint64_t>,
            phmap::priv::hash_default_eq<uint64_t>,
            std::allocator<std::pair<const uint64_t, entry_ptr>>,
            12,
            std::mutex>;
    class ConcurrentTransactionTable{
    public:
        ConcurrentTransactionTable(){}
        inline bool get_status(uint64_t txn_id,uint64_t& status_result){
            return local_table.if_contains(txn_id,[&status_result](typename Map::value_type& pair){status_result=pair.second->status.load();});
        }
        void reduce_op_count(uint64_t txn_id,int64_t op_count){
            entry_ptr ptr;
#if TXN_TABLE_TEST
            if(!local_table.if_contains(txn_id,[&ptr](typename Map::value_type& pair){ ptr = pair.second;})){
                std::cerr<<"panic"<<std::endl;
            }
            if(ptr== nullptr||ptr->op_count<=0){
                throw TransactionTableOpCountException();
            }
#else
            local_table.if_contains(txn_id,[&ptr](typename Map::value_type& pair){ptr = pair.second;});
#endif
            if(ptr->reduce_op_count(op_count)){
                local_table.erase(txn_id);
                delete ptr;//always safe, if op_count reaches 0 no other thread will access it
            }
        }
        inline entry_ptr put_entry(uint64_t txn_id){
            entry_ptr ptr = new txn_table_entry(txn_id);
#if TXN_TABLE_TEST
            if(local_table.contains(txn_id)){
                throw new std::runtime_error("duplicate transaction entry being inserted");
            }
#endif
            local_table.emplace(txn_id,ptr);
            return ptr;
        }
        inline void commit_txn(entry_ptr ptr, uint64_t op_count, uint64_t commit_ts){
#if TXN_TABLE_TEST
            if(!local_table.contains(ptr->txn_id)){
                throw TransactionTableMissingEntryException();
            }
#endif
            ptr->op_count = op_count;
            ptr->status.store(commit_ts);
        }
        void abort_txn(entry_ptr ptr, uint64_t op_count){
            if(!op_count){
                local_table.erase(ptr->txn_id);
                delete ptr;
                return;
            }
            ptr->op_count.store(op_count);
            ptr->status.store(ABORT);
        }
    private:
        Map local_table;
    };
    class ConcurrentTransactionTables {
    public:
        ConcurrentTransactionTables()
        {
            tables.reserve(WORKER_THREAD_NUM);
            for(int i=0;i<WORKER_THREAD_NUM;i++){
                tables.push_back(ConcurrentTransactionTable());
            }
        }
        inline bool get_status(uint64_t txn_id,uint64_t& status_result){
            int32_t thread_id = get_threadID(txn_id);
            return tables[thread_id].get_status(txn_id,status_result);
        }
        inline void reduce_op_count(uint64_t txn_id,int64_t op_count){
            int32_t thread_id = get_threadID(txn_id);
            tables[thread_id].reduce_op_count(txn_id, op_count);
        }
        inline entry_ptr put_entry(uint64_t txn_id){
            int32_t thread_id = get_threadID(txn_id);
            return tables[thread_id].put_entry(txn_id);
        }
        //only read-write transactions are stored in the table, if a read-write transaction commits, it must has op_count>0
        //todo: check if this statement is true, can there be a read-write transaction that commits without any writes.
        inline void commit_txn(entry_ptr ptr, uint64_t op_count, uint64_t commit_ts){
#if TXN_TABLE_TEST
            if(op_count<=0){
                throw TransactionTableOpCountException();
            }
#endif
            ptr->op_count.store(op_count);
            ptr->status.store(commit_ts);
        }
        inline void abort_txn(entry_ptr ptr, uint64_t op_count){
            int32_t thread_id = get_threadID(ptr->txn_id);
            tables[thread_id].abort_txn(ptr,op_count);
        }

    private:
        std::vector<ConcurrentTransactionTable>tables;
    };
    class ArrayTransactionTables;
    struct touched_block_entry{
        touched_block_entry(int64_t vertex_id, label_t label):block_id(generate_block_id(vertex_id,label)){
        }
        uint64_t block_id;
    };
    class ArrayTransactionTable{

    };
}//namespace bwgraph

#endif //BWGRAPH_V2_TRANSACTION_TABLES_HPP
