//
// Created by zhou822 on 5/28/23.
//
#pragma once
//#ifndef BWGRAPH_V2_BWGRAPH_HPP
//#define BWGRAPH_V2_BWGRAPH_HPP
#include "bw_index.hpp"
#include "block_manager.hpp"
#include "transaction_tables.hpp"
#include "exceptions.hpp"
#include "types.hpp"
#include "block_access_ts_table.hpp"
#include "commit_manager.hpp"
#include "worker_thread_manager.hpp"
#include "previous_version_garbage_queue.hpp"
namespace bwgraph{
    class ROTransaction;
    class RWTransaction;


    class BwGraph {
    public:
#if USING_ARRAY_TABLE
        BwGraph(std::string block_path = "",size_t _max_block_size = 1ul << 32,
            std::string wal_path = ""): block_manager(block_path,_max_block_size), vertex_index(block_manager),txn_tables(this),garbage_queues(worker_thread_num, GarbageBlockQueue(&block_manager))/*,commit_manager(txn_tables)*/{
            for(uint32_t i=0; i<worker_thread_num;i++){
                txn_tables.get_table(i).set_garbage_queue(&garbage_queues[i]);
            }
    }
#else
        BwGraph(std::string block_path = "",size_t _max_block_size = 1ul << 32,
            std::string wal_path = ""): block_manager(block_path,_max_block_size), vertex_index(block_manager){
    }
#endif
        ~BwGraph(){
            auto max_vid = vertex_index.get_current_allocated_vid();
            for(vertex_t vid = 1; vid<=max_vid; vid++){
                auto& vertex_index_entry = vertex_index.get_vertex_index_entry(vid);
                auto label_block = block_manager.convert<EdgeLabelBlock>(vertex_index_entry.edge_label_block_ptr);
                label_block->deallocate_all_delta_chains_indices();
            }
        }
        ROTransaction begin_read_only_transaction();
        RWTransaction begin_read_write_transaction();
        inline vertex_t get_max_allocated_vid(){
            return vertex_index.get_current_allocated_vid();
        }
        inline VertexIndexEntry& get_vertex_index_entry(vertex_t vid){
            return vertex_index.get_vertex_index_entry(vid);
        }
        inline BlockManager& get_block_manager(){return block_manager;}
        inline BlockAccessTimestampTable& get_block_access_ts_table(){return block_access_ts_table;}
        inline CommitManager& get_commit_manager(){return commit_manager;}
        inline TxnTables & get_txn_tables(){return txn_tables;}
        inline VertexIndex& get_vertex_index(){return vertex_index;}
        inline uint8_t get_worker_thread_id(){return thread_manager.get_worker_thread_id();}
    private:
        BlockManager block_manager;
        VertexIndex vertex_index;
        TxnTables txn_tables;
        //CommitManager commit_manager;
        CommitManager commit_manager;
        BlockAccessTimestampTable block_access_ts_table;
        WorkerThreadManager thread_manager;
        std::vector<GarbageBlockQueue> garbage_queues;
        std::array<std::queue<vertex_t>,worker_thread_num> recycled_vids;
        tbb::enumerable_thread_specific<size_t> executed_txn_count;
        friend class ROTransaction;
        friend class RWTransaction;
    };
}


//#endif //BWGRAPH_V2_BWGRAPH_HPP
