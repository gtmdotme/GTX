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
            std::string wal_path = ""): block_manager(block_path,_max_block_size), vertex_index(block_manager),txn_tables(this),garbage_queues(worker_thread_num, GarbageBlockQueue(&block_manager))
            , local_thread_vertex_write_time(0),local_thread_edge_write_time(0),local_thread_commit_time(0),local_thread_abort_time(0),local_rwtxn_creation_time(0)
            ,local_get_thread_id_time(0),local_install_txn_entry_time(0),local_garbage_collection_time(0)/*,commit_manager(txn_tables)*/{
            for(uint32_t i=0; i<worker_thread_num;i++){
                txn_tables.get_table(i).set_garbage_queue(&garbage_queues[i]);
#if TRACK_EXECUTION_TIME
            /*    global_vertex_read_time_array[i]=0;
                global_vertex_write_time_array[i]=0;
                global_edge_read_time_array[i]=0;
                global_edge_write_time_array[i]=0;*/
#endif
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
#if TRACK_EXECUTION_TIME
            uint64_t participating_thread_count = 0;
            uint64_t total_v_write_time=0;
            uint64_t total_e_write_time =0;
            uint64_t total_thread_commit_time =0;
            uint64_t total_thread_abort_time =0;
            uint64_t total_rw_txn_creation_time =0;
            uint64_t total_get_thread_id_time =0;
            uint64_t total_install_txn_entry_time =0;
            uint64_t total_garbage_collection_time =0;
            for(auto v_w_t : local_thread_vertex_write_time){
                total_v_write_time+= v_w_t;
                participating_thread_count++;
            }
            for(auto e_w_t : local_thread_edge_write_time){
                total_e_write_time+=e_w_t;
            }
            for(auto thread_c_t : local_thread_commit_time){
                total_thread_commit_time+=thread_c_t;
            }
            for(auto thread_a_t : local_thread_abort_time){
                total_thread_abort_time+= thread_a_t;
            }
            for(auto thread_txn_creation_t :local_rwtxn_creation_time){
                total_rw_txn_creation_time+=thread_txn_creation_t;
            }
            for(auto get_thread_id_t : local_get_thread_id_time){
                total_get_thread_id_time+=get_thread_id_t;
            }
            for(auto install_txn_entry_time : local_install_txn_entry_time){
                total_install_txn_entry_time+=install_txn_entry_time;
            }
            for(auto garbage_collection_time : local_garbage_collection_time){
                total_garbage_collection_time+=garbage_collection_time;
            }
            std::cout<<"total worker thread count is "<<participating_thread_count<<std::endl;
            std::cout<<"average vertex write time per thread is "<<total_v_write_time/participating_thread_count<<std::endl;
            std::cout<<"average edge write time per thread is "<<total_e_write_time/participating_thread_count<<std::endl;
            std::cout<<"average txn commit time per thread is "<<total_thread_commit_time/participating_thread_count<<std::endl;
            std::cout<<"average txn abort time per thread is "<<total_thread_abort_time/participating_thread_count<<std::endl;
            std::cout<<"average txn creation time per thread is "<<total_rw_txn_creation_time/participating_thread_count<<std::endl;
            std::cout<<"average get thread id time per thread is "<<total_get_thread_id_time/participating_thread_count<<std::endl;
            std::cout<<"average txn entry install time per thread is "<<total_install_txn_entry_time/participating_thread_count<<std::endl;
            std::cout<<"average garbage collection time per thread is "<<total_garbage_collection_time/participating_thread_count<<std::endl;
#endif
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
        void execute_manual_delta_block_checking(vertex_t vid);
#if TRACK_EXECUTION_TIME
        tbb::enumerable_thread_specific<size_t> local_thread_vertex_read_time;
        //std::array<std::atomic_uint64_t , worker_thread_num> global_vertex_read_time_array;
        tbb::enumerable_thread_specific<size_t> local_thread_vertex_write_time;
        //std::array<std::atomic_uint64_t , worker_thread_num> global_vertex_write_time_array;
        tbb::enumerable_thread_specific<size_t> local_thread_edge_read_time;
        //std::array<std::atomic_uint64_t , worker_thread_num> global_edge_read_time_array;
        tbb::enumerable_thread_specific<size_t> local_thread_edge_write_time;
        //std::array<std::atomic_uint64_t , worker_thread_num> global_edge_write_time_array;
        tbb::enumerable_thread_specific<size_t> local_thread_commit_time;
        tbb::enumerable_thread_specific<size_t> local_thread_abort_time;
        tbb::enumerable_thread_specific<size_t> local_rwtxn_creation_time;
        tbb::enumerable_thread_specific<size_t> local_get_thread_id_time;
        tbb::enumerable_thread_specific<size_t> local_install_txn_entry_time;
        tbb::enumerable_thread_specific<size_t> local_garbage_collection_time;
#endif
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
