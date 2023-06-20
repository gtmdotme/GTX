//
// Created by zhou822 on 5/28/23.
//

#include "core/bwgraph.hpp"
#include "core/bw_transaction.hpp"
using namespace bwgraph;

/*BwGraph::~BwGraph(){
    auto max_vid = vertex_index.get_current_allocated_vid();
    for(vertex_t vid = 1; vid<=max_vid; vid++){
        auto& vertex_index_entry = vertex_index.get_vertex_index_entry(vid);
        auto label_block = block_manager.convert<EdgeLabelBlock>(vertex_index_entry.edge_label_block_ptr);
        label_block->deallocate_all_delta_chains_indices();
    }
}*/

ROTransaction BwGraph::begin_read_only_transaction() {
    auto read_ts = commit_manager.get_current_read_ts();
    uint8_t worker_thread_id = thread_manager.get_worker_thread_id();
    block_access_ts_table.store_current_ts(worker_thread_id,read_ts);
    if(executed_txn_count.local()==garbage_collection_threshold||garbage_queues[worker_thread_id].get_queue().size()>=garbage_collection_threshold){
        auto safe_ts = block_access_ts_table.calculate_safe_ts();
        garbage_queues[worker_thread_id].free_block(safe_ts);
        executed_txn_count.local()=1;
    }
    return ROTransaction(*this,read_ts,txn_tables,block_manager,garbage_queues[worker_thread_id],block_access_ts_table,worker_thread_id);

}

RWTransaction BwGraph::begin_read_write_transaction() {
    auto read_ts = commit_manager.get_current_read_ts();
    uint8_t worker_thread_id = thread_manager.get_worker_thread_id();
    auto txn_id = txn_tables.get_table(worker_thread_id).generate_txn_id();
    auto txn_entry =  txn_tables.get_table(worker_thread_id).put_entry(txn_id);
    block_access_ts_table.store_current_ts(worker_thread_id,read_ts);
    if(executed_txn_count.local()==garbage_collection_threshold||garbage_queues[worker_thread_id].get_queue().size()>=garbage_collection_threshold){
        auto safe_ts = block_access_ts_table.calculate_safe_ts();
        garbage_queues[worker_thread_id].free_block(safe_ts);
        executed_txn_count.local()=1;
    }else{
        executed_txn_count.local()++;
    }
    return RWTransaction(*this,txn_id,read_ts,txn_entry,txn_tables,commit_manager,block_manager,garbage_queues[worker_thread_id],block_access_ts_table,recycled_vids[worker_thread_id]);
}

