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
        //garbage_queues[worker_thread_id].free_block(safe_ts);
        executed_txn_count.local()=1;
    }
    return ROTransaction(*this,read_ts,txn_tables,block_manager,garbage_queues[worker_thread_id],block_access_ts_table,worker_thread_id);

}

RWTransaction BwGraph::begin_read_write_transaction() {
#if TRACK_EXECUTION_TIME
    auto start = std::chrono::high_resolution_clock::now();
    auto read_ts = commit_manager.get_current_read_ts();
    uint8_t worker_thread_id = thread_manager.get_worker_thread_id();
    auto stop_thread_id = std::chrono::high_resolution_clock::now();
    auto txn_id = txn_tables.get_table(worker_thread_id).generate_txn_id();
    auto stop_txn_id = std::chrono::high_resolution_clock::now();
    auto txn_entry =  txn_tables.get_table(worker_thread_id).put_entry(txn_id);
    auto stop_txn_entry= std::chrono::high_resolution_clock::now();
    block_access_ts_table.store_current_ts(worker_thread_id,read_ts);
    if(executed_txn_count.local()==garbage_collection_threshold||garbage_queues[worker_thread_id].get_queue().size()>=garbage_collection_threshold){
        auto safe_ts = block_access_ts_table.calculate_safe_ts();
        garbage_queues[worker_thread_id].free_block(safe_ts);
        executed_txn_count.local()=1;
    }else{
        executed_txn_count.local()++;
    }
    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
    local_rwtxn_creation_time.local()+= duration.count();
    auto get_thread_id_time = std::chrono::duration_cast<std::chrono::microseconds>(stop_thread_id - start);
    local_get_thread_id_time.local()+=get_thread_id_time.count();
    auto generate_txn_id_time = std::chrono::duration_cast<std::chrono::microseconds>(stop_txn_id-stop_thread_id);
    local_generate_txn_id_time.local()+=generate_txn_id_time.count();
    auto install_txn_entry_time = std::chrono::duration_cast<std::chrono::microseconds>(stop_txn_entry-stop_txn_id);
    local_install_txn_entry_time.local()+=install_txn_entry_time.count();
    auto garbage_collection_time = std::chrono::duration_cast<std::chrono::microseconds>(stop - stop_txn_entry);
    local_garbage_collection_time.local()+=garbage_collection_time.count();
    return RWTransaction(*this,txn_id,read_ts,txn_entry,txn_tables,commit_manager,block_manager,garbage_queues[worker_thread_id],block_access_ts_table,recycled_vids[worker_thread_id]);
#else
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
#endif
}

void BwGraph::execute_manual_delta_block_checking(bwgraph::vertex_t vid) {
    auto& vertex_index_entry = get_vertex_index_entry(vid);
    auto label_block = get_block_manager().convert<EdgeLabelBlock>(vertex_index_entry.edge_label_block_ptr);
    BwLabelEntry* current_label_entry;
    for(label_t label =1; label<=3; label++){
        size_t total_committed_delta_count_from_secondary_index = 0;
        size_t total_committed_delta_count_from_pure_scan = 0;
        if(!label_block->reader_lookup_label(label,current_label_entry)){
            continue;
        }
        auto current_block = get_block_manager().convert<EdgeDeltaBlockHeader>(current_label_entry->block_ptr);
        auto delta_chains_num = current_block->get_delta_chain_num();
        if(delta_chains_num!= static_cast<int32_t>(current_label_entry->delta_chain_index->size())){
            throw std::runtime_error("delta chain num mismatch");
        }
        std::unordered_map<vertex_t , timestamp_t>track_invalidate_ts;
        struct pair_hash{
            std::size_t operator()(std::pair<vertex_t , uint32_t>const & v)const{
                return std::hash<int64_t>()(v.first)+std::hash<uint32_t>()(v.second);
            }
        };
        std::unordered_set<std::pair<vertex_t ,uint32_t>,pair_hash>secondary_index_committed_entries;
        for(size_t i=0; i<current_label_entry->delta_chain_index->size();i++){
            uint32_t offset = current_label_entry->delta_chain_index->at(i).get_offset();
            if(offset&LOCK_MASK){
                throw std::runtime_error("locks should be unlocked already");
            }
            while(offset){
                BaseEdgeDelta* delta = current_block->get_edge_delta(offset);
                if(is_txn_id(delta->creation_ts.load())){
                    throw LazyUpdateException();
                }
                if(delta->creation_ts==ABORT){
                    throw LazyUpdateException();
                }
                if(static_cast<delta_chain_id_t>(delta->toID)%delta_chains_num!=static_cast<delta_chain_id_t>(i)){
                    throw DeltaChainCorruptionException();
                }
                total_committed_delta_count_from_secondary_index++;
                /* char* data = current_block->get_edge_data(delta->data_offset);
                 char to_compare = static_cast<char>(delta->toID%32);
                 for(uint32_t j=0; j<delta->data_length;j++){
                     if(data[j]!=to_compare){
                         throw TransactionReadException();
                     }
                 }*/
                if(!secondary_index_committed_entries.emplace(std::pair<int64_t,uint32_t>(delta->toID,offset)).second){
                    throw new std::runtime_error("error, duplicate entry");
                }
                offset = delta->previous_offset;
            }
        }
        size_t total_size =0;
        uint64_t current_offsets = current_block->get_current_offset();
        uint64_t original_data_offset = current_offsets>>32;
        uint64_t original_delta_offset = current_offsets&SIZE2MASK;
        uint32_t current_delta_offset = static_cast<uint32_t>(current_offsets&SIZE2MASK);
        BaseEdgeDelta* current_delta = current_block->get_edge_delta(current_delta_offset);
        while(current_delta_offset){
            total_size+=current_delta->data_length+ENTRY_DELTA_SIZE;
            if(is_txn_id((current_delta->creation_ts.load()))){
                throw LazyUpdateException();
            }else if(current_delta->creation_ts!=ABORT){
                if(!secondary_index_committed_entries.count(std::pair<vertex_t, uint32_t>(current_delta->toID,current_delta_offset))){
                    throw std::runtime_error("found an entry not captured by the delta chains");
                }
                if(track_invalidate_ts.count(current_delta->toID)){
                    if(current_delta->invalidate_ts!=track_invalidate_ts.at(current_delta->toID)){
                        throw std::runtime_error("invalidation ts mismatch");
                    }
                    track_invalidate_ts.insert_or_assign(current_delta->toID, current_delta->creation_ts.load());
                }else{
                    if(current_delta->invalidate_ts!=0){
                        throw std::runtime_error("invalidation ts mismatch");
                    }
                    if(!track_invalidate_ts.try_emplace(current_delta->toID,current_delta->creation_ts.load()).second){
                        throw std::runtime_error("should succeed");
                    }
                }
                total_committed_delta_count_from_pure_scan++;
            }
            /*  char* data = current_block->get_edge_data(current_delta->data_offset);
              char to_compare = static_cast<char>(current_delta->toID%32);
              for(uint32_t j=0; j<current_delta->data_length;j++){
                  if(data[j]!=to_compare){
                      throw TransactionReadException();
                  }
              }*/
            current_delta++;
            current_delta_offset-=ENTRY_DELTA_SIZE;
        }
        if(original_data_offset+original_delta_offset!=total_size){
            throw new std::runtime_error("error, the offset did not correctly represent the delta allocation");
        }
        if(total_committed_delta_count_from_pure_scan!=total_committed_delta_count_from_secondary_index){
            throw new std::runtime_error("error, secondary index does not contain all committed deltas");
        }
    }
}
