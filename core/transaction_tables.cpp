//
// Created by zhou822 on 5/23/23.
//

#include "transaction_tables.hpp"
#include "edge_delta_block_state_protection.hpp"
#include "bwgraph.hpp"
#include "exceptions.hpp"
#include "previous_version_garbage_queue.hpp"
namespace bwgraph{
    inline bool lazy_update(VertexDeltaHeader* vertex_delta, uint64_t txn_id, uint64_t status){
        return vertex_delta->lazy_update(txn_id,status);
    }
    inline bool lazy_update(BaseEdgeDelta* edge_delta, uint64_t txn_id, uint64_t status){
        return edge_delta->lazy_update(txn_id,status);
    }

    void ArrayTransactionTable::set_garbage_queue(bwgraph::GarbageBlockQueue *input_queue) {
        thread_local_garbage_queue = input_queue;
    }
    void ArrayTransactionTable::lazy_update_block(uintptr_t block_ptr) {
        EdgeDeltaBlockHeader* current_edge_delta_block = bwGraph->get_block_manager().convert<EdgeDeltaBlockHeader>(block_ptr);
        uint64_t current_combined_offset = current_edge_delta_block->get_current_offset();
        //if the offset is overflowing, a consolidation will happen soon, so let me exit and let consolidation do lazy update for me.
        if(current_edge_delta_block->is_overflow_offset(current_combined_offset)){
            return;
        }
        std::unordered_map<uint64_t,int32_t>lazy_update_records;
        uint32_t current_delta_offset = static_cast<uint32_t>(current_combined_offset&SIZE2MASK);
        BaseEdgeDelta* current_delta = current_edge_delta_block->get_edge_delta(current_delta_offset);
        while(current_delta_offset>0){
            if(current_delta->valid.load()){
                uint64_t original_ts = current_delta->creation_ts.load();
                if(is_txn_id(original_ts)){
                    uint64_t status =0;
                    if(txn_tables->get_status(original_ts,status)){
                        if(status!=IN_PROGRESS){
                            if(current_delta->lazy_update(original_ts,status)){
                              auto emplace_result =lazy_update_records.try_emplace(original_ts,1);
                              if(!emplace_result.second){
                                  emplace_result.first->second++;
                              }
                              if(status!=ABORT){
                                  current_edge_delta_block->update_previous_delta_invalidate_ts(current_delta->toID,current_delta->previous_offset,status);
                              }
                            }
                        }
                    }
                }
            }
            current_delta++;
            current_delta_offset-=ENTRY_DELTA_SIZE;
        }
        for(auto it = lazy_update_records.begin();it!=lazy_update_records.end();it++){
            txn_tables->reduce_op_count(it->first,it->second);
        }
    }
    void ArrayTransactionTable::eager_clean(uint64_t index) {
        //need to access BwGraph and its block manager
        auto& entry = local_table[index];
        for(size_t i=0; i<entry.touched_blocks.size();i++){
            //if already cleaned-up, can quickly return
            if(!entry.op_count.load()){
                return;
            }
            auto& touched_block_entry = entry.touched_blocks.at(i);
            auto target_vid_label_pair =decompose_block_id(touched_block_entry.block_id);
            auto& index_entry = bwGraph->get_vertex_index_entry(target_vid_label_pair.first);//get vertex index entry
            //if the entry is already invalid, its edge delta blocks must already be lazy updated and freed.
            //todo: check its correctness
            if (!index_entry.valid.load()){
                continue;
            }
            //if it refers to labeled edge block
            if(target_vid_label_pair.second){
                //label block never gets deleted because we can reuse it
                DeltaLabelBlock* target_label_block = bwGraph->get_block_manager().convert<DeltaLabelBlock>(index_entry.edge_label_block_ptr.load());
                BwLabelEntry* target_label_entry;
                if(target_label_block->reader_lookup_label(target_vid_label_pair.second,target_label_entry)){
                    //if the block is under overflow or installation states, we know another thread is doing consolidation work so it will lazy update our transaction!
                   if( BlockStateVersionProtectionScheme::reader_access_block(thread_id,touched_block_entry.block_id,target_label_entry,bwGraph->get_block_access_ts_table())){
                       //if the block still exists
                        if(target_label_entry->block_ptr){
                            lazy_update_block(target_label_entry->block_ptr);
                        }
                       BlockStateVersionProtectionScheme::release_protection(thread_id,bwGraph->get_block_access_ts_table());
                   }
                }else{
                    //never delete entries, at the worst mark the block ptr as 0. So if I modified this label, the entry must exist
                    throw LabelEntryMissingException();
                }
            }else{
                auto current_vertex_ptr = index_entry.vertex_delta_chain_head_ptr.load();
                if(current_vertex_ptr){
                    VertexDeltaHeader* vertex_delta = bwGraph->get_block_manager().convert<VertexDeltaHeader>(current_vertex_ptr);
                    auto current_ts = vertex_delta->get_creation_ts();
                    if(is_txn_id(current_ts)){
                        uint64_t status =0;
                        if(txn_tables->get_status(current_ts,status)){
                            if(status!=IN_PROGRESS){
                                if(lazy_update(vertex_delta,current_ts,status)){
                                    //todo: throw in garbage queue
                                    uintptr_t previous_ptr = vertex_delta->get_previous_ptr();
                                    if(status==ABORT){
                                        //index_entry.vertex_delta_chain_head_ptr.store(previous_ptr);
                                        //remove aborted delta from the version chain
                                        //let threads compete for this installation
                                        if( index_entry.vertex_delta_chain_head_ptr.compare_exchange_strong(current_vertex_ptr,previous_ptr)){
                                            thread_local_garbage_queue->register_entry(current_vertex_ptr,vertex_delta->get_order(),bwGraph->get_commit_manager().get_current_read_ts()+1);
                                        }
                                    }else{
                                        //register when the previous version becomes outdated
                                        if(previous_ptr){
                                            auto previous_delta = bwGraph->get_block_manager().convert<VertexDeltaHeader>(previous_ptr);
                                            thread_local_garbage_queue->register_entry(previous_ptr,previous_delta->get_order(),status);
                                        }
                                    }
                                    txn_tables->reduce_op_count(current_ts,1);
                                }
                            }
                        }
                    }
                }
            }
        }
        while(!entry.op_count.load());
    }
}