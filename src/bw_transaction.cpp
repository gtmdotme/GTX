//
// Created by zhou822 on 6/1/23.
//
#include "../core/bw_transaction.hpp"
#include "../core/edge_delta_block_state_protection.hpp"
using namespace bwgraph;
//pessimistic mode
#if USING_PESSIMISTIC_MODE
//todo:: check if we should care about insert vs. update
Txn_Operation_Response RWTransaction::put_edge(vertex_t src, vertex_t dst, label_t label, std::string_view edge_data){
    //locate the vertex;
    auto& vertex_index_entry = graph.get_vertex_index_entry(src);
    //cannot insert to invalid vertex entry
    if(!vertex_index_entry.valid.load()){
        return Txn_Operation_Response::FAIL;
    }
    auto edge_label_block = block_manager.convert<EdgeLabelBlock>(vertex_index_entry.edge_label_block_ptr);
    //either access an existing entry or creating a new entry
    BwLabelEntry* target_label_entry = edge_label_block->writer_lookup_label(label);
    //calculate block id
    uint64_t block_id = generate_block_id(src,label);
    //enter the block protection first
    if(BlockStateVersionProtectionScheme::writer_access_block(thread_id,block_id,target_label_entry,block_access_ts_table)){
        if(target_label_entry->block_ptr==0){
            throw GraphNullPointerException();
        }
        auto current_block = block_manager.convert<EdgeDeltaBlockHeader>(target_label_entry->block_ptr);
        //if the block is already overflow, return and wait
        if(current_block->already_overflow()){
            BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
            return Txn_Operation_Response::WRITER_WAIT;
        }
        int32_t total_delta_chain_num = current_block->get_delta_chain_num();
        auto cached_delta_chain_access = per_block_cached_delta_chain_offsets.try_emplace(block_id, LockOffsetCache(target_label_entry->consolidation_time,total_delta_chain_num));
        uint32_t current_delta_chain_head_offset = 0;
        //if there exits
        if(!cached_delta_chain_access.second){
            if(cached_delta_chain_access.first->second.is_outdated(target_label_entry->consolidation_time.load())){
                uint64_t current_block_offset = current_block->get_current_offset();
                if(current_block->is_overflow_offset(current_block_offset)){
                    BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                    return Txn_Operation_Response::WRITER_WAIT;
                }
                bool reclaim_lock_offset_result = cached_delta_chain_access.first->second.reclaim_delta_chain_lock(current_block,target_label_entry,local_txn_id,read_timestamp,current_block_offset);
                if(!reclaim_lock_offset_result){
                    //need to abort: we can always safely use cache to abort
                    cached_delta_chain_access.first->second.eager_abort(current_block,target_label_entry,local_txn_id,current_block_offset);
                    BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                    return Txn_Operation_Response::FAIL;
                }
            }
            current_delta_chain_head_offset =  cached_delta_chain_access.first->second.ensure_delta_chain_cache(dst);//get the cached offset if there is a write already, otherwise saty at 0
        }
        auto* delta_chains_index = target_label_entry->delta_chain_index;
        const char* data = edge_data.data();
        delta_chain_id_t target_delta_chain_id = calculate_owner_delta_chain_id(dst,total_delta_chain_num);
        //indicate the current txn does not have the lock
        if(!current_delta_chain_head_offset){
            //todo: maybe also set protection on delta chain id?
           // auto lock_result = current_block->set_protection(dst,&lazy_update_records, read_timestamp);
           auto lock_result = current_block->set_protection_on_delta_chain(target_delta_chain_id,&lazy_update_records,read_timestamp);
            if(lock_result == Delta_Chain_Lock_Response::CONFLICT){
                BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                return Txn_Operation_Response::FAIL; //abort on write-write conflict
            }
            current_delta_chain_head_offset = delta_chains_index->at(target_delta_chain_id).get_raw_offset();
            auto allocate_delta_result = allocate_delta(current_block, static_cast<int32_t>(edge_data.size()));
            if(allocate_delta_result==EdgeDeltaInstallResult::SUCCESS){
                //todo: maybe add an exist check? if exist, insert delta; otherwise update delta
                current_block->append_edge_delta(dst,local_txn_id,EdgeDeltaType::UPDATE_DELTA, data,static_cast<int32_t>(edge_data.size()),current_delta_chain_head_offset,current_delta_offset,current_data_offset);
                cached_delta_chain_access.first->second.cache_vid_offset(dst,current_delta_offset);
                BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                op_count++;
                return Txn_Operation_Response::SUCCESS;
            }else if (allocate_delta_result==EdgeDeltaInstallResult::ALREADY_OVERFLOW){
               // current_block->release_protection_delta_chain(target_delta_chain_id);// actually no need to release the lock at all
                BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                return Txn_Operation_Response::WRITER_WAIT;
            }else{//I caused overflow
                //current_block->release_protection_delta_chain(target_delta_chain_id);
                consolidation(target_label_entry,block_id);
                return put_edge(src,dst,label,edge_data);
            }
        }else{//the current transaction already locks the delta chain
            //todo: check if this part can be merged together with the previous part
            auto allocate_delta_result = allocate_delta(current_block,static_cast<int32_t>(edge_data.size()));
            if(allocate_delta_result==EdgeDeltaInstallResult::SUCCESS){
                current_block->append_edge_delta(dst,local_txn_id,EdgeDeltaType::UPDATE_DELTA,data,static_cast<int32_t>(edge_data.size()),current_delta_chain_head_offset,current_delta_offset,current_data_offset);
                cached_delta_chain_access.first->second.cache_vid_offset(dst,current_delta_offset);
                BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                op_count++;
                return Txn_Operation_Response::SUCCESS;
            }else if(allocate_delta_result==EdgeDeltaInstallResult::ALREADY_OVERFLOW){
                //do not release lock
                BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                return Txn_Operation_Response::WRITER_WAIT;
            }else{
                consolidation(target_label_entry,block_id);
                return put_edge(src,dst,label,edge_data);
            }
        }
    }else{
        return Txn_Operation_Response::WRITER_WAIT;
    }
}

void RWTransaction::consolidation(bwgraph::BwLabelEntry *current_label_entry, EdgeDeltaBlockHeader* current_block, uint64_t block_id) {
    BlockStateVersionProtectionScheme::install_exclusive_state(EdgeDeltaBlockState::OVERFLOW,thread_id,block_id,current_label_entry,block_access_ts_table);
    uint32_t original_delta_offset = current_delta_offset-ENTRY_DELTA_SIZE;
    uint32_t original_data_offset = current_data_offset;
    uint64_t to_restore_offset = (((uint64_t)original_data_offset)<<32)+(uint64_t)original_delta_offset;
    
}
#endif
