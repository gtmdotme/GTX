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
    BwLabelEntry* target_label_entry = edge_label_block->writer_lookup_label(label,&txn_tables);
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
                consolidation(target_label_entry,current_block, block_id);
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
                consolidation(target_label_entry, current_block, block_id);
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
    uint64_t to_restore_offset = combine_offset(original_delta_offset, original_data_offset);
    current_block->set_offset(to_restore_offset);
    BlockStateVersionProtectionScheme::install_shared_state(EdgeDeltaBlockState::CONSOLIDATION,current_label_entry);
    std::unordered_set<vertex_t> edge_latest_versions_records;
    std::vector<uint32_t> edge_latest_version_offsets;
    BaseEdgeDelta* current_delta = current_block->get_edge_delta(original_delta_offset);
    std::unordered_map<uint64_t, std::vector<uint32_t>>in_progress_delta_per_txn;
    timestamp_t largest_invalidation_ts = std::numeric_limits<uint64_t>::min();//most recent previous version
    timestamp_t largest_creation_ts = std::numeric_limits<uint64_t>::min();//most recent (committed) version
    size_t data_size = 0;
    int32_t current_delta_chain_num = current_block->get_delta_chain_num();
    std::set<delta_chain_id_t> to_check_delta_chains;
    while(original_delta_offset>0){
        //should there be no unvalid deltas
        if(!current_delta->valid.load()){
            throw std::runtime_error("all writer transactions should already installed their deltas");
        }
        timestamp_t original_ts = current_delta->creation_ts.load();
        //do lazy update if possible
        if(is_txn_id(original_ts)){
            uint64_t status = 0;
            if(txn_tables.get_status(original_ts,status)){
                if(status == IN_PROGRESS){
                    //do nothing
                }else{
                    if(current_delta->lazy_update(original_ts,status)){
                        //record lazy update
                        record_lazy_update_record(&lazy_update_records, original_ts);
                        if(status!=ABORT){
                            current_block->update_previous_delta_invalidate_ts(current_delta->toID, current_delta->previous_offset, status);
                        }
                    }else{
                        if(current_delta->creation_ts.load()!=status){
                            throw LazyUpdateException();
                        }
                    }
                }
            }
        }
        original_ts = current_delta->creation_ts.load();
        //now check the status
        if(is_txn_id(original_ts)){
            auto in_progress_txn_deltas_emplace_result = in_progress_delta_per_txn.try_emplace(original_ts, std::vector<uint32_t>());
            in_progress_txn_deltas_emplace_result.first->second.emplace_back(original_delta_offset);
            data_size+=current_delta->data_length+ENTRY_DELTA_SIZE;
            to_check_delta_chains.emplace(calculate_owner_delta_chain_id(current_delta->toID, current_delta_chain_num));
        }else if(original_ts!=ABORT){
            //skip committed delete deltas
            if(current_delta->delta_type!=EdgeDeltaType::DELETE_DELTA){
                vertex_t toID = current_delta->toID;
                auto latest_version_emplace_result = edge_latest_versions_records.emplace(toID);
                //if indeed latest version
                if(latest_version_emplace_result.second){
                    edge_latest_version_offsets.emplace_back(original_delta_offset);
                    largest_creation_ts = (largest_creation_ts>=original_ts)? largest_creation_ts:original_ts;
                    data_size+=current_delta->data_length+ENTRY_DELTA_SIZE;
                }else{
                    if(!current_delta->invalidate_ts){
                        throw LazyUpdateException();
                    }
                }
            }
        }else{
            //do nothing
        }
        original_delta_offset-=ENTRY_DELTA_SIZE;
        current_delta++;
    }
    //handle edge case that the initial block was too small
    data_size = (data_size==0)? ENTRY_DELTA_SIZE:data_size;
    //analyze scan finished, now apply heuristics
    uint64_t lifespan = largest_creation_ts - current_label_entry->consolidation_time; //approximate lifespan of the block
    //todo:; apply different heuristics
    size_t new_block_size = calculate_nw_block_size_from_lifespan(data_size,lifespan,20);
    auto new_order = size_to_order(new_block_size);
    auto new_block_ptr = block_manager.alloc(new_order);
    auto new_block = block_manager.convert<EdgeDeltaBlockHeader>(new_block_ptr);
    new_block->fill_metadata(current_block->get_owner_id(),largest_invalidation_ts,current_label_entry->block_ptr,new_order, &txn_tables);
    int32_t new_block_delta_chain_num = new_block->get_delta_chain_num();
    std::vector<AtomicDeltaOffset> new_delta_chains_index(new_block_delta_chain_num);
    //start installing latest version
    int64_t signed_edge_last_version_size = static_cast<int64_t>(edge_latest_version_offsets.size());
    for(int64_t i = (signed_edge_last_version_size-1); i>=0; i--){
        current_delta = current_block->get_edge_delta(edge_latest_version_offsets.at(i));
        if(is_txn_id(current_delta->creation_ts.load())){
            throw ConsolidationException();//latest versions should all be committed deltas
        }
        //find which delta chain the latest version delta belongs to
        delta_chain_id_t target_delta_chain_id = new_block->get_delta_chain_id(current_delta->toID);
        const char* data = current_block->get_edge_data(current_delta->data_length);
        auto& new_delta_chains_index_entry = new_delta_chains_index.at(target_delta_chain_id);
        uint32_t new_block_delta_offset = new_delta_chains_index_entry.get_offset();//if cannot be locked
        auto consolidation_append_result = new_block->append_edge_delta(current_delta->toID,current_delta->creation_ts.load(),current_delta->delta_type,data,current_delta->data_length,new_block_delta_offset);
        if(consolidation_append_result.first!=EdgeDeltaInstallResult::SUCCESS||!consolidation_append_result.second){
            throw ConsolidationException();
        }
        new_delta_chains_index_entry.update_offset(consolidation_append_result.second);
    }
    //we install all committed latest versions and form delta chains
    //now we start the exclusive installation phase
    BlockStateVersionProtectionScheme::install_exclusive_state(EdgeDeltaBlockState::INSTALLATION,thread_id,block_id,current_label_entry,block_access_ts_table);
    //wait for all validating transactions to finish
    while(true){
        for(auto it = to_check_delta_chains.begin(); it!= to_check_delta_chains.end();){
            //get the delta chain head of this delta chain in the original block, check if a validation is happening
            current_delta = current_block->get_edge_delta(current_label_entry->delta_chain_index->at(*it).get_raw_offset());
            uint64_t original_ts = current_delta->creation_ts.load();
            if(is_txn_id(original_ts)){
                uint64_t status =0;
                if(txn_tables.get_status(original_ts,status)){
                    //this transaction still validating, so we will come back later
                    if(status == IN_PROGRESS){
                        it++;
                        continue;
                    }else if(status ==ABORT){ //validating txn aborted, so consolidating thread will help with lazy update
                        uint32_t to_abort_offset = current_delta->previous_offset;
                        if(!current_delta->lazy_update(original_ts,status)){
                            throw ConsolidationException();//we should never fail lazy update during an exclusive state
                        }
                        auto abort_lazy_update_emplace_result = lazy_update_records.try_emplace(original_ts,1);
                        if(!abort_lazy_update_emplace_result.second){
                            abort_lazy_update_emplace_result.first->second++;
                        }
                        while(to_abort_offset!=0){
                            current_delta = current_block->get_edge_delta(to_abort_offset);
                            if(current_delta->creation_ts!=original_ts&&current_delta->creation_ts!=ABORT){
                                break;
                            }else if(current_delta->creation_ts==original_ts){
                                if(current_delta->lazy_update(original_ts,status)){
                                    abort_lazy_update_emplace_result.first->second++;
                                }else{
                                    throw ConsolidationException();
                                }
                            }
                            to_abort_offset = current_delta->previous_offset;
                        }
                        it = to_check_delta_chains.erase(it);//this delta chain is cleaned
                    }else{//original_ts txn committed after passing validation phase
                        uint32_t to_commit_offset = current_delta->previous_offset;
                        if(!current_delta->lazy_update(original_ts, status)){
                            throw ConsolidationException();
                        }
                        auto commit_lazy_update_emplace_result = lazy_update_records.try_emplace(original_ts,1);
                        if(!commit_lazy_update_emplace_result.second){
                            commit_lazy_update_emplace_result.first->second++;
                        }
                        while(to_commit_offset!=0){
                            current_delta= current_block->get_edge_delta(to_commit_offset);
                            if(current_delta->creation_ts!=original_ts&&current_delta->creation_ts!=status){
#if EDGE_DELTA_TEST
                                if(current_delta->creation_ts==ABORT){
                                    throw LazyUpdateException();
                                }
#endif
                                break;
                            }else if(current_delta->creation_ts==original_ts){
                                if(!current_delta->lazy_update(original_ts,status)){
                                    throw ConsolidationException();
                                }
                                commit_lazy_update_emplace_result.first->second++;
                            }
                            to_commit_offset = current_delta->previous_offset;
                        }
                        it = to_check_delta_chains.erase(it);
                    }
                }else{
                    throw ConsolidationException();//Installation phase is supposed to be exclusive.
                }
            }else{
                //this delta chain is not under validating or someone already lazy updated for it
                it = to_check_delta_chains.erase(it);
            }
        }
        if(to_check_delta_chains.empty()){
            break;
        }
    }
    //now check all in progress deltas recorded earlier
    //need 2 for loops because committed deltas should be before in-progress deltas
    for(auto it = in_progress_delta_per_txn.begin();it!=in_progress_delta_per_txn.end();){
        //uint64_t status;
        auto& all_delta_offsets_of_txn = it->second;
        current_delta = current_block->get_edge_delta(all_delta_offsets_of_txn.at(0));
        //skip them, will be installed at the end.
        if(is_txn_id(current_delta->creation_ts.load())){
            it++;
        }else if(current_delta->creation_ts.load()==ABORT){
            it = in_progress_delta_per_txn.erase(it);
        }else{//committed deltas
            timestamp_t commit_ts = current_delta->creation_ts.load();
            uint64_t txn_id = it->first;
            int64_t txn_own_deltas_size = static_cast<int64_t>(all_delta_offsets_of_txn.size());
            for(int64_t i = txn_own_deltas_size-1; i>=0; i--){
                current_delta= current_block->get_edge_delta(all_delta_offsets_of_txn.at(i));
                if(current_delta->creation_ts.compare_exchange_strong(txn_id,commit_ts)){
                    record_lazy_update_record(&lazy_update_records,txn_id);
                }
                //only install non-delete deltas
                if(current_delta->delta_type!=EdgeDeltaType::DELETE_DELTA){
                    const char* data = current_block->get_edge_data(current_delta->data_offset);
                    delta_chain_id_t new_delta_chain_id = new_block->get_delta_chain_id(current_delta->toID);
                    auto& new_delta_chains_index_entry = new_delta_chains_index.at(new_delta_chain_id);
                    //can get raw offset because no lock yet
                    auto commit_delta_append_result = new_block->append_edge_delta(current_delta->toID,commit_ts,current_delta->delta_type,data,current_delta->data_length,new_delta_chains_index_entry.get_offset());
                    //can be a newer version:
                    new_block->update_previous_delta_invalidate_ts(current_delta->toID,new_delta_chains_index_entry.get_offset(),commit_ts);
                    if(commit_delta_append_result.first!=EdgeDeltaInstallResult::SUCCESS||!commit_delta_append_result.second){
                        throw ConsolidationException();
                    }
                    new_delta_chains_index_entry.update_offset(commit_delta_append_result.second);
                }
            }
            it = in_progress_delta_per_txn.erase(it);
        }
    }
    //finally install those still in-progress deltas per txn
    for(auto it = in_progress_delta_per_txn.begin(); it!=in_progress_delta_per_txn.end();it++){
        uint64_t txn_id = it->first;
#if CONSOLIDATION_TEST
        uint64_t status;
        if(txn_tables.get_status(txn_id, status)){
            if(status!=IN_PROGRESS){
                throw ConsolidationException();
            }
        }else{
            throw ConsolidationException();
        }
#endif
        auto& txn_in_progress_deltas = it->second;
        std::vector<uint32_t>local_delta_chains_index_cache(new_block_delta_chain_num);
        for(int32_t j=0; j<new_block_delta_chain_num;j++){
            local_delta_chains_index_cache[j]=new_delta_chains_index[j].get_offset();
        }
        int64_t txn_delta_num = static_cast<int64_t>(txn_in_progress_deltas.size());
        for(int64_t i =txn_delta_num-1; i>=0; i--){
            current_delta = current_block->get_edge_delta(txn_in_progress_deltas.at(i));
#if CONSOLIDATION_TEST
            if(current_delta->creation_ts.load()!=txn_id){
                throw ConsolidationException();
            }
#endif
            const char* data = current_block->get_edge_data(current_delta->data_offset);
            delta_chain_id_t delta_chain_id = new_block->get_delta_chain_id(current_delta->toID);
            auto in_progress_delta_append_result = new_block->append_edge_delta(current_delta->toID, txn_id, current_delta->delta_type, data, current_delta->data_length, local_delta_chains_index_cache[delta_chain_id]);
#if CONSOLIDATION_TEST
            if(in_progress_delta_append_result.first!=EdgeDeltaInstallResult::SUCCESS||!in_progress_delta_append_result.second){
                throw ConsolidationException();
            }
#endif
            local_delta_chains_index_cache[delta_chain_id]= in_progress_delta_append_result.second;
        }
    }
    //now consolidation is over
    per_thread_garbage_queue.register_entry(current_label_entry->block_ptr,current_block->get_order(),largest_invalidation_ts);
    *current_label_entry->delta_chain_index = std::move(new_delta_chains_index);//todo::check its correctness
    current_label_entry->block_ptr = new_block_ptr;
    current_label_entry->consolidation_time.store(commit_manager.get_current_read_ts());
    BlockStateVersionProtectionScheme::install_shared_state(EdgeDeltaBlockState::NORMAL,current_label_entry);
    BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
}
#endif
