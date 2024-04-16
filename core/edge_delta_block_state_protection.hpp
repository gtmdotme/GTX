//
// Created by zhou822 on 5/28/23.
//
#pragma once
//#ifndef BWGRAPH_V2_EDGE_DELTA_BLOCK_STATE_PROTECTION_HPP
//#define BWGRAPH_V2_EDGE_DELTA_BLOCK_STATE_PROTECTION_HPP
#include "bw_index.hpp"
#include "types.hpp"
//#include "bwgraph.hpp"
#include "block_access_ts_table.hpp"
namespace GTX{
#define STATE_PROTECTION_TEST false
    //protects blocks to always be in consistent states
    class BlockStateVersionProtectionScheme{
    public:
        inline static bool writer_access_block(uint8_t thread_id, uint64_t block_id, BwLabelEntry* target_label_entry,BlockAccessTimestampTable& block_ts_table ){
            EdgeDeltaBlockState current_state =target_label_entry->state.load(/*std::memory_order_acquire*/);
            if(current_state!=EdgeDeltaBlockState::NORMAL){
                return false;
            }else{
                block_ts_table.store_block_access(thread_id,block_id);
                current_state = target_label_entry->state.load(/*std::memory_order_acquire*/);
                if(current_state!=EdgeDeltaBlockState::NORMAL){
                    block_ts_table.release_block_access(thread_id);
                    return false;
                }else{
                    return true;
                }
            }
        }
        inline static bool reader_access_block(uint8_t thread_id, uint64_t block_id, BwLabelEntry* target_label_entry,BlockAccessTimestampTable& block_ts_table ){
            EdgeDeltaBlockState current_state =target_label_entry->state.load(/*std::memory_order_acquire*/);
            if(current_state!=EdgeDeltaBlockState::NORMAL&&current_state!=EdgeDeltaBlockState::CONSOLIDATION){
                return false;
            }else{
                block_ts_table.store_block_access(thread_id,block_id);
                current_state = target_label_entry->state.load(/*std::memory_order_acquire*/);
                if(current_state!=EdgeDeltaBlockState::NORMAL&&current_state!=EdgeDeltaBlockState::CONSOLIDATION){
                    block_ts_table.release_block_access(thread_id);
                    return false;
                }else{
                    return true;
                }
            }
        }
        inline static EdgeDeltaBlockState committer_aborter_access_block(uint8_t thread_id, uint64_t block_id, BwLabelEntry* target_label_entry,BlockAccessTimestampTable& block_ts_table){
            EdgeDeltaBlockState current_state =target_label_entry->state.load(/*std::memory_order_acquire*/);
            if(current_state==EdgeDeltaBlockState::NORMAL||current_state==EdgeDeltaBlockState::CONSOLIDATION){
                block_ts_table.store_block_access(thread_id,block_id);
                current_state = target_label_entry->state.load(/*std::memory_order_acquire*/);
                if(current_state!=EdgeDeltaBlockState::NORMAL&&current_state!=EdgeDeltaBlockState::CONSOLIDATION){
                    block_ts_table.release_block_access(thread_id);
                }
            }
            return current_state;
        }
        inline static void release_protection(uint8_t thread_id,BlockAccessTimestampTable& block_ts_table){
            block_ts_table.release_block_access(thread_id);
        }
        inline static void install_exclusive_state(EdgeDeltaBlockState new_state, uint8_t thread_id, uint64_t block_id, BwLabelEntry* target_label_entry,BlockAccessTimestampTable& block_ts_table){
#if STATE_PROTECTION_TEST
            size_t counter =0;
            if(new_state==EdgeDeltaBlockState::OVERFLOW){
                auto current_state = EdgeDeltaBlockState::NORMAL;
                if(target_label_entry->state.compare_exchange_strong(current_state,new_state ,std::memory_order_acq_rel)){
                    while(!block_ts_table.is_safe(thread_id,block_id)){
                        if(counter++==1000000000){
                            throw BlockStateException();
                        }
                    }
                }else{
                    throw BlockStateException();
                }
            }else if(new_state == EdgeDeltaBlockState::INSTALLATION){
                auto current_state = EdgeDeltaBlockState::CONSOLIDATION;
                if(target_label_entry->state.compare_exchange_strong(current_state,new_state,std::memory_order_acq_rel)){
                    while(!block_ts_table.is_safe(thread_id,block_id)){
                        if(counter++==1000000000){
                            throw BlockStateException();
                        }
                    }
                }else{
                    throw BlockStateException();
                }
            }else{
                throw BlockStateException();
            }
#else
            target_label_entry->state.store(new_state/*,std::memory_order_release*/);
            while(!block_ts_table.is_safe(thread_id,block_id));
#endif
        }
        inline static void install_shared_state(EdgeDeltaBlockState new_state, BwLabelEntry* target_label_entry ){
#if STATE_PROTECTION_TEST
            if(new_state==EdgeDeltaBlockState::CONSOLIDATION){
                auto current_state = EdgeDeltaBlockState::OVERFLOW;
                if(target_label_entry->state.compare_exchange_strong(current_state,new_state,std::memory_order_acq_rel)){
                    return;
                }else{
                    throw BlockStateException();
                }
            }else if(new_state==EdgeDeltaBlockState::NORMAL){
                auto current_state = EdgeDeltaBlockState::INSTALLATION;
                if(target_label_entry->state.compare_exchange_strong(current_state,new_state,std::memory_order_acq_rel)){
                    return;
                }else{
                    throw BlockStateException();
                }
            }
#else
            target_label_entry->state.store(new_state/*,std::memory_order_release*/);
#endif
        }
    };
}
//#endif //BWGRAPH_V2_EDGE_DELTA_BLOCK_STATE_PROTECTION_HPP
