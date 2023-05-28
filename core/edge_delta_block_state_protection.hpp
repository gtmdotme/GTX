//
// Created by zhou822 on 5/28/23.
//

#ifndef BWGRAPH_V2_EDGE_DELTA_BLOCK_STATE_PROTECTION_HPP
#define BWGRAPH_V2_EDGE_DELTA_BLOCK_STATE_PROTECTION_HPP
#include "bw_index.hpp"
#include "types.hpp"
//#include "bwgraph.hpp"
#include "block_access_ts_table.hpp"
namespace bwgraph{
    //protects blocks to always be in consistent states
    class BlockStateVersionProtectionScheme{
    public:
        inline static bool writer_access_block(uint8_t thread_id, uint64_t block_id, BwLabelEntry* target_label_entry,BlockAccessTimestampTable& block_ts_table ){
            EdgeDeltaBlockState current_state =target_label_entry->state.load();
            if(current_state!=EdgeDeltaBlockState::NORMAL){
                return false;
            }else{
                block_ts_table.store_block_access(thread_id,block_id);
                current_state = target_label_entry->state.load();
                if(current_state!=EdgeDeltaBlockState::NORMAL){
                    block_ts_table.release_block_access(thread_id);
                    return false;
                }else{
                    return true;
                }
            }
        }
        inline static bool reader_access_block(uint8_t thread_id, uint64_t block_id, BwLabelEntry* target_label_entry,BlockAccessTimestampTable& block_ts_table ){
            EdgeDeltaBlockState current_state =target_label_entry->state.load();
            if(current_state!=EdgeDeltaBlockState::NORMAL&&current_state!=EdgeDeltaBlockState::CONSOLIDATION){
                return false;
            }else{
                block_ts_table.store_block_access(thread_id,block_id);
                current_state = target_label_entry->state.load();
                if(current_state!=EdgeDeltaBlockState::NORMAL&&current_state!=EdgeDeltaBlockState::CONSOLIDATION){
                    block_ts_table.release_block_access(thread_id);
                    return false;
                }else{
                    return true;
                }
            }
        }
        inline static EdgeDeltaBlockState committer_aborter_access_block(uint8_t thread_id, uint64_t block_id, BwLabelEntry* target_label_entry,BlockAccessTimestampTable& block_ts_table){
            EdgeDeltaBlockState current_state =target_label_entry->state.load();
            if(current_state==EdgeDeltaBlockState::NORMAL||current_state==EdgeDeltaBlockState::CONSOLIDATION){
                block_ts_table.store_block_access(thread_id,block_id);
                current_state = target_label_entry->state.load();
                if(current_state!=EdgeDeltaBlockState::NORMAL&&current_state!=EdgeDeltaBlockState::CONSOLIDATION){
                    block_ts_table.release_block_access(thread_id);
                }
            }
            return current_state;
        }

    };
}
#endif //BWGRAPH_V2_EDGE_DELTA_BLOCK_STATE_PROTECTION_HPP
