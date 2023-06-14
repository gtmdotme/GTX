//
// Created by zhou822 on 5/28/23.
//

#ifndef BWGRAPH_V2_BLOCK_ACCESS_TS_TABLE_HPP
#define BWGRAPH_V2_BLOCK_ACCESS_TS_TABLE_HPP
#include "exceptions.hpp"
#include <atomic>
#include "graph_global.hpp"
#include "types.hpp"
namespace bwgraph{
    struct alignas(64) BlockAccessTSEntry{
        std::atomic_uint64_t accessed_block_id=0;
        std::atomic_uint64_t current_ts=0;
        char padding[48];
    };
    static_assert(sizeof(BlockAccessTSEntry)==64);
    class BlockAccessTimestampTable{
    public:
        inline void store_block_access(uint8_t thread_id, uint64_t block_id){table[thread_id].accessed_block_id.store(block_id);}
        inline void release_block_access(uint8_t thread_id){table[thread_id].accessed_block_id.store(BAD_BLOCK_ID);}
        bool is_safe(uint8_t thread_id, uint64_t block_id){
            for(uint8_t i=0; i<worker_thread_num; i++){
                if(i==thread_id){
                    if(table[i].accessed_block_id!=block_id){
                        throw BlockSafeAccessException();
                    }
                    continue;
                }
                if(table[i].accessed_block_id.load()==block_id){
                    return false;
                }
            }
            return true;
        }
        //thread does not reset this value at txn finish, just overwrite when new txn is created
        inline void store_current_ts(uint8_t thread_id, timestamp_t read_ts){table[thread_id].current_ts.store(read_ts);}
        /*
         * if a read_ts is x, it can read deltas created at x or >x, and it cannot read deltas invalidated at x, so all deltas invalidated at ts <= 10 is considered safe to deallocate
         */
        uint64_t calculate_safe_ts(){
            uint64_t min_ts = std::numeric_limits<uint64_t>::max();
            for(uint32_t i=0; i<worker_thread_num;i++){
                uint64_t current_ts = table[i].current_ts.load();
                min_ts = (current_ts<min_ts)?current_ts:min_ts;
            }
            return min_ts;
        }
    private:
        std::array<BlockAccessTSEntry,worker_thread_num> table;
    };
}
#endif //BWGRAPH_V2_BLOCK_ACCESS_TS_TABLE_HPP
