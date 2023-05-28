//
// Created by zhou822 on 5/28/23.
//

#ifndef BWGRAPH_V2_BW_TRANSACTION_HPP
#define BWGRAPH_V2_BW_TRANSACTION_HPP

//#include <unordered_set>
//#include <map>
#include "bw_index.hpp"
#include "transaction_tables.hpp"
namespace bwgraph{
    struct LockOffsetCache{
        LockOffsetCache(uint64_t input_ts, int32_t input_size):consolidation_ts(input_ts),delta_chain_num(input_size){}
        inline bool is_outdated(uint64_t current_consolidation_ts){
            return current_consolidation_ts != consolidation_ts;
        }

        uint64_t consolidation_ts;
        int32_t delta_chain_num;
        std::unordered_set<int64_t>already_modified_edges;
        std::map<int32_t,uint32_t>updated_delta_chain_head_offsets;
    };
    class ROTransaction{

    };
    class RWTransaction{
    public:
        //implement constructor

    private:
        const uint64_t local_txn_id;
        const uint64_t read_timestamp;
        entry_ptr self_entry;
#if USING_ARRAY_TABLE
        ArrayTransactionTables& txn_tables;
#else
        ConcurrentTransactionTables& txn_tables;
#endif
        std::unordered_map<uint64_t,int32_t>lazy_update_records;
        std::map<uint64_t, LockOffsetCache>cached_delta_chain_offsets;//at commit time do a merge
        int32_t op_count=0;
        BlockManager& block_manager;
        uint8_t thread_id;
        uint32_t current_delta_offset;
        uint32_t current_data_offset;
    };
}
#endif //BWGRAPH_V2_BW_TRANSACTION_HPP
