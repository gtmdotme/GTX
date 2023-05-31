//
// Created by zhou822 on 5/28/23.
//

#ifndef BWGRAPH_V2_BW_TRANSACTION_HPP
#define BWGRAPH_V2_BW_TRANSACTION_HPP

//#include <unordered_set>
//#include <map>
#include "bw_index.hpp"
#include "transaction_tables.hpp"
#include "edge_iterator.hpp"
namespace bwgraph{
    struct LockOffsetCache{
        LockOffsetCache(uint64_t input_ts, int32_t input_size):consolidation_ts(input_ts),delta_chain_num(input_size){}
        inline bool is_outdated(uint64_t current_consolidation_ts){
            return current_consolidation_ts != consolidation_ts;
        }

        uint64_t consolidation_ts;
        int32_t delta_chain_num;
        std::unordered_set<vertex_t>already_modified_edges;
        std::map<int32_t,uint32_t>updated_delta_chain_head_offsets;
    };
    class ROTransaction{

    };
    class RWTransaction{
    public:
        //implement constructor

        //transaction graph operations
        Txn_Operation_Response put_edge(vertex_t src, vertex_t dst, label_t label, std::string_view edge_data);
        vertex_t create_vertex();
        Txn_Operation_Response update_vertex(vertex_t src, std::string_view vertex_data);
        Txn_Operation_Response delete_vertex(vertex_t src);

        std::pair<Txn_Operation_Response,std::string_view> get_edge(vertex_t src, vertex_t dst, label_t label);
        std::pair<Txn_Operation_Response,EdgeDeltaIterator> get_edges(vertex_t src, label_t label);
        std::string_view get_vertex(vertex_t src);
        void abort();
        bool commit();
        //Txn_Operation_Response delete(vertex_t src, vertex_t dst, label_t label);
    private:
        void consolidation();
        bool validation();
        void eager_abort();
        const uint64_t local_txn_id;
        const uint64_t read_timestamp;
        entry_ptr self_entry;
        TxnTables& txn_tables;
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
