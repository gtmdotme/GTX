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
#include "previous_version_garbage_queue.hpp"
#include "types.hpp"
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
    struct validation_to_revise_entry{
        validation_to_revise_entry(int32_t input_delta_id, uint32_t input_offset):/*block_id(input_id),*/delta_chain_id(input_delta_id),original_offset(input_offset){}
        // int64_t block_id;
        int32_t delta_chain_id;
        uint32_t original_offset;
    };
    class ROTransaction{

    };
    class RWTransaction{
    public:
        //implement constructor
        RWTransaction(BwGraph& source_graph,uint64_t input_txn_id, timestamp_t input_ts, entry_ptr input_txn_ptr, TxnTables& input_txn_tables, CommitManager& input_commit_manager,  BlockManager& input_block_manager,GarbageBlockQueue& input_garbage_queue, BlockAccessTimestampTable& input_bts_table):graph(source_graph),local_txn_id(input_txn_id),read_timestamp(input_ts),
        self_entry(input_txn_ptr),txn_tables(input_txn_tables),commit_manager(input_commit_manager), block_manager(input_block_manager),per_thread_garbage_queue(input_garbage_queue), block_access_ts_table(input_bts_table){
            thread_id = get_threadID(local_txn_id);
        }
        //transaction graph write operations
        Txn_Operation_Response put_edge(vertex_t src, vertex_t dst, label_t label, std::string_view edge_data);
        vertex_t create_vertex();
        Txn_Operation_Response update_vertex(vertex_t src, std::string_view vertex_data);
        //Txn_Operation_Response delete_vertex(vertex_t src);
        //Txn_Operation_Response delete(vertex_t src, vertex_t dst, label_t label);
        //transaction graph read operations
        std::pair<Txn_Operation_Response,std::string_view> get_edge(vertex_t src, vertex_t dst, label_t label);
        std::pair<Txn_Operation_Response,EdgeDeltaIterator> get_edges(vertex_t src, label_t label);
        std::string_view get_vertex(vertex_t src);
        //transaction status operation
        void abort();
        bool commit();

    private:
        //handle the scenario that block becomes overflow
        void consolidation(BwLabelEntry& current_label_entry, uint64_t block_id);
        //validation delta chain writes before commit
        bool validation();
        //eagerly abort my deltas. If a transaction validated for a block, then the block enters Installation phase, this txn will not eager abort for that block.
        void eager_abort();
        //for pessimistic mode: release all locks in the current block
        void release_all_locks_of_a_block();//todo: finish signature
        //abort all my deltas using scans
        void abort_all_my_deltas(EdgeDeltaBlockHeader* current_block,uint64_t current_offset);
        //abort all my deltas using cache
        void abort_all_my_deltas_using_cache();//todo: finish signature

        //allocate space in the current block for delta
        EdgeDeltaInstallResult allocate_delta(EdgeDeltaBlockHeader* current_block, int32_t data_size);
        //scan the previous block for an edge delta
        std::string_view scan_previous_block_find_edge(EdgeDeltaBlockHeader* previous_block, vertex_t vid);

        //helper inline functions
        //lazy update
        inline bool lazy_update(BaseEdgeDelta* edgeDelta, uint64_t original_ts, uint64_t status){
            if(edgeDelta->lazy_update(original_ts,status)){
                return true;
            }
#if EDGE_DELTA_TEST
            if(edgeDelta->creation_ts.load()!=status){
                throw LazyUpdateException();
            }
#endif
            return false;
        }
        //check whether the current offset refers to overflow offsets
        inline bool overflow_offset_detected(EdgeDeltaBlockHeader* current_block, uint64_t current_offset){
            auto size = current_block->get_size();
            uint32_t tem_current_data_offset = static_cast<uint32_t>((current_offset>>32));
            uint32_t temp_current_delta_offset = static_cast<uint32_t>(current_offset&SIZE2MASK);
            if((tem_current_data_offset+temp_current_delta_offset)>size){
                return true;
            }else{
                return false;
            }
        }
        inline void batch_lazy_updates(){
            for(auto it = lazy_update_records.begin();it!=lazy_update_records.end();it++){
                if(it->second>0){
                    txn_tables.reduce_op_count(it->first,it->second);
                }
            }
        }

        //txn local fields
        BwGraph& graph;
        const uint64_t local_txn_id;
        const timestamp_t read_timestamp;
        entry_ptr self_entry;
        TxnTables& txn_tables;
        CommitManager& commit_manager;
        lazy_update_map lazy_update_records;
        std::map<uint64_t, LockOffsetCache>cached_delta_chain_offsets;//store cache the blocks accessed
        int64_t op_count=0;
        BlockManager& block_manager;
        GarbageBlockQueue& per_thread_garbage_queue;
        BlockAccessTimestampTable& block_access_ts_table;
        uint8_t thread_id;
        uint32_t current_delta_offset;
        uint32_t current_data_offset;
    };
}
#endif //BWGRAPH_V2_BW_TRANSACTION_HPP
