//
// Created by zhou822 on 5/29/23.
//

#ifndef BWGRAPH_V2_EDGE_ITERATOR_HPP
#define BWGRAPH_V2_EDGE_ITERATOR_HPP

#include "block.hpp"
#include "bwgraph.hpp"
#include "utils.hpp"
#include "exceptions.hpp"
namespace bwgraph {
    //only at rare occasions txn needs to read 2 blocks
    /*
     * 3 scenarios:
     * 1. read the current block, most transactions should do that: read_ts>= block.creation_ts
     * 2. read the previous block, read_ts < block.creation_ts
     * 3. read both current and previous block, read_ts < block.creation_ts and has_deltas
     */
    class EdgeDeltaIterator {
    public:
        EdgeDeltaIterator(){}//empty iterator
        //when iterator ends, need to exit the block protection
        void close(){
            block_access_ts_table->release_block_access(get_threadID(txn_id));
        }
        //give the current block, determine what to read
        EdgeDeltaIterator(EdgeDeltaBlockHeader* input_block, timestamp_t input_ts, uint64_t input_id, bool has_deltas, uint32_t input_offset, BwGraph& source_graph, lazy_update_map* lazy_update_record_ptr, BlockAccessTimestampTable* access_table):current_delta_block(input_block),
        txn_read_ts(input_ts),txn_id(input_id),current_delta_offset(input_offset),txn_tables(&source_graph.get_txn_tables()),block_manager(&source_graph.get_block_manager()), txn_lazy_update_records(lazy_update_record_ptr), block_access_ts_table(access_table){
            if(txn_read_ts>=current_delta_block->get_creation_time()||has_deltas){
                read_current_block = true;
                current_delta = current_delta_block->get_edge_delta(current_delta_offset);
            }
            if(txn_read_ts<current_delta_block->get_creation_time()){
                read_previous_block = true;
                //if no need to read the current block, we just always start from previous block
                if(!read_current_block){
                    while(txn_read_ts<current_delta_block->get_creation_time()){
                        if(current_delta_block->get_previous_ptr()){
                            current_delta_block = block_manager->convert<EdgeDeltaBlockHeader>(current_delta_block->get_previous_ptr());
                        }else{
                            throw EdgeIteratorNoBlockToReadException();
                        }
                    }
                    auto previous_block_offset = current_delta_block->get_current_offset();
                    current_delta_offset = static_cast<uint32_t>(previous_block_offset&SIZE2MASK);
                    current_delta = current_delta_block->get_edge_delta(current_delta_offset);
                }
            }
            if(current_delta== nullptr){
                throw EdgeIteratorNoBlockToReadException();
            }
        }
        BaseEdgeDelta *next() {
            //keep scanning the current block with lazy update, when the current block is exhausted, set "read_current_block" to false and move on
            if(read_current_block){
                //scan the current block, return pointers as appropriate, then maybe switch to the previous block
                while(current_delta_offset>0){
                    if(!current_delta->valid){
                        current_delta_offset-=ENTRY_DELTA_SIZE;
                        current_delta++;
                        continue;
                    }
                    uint64_t original_ts = current_delta->creation_ts.load();
#if EDGE_DELTA_TEST
                    if(!original_ts){
                        throw LazyUpdateException();
                    }
#endif
                    if(is_txn_id(original_ts)){
                        uint64_t status=0;
                        if(txn_tables->get_status(original_ts,status)){
                            if(status == IN_PROGRESS){
                                current_delta_offset-=ENTRY_DELTA_SIZE;
                                current_delta++;
                                continue;
                            }else{
                                if(status!=ABORT){
                                    current_delta_block->update_previous_delta_invalidate_ts(current_delta->toID,current_delta->previous_offset,status);
                                    if(current_delta->lazy_update(original_ts,status)){
#if LAZY_LOCKING
                                        if(current_delta->is_last_delta.load()){
                                            current_delta_block-> release_protection(current_delta->toID);
                                        }
#endif
                                        //record lazy update
                                        record_lazy_update_record(txn_lazy_update_records,original_ts);
                                    }
                                }
#if EDGE_DELTA_TEST
                                if(current_delta->creation_ts!=status){
                                    throw LazyUpdateException();
                                }
#endif
                            }
                        }
                    }
                    if(current_delta->creation_ts.load()==txn_id){
                        current_delta_offset-=  ENTRY_DELTA_SIZE;
                        return current_delta++;
                    }
                    if((current_delta->creation_ts.load()<=txn_read_ts)&&(current_delta->invalidate_ts.load()==0||current_delta->invalidate_ts.load()>txn_read_ts)){
                        current_delta_offset-=  ENTRY_DELTA_SIZE;
                        return current_delta++;
                    }
                    current_delta_offset-=ENTRY_DELTA_SIZE;
                    current_delta++;
                }
                read_current_block=false;
                while(txn_read_ts<current_delta_block->get_creation_time()){
                    if(current_delta_block->get_previous_ptr()){
                        current_delta_block = block_manager->convert<EdgeDeltaBlockHeader>(current_delta_block->get_previous_ptr());
                    }else{
                        throw EdgeIteratorNoBlockToReadException();
                    }
                }
                auto previous_block_offset = current_delta_block->get_current_offset();
                current_delta_offset = static_cast<uint32_t>(previous_block_offset&SIZE2MASK);
                current_delta = current_delta_block->get_edge_delta(current_delta_offset);
            }
            if((!read_current_block)&&read_previous_block){
                 while(current_delta_offset>0){
                     //abort deltas will always have larger creation ts than any read ts
                     if(current_delta->creation_ts.load()<=txn_read_ts&&(current_delta->invalidate_ts==0||current_delta->invalidate_ts>txn_read_ts)){
                        current_delta_offset-=  ENTRY_DELTA_SIZE;
                        return current_delta++;
                     }
                     current_delta_offset-=ENTRY_DELTA_SIZE;
                     current_delta++;
                 }
             }
            return nullptr;
        }
        char* get_data(uint32_t offset){
            return current_delta_block->get_edge_data(offset);
        }
    private:
        EdgeDeltaBlockHeader *current_delta_block;
        timestamp_t txn_read_ts;
        uint64_t txn_id;
        //bool txn_has_deltas;//whether this txn has deltas in the current delta block
        uint32_t current_delta_offset;
        bool read_current_block = false;
        bool read_previous_block = false;
        BaseEdgeDelta* current_delta = nullptr;
        TxnTables* txn_tables;
        BlockManager* block_manager;
        lazy_update_map* txn_lazy_update_records;
        BlockAccessTimestampTable* block_access_ts_table;//necessary at destructor, need to release the protection
    };

}
#endif //BWGRAPH_V2_EDGE_ITERATOR_HPP
