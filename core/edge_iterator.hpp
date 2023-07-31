//
// Created by zhou822 on 5/29/23.
//

//#ifndef BWGRAPH_V2_EDGE_ITERATOR_HPP
//#define BWGRAPH_V2_EDGE_ITERATOR_HPP
#pragma once
#include "block.hpp"
#include "bwgraph.hpp"
//#include "block_access_ts_table.hpp"
//#include "block_manager.hpp"
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
            if(block_access_ts_table)
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
                            break;
                            //throw EdgeIteratorNoBlockToReadException();
                        }
                    }
                    auto previous_block_offset = current_delta_block->get_current_offset();
                    current_delta_offset = static_cast<uint32_t>(previous_block_offset&SIZE2MASK);
                    current_delta = current_delta_block->get_edge_delta(current_delta_offset);
                }
            }
            //todo::if it is null, then there just does not exist any visible blocks at all.
           /* if(current_delta== nullptr){
                throw EdgeIteratorNoBlockToReadException();
            }*/
        }
        BaseEdgeDelta *next_delta() {
            //keep scanning the current block with lazy update, when the current block is exhausted, set "read_current_block" to false and move on
            if(read_current_block){
                //scan the current block, return pointers as appropriate, then maybe switch to the previous block
                while(current_delta_offset>0){
                    if(!current_delta->valid.load(std::memory_order_acquire)){
                        current_delta_offset-=ENTRY_DELTA_SIZE;
                        current_delta++;
                        continue;
                    }
                    uint64_t original_ts = current_delta->creation_ts.load(std::memory_order_acquire);
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
#if CHECKED_PUT_EDGE
                                    current_delta_block->update_previous_delta_invalidate_ts(current_delta->toID,current_delta->previous_version_offset,status);
#else
                                    current_delta_block->update_previous_delta_invalidate_ts(current_delta->toID,current_delta->previous_offset,status);
#endif
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
                                if(current_delta->creation_ts.load(std::memory_order_acquire)!=status){
                                    throw LazyUpdateException();
                                }
#endif
                            }
                        }
                    }
                    if(current_delta->creation_ts.load(std::memory_order_acquire)==txn_id&&current_delta->invalidate_ts.load(std::memory_order_acquire)!=txn_id&&current_delta->delta_type!=EdgeDeltaType::DELETE_DELTA){
                        current_delta_offset-=  ENTRY_DELTA_SIZE;
                        return current_delta++;
                    }
                    //cannot be delete delta
                    if(current_delta->delta_type!=EdgeDeltaType::DELETE_DELTA&&(current_delta->creation_ts.load(std::memory_order_acquire)<=txn_read_ts)&&(current_delta->invalidate_ts.load(std::memory_order_acquire)==0||current_delta->invalidate_ts.load(std::memory_order_acquire)>txn_read_ts)&&current_delta->invalidate_ts.load(std::memory_order_acquire)!=txn_id){
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
                        current_delta= nullptr;
                        return current_delta;
                    }
                }
                auto previous_block_offset = current_delta_block->get_current_offset();
                current_delta_offset = static_cast<uint32_t>(previous_block_offset&SIZE2MASK);
                current_delta = current_delta_block->get_edge_delta(current_delta_offset);
            }
            if((!read_current_block)&&read_previous_block){
                 while(current_delta_offset>0){
                     //abort deltas will always have larger creation ts than any read ts
                     if(current_delta->delta_type!=EdgeDeltaType::DELETE_DELTA&&current_delta->creation_ts.load(std::memory_order_acquire)<=txn_read_ts&&(current_delta->invalidate_ts.load(std::memory_order_acquire)==0||current_delta->invalidate_ts.load(std::memory_order_acquire)>txn_read_ts)){
                        current_delta_offset-=  ENTRY_DELTA_SIZE;
                        return current_delta++;
                     }
                     current_delta_offset-=ENTRY_DELTA_SIZE;
                     current_delta++;
                 }
             }
            return nullptr;
        }
        inline char* get_data(uint32_t offset){
            return current_delta_block->get_edge_data(offset);
        }
  /*      void next(){
            current_visible_delta = nullptr;
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
                        current_visible_delta= current_delta++;
                        return;
                    }
                    if((current_delta->creation_ts.load()<=txn_read_ts)&&(current_delta->invalidate_ts.load()==0||current_delta->invalidate_ts.load()>txn_read_ts)){
                        current_delta_offset-=  ENTRY_DELTA_SIZE;
                        current_visible_delta = current_delta++;
                        return;
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
                        current_visible_delta= current_delta++;
                        return;
                    }
                    current_delta_offset-=ENTRY_DELTA_SIZE;
                    current_delta++;
                }
            }
        }

        inline bool valid(){
            return(!current_delta_offset);
        }
        inline std::string_view get_data(){
            if(current_visible_delta)
                return std::string_view ( current_delta_block->get_edge_data(current_visible_delta->data_offset),current_visible_delta->data_length);
            else
                return std::string_view ();
        }
        //for test
        inline BaseEdgeDelta* get_current_visible_delta(){return current_visible_delta;}*/
    private:
        EdgeDeltaBlockHeader *current_delta_block;
        timestamp_t txn_read_ts;
        uint64_t txn_id;
        //bool txn_has_deltas;//whether this txn has deltas in the current delta block
        uint32_t current_delta_offset;
        bool read_current_block = false;
        bool read_previous_block = false;
        BaseEdgeDelta* current_delta = nullptr;
        //BaseEdgeDelta* current_visible_delta = nullptr;
        TxnTables* txn_tables;
        BlockManager* block_manager;
        lazy_update_map* txn_lazy_update_records;
        BlockAccessTimestampTable* block_access_ts_table=nullptr;//necessary at destructor, need to release the protection
    };
    /*
     * it simplifies the read/write transaction visibility issue, a transaction can only read at most 1 block
     */
    class SimpleEdgeDeltaIterator {
    public:
        SimpleEdgeDeltaIterator(){}//empty iterator
        //when iterator ends, need to exit the block protection
        void close(){
            if(block_access_ts_table)
                block_access_ts_table->release_block_access(get_threadID(txn_id));
        }
        //give the current block, determine what to read
        SimpleEdgeDeltaIterator(EdgeDeltaBlockHeader* input_block, timestamp_t input_ts, uint64_t input_id, uint32_t input_offset, BwGraph& source_graph, lazy_update_map* lazy_update_record_ptr, BlockAccessTimestampTable* access_table):current_delta_block(input_block),
                                                                                                                                                                                                                                                       txn_read_ts(input_ts),txn_id(input_id),current_delta_offset(input_offset),txn_tables(&source_graph.get_txn_tables()),block_manager(&source_graph.get_block_manager()), txn_lazy_update_records(lazy_update_record_ptr), block_access_ts_table(access_table){
            if(txn_read_ts>=current_delta_block->get_creation_time()){
                read_current_block = true;
                current_delta = current_delta_block->get_edge_delta(current_delta_offset);
            }
            else /*(txn_read_ts<current_delta_block->get_creation_time())*/{
               /* bool found = true;
                while(txn_read_ts<current_delta_block->get_creation_time()){
                    if(current_delta_block->get_previous_ptr()){
                        current_delta_block = block_manager->convert<EdgeDeltaBlockHeader>(current_delta_block->get_previous_ptr());
                    }else{
                        found=false;
                        break;
                        //throw EdgeIteratorNoBlockToReadException();
                    }
                }*/
               bool found = false;
               while(current_delta_block->get_previous_ptr()){
                    current_delta_block = block_manager->convert<EdgeDeltaBlockHeader>(current_delta_block->get_previous_ptr());
                    if(txn_read_ts>=current_delta_block->get_creation_time()){
                        found = true;
                        break;
                    }
               }
                if(found){
#if EDGE_DELTA_TEST
                    if(txn_read_ts<current_delta_block->get_creation_time()){
                        throw TransactionReadException();
                    }
#endif
                    auto previous_block_offset = current_delta_block->get_current_offset();
                    current_delta_offset = static_cast<uint32_t>(previous_block_offset&SIZE2MASK);
                    current_delta = current_delta_block->get_edge_delta(current_delta_offset);
                }else{
                    current_delta= nullptr;
                    current_delta_offset=0;
                }
            }
            //todo::if it is null, then there just does not exist any visible blocks at all.
            /* if(current_delta== nullptr){
                 throw EdgeIteratorNoBlockToReadException();
             }*/
        }
        BaseEdgeDelta *next_delta() {
            //keep scanning the current block with lazy update, when the current block is exhausted, set "read_current_block" to false and move on
            if(read_current_block)[[likely]]{
            //use __builtin_expect
            //if(__builtin_expect(read_current_block,true)){
                //scan the current block, return pointers as appropriate, then maybe switch to the previous block
                while(current_delta_offset>0){
                    if(!current_delta->valid.load(std::memory_order_acquire))[[unlikely]]{
                        current_delta_offset-=ENTRY_DELTA_SIZE;
                        current_delta++;
                        continue;
                    }
                    uint64_t original_ts = current_delta->creation_ts.load(std::memory_order_acquire);
#if EDGE_DELTA_TEST
                    if(!original_ts){
                        throw LazyUpdateException();
                    }
#endif
                    //
                    if(is_txn_id(original_ts)&&original_ts!=txn_id)[[unlikely]]{
                        uint64_t status=0;
                        if(txn_tables->get_status(original_ts,status)){
                            if(status == IN_PROGRESS)[[likely]]{
                                current_delta_offset-=ENTRY_DELTA_SIZE;
                                current_delta++;
                                continue;
                            }else{
                                if(status!=ABORT)[[likely]]{
#if CHECKED_PUT_EDGE
                                    current_delta_block->update_previous_delta_invalidate_ts(current_delta->toID,current_delta->previous_version_offset,status);
#else
                                    current_delta_block->update_previous_delta_invalidate_ts(current_delta->toID,current_delta->previous_offset,status);
#endif
                                    if(current_delta->lazy_update(original_ts,status)){
#if LAZY_LOCKING
                                        if(current_delta->is_last_delta.load(std::memory_order_acquire)){
                                            current_delta_block-> release_protection(current_delta->toID);
                                        }
#endif
                                        //record lazy update
                                        record_lazy_update_record(txn_lazy_update_records,original_ts);
                                    }
                                }
#if EDGE_DELTA_TEST
                                if(current_delta->creation_ts.load(std::memory_order_acquire)!=status){
                                    throw LazyUpdateException();
                                }
#endif
                            }
                        }
                    }
                   /* if(current_delta->creation_ts.load(std::memory_order_acquire)==txn_id&&current_delta->invalidate_ts.load()!=txn_id&&current_delta->delta_type!=EdgeDeltaType::DELETE_DELTA){
                        current_delta_offset-=  ENTRY_DELTA_SIZE;
                        return current_delta++;
                    }
                    //cannot be delete delta
                    if(current_delta->delta_type!=EdgeDeltaType::DELETE_DELTA&&(current_delta->creation_ts.load()<=txn_read_ts)&&(current_delta->invalidate_ts.load()==0||current_delta->invalidate_ts.load()>txn_read_ts)&&current_delta->invalidate_ts.load()!=txn_id){
                        current_delta_offset-=  ENTRY_DELTA_SIZE;
                        return current_delta++;
                    }*/
                    //check if it is non-delete delta
                    if(current_delta->delta_type!=EdgeDeltaType::DELETE_DELTA){
                        uint64_t current_creation_ts = current_delta->creation_ts.load(std::memory_order_acquire);
                        uint64_t current_invalidation_ts = current_delta->invalidate_ts.load(std::memory_order_acquire);
                        //cannot be the delta deleted by the current transaction
                        if(current_invalidation_ts!=txn_id)[[likely]]{
                            //visible committed delta
                            if(current_creation_ts<=txn_read_ts&&(current_invalidation_ts==0||current_invalidation_ts>txn_read_ts)){
                                current_delta_offset-=  ENTRY_DELTA_SIZE;
#if USING_PREFETCH
                                //__builtin_prefetch((const void*)(current_delta+1),0,0);
                                // __builtin_prefetch((const void*)(current_delta+2),0,0);
#endif
                                return current_delta++;
                            }
                            //visible delta by myself
                            if(current_creation_ts==txn_id)[[unlikely]]{
                                current_delta_offset-=  ENTRY_DELTA_SIZE;
                                return current_delta++;
                            }
                        }
                    }
                    current_delta_offset-=ENTRY_DELTA_SIZE;
                    current_delta++;
                }
            }else{
                while(current_delta_offset>0){
                    //abort deltas will always have larger creation ts than any read ts
               /*     if(current_delta->delta_type!=EdgeDeltaType::DELETE_DELTA&&current_delta->creation_ts.load()<=txn_read_ts&&(current_delta->invalidate_ts==0||current_delta->invalidate_ts>txn_read_ts)){
                        current_delta_offset-=  ENTRY_DELTA_SIZE;
                        return current_delta++;
                    }*/
                    if(current_delta->delta_type!=EdgeDeltaType::DELETE_DELTA){
                        uint64_t current_creation_ts = current_delta->creation_ts.load(std::memory_order_acquire);
                        uint64_t current_invalidation_ts = current_delta->invalidate_ts.load(std::memory_order_acquire);
                        if(current_creation_ts<=txn_read_ts&&(current_invalidation_ts==0||current_invalidation_ts>txn_read_ts)){
                            current_delta_offset-=  ENTRY_DELTA_SIZE;
                            return current_delta++;
                        }
                    }
                    current_delta_offset-=ENTRY_DELTA_SIZE;
                    current_delta++;
                }
            }
           /* if((!read_current_block)&&read_previous_block){
                while(current_delta_offset>0){
                    //abort deltas will always have larger creation ts than any read ts
                    if(current_delta->delta_type!=EdgeDeltaType::DELETE_DELTA&&current_delta->creation_ts.load()<=txn_read_ts&&(current_delta->invalidate_ts==0||current_delta->invalidate_ts>txn_read_ts)){
                        current_delta_offset-=  ENTRY_DELTA_SIZE;
                        return current_delta++;
                    }
                    current_delta_offset-=ENTRY_DELTA_SIZE;
                    current_delta++;
                }
            }*/
            return nullptr;
        }
        inline char* get_data(uint32_t offset){
            return current_delta_block->get_edge_data(offset);
        }
    private:
        EdgeDeltaBlockHeader *current_delta_block;
        timestamp_t txn_read_ts;
        uint64_t txn_id;
        //bool txn_has_deltas;//whether this txn has deltas in the current delta block
        uint32_t current_delta_offset=0;
        bool read_current_block = false;
        BaseEdgeDelta* current_delta = nullptr;
        //BaseEdgeDelta* current_visible_delta = nullptr;
        TxnTables* txn_tables;
        BlockManager* block_manager;
        lazy_update_map* txn_lazy_update_records;
        BlockAccessTimestampTable* block_access_ts_table=nullptr;//necessary at destructor, need to release the protection
    };
    //the iterator for the static graph
    class StaticEdgeDeltaIterator{
    public:
        StaticEdgeDeltaIterator(){}
        StaticEdgeDeltaIterator(EdgeDeltaBlockHeader* input_block, uint32_t input_offset):current_delta_block(input_block){
            current_delta = current_delta_block->get_edge_delta(input_offset);
        }
        BaseEdgeDelta *next_delta() {
            while(current_delta_offset>0){
                current_delta_offset-=ENTRY_DELTA_SIZE;
                return current_delta++;
            }
            return nullptr;
        }
        inline char* get_data(uint32_t offset){
            return current_delta_block->get_edge_data(offset);
        }
    private:
        EdgeDeltaBlockHeader *current_delta_block;
        //bool txn_has_deltas;//whether this txn has deltas in the current delta block
        uint32_t current_delta_offset;
        BaseEdgeDelta* current_delta = nullptr;
    };
}
//#endif //BWGRAPH_V2_EDGE_ITERATOR_HPP
