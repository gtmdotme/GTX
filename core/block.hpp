//
// Created by zhou822 on 5/22/23.
//

#ifndef BWGRAPH_V2_BLOCK_HPP
#define BWGRAPH_V2_BLOCK_HPP
#include <atomic>
#include <cassert>
#include <cstddef>
#include <iostream>
#include <vector>
#include "assert.h"
#include "types.hpp"
#include "utils.hpp"
#include "exceptions.hpp"
namespace bwgraph{
#define ENTRY_DELTA_SIZE 64
#define SIZE2MASK 0x00000000FFFFFFFF
#define LOCK_MASK 0x80000000
#define UNLOCK_MASK 0x7FFFFFFF
#define FILTER_INITIAL_SIZE 8
#define OVERFLOW_BIT 0x8000000080000000
#define MAX_LOCK_INHERITANCE_ROUND 3
#define ERROR_ENTRY_OFFSET 0xFFFFFFFF
#define BW_LABEL_BLOCK_SIZE 5
#define EDGE_DELTA_TEST true
#define Count_Lazy_Protocol true
#define PESSIMISTIC_DELTA_BLOCK true
    //the atomic offset variable that points to the head of a delta chain
    struct Atomic_Delta_Offset{
        std::atomic_uint32_t delta_offset;//0 means the end of the block which is impossible, the minimum meaningful offset value is 64
        Atomic_Delta_Offset(){
            delta_offset.store(0);
        }
        Atomic_Delta_Offset(const Atomic_Delta_Offset& other){
            this->delta_offset.store(other.delta_offset.load());
        }
        Atomic_Delta_Offset& operator = (const Atomic_Delta_Offset& other){
            delta_offset.store(other.delta_offset);
            return *this;
        }
        inline uint32_t get_offset(){
            return delta_offset.load();
        }
        inline void update_offset(uint32_t offset){
            delta_offset.store(offset);
        }
        //todo: check if we can use weak
        bool try_set_lock(){
            while(true){
                uint32_t current_offset = delta_offset.load();
                if(current_offset&LOCK_MASK){
                    return false;
                }
                uint32_t new_offset =  current_offset|LOCK_MASK;
                if(delta_offset.compare_exchange_strong(current_offset,new_offset)){
                    return true;
                }
            }
        }
        void release_lock(){
            uint32_t current_offset = delta_offset.load();
#if EDGE_DELTA_TEST
            uint32_t new_offset = current_offset&UNLOCK_MASK;
            if(delta_offset.compare_exchange_strong(current_offset,new_offset)){
                return;
            }else{
                throw DeltaLockExecption();
            }
#else
            current_offset &= UNLOCK_MASK;
            delta_offset.store(current_offset);
#endif
        }

    };
    class alignas(64) BaseEdgeDelta{
    public:
        int64_t delta_type;
        int64_t toID;
        // timestamp_t creation_ts;
        std::atomic_uint64_t  creation_ts;
        uint64_t invalidate_ts;
        uint32_t data_length;
        uint32_t data_offset;//where its data entry is at
        uint32_t previous_offset;//todo:the delta chain chains all deltas of the same filter lock together
        //int32_t next_offset;
        bool is_last_delta;
        std::atomic_bool valid;
        char data[16];//padding to make it one cache line.
    };
    class EdgeDeltaBlockHeader{
    public:
        //metadata accessor:
        inline timestamp_t get_creation_time(){
            return creation_time;
        }
        inline void set_offset(uint64_t input_offset){
            combined_offsets.store(input_offset);
        }
        inline uintptr_t get_previous_ptr(){
            return prev_pointer;
        }
        inline uint64_t get_current_offset(){
            return combined_offsets.load();
        }
        inline char* get_edge_data(uint32_t offset){
            return data+offset;
        }
        inline uint32_t get_size(){
            return (uint32_t)((1ul<<order)-sizeof (EdgeDeltaBlockHeader));
        }
        int32_t get_order(){
            return order;
        }
        inline int32_t get_delta_chain_num(){
            return delta_chain_num;
        }
        //delta reader
        BaseEdgeDelta *get_edge_delta(uint32_t entry_offset){
            return (BaseEdgeDelta*) ((uint8_t*)this+(1ul<<order)-entry_offset);
        }
        //delta chain append functions
        inline uint64_t allocate_space_for_new_delta(uint32_t data_size){
            uint64_t to_atomic = ((static_cast<uint64_t>(data_size))<<32)+ENTRY_DELTA_SIZE;
            return combined_offsets.fetch_add(to_atomic);
        }
        Delta_Chain_Lock_Response lock_inheritance(int64_t vid);
        //try to set the lock, eagerlly try lock_inheritance
        Delta_Chain_Lock_Response set_protection(int64_t vid){

            return Delta_Chain_Lock_Response::CONFLICT;
        }
        bool try_set_protection(int64_t vid){
            return false;
        }

    private:
        int64_t owner_id;
        std::atomic_uint64_t  combined_offsets;
        timestamp_t creation_time;
        uintptr_t prev_pointer;
        int32_t order;
        int32_t delta_chain_num;
        std::vector<Atomic_Delta_Offset>delta_chains_index;
        char data[0];
    };

    static_assert(sizeof(EdgeDeltaBlockHeader)==64);
}
#endif //BWGRAPH_V2_BLOCK_HPP
