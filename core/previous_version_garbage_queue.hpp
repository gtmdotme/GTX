//
// Created by zhou822 on 5/28/23.
//

#ifndef BWGRAPH_V2_PREVIOUS_VERSION_GARBAGE_QUEUE_HPP
#define BWGRAPH_V2_PREVIOUS_VERSION_GARBAGE_QUEUE_HPP
#include "types.hpp"
#include "block_manager.hpp"
#include <queue>
namespace bwgraph{
    struct PreviousVersionBlockEntry{
        PreviousVersionBlockEntry(){}
        PreviousVersionBlockEntry( const PreviousVersionBlockEntry& other){
            block_ptr = other.block_ptr;
            order = other.order;
            updated_ts = other.updated_ts;
        }
        PreviousVersionBlockEntry& operator = (const PreviousVersionBlockEntry& other){
            block_ptr = other.block_ptr;
            order = other.order;
            updated_ts = other.updated_ts;
            return *this;
        }
        PreviousVersionBlockEntry(uintptr_t input_ptr,order_t input_order, uint64_t input_ts):block_ptr(input_ptr), order(input_order),updated_ts(input_ts){}
        bool operator < (const PreviousVersionBlockEntry& other)const{
            return updated_ts<other.updated_ts;
        }
        bool operator ==(const PreviousVersionBlockEntry& other)const{
            return updated_ts == other.updated_ts;
        }
        uintptr_t block_ptr;
        order_t order;
        uint64_t updated_ts;//the timestamp the new version was created
    };
    class GarbageBlockQueue{
    public:
        GarbageBlockQueue(BlockManager* input_manager):block_manager(input_manager){}
        inline void free_block(uint64_t safe_ts){
            while(!previous_versions_queue.empty()){
                if(previous_versions_queue.top().updated_ts<=safe_ts){
                    auto current_top_entry = previous_versions_queue.top();
                    previous_versions_queue.pop();
                    uint8_t * ptr = block_manager->convert<uint8_t>(current_top_entry.block_ptr);
                    memset(ptr,0,1ul<<current_top_entry.order);//zero out memory I used
                    block_manager->free(current_top_entry.block_ptr,current_top_entry.order);
                }else{
                    return;
                }
            }
        }
        inline void register_entry(uintptr_t block_ptr, order_t order, uint64_t updated_ts){
            previous_versions_queue.emplace(block_ptr,order,updated_ts);
        }
        inline std::priority_queue<PreviousVersionBlockEntry>& get_queue(){return previous_versions_queue;}
    private:
        std::priority_queue<PreviousVersionBlockEntry> previous_versions_queue;
        BlockManager* block_manager;
    };
}
#endif //BWGRAPH_V2_PREVIOUS_VERSION_GARBAGE_QUEUE_HPP
