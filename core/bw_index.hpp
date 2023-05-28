//
// Created by zhou822 on 5/27/23.
//

#ifndef BWGRAPH_V2_BW_INDEX_HPP
#define BWGRAPH_V2_BW_INDEX_HPP

#include "types.hpp"
#include "block.hpp"
#include "block_manager.hpp"
#include "utils.hpp"
namespace bwgraph {
#define BW_LABEL_BLOCK_SIZE 3
    class BlockManager;
    struct BwLabelEntry {
        BwLabelEntry(){}
        EdgeDeltaBlockState state=EdgeDeltaBlockState::NORMAL;
        std::atomic_bool valid=false;
        label_t label;//label starts from 1
        uintptr_t block_ptr=0;//todo::check if we can directly use raw pointer
        std::atomic_uint64_t consolidation_time = 0;//the last time consolidation took place
        std::vector<AtomicDeltaOffset>* delta_chain_index=nullptr;//this needs to be a pointer!
    };

    class DeltaLabelBlock {
    public:
        inline uint8_t get_offset(){
            return offset.load();
        }
        inline void fill_information(int64_t input_vid,BlockManager* input_block_mgr_ptr){
            owner_id= input_vid;
            block_manager=input_block_mgr_ptr;
        }
        //reader will locate a corresponding label entry if it exists, otherwise it returns false
        bool reader_lookup_label(label_t target_label,  BwLabelEntry*& target_entry);
        BwLabelEntry* writer_lookup_label(label_t target_label);
        int64_t owner_id;
        std::atomic_uint8_t offset=0;
        BlockManager* block_manager;
        //BwLabelEntry label_entries[BW_LABEL_BLOCK_SIZE];
        std::array<BwLabelEntry,BW_LABEL_BLOCK_SIZE> label_entries;
        std::atomic_uintptr_t next_ptr=0;
    };
    struct VertexIndexEntry{
        std::atomic_bool valid = false;
        std::atomic_uintptr_t vertex_delta_chain_head_ptr=0;
        std::atomic_uintptr_t edge_label_block_ptr=0;
    };
    class VertexIndexBucket{
    public:
        inline VertexIndexEntry& get_vertex_index_entry(int64_t vid){
            return index_entries[vid%BUCKET_SIZE];
        }
        inline void allocate_vertex_index_entry(int64_t vid){

            index_entries[vid%BUCKET_SIZE].valid=true;
        }
    private:
        std::array<VertexIndexEntry,BUCKET_SIZE> index_entries;
    };
    struct BucketPointer{
    public:
        inline bool is_valid(){return valid.load();}
        inline void allocate_block(VertexIndexBucket* input_bucket_ptr){
            index_bucket_ptr = input_bucket_ptr;
        }
        inline VertexIndexBucket* get_index_bucket_ptr(){return index_bucket_ptr;}
        inline void make_valid(){valid.store(true);}
    private:
        std::atomic_bool valid = false;
        VertexIndexBucket* index_bucket_ptr= nullptr;
    };
    class VertexIndex{
    public:
        const size_t bucket_size = BUCKET_SIZE;
        VertexIndex(BlockManager& input_block_manager):global_vertex_id(1),block_manager(input_block_manager){
            auto new_bucket_ptr = block_manager.alloc(size_to_order(sizeof(VertexIndexBucket)));
            bucket_index[0].allocate_block(block_manager.convert<VertexIndexBucket>(new_bucket_ptr));
            bucket_index[0].make_valid();
        }
        inline int64_t get_next_vid(){
            auto new_id =  global_vertex_id.fetch_add(1);
            auto bucket_id = new_id / BUCKET_SIZE;
            if(!(new_id%BUCKET_SIZE)){
                auto new_bucket_ptr = block_manager.alloc(size_to_order(sizeof(VertexIndexBucket)));
                bucket_index[bucket_id].allocate_block(block_manager.convert<VertexIndexBucket>(new_bucket_ptr));
                bucket_index[bucket_id].make_valid();
            }
            else if(!bucket_index[bucket_id].is_valid()){
                while(!bucket_index[bucket_id].is_valid());
            }
            return new_id;
        }
        inline VertexIndexEntry& get_vertex_index_entry(int64_t vid){
            return bucket_index[vid/BUCKET_SIZE].get_index_bucket_ptr()->get_vertex_index_entry(vid);
        }
    private:
        std::atomic_int64_t global_vertex_id;
        std::array<BucketPointer,BUCKET_NUM>bucket_index;
        BlockManager& block_manager;
    };
}
#endif //BWGRAPH_V2_BW_INDEX_HPP
