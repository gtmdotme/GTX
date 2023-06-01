//
// Created by zhou822 on 6/1/23.
//
#include "../core/bw_transaction.hpp"
#include "../core/edge_delta_block_state_protection.hpp"
using namespace bwgraph;

Txn_Operation_Response RWTransaction::put_edge(vertex_t src, vertex_t dst, label_t label, std::string_view edge_data){
    //locate the vertex;
    auto& vertex_index_entry = graph.get_vertex_index_entry(src);
    //cannot insert to non valid entry
    if(!vertex_index_entry.valid.load()){
        return Txn_Operation_Response::FAIL;
    }
    auto edge_label_block = block_manager.convert<EdgeLabelBlock>(vertex_index_entry.edge_label_block_ptr);
    BwLabelEntry* target_label_entry = edge_label_block->writer_lookup_label(label);
    //calculate block id
    uint64_t block_id = generate_block_id(src,label);
    //enter the block protection first
    if(BlockStateVersionProtectionScheme::writer_access_block(thread_id,block_id,target_label_entry,block_access_ts_table)){
        if(target_label_entry->block_ptr==0){
            throw GraphNullPointerException();
        }
        auto current_block = block_manager.convert<EdgeDeltaBlockHeader>(target_label_entry->block_ptr);
        
    }else{

    }

}
