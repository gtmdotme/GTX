//
// Created by zhou822 on 6/10/23.
//

#ifndef BWGRAPH_V2_MINI_BWGRAPH_HPP
#define BWGRAPH_V2_MINI_BWGRAPH_HPP
#include"core/bwgraph.hpp"
using namespace bwgraph;
class MiniBwGraph{
public:
    MiniBwGraph(){
        auto& commit_manager = bwGraph.get_commit_manager();
        //manually setup some blocks
        auto& vertex_index = bwGraph.get_vertex_index();
        auto& block_manager = bwGraph.get_block_manager();
        //initial state setup
        for(int32_t i=0; i<1000000; i++){
            vertex_t vid = vertex_index.get_next_vid();
            auto& vertex_entry = bwGraph.get_vertex_index_entry(vid);
            //allocate initial blocks
            vertex_entry.vertex_delta_chain_head_ptr = block_manager.alloc(size_to_order(sizeof(VertexDeltaHeader))+1);
            VertexDeltaHeader* vertex_delta = block_manager.convert<VertexDeltaHeader>( vertex_entry.vertex_delta_chain_head_ptr.load());
            vertex_delta->fill_metadata(0,32,6);
            char data[32];
            for(int j=0;j<32; j++){
                data[j]=static_cast<char>(i%32);
            }
            vertex_delta->write_data(data);
            vertex_entry.edge_label_block_ptr= block_manager.alloc(size_to_order(sizeof(EdgeLabelBlock)));
            EdgeLabelBlock* edge_label_block = block_manager.convert<EdgeLabelBlock>(vertex_entry.edge_label_block_ptr);
            edge_label_block->fill_information(vid,&block_manager);
            edge_label_block->writer_lookup_label(1,&bwGraph.get_txn_tables(),0);
            vertex_entry.valid.store(true);
        }
        commit_manager_worker = std::thread(&CommitManager::server_loop, &commit_manager);//start executing the commit manager
    }
private:
    BwGraph bwGraph;
    std::thread commit_manager_worker;
};
#endif //BWGRAPH_V2_MINI_BWGRAPH_HPP
