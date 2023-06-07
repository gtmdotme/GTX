//
// Created by zhou822 on 6/7/23.
//
#include <doctest/doctest/doctest.h>
#include <core/bw_index.hpp>
#include "core/bwgraph.hpp"
using namespace bwgraph;

TEST_CASE("Test DocTest"){
    int i=0;
    CHECK_EQ(i,0);
    std::cout<<"passed"<<std::endl;
}
//passed, with vertex delta creation
TEST_CASE("Vertex Index Test 1"){
    BlockManager block_manager("", 1e+11);
    VertexIndex vertex_index(block_manager);
    for(uint64_t i=0; i<10; i++){
        vertex_t vid = vertex_index.get_next_vid();
        auto& entry = vertex_index.get_vertex_index_entry(vid);
        entry.valid.store(true);
        auto vertex_delta_ptr = block_manager.alloc(6);
        entry.vertex_delta_chain_head_ptr = vertex_delta_ptr;
        auto vertex_delta = block_manager.convert<VertexDeltaHeader>(vertex_delta_ptr);
        vertex_delta->fill_metadata(vid,0,6,0);
        CHECK_EQ(vid,i+1);
    }
    for(uint64_t i=1; i<=10; i++){
        auto& entry = vertex_index.get_vertex_index_entry(i);
        CHECK(entry.valid);//txn will manually set it true
        CHECK_EQ(entry.edge_label_block_ptr,0);
        auto vertex_delta = block_manager.convert<VertexDeltaHeader>(entry.vertex_delta_chain_head_ptr.load());
        CHECK_EQ(vertex_delta->get_order(),6);
        CHECK_EQ(vertex_delta->get_creation_ts(),i);
        //std::cout<<entry.vertex_delta_chain_head_ptr.load()<<std::endl;
        CHECK_EQ(vertex_delta->get_previous_ptr(),0);
        CHECK_EQ(vertex_delta->get_data_size(),0);
        CHECK_EQ(vertex_delta->get_max_data_storage(),32);
        //CHECK_EQ(entry.vertex_delta_chain_head_ptr,0);
    }
}
//test edge label entry
TEST_CASE("Edge Label Entry Test 1"){
    BwGraph* g = new BwGraph();
    TxnTables txn_table(g);
    VertexIndex vertex_index(g->get_block_manager());
    for(uint64_t i=0; i<10; i++){
        vertex_t vid = vertex_index.get_next_vid();
        auto& entry = vertex_index.get_vertex_index_entry(vid);
        entry.valid.store(true);
        auto vertex_delta_ptr = g->get_block_manager().alloc(6);
        entry.vertex_delta_chain_head_ptr = vertex_delta_ptr;
        auto vertex_delta = g->get_block_manager().convert<VertexDeltaHeader>(vertex_delta_ptr);
        vertex_delta->fill_metadata(vid,0,6,0);
        CHECK_EQ(vid,i+1);
        auto label_block_ptr= g->get_block_manager().alloc(size_to_order(sizeof(EdgeLabelBlock)));
        entry.edge_label_block_ptr = label_block_ptr;
        auto edge_label_block = g->get_block_manager().convert<EdgeLabelBlock>(label_block_ptr);
        edge_label_block->fill_information(vid,&g->get_block_manager());
        BwLabelEntry* test_entry;
        CHECK_FALSE(edge_label_block->reader_lookup_label(1,test_entry));
        auto label_entry = edge_label_block->writer_lookup_label(1,&txn_table);
        CHECK(label_entry);
        CHECK_EQ(label_entry->label,1);
    }
    for(uint64_t i=1; i<=10; i++){
        auto& entry = vertex_index.get_vertex_index_entry(i);
        auto edge_label_block = g->get_block_manager().convert<EdgeLabelBlock>(entry.edge_label_block_ptr);
        BwLabelEntry* test_entry;
        CHECK(edge_label_block->reader_lookup_label(1,test_entry));
        CHECK(test_entry->valid);
        CHECK_EQ(test_entry->label,1);
        auto block = g->get_block_manager().convert<EdgeDeltaBlockHeader>(test_entry->block_ptr);
        CHECK_EQ(block->get_order(),DEFAULT_EDGE_DELTA_BLOCK_ORDER);
        CHECK_EQ(block->get_owner_id(),i);
        CHECK_FALSE(block->get_creation_time());
        CHECK_FALSE(block->get_previous_ptr());
        delete test_entry->delta_chain_index;
    }
    delete g;
}