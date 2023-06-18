//
// Created by zhou822 on 6/17/23.
//

#include "bwgraph.hpp"
#include "core/bwgraph_include.hpp"
using namespace bg;
namespace impl = bwgraph;

bg::Graph:: ~Graph()= default;
bg::Graph::Graph(std::string block_path, size_t _max_block_size, std::string wal_path) :graph(std::make_unique<impl::BwGraph>(block_path, _max_block_size, wal_path)){

}

void test(){
    impl::BwGraph graph;

}