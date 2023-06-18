//
// Created by zhou822 on 5/28/23.
//

#include "core/bwgraph.hpp"
#include "core/bw_transaction.hpp"
using namespace bwgraph;

/*BwGraph::~BwGraph(){
    auto max_vid = vertex_index.get_current_allocated_vid();
    for(vertex_t vid = 1; vid<=max_vid; vid++){
        auto& vertex_index_entry = vertex_index.get_vertex_index_entry(vid);
        auto label_block = block_manager.convert<EdgeLabelBlock>(vertex_index_entry.edge_label_block_ptr);
        label_block->deallocate_all_delta_chains_indices();
    }
}*/
/*ROTransaction BwGraph::begin_read_only_transaction() {
    auto read_ts = commit_manager.get_current_read_ts();
    std::thread::id this_id = std::this_thread::get_id();
    //auto worker_thread_id = static_cast<uint8_t>(this_id%worker_thread_num);
    ROTransaction transaction();
}
RWTransaction BwGraph::begin_read_write_transaction() {
    return RWTransaction();
}*/