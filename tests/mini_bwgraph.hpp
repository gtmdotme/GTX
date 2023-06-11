//
// Created by zhou822 on 6/10/23.
//

#ifndef BWGRAPH_V2_MINI_BWGRAPH_HPP
#define BWGRAPH_V2_MINI_BWGRAPH_HPP
#include"core/bwgraph.hpp"
#include "core/previous_version_garbage_queue.hpp"
#include "core/bw_transaction.hpp"
#include <queue>
#include <random>
#include "core/exceptions.hpp"
using namespace bwgraph;
constexpr vertex_t txn_id_range = 1000000;
constexpr int32_t total_txn_count = 10000;
constexpr int32_t op_count_range = 100;
constexpr float write_ratio = 0.4;
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
    void thread_execute_edge_txn_operation(uint8_t thread_id){
        //random operations
        double threshold = 10000*write_ratio;
        std::uniform_int_distribution<> read_write_random(0,10000);
        std::random_device rd; // obtain a random number from hardware
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> vertex_dist(1,txn_id_range);
        std::uniform_int_distribution<> op_count_dist(1,txn_id_range);
        std::uniform_int_distribution<> write_size_dist(1,128);
        //thread local structures
        auto& thread_txn_table = bwGraph.get_txn_tables().get_table(thread_id);
        std::queue<vertex_t> recycled_vid_queue;
        GarbageBlockQueue local_garbage_queue;
        //txn generation
        for(int32_t i=0; i<total_txn_count;i++){
            uint64_t txn_id = thread_txn_table.generate_txn_id();
            entry_ptr txn_entry = thread_txn_table.put_entry(txn_id);
            timestamp_t read_ts = bwGraph.get_commit_manager().get_current_read_ts();
            RWTransaction txn(bwGraph,txn_id,read_ts,txn_entry,bwGraph.get_txn_tables(),bwGraph.get_commit_manager(),bwGraph.get_block_manager(),local_garbage_queue,bwGraph.get_block_access_ts_table(),recycled_vid_queue);
            int32_t op_count = op_count_dist(gen);
            bool to_abort = false;
            for(int32_t j=0; j<op_count;j++){
                if(to_abort){
                    break;
                }
                vertex_t src = vertex_dist(gen);
                if(read_write_random(gen)<threshold){
                    vertex_t dst = vertex_dist(gen);
                    int32_t write_size = write_size_dist(gen);
                    std::string edge_data = generate_string_random_length(static_cast<char>(src%32),write_size);
                    while(true){
                        auto op_response = txn.put_edge(src,dst,1,edge_data);
                        if(op_response==bwgraph::Txn_Operation_Response::FAIL){
                            to_abort=true;
                            break;
                        }else if(op_response==bwgraph::Txn_Operation_Response::SUCCESS){
                            break;
                        }
                    }
                }else{
                    if(j%2){
                        vertex_t dst = vertex_dist(gen);
                        while(true){
                            auto op_response = txn.get_edge(src,dst,1);
                            if(op_response.first==bwgraph::Txn_Operation_Response::FAIL){
                                to_abort=true;
                                break;
                            }else if(op_response.first==bwgraph::Txn_Operation_Response::SUCCESS){
                                for(size_t z=0; z<op_response.second.size();z++){
                                    if(op_response.second.at(z)!=static_cast<char>(src%32)){
                                        throw TransactionReadException();
                                    }
                                }
                                break;
                            }
                        }
                    }else{
                        while(true){
                            auto op_response = txn.get_edges(src,1);
                            if(op_response.first==bwgraph::Txn_Operation_Response::FAIL){
                                to_abort=true;
                                break;
                            }else if(op_response.first==bwgraph::Txn_Operation_Response::SUCCESS){
                                auto& edge_delta_iterator = op_response.second;
                                BaseEdgeDelta* current_delta;
                                while((current_delta=edge_delta_iterator.next())!= nullptr){
                                    if(!is_visible_check(txn_id,read_ts,current_delta)){
                                        throw TransactionReadException();
                                    }
                                    char* data = edge_delta_iterator.get_data(current_delta->data_offset);
                                    for(size_t z=0; z<current_delta->data_length; current_delta++){
                                        if(static_cast<char>(current_delta->toID%32)!=data[z]){
                                            throw TransactionReadException();
                                        }
                                    }
                                }
                                break;
                            }
                        }
                    }
                }
            }
        }
    }
private:
    std::string generate_string_random_length(char to_write, int32_t length){
        std::string tmp_s;
        tmp_s.reserve(length);
        for(int32_t i=0; i<length; i++){
            tmp_s+=to_write;
        }
        return tmp_s;
    }
    inline bool is_visible_check(uint64_t txn_id, timestamp_t read_ts, BaseEdgeDelta* current_delta){
        if((current_delta->creation_ts.load()<=read_ts)&&(current_delta->invalidate_ts>read_ts||current_delta->invalidate_ts==0)){
            return true;
        }
        if(current_delta->creation_ts.load()==txn_id){
            return true;
        }
        return false;
    }
    BwGraph bwGraph;
    std::thread commit_manager_worker;
};
#endif //BWGRAPH_V2_MINI_BWGRAPH_HPP
