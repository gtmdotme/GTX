//
// Created by zhou822 on 6/11/23.
//

#ifndef BWGRAPH_V2_MINI_BWGRAPH_HPP
#define BWGRAPH_V2_MINI_BWGRAPH_HPP
#include"../core/bwgraph.hpp"
#include "../core/previous_version_garbage_queue.hpp"
#include "../core/bw_transaction.hpp"
#include <queue>
#include <random>
#include "../core/exceptions.hpp"
#include "../core/graph_global.hpp"
using namespace bwgraph;
constexpr vertex_t vertex_id_range = 1000000;
constexpr vertex_t dst_id_range = 1000000;
constexpr int32_t total_txn_count = 100000;
constexpr int32_t op_count_range = 25;
constexpr float write_ratio = 0.5;
constexpr bool print_block_stats = true;
class MiniBwGraph{
public:
    MiniBwGraph(): bwGraph("",1ul << 36){
        auto& commit_manager = bwGraph.get_commit_manager();
        //manually setup some blocks
        auto& vertex_index = bwGraph.get_vertex_index();
        auto& block_manager = bwGraph.get_block_manager();
        //initial state setup
        for(uint64_t i=0; i<vertex_id_range; i++){
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
        std::cout<<"test graph allocated"<<std::endl;
    }
    ~MiniBwGraph(){
        bwGraph.get_commit_manager().shutdown_signal();
        commit_manager_worker.join();
        auto& block_manager = bwGraph.get_block_manager();
        for(int32_t i=0; i<vertex_id_range; i++){
            vertex_t vid =  i+1;
            auto& vertex_entry = bwGraph.get_vertex_index_entry(vid);
            EdgeLabelBlock* edge_label_block = block_manager.convert<EdgeLabelBlock>(vertex_entry.edge_label_block_ptr);
            auto entry = edge_label_block->writer_lookup_label(1,&bwGraph.get_txn_tables(),0);
            delete entry->delta_chain_index;
        }

    }
    void cleanup_read_only_txn(){
        GarbageBlockQueue local_garbage_queue(&bwGraph.get_block_manager());
        timestamp_t read_ts = bwGraph.get_commit_manager().get_current_read_ts()+10;
        ROTransaction cleanup_txn(bwGraph,read_ts,bwGraph.get_txn_tables(),bwGraph.get_block_manager(),local_garbage_queue,bwGraph.get_block_access_ts_table(),0);
        for(vertex_t i=1; i<=vertex_id_range;i++){
            auto scan_response = cleanup_txn.get_edges(i,1);
            if(scan_response.first!=bwgraph::Txn_Operation_Response::SUCCESS){
                throw TransactionReadException();
            }
            auto& edge_delta_iterator = scan_response.second;
            BaseEdgeDelta* current_delta;
            while((current_delta=edge_delta_iterator.next())!= nullptr){
                if(!is_visible_check(placeholder_txn_id,read_ts,current_delta)){
                    throw TransactionReadException();
                }
                char* data = edge_delta_iterator.get_data(current_delta->data_offset);
                for(size_t z=0; z<current_delta->data_length; z++){
                    if(static_cast<char>(current_delta->toID%32)!=data[z]){
                        throw TransactionReadException();
                    }
                }
            }
            edge_delta_iterator.close();
        }
        cleanup_txn.commit();
    }
    void execute_edge_only_test(){
        std::vector<std::thread>workers;
        for(uint8_t i=0; i<worker_thread_num;i++){
            workers.push_back(std::thread(&MiniBwGraph::thread_execute_edge_txn_operation, this, i));
        }
        for(uint8_t i=0; i<worker_thread_num;i++){
            workers.at(i).join();
        }
        std::cout<<"txn execution finished"<<std::endl;
        //return;
        //execute a readonly txn
        cleanup_read_only_txn();
        for(uint8_t i=0; i<worker_thread_num;i++){
            auto& txn_table = bwGraph.get_txn_tables().get_table(i);
            if(!txn_table.is_empty()){
                throw LazyUpdateException();
            }
        }
        //std::cout<<"final read ts is "<<bwGraph.get_commit_manager().get_current_read_ts()<<std::endl;
        std::cout<<"total abort is "<<total_abort<< std::endl;
        std::cout<<"total commit is "<<total_commit<<std::endl;
        std::cout<<"total op count is "<<total_op_count<<std::endl;
        if(print_block_stats){
            std::vector<uint32_t> order_nums(40);
            for(int i=0; i<40; i++){
                order_nums[i]=0;
            }
            for(int32_t i=0; i<vertex_id_range; i++){
                vertex_t vid =  i+1;
                auto& vertex_entry = bwGraph.get_vertex_index_entry(vid);
                EdgeLabelBlock* edge_label_block = bwGraph.get_block_manager().convert<EdgeLabelBlock>(vertex_entry.edge_label_block_ptr);
                auto entry = edge_label_block->writer_lookup_label(1,&bwGraph.get_txn_tables(),0);
                auto current_block = bwGraph.get_block_manager().convert<EdgeDeltaBlockHeader>(entry->block_ptr);
                order_nums[current_block->get_order()]++;
            }
            for(int i=0; i<40; i++){
                std::cout<<"order "<<i<<" has "<<order_nums[i]<<" blocks"<<std::endl;
            }
        }
    }
    void thread_execute_edge_txn_operation(uint8_t thread_id){
        //stats
        size_t local_abort=0;
        size_t local_commit =0 ;
        size_t local_op_count = 0;
        //random operations
        double threshold = 10000*write_ratio;
        std::uniform_int_distribution<> read_write_random(0,10000);
        std::random_device rd; // obtain a random number from hardware
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> vertex_dist(1,vertex_id_range);
        std::uniform_int_distribution<> dst_dist(1,dst_id_range);
        std::uniform_int_distribution<> op_count_dist(1,op_count_range);
        std::uniform_int_distribution<> write_size_dist(1,128);
        //thread local structure
        GarbageBlockQueue local_garbage_queue(&bwGraph.get_block_manager());
        auto& thread_txn_table = bwGraph.get_txn_tables().get_table(thread_id);
        thread_txn_table.set_garbage_queue(&local_garbage_queue);
        std::queue<vertex_t> recycled_vid_queue;
        //txn generation
        for(int32_t i=0; i<total_txn_count;i++){
            uint64_t txn_id = thread_txn_table.generate_txn_id();
            entry_ptr txn_entry = thread_txn_table.put_entry(txn_id);
            timestamp_t read_ts = bwGraph.get_commit_manager().get_current_read_ts();
            bwGraph.get_block_access_ts_table().store_current_ts(thread_id,read_ts);
            RWTransaction txn(bwGraph,txn_id,read_ts,txn_entry,bwGraph.get_txn_tables(),bwGraph.get_commit_manager(),bwGraph.get_block_manager(),local_garbage_queue,bwGraph.get_block_access_ts_table(),recycled_vid_queue);
            int32_t op_count = op_count_dist(gen);
            bool to_abort = false;
           /* if(i%3000==0&&i>0){
                std::cout<<local_garbage_queue.get_queue().size()<<std::endl;
            }*/
            for(int32_t j=0; j<op_count;j++){
                if(to_abort){
                    break;
                }
                vertex_t src = vertex_dist(gen);
                if(read_write_random(gen)<threshold){
                    vertex_t dst = dst_dist(gen);
                    int32_t write_size = write_size_dist(gen);
                    std::string edge_data = generate_string_random_length(static_cast<char>(dst%32),write_size);
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
                        vertex_t dst = dst_dist(gen);
                        while(true){
                            auto op_response = txn.get_edge(src,dst,1);
                            if(op_response.first==bwgraph::Txn_Operation_Response::FAIL){
                                to_abort=true;
                                break;
                            }else if(op_response.first==bwgraph::Txn_Operation_Response::SUCCESS){
                                for(size_t z=0; z<op_response.second.size();z++){
                                    if(op_response.second.at(z)!=static_cast<char>(dst%32)){
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
                                    for(size_t z=0; z<current_delta->data_length; z++){
                                        if(static_cast<char>(current_delta->toID%32)!=data[z]){
                                            throw TransactionReadException();
                                        }
                                    }
                                }
                                edge_delta_iterator.close();
                                break;
                            }
                        }
                    }
                }
            }
            if(to_abort){
                local_abort++;
                txn.abort();
            }else{
                local_op_count+=op_count;
                local_commit++;
                txn.commit();
            }
            if((i%20)==0){
                timestamp_t safe_timestamp = bwGraph.get_block_access_ts_table().calculate_safe_ts();
                local_garbage_queue.free_block(safe_timestamp);
            }
        }
        bwGraph.get_block_access_ts_table().store_current_ts(thread_id,std::numeric_limits<uint64_t>::max());//exit the thread in the global table
        total_commit.fetch_add(local_commit);
        total_abort.fetch_add(local_abort);
        total_op_count.fetch_add(local_op_count);
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
    std::atomic_uint64_t total_commit =0;
    std::atomic_uint64_t total_abort = 0;
    std::atomic_uint64_t total_op_count = 0;
};


#endif //BWGRAPH_V2_MINI_BWGRAPH_HPP
