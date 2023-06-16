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
constexpr int32_t total_txn_count = 600000;
constexpr int32_t op_count_range = 25;
constexpr float write_ratio = 0.6;
constexpr double vertex_operation_ratio = 0.1;
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
                data[j]=static_cast<char>(vid%32);
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
    //assume initialization_thread_count can evenly divide vertex id range
    MiniBwGraph(int32_t initialization_thread_count): bwGraph("",1ul << 36){
        auto& commit_manager = bwGraph.get_commit_manager();
        //manually setup some blocks
        auto& vertex_index = bwGraph.get_vertex_index();
        auto& block_manager = bwGraph.get_block_manager();
        //initial state setup
        std::vector<std::thread>workers;
        workers.reserve(initialization_thread_count);
        for(int i=0; i<initialization_thread_count;i++){
            workers.emplace_back(&MiniBwGraph::setup_graph, this, vertex_id_range/initialization_thread_count);
        }
        for(int i=0; i<initialization_thread_count;i++){
            workers.at(i).join();
        }
 /*       for(uint64_t i=0; i<vertex_id_range; i++){
            vertex_t vid = vertex_index.get_next_vid();
            auto& vertex_entry = bwGraph.get_vertex_index_entry(vid);
            //allocate initial blocks
            vertex_entry.vertex_delta_chain_head_ptr = block_manager.alloc(size_to_order(sizeof(VertexDeltaHeader))+1);
            VertexDeltaHeader* vertex_delta = block_manager.convert<VertexDeltaHeader>( vertex_entry.vertex_delta_chain_head_ptr.load());
            vertex_delta->fill_metadata(0,32,6);
            char data[32];
            for(int j=0;j<32; j++){
                data[j]=static_cast<char>(vid%32);
            }
            vertex_delta->write_data(data);
            vertex_entry.edge_label_block_ptr= block_manager.alloc(size_to_order(sizeof(EdgeLabelBlock)));
            EdgeLabelBlock* edge_label_block = block_manager.convert<EdgeLabelBlock>(vertex_entry.edge_label_block_ptr);
            edge_label_block->fill_information(vid,&block_manager);
            edge_label_block->writer_lookup_label(1,&bwGraph.get_txn_tables(),0);
            vertex_entry.valid.store(true);
        }*/
        commit_manager_worker = std::thread(&CommitManager::server_loop, &commit_manager);//start executing the commit manager
        std::cout<<"test graph allocated"<<std::endl;
    }
    //use multi threading to initialize the graph, test its performance.
    void setup_graph(size_t workload_size){
        auto& vertex_index = bwGraph.get_vertex_index();
        auto& block_manager = bwGraph.get_block_manager();
        for(size_t i=0; i<workload_size;i++){
            vertex_t vid = vertex_index.get_next_vid();
            auto& vertex_entry = bwGraph.get_vertex_index_entry(vid);
            //allocate initial blocks
            vertex_entry.vertex_delta_chain_head_ptr = block_manager.alloc(size_to_order(sizeof(VertexDeltaHeader))+1);
            VertexDeltaHeader* vertex_delta = block_manager.convert<VertexDeltaHeader>( vertex_entry.vertex_delta_chain_head_ptr.load());
            vertex_delta->fill_metadata(0,32,6);
            char data[32];
            for(int j=0;j<32; j++){
                data[j]=static_cast<char>(vid%32);
            }
            vertex_delta->write_data(data);
            vertex_entry.edge_label_block_ptr= block_manager.alloc(size_to_order(sizeof(EdgeLabelBlock)));
            EdgeLabelBlock* edge_label_block = block_manager.convert<EdgeLabelBlock>(vertex_entry.edge_label_block_ptr);
            edge_label_block->fill_information(vid,&block_manager);
            edge_label_block->writer_lookup_label(1,&bwGraph.get_txn_tables(),0);
            vertex_entry.valid.store(true);
        }
    }
    ~MiniBwGraph(){
        //commit_manager_worker.join();
        auto& block_manager = bwGraph.get_block_manager();
        for(uint64_t i=0; i<vertex_id_range; i++){
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
            auto vertex_delta = cleanup_txn.get_vertex(i);
            for(size_t j=0; j<vertex_delta.size();j++){
                if(vertex_delta.at(j)!=static_cast<char>(i%32)){
                    throw TransactionReadException();
                }
            }
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
               /* char* data = edge_delta_iterator.get_data(current_delta->data_offset);
                for(size_t z=0; z<current_delta->data_length; z++){
                    if(static_cast<char>(current_delta->toID%32)!=data[z]){
                        throw TransactionReadException();
                    }
                }*/
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
        bwGraph.get_commit_manager().shutdown_signal();
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
            for(uint64_t i=0; i<vertex_id_range; i++){
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
    void execute_edge_vertex_test(){
        std::vector<std::thread>workers;
        for(uint8_t i=0; i<worker_thread_num;i++){
            workers.push_back(std::thread(&MiniBwGraph::thread_execute_vertex_edge_txn_operation, this, i));
        }
        for(uint8_t i=0; i<worker_thread_num;i++){
            workers.at(i).join();
        }
        std::cout<<"txn execution finished"<<std::endl;
        bwGraph.get_commit_manager().shutdown_signal();//for all remaining transactions in the commit group to commit.
        commit_manager_worker.join();
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
            for(uint64_t i=0; i<vertex_id_range; i++){
                vertex_t vid =  i+1;
                execute_manual_delta_block_checking(vid);
                auto& vertex_entry = bwGraph.get_vertex_index_entry(vid);
                auto vertex_delta = bwGraph.get_block_manager().convert<VertexDeltaHeader>(vertex_entry.vertex_delta_chain_head_ptr);
                order_nums[vertex_delta->get_order()]++;
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
    void execute_manual_delta_block_checking(vertex_t vid){
        auto& vertex_index_entry = bwGraph.get_vertex_index_entry(vid);
        auto label_block = bwGraph.get_block_manager().convert<EdgeLabelBlock>(vertex_index_entry.edge_label_block_ptr);
        BwLabelEntry* current_label_entry;
        size_t total_committed_delta_count_from_secondary_index = 0;
        size_t total_committed_delta_count_from_pure_scan = 0;
        if(!label_block->reader_lookup_label(1,current_label_entry)){
            throw LabelEntryMissingException();
        }
        auto current_block = bwGraph.get_block_manager().convert<EdgeDeltaBlockHeader>(current_label_entry->block_ptr);
        auto delta_chains_num = current_block->get_delta_chain_num();
        if(delta_chains_num!= static_cast<int32_t>(current_label_entry->delta_chain_index->size())){
            throw std::runtime_error("delta chain num mismatch");
        }
        std::unordered_map<vertex_t , timestamp_t>track_invalidate_ts;
        struct pair_hash{
            std::size_t operator()(std::pair<vertex_t , uint32_t>const & v)const{
                return std::hash<int64_t>()(v.first)+std::hash<uint32_t>()(v.second);
            }
        };
        std::unordered_set<std::pair<vertex_t ,uint32_t>,pair_hash>secondary_index_committed_entries;
        for(size_t i=0; i<current_label_entry->delta_chain_index->size();i++){
            uint32_t offset = current_label_entry->delta_chain_index->at(i).get_offset();
            if(offset&LOCK_MASK){
                throw std::runtime_error("locks should be unlocked already");
            }
            while(offset){
                BaseEdgeDelta* delta = current_block->get_edge_delta(offset);
                if(is_txn_id(delta->creation_ts.load())){
                    throw LazyUpdateException();
                }
                if(delta->creation_ts==ABORT){
                    throw LazyUpdateException();
                }
                if(static_cast<delta_chain_id_t>(delta->toID)%delta_chains_num!=static_cast<delta_chain_id_t>(i)){
                    throw DeltaChainCorruptionException();
                }
                total_committed_delta_count_from_secondary_index++;
                char* data = current_block->get_edge_data(delta->data_offset);
                char to_compare = static_cast<char>(delta->toID%32);
                for(uint32_t j=0; j<delta->data_length;j++){
                    if(data[j]!=to_compare){
                        throw TransactionReadException();
                    }
                }
                if(!secondary_index_committed_entries.emplace(std::pair<int64_t,uint32_t>(delta->toID,offset)).second){
                    throw new std::runtime_error("error, duplicate entry");
                }
                offset = delta->previous_offset;
            }
        }
        size_t total_size =0;
        uint64_t current_offsets = current_block->get_current_offset();
        uint64_t original_data_offset = current_offsets>>32;
        uint64_t original_delta_offset = current_offsets&SIZE2MASK;
        uint32_t current_delta_offset = static_cast<uint32_t>(current_offsets&SIZE2MASK);
        BaseEdgeDelta* current_delta = current_block->get_edge_delta(current_delta_offset);
        while(current_delta_offset){
            total_size+=current_delta->data_length+ENTRY_DELTA_SIZE;
            if(is_txn_id((current_delta->creation_ts.load()))){
                throw LazyUpdateException();
            }else if(current_delta->creation_ts!=ABORT){
                if(!secondary_index_committed_entries.count(std::pair<vertex_t, uint32_t>(current_delta->toID,current_delta_offset))){
                    throw std::runtime_error("found an entry not captured by the delta chains");
                }
                if(track_invalidate_ts.count(current_delta->toID)){
                    if(current_delta->invalidate_ts!=track_invalidate_ts.at(current_delta->toID)){
                        throw std::runtime_error("invalidation ts mismatch");
                    }
                    track_invalidate_ts.insert_or_assign(current_delta->toID, current_delta->creation_ts.load());
                }else{
                    if(current_delta->invalidate_ts!=0){
                        throw std::runtime_error("invalidation ts mismatch");
                    }
                    if(!track_invalidate_ts.try_emplace(current_delta->toID,current_delta->creation_ts.load()).second){
                        throw std::runtime_error("should succeed");
                    }
                }
                total_committed_delta_count_from_pure_scan++;
            }
            char* data = current_block->get_edge_data(current_delta->data_offset);
            char to_compare = static_cast<char>(current_delta->toID%32);
            for(uint32_t j=0; j<current_delta->data_length;j++){
                if(data[j]!=to_compare){
                    throw TransactionReadException();
                }
            }
            current_delta++;
            current_delta_offset-=ENTRY_DELTA_SIZE;
        }
        if(original_data_offset+original_delta_offset!=total_size){
            throw new std::runtime_error("error, the offset did not correctly represent the delta allocation");
        }
        if(total_committed_delta_count_from_pure_scan!=total_committed_delta_count_from_secondary_index){
            throw new std::runtime_error("error, secondary index does not contain all committed deltas");
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
    void execute_read_only_txn(ROTransaction& txn, int32_t op_count, timestamp_t read_ts){
        std::random_device rd; // obtain a random number from hardware
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> vertex_dist(1,vertex_id_range);
        std::uniform_int_distribution<> dst_dist(1,dst_id_range);
        for(int j=0; j<op_count;j++){
            vertex_t src = vertex_dist(gen);
            if(j%2){
                vertex_t dst = dst_dist(gen);
                while(true){
                    auto op_response = txn.get_edge(src,dst,1);
                    if(op_response.first==bwgraph::Txn_Operation_Response::FAIL){
                        throw TransactionReadException();
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
                        throw TransactionReadException();
                    }else if(op_response.first==bwgraph::Txn_Operation_Response::SUCCESS){
                        auto& edge_delta_iterator = op_response.second;
                        BaseEdgeDelta* current_delta;
                        while((current_delta=edge_delta_iterator.next())!= nullptr){
                            if(current_delta->creation_ts.load()>read_ts|| (current_delta->invalidate_ts!=0&&current_delta->invalidate_ts<=read_ts)){
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
        txn.commit();
    }
    void thread_execute_vertex_edge_txn_operation(uint8_t thread_id){
        //stats
        size_t local_abort=0;
        size_t local_commit =0 ;
        size_t local_op_count = 0;
        //random operations
        double vertex_operation_threshold = 1000*vertex_operation_ratio;
        std::uniform_int_distribution<> vertex_edge_random(0,10000);
        double threshold = 10000*write_ratio;
        std::uniform_int_distribution<> read_write_random(0,10000);
        std::random_device rd; // obtain a random number from hardware
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> vertex_dist(1,vertex_id_range);
        std::uniform_int_distribution<> dst_dist(1,dst_id_range);
        std::uniform_int_distribution<> op_count_dist(1,op_count_range);
        std::uniform_int_distribution<> write_size_dist(1,128);
        std::uniform_int_distribution<> read_only_dist(0,3);
        //thread local structure
        GarbageBlockQueue local_garbage_queue(&bwGraph.get_block_manager());
        auto& thread_txn_table = bwGraph.get_txn_tables().get_table(thread_id);
        thread_txn_table.set_garbage_queue(&local_garbage_queue);
        std::queue<vertex_t> recycled_vid_queue;
        //txn generation
        for(int32_t i=0; i<total_txn_count;i++){
            if(!read_only_dist(gen)){
                timestamp_t read_ts = bwGraph.get_commit_manager().get_current_read_ts();
                bwGraph.get_block_access_ts_table().store_current_ts(thread_id,read_ts);
                ROTransaction txn(bwGraph,read_ts,bwGraph.get_txn_tables(),bwGraph.get_block_manager(),local_garbage_queue,bwGraph.get_block_access_ts_table(),thread_id);
                int32_t op_count = op_count_dist(gen);
                execute_read_only_txn(txn,op_count,read_ts);
                local_commit++;
                local_op_count+=op_count;
                if((i%20)==0){
                    timestamp_t safe_timestamp = bwGraph.get_block_access_ts_table().calculate_safe_ts();
                    local_garbage_queue.free_block(safe_timestamp);
                }
                continue;
            }
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
                if(vertex_edge_random(gen)<vertex_operation_threshold){
                    //do vertex operation
                    if(read_write_random(gen)<threshold){
                        //vertex write
                        int32_t write_size = write_size_dist(gen);
                        std::string vertex_data = generate_string_random_length(static_cast<char>(src%32),write_size);
                        auto op_response = txn.update_vertex(src,vertex_data);
                        if(op_response==bwgraph::Txn_Operation_Response::FAIL){
                            to_abort=true;
                            break;
                        }
                    }else{
                        //read operation
                        auto vertex_delta = txn.get_vertex(src);
                        for(size_t z=0; z<vertex_delta.size();z++){
                            if(vertex_delta.at(z)!=static_cast<char>(src%32)){
                                throw TransactionReadException();
                            }
                        }
                    }
                }else{
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
