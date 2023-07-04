//
// Created by zhou822 on 6/17/23.
//

#include "bwgraph.hpp"
#include "core/bwgraph_include.hpp"
using namespace bg;
namespace impl = bwgraph;

Graph:: ~Graph(){
    commit_server_shutdown();
    commit_manager_worker.join();
}
Graph::Graph(std::string block_path, size_t _max_block_size, std::string wal_path) :graph(std::make_unique<impl::BwGraph>(block_path, _max_block_size, wal_path)){
    commit_manager_worker =std::thread(&Graph::commit_server_start, this);
    //commit_manager_worker =
}

vertex_t Graph::get_max_allocated_vid() {return graph->get_max_allocated_vid();}

RWTransaction Graph::begin_read_write_transaction() {
    return std::make_unique<impl::RWTransaction>(graph->begin_read_write_transaction());
}

ROTransaction Graph::begin_read_only_transaction() {
   // std::cout<<"ro"<<std::endl;
    return std::make_unique<impl::ROTransaction>(graph->begin_read_only_transaction());
}

SharedROTransaction Graph::begin_shared_read_only_transaction() {
    return std::make_unique<impl::SharedROTransaction>(graph->begin_shared_ro_transaction());
}

void Graph::commit_server_start() {
    graph->get_commit_manager().server_loop();
}
void Graph::commit_server_shutdown() {
    graph->get_commit_manager().shutdown_signal();
}

uint8_t Graph::get_worker_thread_id() {
    return graph->get_worker_thread_id();
}

void Graph::execute_manual_checking(bg::vertex_t vid) {
    graph->execute_manual_delta_block_checking(vid);
}
bool Graph::is_txn_table_empty() {
    auto& txn_tables = graph->get_txn_tables();
    for(auto i=0; i<worker_thread_num-1; i++){
        if(!txn_tables.get_table(i).is_empty()){
            return false;
        }
    }
    return true;
}
//read only transactions
ROTransaction::ROTransaction(std::unique_ptr<bwgraph::ROTransaction> _txn) :txn(std::move(_txn)){}

ROTransaction:: ~ROTransaction() = default;

std::string_view ROTransaction::get_vertex(bg::vertex_t src) {return txn->get_vertex(src);}

std::string_view ROTransaction::get_edge(bg::vertex_t src, bg::vertex_t dst, bg::label_t label) {
   // std::cout<<"ro"<<std::endl;
    while(true){
        auto result = txn->get_edge(src,dst,label);
        if(result.first==bwgraph::Txn_Operation_Response::SUCCESS){
            return result.second;
        }
    }
}

EdgeDeltaIterator ROTransaction::get_edges(bg::vertex_t src, bg::label_t label) {
   // std::cout<<"ro"<<std::endl;
    while(true){
        auto result = txn->get_edges(src,label);
        if(result.first==bwgraph::Txn_Operation_Response::SUCCESS){
            return std::make_unique<impl::EdgeDeltaIterator>(result.second);
        }
    }
}

SimpleEdgeDeltaIterator ROTransaction::simple_get_edges(bg::vertex_t src, bg::label_t label) {
    while(true){
        auto result = txn->simple_get_edges(src,label);
        if(result.first==bwgraph::Txn_Operation_Response::SUCCESS){
            return std::make_unique<impl::SimpleEdgeDeltaIterator>(result.second);
        }
    }
}

void ROTransaction::commit() {txn->commit();}

SharedROTransaction::SharedROTransaction(std::unique_ptr<bwgraph::SharedROTransaction> _txn): txn(std::move(_txn)) {}

SharedROTransaction::~SharedROTransaction() = default;

void SharedROTransaction::commit() {txn->commit();}

std::string_view SharedROTransaction::static_get_vertex(bg::vertex_t src) {
    return txn->static_get_vertex(src);
}

std::string_view SharedROTransaction::static_get_edge(bg::vertex_t src, bg::vertex_t dst, bg::label_t label) {
    return txn->static_get_edge(src,dst,label);
}

StaticEdgeDeltaIterator SharedROTransaction::static_get_edges(bg::vertex_t src, bg::label_t label) {
    return std::make_unique<impl::StaticEdgeDeltaIterator>(txn->static_get_edges(src,label));
}

std::string_view SharedROTransaction::get_vertex(bg::vertex_t src) {
    return txn->get_vertex(src);
}

std::string_view SharedROTransaction::get_edge(bg::vertex_t src, bg::vertex_t dst, bg::label_t label) {
    while(true){
        auto result = txn->get_edge(src,dst,label);
        if(result.first==bwgraph::Txn_Operation_Response::SUCCESS){
            return result.second;
        }
    }
}

EdgeDeltaIterator SharedROTransaction::get_edges(bg::vertex_t src, bg::label_t label) {
    while(true){
        auto result = txn->get_edges(src,label);
        if(result.first==bwgraph::Txn_Operation_Response::SUCCESS){
            return std::make_unique<impl::EdgeDeltaIterator>(result.second);
        }
    }
}

SimpleEdgeDeltaIterator SharedROTransaction::simple_get_edges(bg::vertex_t src, bg::label_t label) {
    while(true){
        auto result = txn->simple_get_edges(src,label);
        if(result.first==bwgraph::Txn_Operation_Response::SUCCESS){
            return std::make_unique<impl::SimpleEdgeDeltaIterator>(result.second);
        }
    }
}

//read-write transactions
RWTransaction::~RWTransaction() = default;

RWTransaction::RWTransaction(std::unique_ptr<bwgraph::RWTransaction> _txn):txn(std::move(_txn)) {}

vertex_t RWTransaction::new_vertex() {return txn->create_vertex();}

void RWTransaction::put_vertex(bg::vertex_t vertex_id, std::string_view data) {
#if TRACK_EXECUTION_TIME
    auto start = std::chrono::high_resolution_clock::now();
#endif
    auto result = txn->update_vertex(vertex_id,data);
#if TRACK_EXECUTION_TIME
    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
    txn->get_graph().local_thread_vertex_write_time.local()+= duration.count();
#endif
    if(result==bwgraph::Txn_Operation_Response::SUCCESS){
        return;
    }else{
        throw RollbackExcept("write write conflict vertex");
    }
}

void RWTransaction::put_edge(bg::vertex_t src, bg::label_t label, bg::vertex_t dst, std::string_view edge_data) {
    while(true){
#if TRACK_EXECUTION_TIME
        auto start = std::chrono::high_resolution_clock::now();
#endif
        auto result = txn->put_edge(src,dst,label,edge_data);
#if TRACK_EXECUTION_TIME
        auto stop = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
        txn->get_graph().local_thread_edge_write_time.local()+= duration.count();
#endif
        if(result == bwgraph::Txn_Operation_Response::SUCCESS){
            return;
        }else if(result ==bwgraph::Txn_Operation_Response::FAIL){
            throw RollbackExcept("write write conflict edge");
        }
    }
}

bool
RWTransaction::checked_put_edge(bg::vertex_t src, bg::label_t label, bg::vertex_t dst, std::string_view edge_data) {
    while(true){
#if TRACK_EXECUTION_TIME
        auto start = std::chrono::high_resolution_clock::now();
#endif
        auto result = txn->checked_put_edge(src,dst,label,edge_data);
#if TRACK_EXECUTION_TIME
        auto stop = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
        txn->get_graph().local_thread_edge_write_time.local()+= duration.count();
#endif
        if(result == bwgraph::Txn_Operation_Response::SUCCESS_NEW_DELTA){
            return true;
        }else if(result == bwgraph::Txn_Operation_Response::SUCCESS_EXISTING_DELTA){
            return false;
        }
        else if(result ==bwgraph::Txn_Operation_Response::FAIL){
            throw RollbackExcept("write write conflict edge");
        }
    }
}
void RWTransaction::delete_edge(bg::vertex_t src, bg::label_t label, bg::vertex_t dst) {
    while(true){
#if TRACK_EXECUTION_TIME
        auto start = std::chrono::high_resolution_clock::now();
#endif
        auto result = txn->delete_edge(src,dst,label);
#if TRACK_EXECUTION_TIME
        auto stop = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
        txn->get_graph().local_thread_edge_write_time.local()+= duration.count();
#endif
        if(result == bwgraph::Txn_Operation_Response::SUCCESS){
            return;
        }else if(result ==bwgraph::Txn_Operation_Response::FAIL){
            throw RollbackExcept("write write conflict edge");
        }
    }
}

bool RWTransaction::checked_delete_edge(bg::vertex_t src, bg::label_t label, bg::vertex_t dst) {
    while(true){
#if TRACK_EXECUTION_TIME
        auto start = std::chrono::high_resolution_clock::now();
#endif
        auto result = txn->checked_delete_edge(src,dst,label);
#if TRACK_EXECUTION_TIME
        auto stop = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
        txn->get_graph().local_thread_edge_write_time.local()+= duration.count();
#endif
        if(result == bwgraph::Txn_Operation_Response::SUCCESS_EXISTING_DELTA){
            return true;
        }else if(result == bwgraph::Txn_Operation_Response::SUCCESS_NEW_DELTA){
            return false;
        }
        else if(result ==bwgraph::Txn_Operation_Response::FAIL){
            throw RollbackExcept("write write conflict edge");
        }
    }
}
std::string_view RWTransaction::get_vertex(bg::vertex_t src) {
   // std::cout<<"rw r"<<std::endl;
    return txn->get_vertex(src);
}

std::string_view RWTransaction::get_edge(bg::vertex_t src, bg::vertex_t dst, bg::label_t label) {
    //std::cout<<"rw r"<<std::endl;
    while(true){
        auto result = txn->get_edge(src,dst,label);
        if(result.first==bwgraph::Txn_Operation_Response::SUCCESS){
            return result.second;
        }else if(result.first == bwgraph::Txn_Operation_Response::FAIL){
            throw RollbackExcept("found write write conflict on previous write when reading edge");
        }
    }
}

EdgeDeltaIterator RWTransaction::get_edges(bg::vertex_t src, bg::label_t label) {
   // std::cout<<"rw r"<<std::endl;
    while(true){
        auto result = txn->get_edges(src,label);
        if(result.first==bwgraph::Txn_Operation_Response::SUCCESS){
            return std::make_unique<impl::EdgeDeltaIterator>(result.second);
        }else if(result.first == bwgraph::Txn_Operation_Response::FAIL){
            throw RollbackExcept("found write write conflict on previous write when scanning edges");
        }
    }
}

SimpleEdgeDeltaIterator RWTransaction::simple_get_edges(bg::vertex_t src, bg::label_t label) {
    while(true){
        auto result = txn->simple_get_edges(src,label);
        if(result.first==bwgraph::Txn_Operation_Response::SUCCESS){
            return std::make_unique<impl::SimpleEdgeDeltaIterator>(result.second);
        }else if(result.first == bwgraph::Txn_Operation_Response::FAIL){
            throw RollbackExcept("found write write conflict on previous write when scanning edges");
        }
    }
}

bool RWTransaction::commit() {
#if USING_EAGER_COMMIT
    return txn->eager_commit();
#else
    return txn->commit();
#endif
}

void RWTransaction::abort() {
    txn->abort();
}

EdgeDeltaIterator::EdgeDeltaIterator(std::unique_ptr<bwgraph::EdgeDeltaIterator> _iter):iterator(std::move(_iter)) {}

EdgeDeltaIterator::~EdgeDeltaIterator() = default;

void EdgeDeltaIterator::close() {iterator->close();}

void EdgeDeltaIterator::next() {
    current_delta = iterator->next_delta();
}
bool EdgeDeltaIterator::valid() {
    next();
    return current_delta!= nullptr;
}
vertex_t EdgeDeltaIterator::dst_id() const {
    return current_delta->toID;
}

std::string_view EdgeDeltaIterator::edge_delta_data() const {
    return std::string_view (iterator->get_data(current_delta->data_offset),current_delta->data_length);
}

//simple edge iterator
SimpleEdgeDeltaIterator::SimpleEdgeDeltaIterator(std::unique_ptr<bwgraph::SimpleEdgeDeltaIterator> _iter):iterator(std::move(_iter)) {}

SimpleEdgeDeltaIterator::~SimpleEdgeDeltaIterator() = default;

void SimpleEdgeDeltaIterator::close() {iterator->close();}

void SimpleEdgeDeltaIterator::next() {
    current_delta = iterator->next_delta();
}
bool SimpleEdgeDeltaIterator::valid() {
    next();
    return current_delta!= nullptr;
}
vertex_t SimpleEdgeDeltaIterator::dst_id() const {
    return current_delta->toID;
}

std::string_view SimpleEdgeDeltaIterator::edge_delta_data() const {
    return std::string_view (iterator->get_data(current_delta->data_offset),current_delta->data_length);
}

StaticEdgeDeltaIterator::StaticEdgeDeltaIterator(std::unique_ptr<bwgraph::StaticEdgeDeltaIterator> _iter):iterator(std::move(_iter)) {}

StaticEdgeDeltaIterator::~StaticEdgeDeltaIterator() = default;

void StaticEdgeDeltaIterator::next() {
    current_delta = iterator->next_delta();
}

bool StaticEdgeDeltaIterator::valid() {
    next();
    return current_delta!= nullptr;
}

vertex_t StaticEdgeDeltaIterator::dst_id() const {
    return current_delta->toID;
}

std::string_view StaticEdgeDeltaIterator::edge_delta_data() const {
    return std::string_view (iterator->get_data(current_delta->data_offset),current_delta->data_length);
}