//
// Created by zhou822 on 6/17/23.
//

#include "bwgraph.hpp"
#include "core/bwgraph_include.hpp"
using namespace bg;
namespace impl = bwgraph;

Graph:: ~Graph()= default;
Graph::Graph(std::string block_path, size_t _max_block_size, std::string wal_path) :graph(std::make_unique<impl::BwGraph>(block_path, _max_block_size, wal_path)){

}

vertex_t Graph::get_max_allocated_vid() {return graph->get_max_allocated_vid();}

RWTransaction Graph::begin_read_write_transaction() {
    return std::make_unique<impl::RWTransaction>(graph->begin_read_write_transaction());
}

ROTransaction Graph::begin_read_only_transaction() {
    return std::make_unique<impl::ROTransaction>(graph->begin_read_only_transaction());
}
//read only transactions
ROTransaction::ROTransaction(std::unique_ptr<bwgraph::ROTransaction> _txn) :txn(std::move(_txn)){}

ROTransaction:: ~ROTransaction() = default;

std::string_view ROTransaction::get_vertex(bg::vertex_t src) {return txn->get_vertex(src);}

std::string_view ROTransaction::get_edge(bg::vertex_t src, bg::vertex_t dst, bg::label_t label) {
    while(true){
        auto result = txn->get_edge(src,dst,label);
        if(result.first==bwgraph::Txn_Operation_Response::SUCCESS){
            return result.second;
        }
    }
}

EdgeDeltaIterator ROTransaction::get_edges(bg::vertex_t src, bg::label_t label) {
    while(true){
        auto result = txn->get_edges(src,label);
        if(result.first==bwgraph::Txn_Operation_Response::SUCCESS){
            return std::make_unique<impl::EdgeDeltaIterator>(result.second);
        }
    }
}

void ROTransaction::commit() {txn->commit();}

//read-write transactions
vertex_t RWTransaction::new_vertex() {return txn->create_vertex();}

void RWTransaction::put_vertex(bg::vertex_t vertex_id, std::string_view data) {
    auto result = txn->update_vertex(vertex_id,data);
    if(result==bwgraph::Txn_Operation_Response::SUCCESS){
        return;
    }else{
        throw RollbackExcept("write write conflict");
    }
}

void RWTransaction::put_edge(bg::vertex_t src, bg::label_t label, bg::vertex_t dst, std::string_view edge_data) {
    while(true){
        auto result = txn->put_edge(src,label,dst,edge_data);
        if(result == bwgraph::Txn_Operation_Response::SUCCESS){
            return;
        }else if(result ==bwgraph::Txn_Operation_Response::FAIL){
            throw RollbackExcept("write write conflict");
        }
    }
}

void RWTransaction::delete_edge(bg::vertex_t src, bg::label_t label, bg::vertex_t dst) {
    while(true){
        auto result = txn->delete_edge(src,label,dst);
        if(result == bwgraph::Txn_Operation_Response::SUCCESS){
            return;
        }else if(result ==bwgraph::Txn_Operation_Response::FAIL){
            throw RollbackExcept("write write conflict");
        }
    }
}

std::string_view RWTransaction::get_vertex(bg::vertex_t src) {
    return txn->get_vertex(src);
}

std::string_view RWTransaction::get_edge(bg::vertex_t src, bg::vertex_t dst, bg::label_t label) {
    while(true){
        auto result = txn->get_edge(src,dst,label);
        if(result.first==bwgraph::Txn_Operation_Response::SUCCESS){
            return result.second;
        }else if(result.first == bwgraph::Txn_Operation_Response::FAIL){
            throw RollbackExcept("write write conflict on previous write");
        }
    }
}

EdgeDeltaIterator RWTransaction::get_edges(bg::vertex_t src, bg::label_t label) {
    while(true){
        auto result = txn->get_edges(src,label);
        if(result.first==bwgraph::Txn_Operation_Response::SUCCESS){
            return std::make_unique<impl::EdgeDeltaIterator>(result.second);
        }else if(result.first == bwgraph::Txn_Operation_Response::FAIL){
            throw RollbackExcept("write write conflict on previous write");
        }
    }
}

bool RWTransaction::commit() {
    return txn->commit();
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

vertex_t EdgeDeltaIterator::dst_id() const {
    return current_delta->toID;
}

std::string_view EdgeDeltaIterator::edge_delta_data() const {
    return std::string_view (iterator->get_data(current_delta->data_offset),current_delta->data_length);
}

