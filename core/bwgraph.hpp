//
// Created by zhou822 on 5/28/23.
//

#ifndef BWGRAPH_V2_BWGRAPH_HPP
#define BWGRAPH_V2_BWGRAPH_HPP
#include "bw_index.hpp"
#include "block_manager.hpp"
#include "transaction_tables.hpp"
#include "exceptions.hpp"
#include "types.hpp"
#include "block_access_ts_table.hpp"
#include "commit_manager.hpp"
namespace bwgraph{
    class ROTransaction;
    class RWTransaction;


    class BwGraph {
    public:
#if USING_ARRAY_TABLE
        BwGraph(std::string block_path = "",size_t _max_block_size = 1ul << 32,
            std::string wal_path = ""): block_manager(block_path,_max_block_size), vertex_index(block_manager),txn_tables(this)/*,commit_manager(txn_tables)*/{
    }
#else
        BwGraph(std::string block_path = "",size_t _max_block_size = 1ul << 32,
            std::string wal_path = ""): block_manager(block_path,_max_block_size), vertex_index(block_manager){
    }
#endif
        ROTransaction begin_read_only_transaction();
        RWTransaction begin_read_write_transaction();
        inline VertexIndexEntry& get_vertex_index_entry(vertex_t vid){
            return vertex_index.get_vertex_index_entry(vid);
        }
        inline BlockManager& get_block_manager(){return block_manager;}
        inline BlockAccessTimestampTable& get_block_access_ts_table(){return block_access_ts_table;}
        inline CommitManager& get_commit_manager(){return commit_manager;}
        inline TxnTables & get_txn_tables(){return txn_tables;}
    private:
        BlockManager block_manager;
        VertexIndex vertex_index;
        TxnTables txn_tables;
        CommitManager commit_manager;
        BlockAccessTimestampTable block_access_ts_table;
        friend class ROTransaction;
        friend class RWTransaction;
    };
}


#endif //BWGRAPH_V2_BWGRAPH_HPP
