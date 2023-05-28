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
namespace bwgraph{
    class ROTransaction;
    class RWTransaction;


    class BwGraph {
    public:
#if USING_ARRAY_TABLE
        BwGraph(std::string block_path = "",size_t _max_block_size = 1ul << 32,
            std::string wal_path = ""): block_manager(block_path,_max_block_size), vertex_index(block_manager),txn_tables(this){
    }
#else
        BwGraph(std::string block_path = "",size_t _max_block_size = 1ul << 32,
            std::string wal_path = ""): block_manager(block_path,_max_block_size), vertex_index(block_manager){
    }
#endif
        ROTransaction begin_read_only_transaction();
        RWTransaction begin_read_write_transaction();


    private:
        BlockManager block_manager;
        VertexIndex vertex_index;
#if USING_ARRAY_TABLE
        ArrayTransactionTables txn_tables;
#else
        ConcurrentTransactionTables txn_tables;
#endif
        //Commit Manager
        //Block Access and TS table
        BlockAccessTimestampTable block_access_ts_table;
        friend class ROTransaction;
        friend class RWTransaction;
    };
}


#endif //BWGRAPH_V2_BWGRAPH_HPP
