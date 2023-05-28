#include <iostream>
#include "core/block_manager.hpp"
#include "core/transaction_tables.hpp"
#include "core/block.hpp"
#include "core/bw_index.hpp"
using namespace bwgraph;
int main() {
    bwgraph::BlockManager manager("", 1e+11);
    auto ptr = manager.alloc(5);
    std::cout << "Hello, World! " <<ptr<< std::endl;
    std::cout<<sizeof(ArrayTransactionTables)<<std::endl;
    std::cout<<sizeof(EdgeDeltaBlockHeader)<<std::endl;
    DeltaLabelBlock label_block;
    std::cout<<sizeof(label_block)<<std::endl;
    return 0;
}
