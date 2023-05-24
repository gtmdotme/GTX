#include <iostream>
#include "core/block_manager.hpp"
#include "core/transaction_tables.hpp"
using namespace bwgraph;
int main() {
    bwgraph::BlockManager manager("", 1e+11);
    auto ptr = manager.alloc(5);
    std::cout << "Hello, World! " <<ptr<< std::endl;
    return 0;
}
