//
// Created by zhou822 on 5/23/23.
//

#include "transaction_tables.hpp"

namespace bwgraph{
    void ArrayTransactionTable::eager_clean(uint64_t index) {
        //need to access BwGraph and its block manager
        auto& entry = local_table[index];

    }
}