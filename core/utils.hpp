//
// Created by zhou822 on 5/22/23.
//

#ifndef BWGRAPH_V2_UTILS_HPP
#define BWGRAPH_V2_UTILS_HPP
#pragma once

#include <string>

#include "types.hpp"
#define txnIDMask 0x00FFFFFFFFFFFFFF
#define TS_ID_MASK 0x8000000000000000
#define Not_TS_ID_MASK 0x7FFFFFFFFFFFFFFF
#define VERTEX_ID_MASK 0x0000FFFFFFFFFFFF
namespace bwgraph{
    inline bool is_txn_id(uint64_t txnID){
        if((txnID&TS_ID_MASK)){
            return true;
        }
        return false;
    }
    inline int32_t get_threadID(uint64_t txnID){
        //return (txnID >> 56)&(~TS_ID_MASK);
        return (int32_t) ((txnID&Not_TS_ID_MASK)>>56);
    }
    //first bit tells it is a txn id
    //next 7 bits tell the thread id
    //final 56 bits tell the local txn id
    inline uint64_t generate_txnID(uint8_t threadID,uint64_t txnID){
        return (((uint64_t)threadID)<<56)+(txnID&txnIDMask)+TS_ID_MASK;
    }
    inline uint64_t get_local_txn_id(uint64_t txnID){
        return ((txnID<<8)>>8);
    }
    inline uint64_t generate_block_id(int64_t vertex_id, label_t label){
        return (static_cast<uint64_t>(label)<<48)|static_cast<uint64_t>(vertex_id);
    }
    inline std::pair<int64_t, uint16_t> decompose_block_id(uint64_t block_id){
        return std::pair<int64_t,uint16_t>((block_id&VERTEX_ID_MASK),(static_cast<uint16_t>(block_id>>48)));
    }
}
#endif //BWGRAPH_V2_UTILS_HPP
