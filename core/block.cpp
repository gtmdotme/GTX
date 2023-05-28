//
// Created by zhou822 on 5/25/23.
//
#include "block.hpp"
#include "exceptions.hpp"
using namespace bwgraph;

Delta_Chain_Lock_Response EdgeDeltaBlockHeader::lock_inheritance(int64_t vid,
                                                                 std::unordered_map<uint64_t, int32_t> *lazy_update_map_ptr,
                                                                 uint64_t txn_read_ts,
                                                                 uint32_t current_offset,
                                                                 uint64_t original_ts) {
    BaseEdgeDelta* current_delta = get_edge_delta(current_offset);
#if EDGE_DELTA_TEST
    if(get_delta_chain_id(vid)!= get_delta_chain_id(current_delta->toID)){
        throw DeltaChainMismatchException();
    }
    if(!is_txn_id(original_ts)){
        throw LazyUpdateException();
    }
#endif
    uint64_t status=0;
    if(txn_tables->get_status(original_ts,status)){
        if (status==IN_PROGRESS){
            return Delta_Chain_Lock_Response::CONFLICT;
        }else{
            if(current_delta->lazy_update(original_ts,status)){
#if EDGE_DELTA_TEST
                //if the current transaction lazy updates the delta chain head, it must be from a committed delta because aborted transaction will eager abort their deltas,
                //so I will have no chance for lazy update
                if (status == ABORT) {
                    throw LazyUpdateException();
                }
                //on the other hand, no other transactions can write to this chain unless they lazy uodate the current head.
                if (!current_delta->is_last_delta) {
                    throw LazyUpdateException();
                }
#endif
                //record lazy update is done
                auto lazy_update_emplace_result = lazy_update_map_ptr->try_emplace(original_ts, 1);
                if (!lazy_update_emplace_result.second) {
                    lazy_update_emplace_result.first->second++;
                }
                update_previous_delta_invalidate_ts(current_delta->toID,current_delta->previous_offset,status);
                if(status>txn_read_ts){
                    //release the lock
                    release_protection(vid);
                    return Delta_Chain_Lock_Response::CONFLICT;
                }else{
                    return Delta_Chain_Lock_Response::LOCK_INHERIT;
                }
            }
        }
    }
    //return set_protection(vid,lazy_update_map_ptr,txn_read_ts);
    return Delta_Chain_Lock_Response::UNCLEAR;
}