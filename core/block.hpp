//
// Created by zhou822 on 5/22/23.
//

//#ifndef BWGRAPH_V2_BLOCK_HPP
//#define BWGRAPH_V2_BLOCK_HPP
#pragma once

#include <atomic>
#include <cassert>
#include <cstddef>
#include <iostream>
#include <vector>
#include <unordered_map>
#include "assert.h"
#include "types.hpp"
#include "utils.hpp"
#include "exceptions.hpp"
#include "transaction_tables.hpp"

namespace bwgraph {
#define LAZY_LOCKING false
#define ENTRY_DELTA_SIZE 64
#define SIZE2MASK 0x00000000FFFFFFFF
#define LOCK_MASK 0x80000000
#define UNLOCK_MASK 0x7FFFFFFF
#define FILTER_INITIAL_SIZE 8
#define OVERFLOW_BIT 0x8000000080000000
#define MAX_LOCK_INHERITANCE_ROUND 3
#define ERROR_ENTRY_OFFSET 0xFFFFFFFF

#define EDGE_DELTA_TEST false
#define Count_Lazy_Protocol true
#define PESSIMISTIC_DELTA_BLOCK true

    //the atomic offset variable that points to the head of a delta chain
    struct AtomicDeltaOffset {
        std::atomic_uint32_t delta_offset;//0 means the end of the block which is impossible, the minimum meaningful offset value is 64
        AtomicDeltaOffset() {
            delta_offset.store(0);
        }

        AtomicDeltaOffset(const AtomicDeltaOffset &other) {
            this->delta_offset.store(other.delta_offset.load());
        }

        AtomicDeltaOffset &operator=(const AtomicDeltaOffset &other) {
            delta_offset.store(other.delta_offset);
            return *this;
        }

        inline uint32_t get_offset() {
            return delta_offset.load();
        }

        //return the offset ignoring lock bit
        inline uint32_t get_raw_offset() {
            return delta_offset.load() & UNLOCK_MASK;
        }

        inline void update_offset(uint32_t offset) {
            delta_offset.store(offset);
        }

        //todo: check if we can use weak
        inline bool try_set_lock() {
            while (true) {
                uint32_t current_offset = delta_offset.load();
                if (current_offset & LOCK_MASK) {
                    return false;
                }
                uint32_t new_offset = current_offset | LOCK_MASK;
                if (delta_offset.compare_exchange_strong(current_offset, new_offset)) {
                    return true;
                }
            }
        }

        inline bool try_set_lock(uint32_t expected_offset) {
            uint32_t new_offset = expected_offset | LOCK_MASK;
            return delta_offset.compare_exchange_strong(expected_offset, new_offset);
        }

        void release_lock() {
            uint32_t current_offset = delta_offset.load();
#if EDGE_DELTA_TEST
            if(!(current_offset&LOCK_MASK)){
                throw DeltaLockException();
            }
            uint32_t new_offset = current_offset&UNLOCK_MASK;
            if(delta_offset.compare_exchange_strong(current_offset,new_offset)){
                return;
            }else{
                throw DeltaLockException();
            }
#else
            current_offset &= UNLOCK_MASK;
            delta_offset.store(current_offset);
#endif
        }

    };

    struct atomic_offset_with_bool_flag {
        bool flag;
        std::atomic_uint32_t offset;
    };

    class alignas(64) BaseEdgeDelta {
    public:
        BaseEdgeDelta &operator=(const BaseEdgeDelta &other) {
            toID = other.toID;
            delta_type = other.delta_type;
            creation_ts.store(other.creation_ts.load());
            invalidate_ts.store(other.invalidate_ts.load());
            data_length = other.data_length;
            data_offset = other.data_offset;
            previous_offset = other.previous_offset;
            is_last_delta.store(other.is_last_delta);
            valid.store(other.valid);
            return *this;
        }

        BaseEdgeDelta(const BaseEdgeDelta &other) {
            toID = other.toID;
            delta_type = other.delta_type;
            creation_ts.store(other.creation_ts.load());
            invalidate_ts.store(other.invalidate_ts.load());
            data_length = other.data_length;
            data_offset = other.data_offset;
            previous_offset = other.previous_offset;
            is_last_delta.store(other.is_last_delta);
            valid.store(other.valid);
        }

        inline bool lazy_update(uint64_t original_txn_id, uint64_t status) {
            return creation_ts.compare_exchange_strong(original_txn_id, status);
        }

        //eager abort should always succeed
        inline void eager_abort(uint64_t original_txn_id = 0) {
#if EDGE_DELTA_TEST
            if(!creation_ts.compare_exchange_strong(original_txn_id,ABORT)){
                throw EagerAbortException();//eager update should always succeed here
            }
#else
            creation_ts.store(ABORT);
#endif
        }

        vertex_t toID;
        EdgeDeltaType delta_type;
        // timestamp_t creation_ts;
        std::atomic_uint64_t creation_ts;
        std::atomic_uint64_t invalidate_ts;//todo: think a bit more about whether it needs to be atomic
        uint32_t data_length;
        uint32_t data_offset;//where its data entry is at
        uint32_t previous_offset;//todo:the delta chain chains all deltas of the same filter lock together
        uint32_t previous_version_offset;
        //int32_t next_offset;
        std::atomic_bool is_last_delta;
        std::atomic_bool valid;
        char data[14];//padding to make it one cache line.
    };

    static_assert(sizeof(BaseEdgeDelta) == 64);

    class EdgeDeltaBlockHeader {
    public:
        inline static uint32_t get_delta_offset_from_combined_offset(uint64_t input_offset) {
            return static_cast<uint32_t>(input_offset & SIZE2MASK);
        }

        inline vertex_t get_owner_id() {
            return owner_id;
        }

        //metadata accessor:
        inline timestamp_t get_creation_time() {
            return creation_time;
        }

        inline void set_offset(uint64_t input_offset) {
            combined_offsets.store(input_offset);
        }

        inline uintptr_t get_previous_ptr() {
            return prev_pointer;
        }

        inline uint64_t get_current_offset() {
            return combined_offsets.load();
        }

        inline char *get_edge_data(uint32_t offset) {
            return get_edge_data() + offset;
        }

        inline char *get_edge_data() {
            return data;
        }

        inline uint32_t get_size() {
            return (uint32_t) ((1ul << order) - sizeof(EdgeDeltaBlockHeader));
        }

        order_t get_order() {
            return order;
        }

        inline int32_t get_delta_chain_num() {
            return delta_chain_num;
        }

        inline bool already_overflow() {
            return is_overflow_offset(combined_offsets.load());
        }

        //metadata modifier
        inline void
        fill_metadata(vertex_t input_owner_id, timestamp_t input_creation_time, uintptr_t input_prev_pointer,
                      order_t input_order, TxnTables *txn_table_ptr, std::vector<AtomicDeltaOffset> *input_index_ptr) {
            owner_id = input_owner_id;
            creation_time = input_creation_time;
            prev_pointer = input_prev_pointer;
            order = input_order;
            //define a function that determines how many delta chains it has:
            delta_chain_num = 1ul << ((order == DEFAULT_EDGE_DELTA_BLOCK_ORDER) ? 1 : (order + 1 -
                                                                                       DEFAULT_EDGE_DELTA_BLOCK_ORDER));//index takes less than 1% in storage
            txn_tables = txn_table_ptr;
            delta_chains_index = input_index_ptr;
        }

        //get a specific delta
        BaseEdgeDelta *get_edge_delta(uint32_t entry_offset) {
#if EDGE_DELTA_TEST
            if(entry_offset>=LOCK_MASK){
                throw std::runtime_error("error, the offset is not raw offset");
            }
#endif
            return (BaseEdgeDelta * )((uint8_t *) this + (1ul << order) - entry_offset);
        }

        //read operation: this can either be the current delta chain head or start from transaction's own deltas
        BaseEdgeDelta *get_visible_target_delta_using_delta_chain(uint32_t offset, vertex_t dst, uint64_t txn_read_ts,
                                                                  std::unordered_map<uint64_t, int32_t> &lazy_update_records,
                                                                  uint64_t txn_id) {
            BaseEdgeDelta *current_delta;
            while (offset) {
                current_delta = get_edge_delta(offset);
#if EDGE_DELTA_TEST
                if(!current_delta->valid){
                    throw DeltaChainCorruptionException();
                }
#endif //EDGE_DELTA_TEST
                if (current_delta->creation_ts == txn_id && current_delta->toID == dst) {
                    return current_delta;
                }//else if(current_delta->creation_ts==txn_id){continue}
                uint64_t original_ts = current_delta->creation_ts.load();
                if (is_txn_id(original_ts)) {
                    uint64_t status = 0;
                    if (txn_tables->get_status(original_ts, status)) {
                        if (status == IN_PROGRESS) {
                            offset = current_delta->previous_offset;
                            continue;
                        } else {
                            if (status != ABORT) {
#if CHECKED_PUT_EDGE
                                update_previous_delta_invalidate_ts(current_delta->toID, current_delta->previous_version_offset,
                                                                    status);
#else
                                update_previous_delta_invalidate_ts(current_delta->toID, current_delta->previous_offset,
                                                                    status);
#endif
                                if (current_delta->lazy_update(original_ts, status)) {
#if LAZY_LOCKING
                                    if(current_delta->is_last_delta){
                                        release_protection(current_delta->toID);
                                    }
#endif
                                    auto result = lazy_update_records.try_emplace(original_ts, 1);
                                    if (!result.second) {
                                        result.first->second++;
                                    }
                                }
                            }
#if EDGE_DELTA_TEST
                            if(current_delta->creation_ts!=status){
                                throw LazyUpdateException();
                            }
#endif
                        }
                    }
                }
#if EDGE_DELTA_TEST
                if(current_delta->creation_ts==IN_PROGRESS|| is_txn_id(current_delta->creation_ts)){
                    throw new std::runtime_error("error, lazy update failed");
                }
#endif
#if CHECKED_PUT_EDGE
                if (current_delta->toID == dst ) {//aborted delta should not be in the secondary index, only special case is when there is a system crash
                    if(current_delta->creation_ts.load() <=txn_read_ts)
                        return current_delta;
                    offset = current_delta->previous_version_offset;
                }else{
                    offset = current_delta->previous_offset;
                }
                //offset = current_delta->previous_offset;
#else
                if (current_delta->toID == dst &&
                    /*current_delta->creation_ts!=ABORT&&*/current_delta->creation_ts.load() <=
                                                           txn_read_ts) {//aborted delta should not be in the secondary index, only special case is when there is a system crash
                    return current_delta;
                }
                offset = current_delta->previous_offset;
#endif
            }
            return nullptr;
        }

        /*
         * for read only txns
         */
        BaseEdgeDelta *get_visible_target_delta_using_delta_chain(uint32_t offset, vertex_t dst, uint64_t txn_read_ts,
                                                                  std::unordered_map<uint64_t, int32_t> &lazy_update_records) {
            BaseEdgeDelta *current_delta;
            while (offset) {
                current_delta = get_edge_delta(offset);
#if EDGE_DELTA_TEST
                if(!current_delta->valid){
                    throw DeltaChainCorruptionException();
                }
#endif
                uint64_t original_ts = current_delta->creation_ts.load();
                if (is_txn_id(original_ts)) {
                    uint64_t status = 0;
                    if (txn_tables->get_status(original_ts, status)) {
                        if (status == IN_PROGRESS) {
                            offset = current_delta->previous_offset;
                            continue;
                        } else {
                            //status can still be abort because of eager abort from validation txns
                            if (status != ABORT) {
                                //move it before lazy update to enforce serialization
#if CHECKED_PUT_EDGE
                                update_previous_delta_invalidate_ts(current_delta->toID, current_delta->previous_version_offset,
                                                                    status);
#else
                                update_previous_delta_invalidate_ts(current_delta->toID, current_delta->previous_offset,
                                                                    status);
#endif
                                if (current_delta->lazy_update(original_ts, status)) {
#if LAZY_LOCKING
                                    if(current_delta->is_last_delta){
                                        release_protection(current_delta->toID);
                                    }
#endif
                                    auto result = lazy_update_records.try_emplace(original_ts, 1);
                                    if (!result.second) {
                                        result.first->second++;
                                    }
                                }
                            }
#if EDGE_DELTA_TEST
                            if(current_delta->creation_ts!=status){
                                throw LazyUpdateException();
                            }
#endif
                        }
                    }
                }
#if EDGE_DELTA_TEST
                if(current_delta->creation_ts==IN_PROGRESS|| is_txn_id(current_delta->creation_ts)){
                    throw new std::runtime_error("error, lazy update failed");
                }
#endif
#if CHECKED_PUT_EDGE
                if (current_delta->toID == dst) {//aborted delta should not be in the secondary index, only special case is when there is a system crash
                    if(current_delta->creation_ts.load() <=txn_read_ts)
                        return current_delta;
                    offset = current_delta->previous_version_offset;
                }else{
                    offset = current_delta->previous_offset;
                }
#else
                if (current_delta->toID == dst &&
                    /*current_delta->creation_ts!=ABORT&&*/current_delta->creation_ts.load() <=
                                                           txn_read_ts) {//aborted delta should not be in the secondary index, only special case is when there is a system crash
                    return current_delta;
                }
                offset = current_delta->previous_offset;
#endif
            }
            return nullptr;
        }

        /*
         * use scan to locate the previous version of the edge to update invalidate ts
         */
        void update_previous_delta_invalidate_ts_scan() {

        }

        //lazy update function
        inline void update_previous_delta_invalidate_ts(vertex_t target_vid, uint32_t offset, uint64_t invalidate_ts) {
#if CHECKED_PUT_EDGE
            if(offset){
                auto target_delta = get_edge_delta(offset);
#if EDGE_DELTA_TEST
                if(target_delta->toID!=target_vid){
                    throw std::runtime_error("error, the previous version is not actually a previous versiom");
                }
#endif
                target_delta->invalidate_ts.store(invalidate_ts);
            }
#else
            while (offset) {
                BaseEdgeDelta *current_delta = get_edge_delta(offset);
                if (current_delta->toID == target_vid) {//for the first entry of a lazy updated entry,
                    current_delta->invalidate_ts = invalidate_ts;
                    return;
                }
                offset = current_delta->previous_offset;
            }
#endif
        }
        //maybe unused
        inline void update_previous_delta_invalidate_ts(BaseEdgeDelta* current_delta){
#if CHECKED_PUT_EDGE
            auto target_delta = get_edge_delta(current_delta->previous_version_offset);
#if EDGE_DELTA_TEST
            if(target_delta->toID!=current_delta->toID){
                throw std::runtime_error("error, the previous version is not actually a previous version");
            }
#endif //EDGE_DELTA_TEST
            target_delta->invalidate_ts.store(current_delta->creation_ts.load());
#else //CHECKED_PUT_EDGE
            uint32_t offset = current_delta->previous_offset;
            while (offset) {
                BaseEdgeDelta *previous_delta = get_edge_delta(offset);
                if (previous_delta->toID == current_delta->toID) {//for the first entry of a lazy updated entry,
                    previous_delta->invalidate_ts.store(current_delta->creation_ts.load());
                    return;
                }
                offset = previous_delta->previous_offset;
            }
#endif //CHECKED_PUT_EDGE
        }

        inline void update_previous_version_delta_invalidate_ts(vertex_t target_vid, uint32_t offset, uint64_t invalidate_ts){
            auto target_delta = get_edge_delta(offset);
#if EDGE_DELTA_TEST
            if(target_delta->toID!=target_vid){
                throw std::runtime_error("error, the previous version is not actually a previous versiom");
            }
#endif
            target_delta->invalidate_ts.store(invalidate_ts);
        }
        //concurrency functions
        Delta_Chain_Lock_Response lock_inheritance(vertex_t vid,
                                                   std::unordered_map<uint64_t, int32_t> *lazy_update_map_ptr,
                                                   uint64_t txn_read_ts,
                                                   uint32_t current_offset,
                                                   uint64_t original_ts);

        Delta_Chain_Lock_Response lock_inheritance_on_delta_chain(delta_chain_id_t delta_chain_id,
                                                                  std::unordered_map<uint64_t, int32_t> *lazy_update_map_ptr,
                                                                  uint64_t txn_read_ts,
                                                                  uint32_t current_offset,
                                                                  uint64_t original_ts);

        //try to set the lock, eagerlly try lock_inheritance
        Delta_Chain_Lock_Response
        set_protection(vertex_t vid, std::unordered_map<uint64_t, int32_t> *lazy_update_map_ptr, uint64_t txn_read_ts) {
            int32_t delta_chain_id = get_delta_chain_id(vid);
            auto &target_chain_index_entry = delta_chains_index->at(delta_chain_id);
            uint32_t latest_delta_chain_head_offset = target_chain_index_entry.get_offset();
            uint32_t raw_delta_chain_offset = latest_delta_chain_head_offset & UNLOCK_MASK;
            BaseEdgeDelta *delta_chain_head = nullptr;
            //todo: also check lock?
            if (raw_delta_chain_offset) {
                delta_chain_head = get_edge_delta(raw_delta_chain_offset);
                uint64_t original_ts = delta_chain_head->creation_ts.load();
                if (is_txn_id(original_ts)) {
#if EDGE_DELTA_TEST
                    if(!delta_chain_head->valid){
                        throw DeltaChainCorruptionException();
                    }
                    if(!(latest_delta_chain_head_offset&LOCK_MASK)){
                        throw DeltaLockException();
                    }
#endif
                    auto temporary_result = lock_inheritance(vid, lazy_update_map_ptr, txn_read_ts,
                                                             raw_delta_chain_offset, original_ts);
                    if (temporary_result == Delta_Chain_Lock_Response::LOCK_INHERIT ||
                        temporary_result == Delta_Chain_Lock_Response::CONFLICT) {
                        return temporary_result;
                    }
                } else if (original_ts > txn_read_ts) {
                    return Delta_Chain_Lock_Response::CONFLICT;
                }
            }
            if (target_chain_index_entry.try_set_lock()) {
                latest_delta_chain_head_offset = target_chain_index_entry.get_offset();
                raw_delta_chain_offset = latest_delta_chain_head_offset & UNLOCK_MASK;
                if (raw_delta_chain_offset) {
                    delta_chain_head = get_edge_delta(raw_delta_chain_offset);
#if EDGE_DELTA_TEST
                    if(!delta_chain_head->valid){
                        throw DeltaChainCorruptionException();
                    }
                    if(is_txn_id(delta_chain_head->creation_ts.load())){
                        throw LazyUpdateException();
                    }
#endif
                    //most recent update is concurrent: we must abort
                    if (delta_chain_head->creation_ts.load() > txn_read_ts) {
                        target_chain_index_entry.release_lock();
                        return Delta_Chain_Lock_Response::CONFLICT;
                    }
                }
                return Delta_Chain_Lock_Response::SUCCESS;
            } else {
                return Delta_Chain_Lock_Response::CONFLICT;
            }
        }

        Delta_Chain_Lock_Response set_protection_on_delta_chain(delta_chain_id_t delta_chain_id,
                                                                std::unordered_map<uint64_t, int32_t> *lazy_update_map_ptr,
                                                                uint64_t txn_read_ts) {
            auto &target_chain_index_entry = delta_chains_index->at(delta_chain_id);
            uint32_t latest_delta_chain_head_offset = target_chain_index_entry.get_offset();
            uint32_t raw_delta_chain_offset = latest_delta_chain_head_offset & UNLOCK_MASK;
            BaseEdgeDelta *delta_chain_head = nullptr;
            //todo: also check lock?
            //Delta_Chain_Lock_Response temporary_result;
            if (raw_delta_chain_offset) {
                delta_chain_head = get_edge_delta(raw_delta_chain_offset);
                uint64_t original_ts = delta_chain_head->creation_ts.load();
                if (is_txn_id(original_ts)) {
#if EDGE_DELTA_TEST
                    if(!delta_chain_head->valid){
                        throw DeltaChainCorruptionException();
                    }
                    if(!(latest_delta_chain_head_offset&LOCK_MASK)){
                        throw DeltaLockException();
                    }
#endif
                    auto temporary_result = lock_inheritance_on_delta_chain(delta_chain_id, lazy_update_map_ptr,
                                                                            txn_read_ts, raw_delta_chain_offset,
                                                                            original_ts);
                    if (temporary_result == Delta_Chain_Lock_Response::LOCK_INHERIT ||
                        temporary_result == Delta_Chain_Lock_Response::CONFLICT) {
                        return temporary_result;
                    }
                } else if (original_ts > txn_read_ts) {
                    return Delta_Chain_Lock_Response::CONFLICT;
                }
            }
            if (target_chain_index_entry.try_set_lock()) {
                latest_delta_chain_head_offset = target_chain_index_entry.get_offset();
                raw_delta_chain_offset = latest_delta_chain_head_offset & UNLOCK_MASK;
                if (raw_delta_chain_offset) {
                    delta_chain_head = get_edge_delta(raw_delta_chain_offset);
#if EDGE_DELTA_TEST
                    if(!delta_chain_head->valid){
                        throw DeltaChainCorruptionException();
                    }
                    if(is_txn_id(delta_chain_head->creation_ts.load())){
                        throw LazyUpdateException();
                    }
#endif
                    //most recent update is concurrent: we must abort
                    if (delta_chain_head->creation_ts.load() > txn_read_ts) {
                        target_chain_index_entry.release_lock();
                        return Delta_Chain_Lock_Response::CONFLICT;
                    }
                }
                return Delta_Chain_Lock_Response::SUCCESS;
            } else {
                return Delta_Chain_Lock_Response::CONFLICT;
            }
        }

        /*
         * this function assumes simpler locking protocol: when a transaction validates its delta chain, it also releases the lock.
         * The write-write conflict is prevented by first checking the bit, then check the delta chain head
         */
        Delta_Chain_Lock_Response simple_set_protection_on_delta_chain(delta_chain_id_t delta_chain_id,
                                                                       std::unordered_map<uint64_t, int32_t> *lazy_update_map_ptr,
                                                                       uint64_t txn_read_ts) {
            auto &target_chain_index_entry = delta_chains_index->at(delta_chain_id);
            uint32_t latest_delta_chain_head_offset = target_chain_index_entry.get_offset();
            if (latest_delta_chain_head_offset & LOCK_MASK) {
                return Delta_Chain_Lock_Response::CONFLICT;
            }
            if (latest_delta_chain_head_offset) {
                auto current_head_delta = get_edge_delta(latest_delta_chain_head_offset);
                auto current_head_ts = current_head_delta->creation_ts.load();
                if (is_txn_id(current_head_ts)) {
                    uint64_t status = 0;
                    if (txn_tables->get_status(current_head_ts, status)) {
                        if (status != IN_PROGRESS && status != ABORT) {
#if CHECKED_PUT_EDGE
                            update_previous_delta_invalidate_ts(current_head_delta->toID,
                                                                current_head_delta->previous_version_offset, status);
#else
                            update_previous_delta_invalidate_ts(current_head_delta->toID,
                                                                current_head_delta->previous_offset, status);
#endif

                            if (current_head_delta->lazy_update(current_head_ts, status)) {
                                record_lazy_update_record(lazy_update_map_ptr, current_head_ts);
                            }
#if EDGE_DELTA_TEST
                            if(current_head_delta->creation_ts!=status){
                                throw LazyUpdateException();
                            }
#endif
                            if (status <= txn_read_ts) {
                                if (target_chain_index_entry.try_set_lock(latest_delta_chain_head_offset)) {
                                    return Delta_Chain_Lock_Response::SUCCESS;
                                } else {
                                    return Delta_Chain_Lock_Response::CONFLICT;
                                }
                            } else {
                                return Delta_Chain_Lock_Response::CONFLICT;
                            }
                        } else if (status == IN_PROGRESS) {
                            return Delta_Chain_Lock_Response::CONFLICT;
                        } else {
#if EDGE_DELTA_TEST
                            if(current_head_delta->creation_ts!=ABORT){
                                throw EagerAbortException();
                            }
#endif
                            return Delta_Chain_Lock_Response::UNCLEAR;//is concurrently eager aborted, so retry the get protection
                        }
                    } else {
                        return Delta_Chain_Lock_Response::UNCLEAR;//the txn entry is missing, someone else did lazy update, let's retry
                    }
                } else if (current_head_ts != ABORT) {
                    if (current_head_ts <= txn_read_ts) {
                        if (target_chain_index_entry.try_set_lock(latest_delta_chain_head_offset)) {
                            return Delta_Chain_Lock_Response::SUCCESS;
                        } else {
                            return Delta_Chain_Lock_Response::CONFLICT;
                        }
                    } else {
                        return Delta_Chain_Lock_Response::CONFLICT;
                    }
                } else {
                    //todo: abort should happen after the restore? so txn will never observe unlocked offset to abort deltas
                    throw EagerAbortException();
                    //delta chain head failed during validation, so we need to retry
                    //return Delta_Chain_Lock_Response::UNCLEAR;
                }
            } else {
                if (target_chain_index_entry.try_set_lock(latest_delta_chain_head_offset)) {
                    return Delta_Chain_Lock_Response::SUCCESS;
                } else {
                    return Delta_Chain_Lock_Response::CONFLICT;
                }
            }
        }

        Delta_Chain_Lock_Response
        simple_set_protection(vertex_t vid, std::unordered_map<uint64_t, int32_t> *lazy_update_map_ptr,
                              uint64_t txn_read_ts) {
            delta_chain_id_t delta_chain_id = get_delta_chain_id(vid);
            auto &target_chain_index_entry = delta_chains_index->at(delta_chain_id);
            uint32_t latest_delta_chain_head_offset = target_chain_index_entry.get_offset();
            if (latest_delta_chain_head_offset & LOCK_MASK) {
                return Delta_Chain_Lock_Response::CONFLICT;
            }
            auto current_head_delta = get_edge_delta(latest_delta_chain_head_offset);
            auto current_head_ts = current_head_delta->creation_ts.load();
            if (is_txn_id(current_head_ts)) {
                uint64_t status = 0;
                if (txn_tables->get_status(current_head_ts, status)) {
                    if (status != IN_PROGRESS && status != ABORT) {
                        update_previous_delta_invalidate_ts(current_head_delta->toID,
                                                            current_head_delta->previous_offset, status);
                        if (current_head_delta->lazy_update(current_head_ts, status)) {
                            record_lazy_update_record(lazy_update_map_ptr, current_head_ts);
                        }
#if EDGE_DELTA_TEST
                        if(current_head_delta->creation_ts!=status){
                            throw LazyUpdateException();
                        }
#endif
                        if (status <= txn_read_ts) {
                            if (target_chain_index_entry.try_set_lock(latest_delta_chain_head_offset)) {
                                return Delta_Chain_Lock_Response::SUCCESS;
                            } else {
                                return Delta_Chain_Lock_Response::CONFLICT;
                            }
                        } else {
                            return Delta_Chain_Lock_Response::CONFLICT;
                        }
                    } else if (status == IN_PROGRESS) {
                        return Delta_Chain_Lock_Response::CONFLICT;
                    } else {
#if EDGE_DELTA_TEST
                        if(current_head_delta->creation_ts!=ABORT){
                            throw EagerAbortException();
                        }
#endif
                        return Delta_Chain_Lock_Response::UNCLEAR;//is concurrently eager aborted, so retry the get protection
                    }
                } else {//the txn entry is missing, someone else did lazy update, let's retry
                    return Delta_Chain_Lock_Response::UNCLEAR;
                }
            } else if (current_head_ts != ABORT) {
                if (current_head_ts <= txn_read_ts) {
                    if (target_chain_index_entry.try_set_lock(latest_delta_chain_head_offset)) {
                        return Delta_Chain_Lock_Response::SUCCESS;
                    } else {
                        return Delta_Chain_Lock_Response::CONFLICT;
                    }
                } else {
                    return Delta_Chain_Lock_Response::CONFLICT;
                }
            } else {
                //delta chain head failed during validation, so we need to retry
                return Delta_Chain_Lock_Response::UNCLEAR;
            }
        }

        inline void release_protection(vertex_t vid) {
            delta_chain_id_t delta_chain_id = get_delta_chain_id(vid);
            auto &current_entry = delta_chains_index->at(delta_chain_id);
            current_entry.release_lock();
        }

        inline void release_protection_delta_chain(delta_chain_id_t id) {
            auto &current_entry = delta_chains_index->at(id);
            current_entry.release_lock();
        }

        inline bool try_set_protection(vertex_t vid) {
            int32_t delta_chain_id = get_delta_chain_id(vid);
            auto &current_entry = delta_chains_index->at(delta_chain_id);
            return current_entry.try_set_lock();
        }

        inline bool try_set_protection_on_delta_chain(delta_chain_id_t delta_chain_id) {
#if EDGE_DELTA_TEST
            if(delta_chain_id>=delta_chain_num){
                throw DeltaChainNumberException();
            }
#endif
            auto &current_entry = delta_chains_index->at(delta_chain_id);
            return current_entry.try_set_lock();
        }

        inline int32_t get_delta_chain_id(vertex_t vid) {
            return static_cast<int32_t>(vid % delta_chain_num);
        }

        //delta allocation
        inline uint64_t allocate_space_for_new_delta(uint32_t data_size) {
            uint64_t to_atomic = ((static_cast<uint64_t>(data_size)) << 32) + ENTRY_DELTA_SIZE;
            return combined_offsets.fetch_add(to_atomic);
        }

        void print_metadata() {
            std::cout << "order is " << static_cast<uint32_t>(order) << std::endl;
            std::cout << "creation time is " << creation_time << std::endl;
            std::cout << "owner id is " << owner_id << std::endl;
            std::cout << "current offset is " << combined_offsets.load() << std::endl;
            std::cout << "previous address is " << prev_pointer << std::endl;
        }

        /*
         * It finds where the previous version is:
         * only invoked after grabbing the lock, so there is no way we meet an aborted delta
         */
        uint32_t fetch_previous_version_offset(vertex_t vid, uint32_t start_offset, uint64_t txn_id, lazy_update_map &lazy_update_records) {
            while (start_offset) {
                auto current_delta = get_edge_delta(start_offset);
                uint64_t original_ts = current_delta->creation_ts.load();
                //still do lazy update
                if (original_ts!=txn_id && is_txn_id(original_ts)) {
                    uint64_t status = 0;
                    if (txn_tables->get_status(original_ts, status)) {
                        //status can still be abort because of eager abort from validation txns
                        //move it before lazy update to enforce serialization
#if CHECKED_PUT_EDGE
                        update_previous_delta_invalidate_ts(current_delta->toID, current_delta->previous_version_offset, status);
#else
                        update_previous_delta_invalidate_ts(current_delta->toID, current_delta->previous_offset, status);
#endif
                        if (current_delta->lazy_update(original_ts, status)) {
#if LAZY_LOCKING
                            if(current_delta->is_last_delta){
                                release_protection(current_delta->toID);
                            }
#endif
                            auto result = lazy_update_records.try_emplace(original_ts, 1);
                            if (!result.second) {
                                result.first->second++;
                            }
                        }

#if EDGE_DELTA_TEST
                        if(status == IN_PROGRESS||status == ABORT){
                            throw LazyUpdateException();
                        }
                        if(current_delta->creation_ts!=status){
                            throw LazyUpdateException();
                        }
#endif
                    }
                }
#if EDGE_DELTA_TEST
                else if(original_ts == ABORT){
                    throw EagerAbortException();//should not meet aborted delta in the chain
                }
#endif
                if(current_delta->toID== vid){
                    current_delta->invalidate_ts.store(txn_id);
                    if(current_delta->delta_type!=EdgeDeltaType::DELETE_DELTA){
                        return start_offset;
                    }else{
                        return 0;//for delete delta, just return 0 as if no previous version exist
                    }
                }
                start_offset = current_delta->previous_offset;
            }
            return 0;
        }

        inline bool is_overflow_offset(uint64_t current_offset) {
            uint32_t data_size = (uint32_t) (current_offset >> 32);
            uint32_t delta_size = (uint32_t) (current_offset & SIZE2MASK);
            return get_size() < (data_size + delta_size);
        }

        //delta append
        void append_edge_delta(vertex_t toID, uint64_t txnID, EdgeDeltaType type, const char *edge_data, int data_size,
                               uint32_t previous_delta_offset, uint32_t current_delta_offset,
                               uint32_t current_data_offset) {
            BaseEdgeDelta *edgeDelta = (get_edge_delta(current_delta_offset));
            edgeDelta->toID = toID;
            edgeDelta->delta_type = type;
            edgeDelta->creation_ts = txnID;//todo in the future we need to handle checking txn id and ts
            edgeDelta->data_length = data_size;
            edgeDelta->data_offset = current_data_offset;
            edgeDelta->is_last_delta = true;
            for (int i = 0; i < data_size; i++) {
                (get_edge_data(current_data_offset))[i] = edge_data[i];
            }
            edgeDelta->valid.store(true);
            if (previous_delta_offset) {
                edgeDelta->previous_offset = previous_delta_offset;
#if LAZY_LOCKING
                BaseEdgeDelta* previous_edge_delta = (get_edge_delta(previous_delta_offset));
#if EDGE_DELTA_TEST
                if(get_delta_chain_id(toID)!= get_delta_chain_id(previous_edge_delta->toID)){
                    throw DeltaChainCorruptionException();
                }
#endif// EDGE_DELTA_TEST

                previous_edge_delta->is_last_delta=false;
#endif//LAZY_LOCKING
            }
            return;
        }

        //it is only used during consolidation
        std::pair<EdgeDeltaInstallResult, uint32_t>
        append_edge_delta(vertex_t toID, timestamp_t creation_ts, EdgeDeltaType type, const char *edge_data,
                          int data_size, uint32_t previous_delta_offset) {
            //allocate space for new delta installation
            uint32_t size = get_size();
            uint64_t originalOffset = allocate_space_for_new_delta(data_size);
            uint32_t originalDataOffset = (uint32_t) (originalOffset >> 32);
            originalOffset &= SIZE2MASK;
            uint32_t originalEntryOffset = (uint32_t) originalOffset;
            uint32_t newDataOffset = originalDataOffset + data_size;
            uint32_t newEntryOffset = originalEntryOffset + ENTRY_DELTA_SIZE;
            //ideally those code should not execute at all
            if (newEntryOffset < originalEntryOffset || newDataOffset < originalDataOffset) {
                throw new std::runtime_error("new offset is not atomically picked correctly");
            }
            if ((newDataOffset + newEntryOffset) > size) {
                //do consolidation.
                if ((originalDataOffset + originalEntryOffset) <= size) {
                    return std::pair<EdgeDeltaInstallResult, uint32_t>(EdgeDeltaInstallResult::CAUSE_OVERFLOW, 0);
                }
                //unset_protection(toID);
                return std::pair<EdgeDeltaInstallResult, uint32_t>(EdgeDeltaInstallResult::ALREADY_OVERFLOW, 0);
            }

            BaseEdgeDelta *edgeDelta = (get_edge_delta(newEntryOffset));
            edgeDelta->toID = toID;
            edgeDelta->delta_type = type;
            edgeDelta->creation_ts = creation_ts;//it is guaranteed a ts
            edgeDelta->data_length = data_size;
            edgeDelta->data_offset = originalDataOffset;
            edgeDelta->is_last_delta = true;
            for (int i = 0; i < data_size; i++) {
                (get_edge_data(originalDataOffset))[i] = edge_data[i];
            }
            edgeDelta->valid.store(true);
            if (previous_delta_offset) {
                edgeDelta->previous_offset = previous_delta_offset;
#if LAZY_LOCKING
                BaseEdgeDelta* previous_edge_delta = (get_edge_delta(previous_delta_offset));
#if EDGE_DELTA_TEST
                if(get_delta_chain_id(toID)!= get_delta_chain_id(previous_edge_delta->toID)){
                    throw DeltaChainCorruptionException();
                }
#endif //EDGE_DELTA_TEST
                previous_edge_delta->is_last_delta=false;
#endif //LAZY_LOCKING
            }
            return std::pair<EdgeDeltaInstallResult, uint32_t>(EdgeDeltaInstallResult::SUCCESS, newEntryOffset);
        }

        //delta append
        void checked_append_edge_delta(vertex_t toID, uint64_t txnID, EdgeDeltaType type, const char *edge_data, int data_size,
                               uint32_t previous_delta_offset, uint32_t previous_version_offset, uint32_t current_delta_offset,
                               uint32_t current_data_offset) {
            BaseEdgeDelta *edgeDelta = (get_edge_delta(current_delta_offset));
            edgeDelta->toID = toID;
            edgeDelta->delta_type = type;
            edgeDelta->creation_ts = txnID;//todo in the future we need to handle checking txn id and ts
            edgeDelta->data_length = data_size;
            edgeDelta->data_offset = current_data_offset;
            edgeDelta->previous_version_offset = previous_version_offset;
            edgeDelta->is_last_delta = true;
            for (int i = 0; i < data_size; i++) {
                (get_edge_data(current_data_offset))[i] = edge_data[i];
            }
            edgeDelta->valid.store(true);
            if (previous_delta_offset) {
                edgeDelta->previous_offset = previous_delta_offset;
#if LAZY_LOCKING
                BaseEdgeDelta* previous_edge_delta = (get_edge_delta(previous_delta_offset));
#if EDGE_DELTA_TEST
                if(get_delta_chain_id(toID)!= get_delta_chain_id(previous_edge_delta->toID)){
                    throw DeltaChainCorruptionException();
                }
#endif// EDGE_DELTA_TEST

                previous_edge_delta->is_last_delta=false;
#endif//LAZY_LOCKING
            }
            return;
        }

        //it is only used during consolidation
        std::pair<EdgeDeltaInstallResult, uint32_t>
        checked_append_edge_delta(vertex_t toID, timestamp_t creation_ts, EdgeDeltaType type, const char *edge_data,
                          int data_size, uint32_t previous_delta_offset, uint32_t previous_version_offset) {
            //allocate space for new delta installation
            uint32_t size = get_size();
            uint64_t originalOffset = allocate_space_for_new_delta(data_size);
            uint32_t originalDataOffset = (uint32_t) (originalOffset >> 32);
            originalOffset &= SIZE2MASK;
            uint32_t originalEntryOffset = (uint32_t) originalOffset;
            uint32_t newDataOffset = originalDataOffset + data_size;
            uint32_t newEntryOffset = originalEntryOffset + ENTRY_DELTA_SIZE;
            //ideally those code should not execute at all
            if (newEntryOffset < originalEntryOffset || newDataOffset < originalDataOffset) {
                throw new std::runtime_error("new offset is not atomically picked correctly");
            }
            if ((newDataOffset + newEntryOffset) > size) {
                //do consolidation.
                if ((originalDataOffset + originalEntryOffset) <= size) {
                    return std::pair<EdgeDeltaInstallResult, uint32_t>(EdgeDeltaInstallResult::CAUSE_OVERFLOW, 0);
                }
                //unset_protection(toID);
                return std::pair<EdgeDeltaInstallResult, uint32_t>(EdgeDeltaInstallResult::ALREADY_OVERFLOW, 0);
            }

            BaseEdgeDelta *edgeDelta = (get_edge_delta(newEntryOffset));
            edgeDelta->toID = toID;
            edgeDelta->delta_type = type;
            edgeDelta->creation_ts = creation_ts;//it is guaranteed a ts
            edgeDelta->data_length = data_size;
            edgeDelta->data_offset = originalDataOffset;
            edgeDelta->previous_version_offset = previous_version_offset;
            edgeDelta->is_last_delta = true;
            for (int i = 0; i < data_size; i++) {
                (get_edge_data(originalDataOffset))[i] = edge_data[i];
            }
            edgeDelta->valid.store(true);
            if (previous_delta_offset) {
                edgeDelta->previous_offset = previous_delta_offset;
#if LAZY_LOCKING
                BaseEdgeDelta* previous_edge_delta = (get_edge_delta(previous_delta_offset));
#if EDGE_DELTA_TEST
                if(get_delta_chain_id(toID)!= get_delta_chain_id(previous_edge_delta->toID)){
                    throw DeltaChainCorruptionException();
                }
#endif //EDGE_DELTA_TEST
                previous_edge_delta->is_last_delta=false;
#endif //LAZY_LOCKING
            }
            return std::pair<EdgeDeltaInstallResult, uint32_t>(EdgeDeltaInstallResult::SUCCESS, newEntryOffset);
        }
    private:
        vertex_t owner_id;
        std::atomic_uint64_t combined_offsets;
        timestamp_t creation_time;
        uintptr_t prev_pointer;
        //std::vector<Atomic_Delta_Offset>delta_chains_index;
        std::vector<AtomicDeltaOffset> *delta_chains_index;//point to the secondary index vector stored outside the block
        TxnTables *txn_tables;
        int32_t delta_chain_num;
        order_t order;
        char padding[11];//todo check whether this is actually needed
        char data[0];
    };

    //todo:: do something about delta flags, we need to care a bit more about deletion.
    class VertexDeltaHeader {
    public:
        inline bool is_visible(uint64_t txn_read_timestamp) {
            return txn_read_timestamp >= creation_time;
        }

        void print_metadata() {
            std::cout << "order is " << order << std::endl;
            std::cout << "creation time is " << creation_time << std::endl;
            std::cout << "size is " << data_size << std::endl;
        }

        inline void eager_abort() { creation_time.store(ABORT); }

        inline void fill_metadata(uint64_t input_creation_ts, size_t input_to_write_size, order_t input_block_order,
                                  uintptr_t input_previous_ptr = 0) {
            creation_time = input_creation_ts;
            previous_ptr = input_previous_ptr;
            //todo: throw exception is data size + header size is greater than 2^ block order
            data_size = input_to_write_size;
            order = input_block_order;
        }

        inline void set_data_size(size_t input_data_size) {
            data_size = input_data_size;
        }

        inline uint64_t get_creation_ts() {
            return creation_time;
        }

        inline size_t get_data_size() {
            return data_size;
        }

        inline char *get_data() {
            return data;
        }

        inline size_t get_max_data_storage() {
            return (1ul << order) - sizeof(VertexDeltaHeader);
        }

        inline size_t get_block_size() {
            return (1ul << order);
        }

        inline bool lazy_update(uint64_t original_txn_id, uint64_t new_ts) {
            return creation_time.compare_exchange_strong(original_txn_id, new_ts);
        }

        inline uintptr_t get_previous_ptr() {
            return previous_ptr;
        }

        inline order_t get_order() {
            return order;
        }

        inline void write_data(const char *data) {
            for (size_t i = 0; i < data_size; i++) {
                get_data()[i] = data[i];
            }
        }
        //todo:: implement a write function
    private:
        //int64_t owner_id;
        std::atomic_uint64_t creation_time;
        uintptr_t previous_ptr;
        size_t data_size;//the amount of data that is truly meaningful.
        //uint32_t status_flags;
        order_t order;//block size
        char data[0];
    };

    static_assert(sizeof(EdgeDeltaBlockHeader) == 64);
    static_assert(sizeof(VertexDeltaHeader) == 32);
}
//#endif //BWGRAPH_V2_BLOCK_HPP
