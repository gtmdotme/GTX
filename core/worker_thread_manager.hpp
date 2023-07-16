//
// Created by zhou822 on 6/18/23.
//
#pragma once
//#ifndef BWGRAPH_V2_WORKER_THREAD_MANAGER_HPP
//#define BWGRAPH_V2_WORKER_THREAD_MANAGER_HPP
//todo:: 2 versions, one use parallel hashmap, the other uses tbb
#include "../Libraries/parallel_hashmap/phmap.h"
#include <thread>
#include <atomic>
namespace bwgraph{
    using thread_id =  std::thread::id;
    using ThreadIDMap = phmap::parallel_flat_hash_map<
            thread_id,
            uint8_t ,
            phmap::priv::hash_default_hash<thread_id>,
            phmap::priv::hash_default_eq<thread_id>,
            std::allocator<std::pair<const thread_id, uint8_t>>,
            12,
            std::mutex>;
    class WorkerThreadManager{
    public:
        inline uint64_t get_real_worker_thread_size(){
            return thread_id_map.size();
        }
        inline uint8_t get_worker_thread_id(){
            uint8_t thread_id;
            if(!thread_id_map.if_contains(std::this_thread::get_id(),[&thread_id](typename ThreadIDMap::value_type& pair){ thread_id = pair.second;})) {
                thread_id = global_thread_id_allocation.fetch_add(1);
                thread_id_map.try_emplace(std::this_thread::get_id(),thread_id);
            }
            return thread_id;
        }
        inline void reset_worker_thread_id(){
            global_thread_id_allocation=0;
            thread_id_map.clear();
        }
    private:
        ThreadIDMap thread_id_map;
        std::atomic_uint8_t global_thread_id_allocation = 0;
    };
}//bwgraph

//#endif //BWGRAPH_V2_WORKER_THREAD_MANAGER_HPP
