//
// Created by zhou822 on 6/18/23.
//

#ifndef BWGRAPH_V2_WORKER_THREAD_MANAGER_HPP
#define BWGRAPH_V2_WORKER_THREAD_MANAGER_HPP
//todo:: 2 versions, one use parallel hashmap, the other uses tbb
#include "Libraries/parallel_hashmap/phmap.h"
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
        inline uint8_t get_worker_thread_id(){
            auto result = thread_id_map.find(std::this_thread::get_id());
            if(result!=thread_id_map.end()){
                return result->second;
            }else{
                result->second = global_thread_if_allocation.fetch_add(1);
                return result->second;
            }
        }
    private:
        ThreadIDMap thread_id_map;
        std::atomic_uint8_t global_thread_if_allocation = 0;
    };
}//bwgraph

#endif //BWGRAPH_V2_WORKER_THREAD_MANAGER_HPP
