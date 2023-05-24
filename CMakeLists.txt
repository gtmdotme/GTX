cmake_minimum_required(VERSION 3.25)
project(BwGraph_v2)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${CMAKE_SOURCE_DIR}/cmake)
find_package(TBB REQUIRED PATHS /tmp/my_installed_onetbb/lib/cmake/TBB)
add_library(parallel_hashmap_lib STATIC Libraries/parallel_hashmap/something.cpp)
target_include_directories(parallel_hashmap_lib PUBLIC parallel_hashmap)
add_executable(BwGraph_v2 main.cpp) #core/types.hpp core/block_manager.hpp core/utils.hpp core/block.hpp core/exceptions.hpp core/transaction_tables.cpp core/transaction_tables.hpp core/graph_global.hpp Libraries/parallel_hashmap/something.cpp
target_link_directories(BwGraph_v2 PUBLIC core/ )
target_link_libraries(BwGraph_v2 TBB::tbb parallel_hashmap_lib)