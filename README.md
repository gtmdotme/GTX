# Bw-Graph

## Description
This is Bw-Graph, a main memory graph system that manages and queries dynamic graphies. Bw-Graph supports concurrent read-write and read-only transactions with snapshot isolation. It supports graph analytics using its OpenMP-tailored read-only transactions and their adjacency list scan protocol. 

## Build
### Prerequisites 
- Only Linux is supported.
- [CMake](https://gitlab.kitware.com/cmake/cmake)
- [TBB](https://github.com/oneapi-src/oneTBB) 
- C++20 although it can run with C++17 

### Build instructions 
```
- mkdir build && cd build
- cmake -DCMAKE_BUILD_TYPE=Release ..
- make -j
```
## Usage
### Include Bw-Graph in your own project
todo: test a bit more about how to link the library
- include llibbwgraph.so in your project (e.g. CMakeLists.txt) and link against it during compilation.

### API
The full Bw-Graph APIs can be found in /bind/bwgraph.hpp.
Here we list the core APIs to manage and query a dynamic graph using Bw-Graph

#### BwGraph
- public BwGraph() 
- ROTransaction begin_read_only_transaction()
- SharedROTransaction begin_shared_read_only_transaction()
- RWTransaction begin_read_write_transaction()
- vertex_t get_max_allocated_vid()
- void set_worker_thread_num(uint64_t new_size)
- void set_writer_thread_num(uint64_t writer_thread_num)
- void whole_label_graph_eager_consolidation(label_t label)
- bwgraph::EdgeDeltaBlockHeader* get_edge_block(vertex_t vid, label_t l)
#### ROTransaction

#### SharedROTransaction

#### RWTransaction

#### SimpleEdgeDeltaIterator

#### StaticEdgeDeltaIterator