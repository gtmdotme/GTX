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
- 

#### ROTransaction

#### SharedROTransaction

#### RWTransaction

#### SimpleEdgeDeltaIterator

#### StaticEdgeDeltaIterator