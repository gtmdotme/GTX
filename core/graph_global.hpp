//
// Created by zhou822 on 5/23/23.
//

#ifndef BWGRAPH_V2_GRAPH_GLOBAL_HPP
#define BWGRAPH_V2_GRAPH_GLOBAL_HPP
//system-wise worker thread count
#ifndef WORKER_THREAD_NUM
#define WORKER_THREAD_NUM 64
#endif

//transaction states
#define ABORT 0x7FFFFFFFFFFFFFFF
#define IN_PROGRESS 0
#define MAX_TS 0x6FFFFFFFFFFFFFFF
//transaction table states
#define USING_ARRAY_TABLE true
//for array txn table only
#define NO_TXN_ENTRY 0
#define Per_Thread_Table_Size 32
//block allocation
#define DEFAULT_EDGE_DELTA_BLOCK_ORDER 8
#define BUCKET_SIZE 536870912
#define BUCKET_NUM 8
#define BAD_BLOCK_ID 0
#define USING_WAL false
#endif //BWGRAPH_V2_GRAPH_GLOBAL_HPP
