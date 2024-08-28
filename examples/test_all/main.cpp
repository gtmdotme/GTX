#include <torch/script.h>
#include <torchscatter/scatter.h>
#include <torchsparse/sparse.h>
#include <iostream>
#include "GTX.hpp"

// using GTX = gt::Graph;
int main(int argc, const char* argv[]) {
    // GTX g;
    gt::Graph g;

    //create a read-write transaction
    auto txn = g.begin_read_write_transaction();

    std::string data = "abc";

    //create a new vertex, return its internal vertex id
    auto vid1 = txn.new_vertex();
    std::cout<<"vid1: "<<vid1<<std::endl;
    auto vid2 = txn.new_vertex();
    std::cout<<"vid2: "<<vid2<<std::endl;
    auto vid3 = txn.new_vertex();
    std::cout<<"vid3: "<<vid3<<std::endl;
    auto vid4 = txn.new_vertex();
    std::cout<<"vid4: "<<vid4<<std::endl;
    auto vid5 = txn.new_vertex();
    std::cout<<"vid5: "<<vid5<<std::endl;
    auto vid6 = txn.new_vertex();
    std::cout<<"vid6: "<<vid6<<std::endl;

    //create a new version of the vertex the txn just created
    txn.put_vertex(vid1, data);
    txn.put_vertex(vid2, data);
    txn.put_vertex(vid3, "");
    txn.put_vertex(vid4, "");
    txn.put_vertex(vid5, "");
    txn.put_vertex(vid6, "");

    std::cout<<"get_vertex: vid1: "<<txn.get_vertex(vid1)<<std::endl;
    std::cout<<"get_vertex: vid2: "<<txn.get_vertex(vid2)<<std::endl;
    std::cout<<"get_vertex: vid3: "<<txn.get_vertex(vid3)<<std::endl;


    //create/update a new version of the edge of label 1 and data.
    // txn.checked_put_edge(vid2, 1, vid1, data);
    // edgelist
    // 1 2
    // 1 3
    // 2 3 
    // 2 6
    // 3 4
    // 3 5
    // 5 6
    txn.checked_put_edge(vid1, 1, vid2, data);
    // reverse edge
    txn.checked_put_edge(vid2, 1, vid1, data);
    
    txn.checked_put_edge(vid1, 1, vid3, data);
    // reverse edge
    txn.checked_put_edge(vid3, 1, vid1, data);

    txn.checked_put_edge(vid2, 1, vid3, data);
    // reverse edge
    txn.checked_put_edge(vid3, 1, vid2, data);

    txn.checked_put_edge(vid2, 1, vid6, data);
    // reverse edge
    txn.checked_put_edge(vid6, 1, vid2, data);

    txn.checked_put_edge(vid3, 1, vid4, data);
    // reverse edge
    txn.checked_put_edge(vid4, 1, vid3, data);

    txn.checked_put_edge(vid3, 1, vid5, data);
    // reverse edge
    txn.checked_put_edge(vid5, 1, vid3, data);

    txn.checked_put_edge(vid5, 1, vid6, data);
    // reverse edge
    txn.checked_put_edge(vid6, 1, vid5, data);

    std::cout<<"get_edge: (1, 2): "<<txn.get_edge(vid1, vid2, 1)<<std::endl;
    std::cout<<"get_edge: (2, 1): "<<txn.get_edge(vid2, vid1, 2)<<std::endl;

    txn.commit();

    //create a read-only transaction
    auto r_txn = g.begin_read_only_transaction();

    // //single edge read
    // auto result = r_txn.get_edge(vid1,vid2,1);
    // if(result.at(0)!='a'||result.at(1)!='b'||result.at(2)!='c'){
    //     std::cout<<"error"<<std::endl;
    // }else{
    //     std::cout<<"no error"<<std::endl;
    // }

    //create an iterator to scan the adjacency list of a vertex
    // auto iterator = r_txn.simple_get_edges(vid1, 1);
    // auto iterator = r_txn.get_edges(vid1, 1);

    // //scan the adjacency list
    // while(iterator.valid()){
    //     auto v = iterator.dst_id();
    //     if(v!=vid2){
    //         std::cout<<"error"<<std::endl;
    //     }
    //     auto result2 = iterator.edge_delta_data();
    //     if(result2.at(0)!='a'||result2.at(1)!='b'||result2.at(2)!='c'){
    //         std::cout<<"error"<<std::endl;
    //     }else{
    //         std::cout<<"no error"<<std::endl;
    //     }
    // }

    // for loop to scan the adjacency list for each vertex
    // create a vector of src vertices
    std::vector<gt::vertex_t> srcVertices;
    // create a vector of dst vertices
    std::vector<gt::vertex_t> dstVertices;
    for (gt::vertex_t src = 1; src <= 6; src++) {
        auto iterator = r_txn.get_edges(src, 1);
        while(iterator.valid()){
            auto dst = iterator.dst_id();
            auto e_data = iterator.edge_delta_data();
            srcVertices.push_back(src);
            dstVertices.push_back(dst);
            std::cout<<"("<<src<<", "<<dst<<", "<<e_data<<")"<<std::endl;

            // iterator.next();
        }
    }

    for (int i = 0; i < srcVertices.size(); i++) {
        std::cout<<"("<<srcVertices[i]<<", "<<dstVertices[i]<<")"<<std::endl;
    }
    r_txn.commit();


    // //do analytics:
    // gt::vertex_t root =8;//start algorithm from vertex with id =8
    // int32_t num_iterations = 10;
    // double damping_factor = 0.5;
    // int num_vertices = 10;
    // int max_vertex_id = 11;
    // // print the number of vertices in the graph
    // std::cout << "Number of vertices: " << g.get_num_vertices() << std::endl;

    // auto bfs_handler =  g.get_bfs_handler(num_vertices);
    // bfs_handler.compute(root);
    // auto pr_handler = g.get_pagerank_handler(num_vertices);
    // pr_handler.compute(num_iterations,damping_factor);
    // auto handler = g.get_sssp_handler(max_vertex_id);
    // double delta = 2.0;
    // handler.compute(root,delta);

    if (argc != 2) {
        std::cerr << "usage: hello-world <path-to-exported-script-module>\n";
        return -1;
    }

    torch::jit::script::Module model;
    try {
        model = torch::jit::load(argv[1]);
    } catch (const c10::Error &e) {
        std::cerr << "error loading the model\n";
        return -1;
    }

    auto x = torch::randn({5, 32});
    auto edge_index = torch::tensor({
        {0, 1, 1, 2, 2, 3, 3, 4},
        {1, 0, 2, 1, 3, 2, 4, 3},
    });

    std::vector<torch::jit::IValue> inputs;
    inputs.push_back(x);
    inputs.push_back(edge_index);

    auto out = model.forward(inputs).toTensor();
    std::cout << "output tensor shape: " << out.sizes() << std::endl;
    
    return 0;
}