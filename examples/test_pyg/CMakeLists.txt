# USAGE:
# rm -rf build && mkdir build && cd build
# cmake ..
# cmake --build . --config Release
# ./test_pyg ../model.pt

cmake_minimum_required(VERSION 3.10)
project(test_pyg)

set(CMAKE_CXX_STANDARD 17)

# The first thing do is to tell cmake to find the TorchScatter
# and TorchSparse libraries. The package pulls in all the necessary
# torch libraries, so there is no need to add `find_package(Torch)`.
find_package(TorchScatter REQUIRED)
find_package(TorchSparse REQUIRED)
find_package(Python3 COMPONENTS Development)

add_executable(${PROJECT_NAME} main.cpp)

# We now need to link the TorchScatter and TorchSparse libraries
# to our executable. We can do that by using the
# TorchScatter::TorchScatter and TorchSparse::TorchSparse targets,
# which also adds all the necessary torch dependencies.
target_compile_features(${PROJECT_NAME} PUBLIC cxx_range_for)
target_link_libraries(${PROJECT_NAME} TorchScatter::TorchScatter)
target_link_libraries(${PROJECT_NAME} TorchSparse::TorchSparse)
# target_link_libraries(${PROJECT_NAME} ${CUDA_cusparse_LIBRARY})
# set_property(TARGET ${PROJECT_NAME} PROPERTY CXX_STANDARD 17)
