# USAGE:
# rm -rf build && mkdir build && cd build
# cmake ..
# make
# ./test_gtx

cmake_minimum_required(VERSION 3.24)
project(test_all)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS_DEBUG "-std=c++17 -g -fno-omit-frame-pointer -Wall -Wextra -Wnon-virtual-dtor -pedantic -Wconversion -Wlogical-op")
set(CMAKE_CXX_FLAGS_RELEASE "-std=c++17 -g -fno-omit-frame-pointer -Wall -Wextra -Wnon-virtual-dtor -pedantic -Wconversion -Wlogical-op -O3 -DNDEBUG")

link_directories(/home/GTX/build/)
find_package(Torch REQUIRED)
find_package(TorchScatter REQUIRED)
find_package(TorchSparse REQUIRED)
find_package(Python3 COMPONENTS Development)

add_executable(${PROJECT_NAME} main.cpp)

target_link_libraries(${PROJECT_NAME} GTX)
target_link_libraries(${PROJECT_NAME} "${TORCH_LIBRARIES}")
target_compile_features(${PROJECT_NAME} PUBLIC cxx_range_for)
target_link_libraries(${PROJECT_NAME} TorchScatter::TorchScatter)
target_link_libraries(${PROJECT_NAME} TorchSparse::TorchSparse)
