# USAGE:
# rm -rf build && mkdir build && cd build
# cmake ..
# make
# ./test_gtx

cmake_minimum_required(VERSION 3.24)
project(test_gtx)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS_DEBUG "-std=c++17 -D_GLIBCXX_USE_CXX11_ABI=0 -g -fno-omit-frame-pointer -Wall -Wextra -Wnon-virtual-dtor -pedantic -Wconversion -Wlogical-op")
set(CMAKE_CXX_FLAGS_RELEASE "-std=c++17 -D_GLIBCXX_USE_CXX11_ABI=0 -g -fno-omit-frame-pointer -Wall -Wextra -Wnon-virtual-dtor -pedantic -Wconversion -Wlogical-op -O3 -DNDEBUG")

link_directories(/home/GTX/build/)

add_executable(${PROJECT_NAME} main.cpp)

target_link_libraries(${PROJECT_NAME} GTX)
