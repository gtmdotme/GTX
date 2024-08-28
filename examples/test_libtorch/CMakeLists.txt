# USAGE:
# rm -rf build && mkdir build && cd build
# cmake ..
# cmake --build . --config Release
# ./test_libtorch

cmake_minimum_required(VERSION 3.18 FATAL_ERROR)
project(test_libtorch)
set(CMAKE_CXX_STANDARD 17)

find_package(Torch REQUIRED)

add_executable(${PROJECT_NAME} example-app.cpp)

message(STATUS "TORCH_LIBRARIES: ${TORCH_LIBRARIES}")
target_link_libraries(${PROJECT_NAME} "${TORCH_LIBRARIES}")
