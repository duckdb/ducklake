cmake_minimum_required(VERSION 3.5)
project(repro CXX)

add_executable(repro repro.cpp)

target_compile_options(repro PRIVATE -O3 -DNDEBUG)
set(DUCKLAKE_SRC_ROOT ${CMAKE_CURRENT_LIST_DIR}/../../)
target_include_directories(repro
                           PRIVATE ${DUCKLAKE_SRC_ROOT}/duckdb/src/include/)
target_link_libraries(
  repro PRIVATE ${DUCKLAKE_SRC_ROOT}/build/release/src/libduckdb.so)
