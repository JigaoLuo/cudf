# Copyright (c) 2024-2025, NVIDIA CORPORATION.
# NVCC_PREPEND_FLAGS="-ccbin=/home/jluo/miniconda3/bin/x86_64-conda-linux-gn
# u-c++"
# export CPATH=$CPATH:/home/jluo/cudf-dev/cpp/src
cmake_minimum_required(VERSION 3.30.4 FATAL_ERROR)

include(../set_cuda_architecture.cmake)

# initialize cuda architecture
rapids_cuda_init_architectures(parquet_io)

project(
  parquet_io
  VERSION 0.0.1
  LANGUAGES CXX CUDA
)

include(../fetch_dependencies.cmake)
include(rapids-cmake)
rapids_cmake_build_type("Release")

add_library(parquet_io_utils OBJECT common_utils.cpp io_source.cpp)
# target_compile_features(parquet_io_utils PRIVATE cxx_std_17)
target_link_libraries(parquet_io_utils PRIVATE cudf::cudf)

# Build and install read_parquet
add_executable(read_parquet read_parquet.cpp)
target_link_libraries(read_parquet PRIVATE cudf::cudf nvToolsExt $<TARGET_OBJECTS:parquet_io_utils>)
# target_compile_features(read_parquet PRIVATE cxx_std_17)
install(TARGETS read_parquet DESTINATION bin/examples/libcudf)

# Build and install parquet_io
add_executable(parquet_io parquet_io.cpp)
target_link_libraries(parquet_io PRIVATE cudf::cudf nvToolsExt $<TARGET_OBJECTS:parquet_io_utils>)
# target_compile_features(parquet_io PRIVATE cxx_std_17)
install(TARGETS parquet_io DESTINATION bin/examples/libcudf)

# Build and install parquet_io_multithreaded
add_executable(parquet_io_multithreaded parquet_io_multithreaded.cpp)
target_link_libraries(
  parquet_io_multithreaded PRIVATE cudf::cudf nvToolsExt $<TARGET_OBJECTS:parquet_io_utils>
)
# target_compile_features(parquet_io_multithreaded PRIVATE cxx_std_17)
install(TARGETS parquet_io_multithreaded DESTINATION bin/examples/libcudf)

# Install the example.parquet file
install(FILES ${CMAKE_CURRENT_LIST_DIR}/example.parquet DESTINATION bin/examples/libcudf)
