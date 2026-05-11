cmake_minimum_required(VERSION 3.16)
project(tf_pipeline CXX)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Search order:
#   1. User-supplied -DTASKFLOW_DIR=...
#   2. TASKFLOW_ROOT environment variable
#   3. Vendored copy in the anti-diag-bench sibling directory
if(NOT DEFINED TASKFLOW_DIR)
    if(DEFINED ENV{TASKFLOW_ROOT} AND EXISTS "$ENV{TASKFLOW_ROOT}/taskflow/taskflow.hpp")
        set(TASKFLOW_DIR "$ENV{TASKFLOW_ROOT}")
    elseif(EXISTS "${CMAKE_SOURCE_DIR}/../../anti-diag-bench/taskflow/src/taskflow/taskflow.hpp")
        set(TASKFLOW_DIR "${CMAKE_SOURCE_DIR}/../../anti-diag-bench/taskflow/src")
    else()
        message(FATAL_ERROR
            "Cannot find Taskflow headers. "
            "Pass -DTASKFLOW_DIR=/path/to/taskflow or set TASKFLOW_ROOT env var.")
    endif()
endif()

add_executable(tf_pipeline src/main.cpp)
target_include_directories(tf_pipeline PRIVATE
    "${TASKFLOW_DIR}"
    "${CMAKE_SOURCE_DIR}/src"
)
target_compile_options(tf_pipeline PRIVATE -O3 -march=native)
target_link_libraries(tf_pipeline PRIVATE pthread)
