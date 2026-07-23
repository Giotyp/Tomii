cmake_minimum_required(VERSION 3.16)
project(tf_mimo CXX)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# 3-tier Taskflow lookup (mirrors pipeline-bench/taskflow/CMakeLists.txt)
if(NOT DEFINED TASKFLOW_DIR)
    if(DEFINED ENV{TASKFLOW_ROOT} AND EXISTS "$ENV{TASKFLOW_ROOT}/taskflow/taskflow.hpp")
        set(TASKFLOW_DIR "$ENV{TASKFLOW_ROOT}")
    elseif(EXISTS "${CMAKE_SOURCE_DIR}/../../anti-diag-bench/taskflow/src/taskflow/taskflow.hpp")
        set(TASKFLOW_DIR "${CMAKE_SOURCE_DIR}/../../anti-diag-bench/taskflow/src")
    else()
        message(FATAL_ERROR
            "Cannot find Taskflow headers. Pass -DTASKFLOW_DIR=/path/to/taskflow or set TASKFLOW_ROOT.")
    endif()
endif()

# Intel MKL
set(MKL_ROOT "/opt/intel/oneapi/mkl/2024.0")
find_path(MKL_INCLUDE_DIR mkl.h HINTS "${MKL_ROOT}/include" REQUIRED)

# Precompiled MIMO libs (vendored alongside the Tomii side)
set(MIMO_LIB_DIR "${CMAKE_SOURCE_DIR}/../tomii/lib")

add_executable(tf_mimo src/main.cpp)

target_include_directories(tf_mimo PRIVATE
    "${TASKFLOW_DIR}"
    "${CMAKE_SOURCE_DIR}/src"
    "${MKL_INCLUDE_DIR}"
)

target_compile_options(tf_mimo PRIVATE -O3 -march=native)

target_link_directories(tf_mimo PRIVATE
    "${MKL_ROOT}/lib"
    "/lib/x86_64-linux-gnu"
    "${MIMO_LIB_DIR}"
)

target_link_libraries(tf_mimo PRIVATE
    -Wl,--start-group
    mkl_intel_lp64
    mkl_core
    mkl_sequential
    -Wl,--end-group
    demod
    beamfuncs
    fftfuncs
    armadillo
    pthread m dl stdc++
)

target_link_options(tf_mimo PRIVATE
    "-Wl,-rpath,${MIMO_LIB_DIR}"
    "-Wl,-z,notext"
)
