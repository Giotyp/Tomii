cmake_minimum_required(VERSION 3.16)
project(mimo_e1 CXX)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Taskflow headers (same lookup as taskflow/CMakeLists.txt)
if(NOT DEFINED TASKFLOW_DIR)
    if(DEFINED ENV{TASKFLOW_ROOT} AND EXISTS "$ENV{TASKFLOW_ROOT}/taskflow/taskflow.hpp")
        set(TASKFLOW_DIR "$ENV{TASKFLOW_ROOT}")
    elseif(EXISTS "${CMAKE_SOURCE_DIR}/../../anti-diag-bench/taskflow/src/taskflow/taskflow.hpp")
        set(TASKFLOW_DIR "${CMAKE_SOURCE_DIR}/../../anti-diag-bench/taskflow/src")
    else()
        message(FATAL_ERROR "Cannot find Taskflow headers (-DTASKFLOW_DIR=...)")
    endif()
endif()

set(MKL_ROOT "/opt/intel/oneapi/mkl/2024.0")
find_path(MKL_INCLUDE_DIR mkl.h HINTS "${MKL_ROOT}/include" REQUIRED)
set(MIMO_LIB_DIR "${CMAKE_SOURCE_DIR}/../tomii/lib")   # identical kernel .so as Tomii

function(mimo_target name src)
    add_executable(${name} ${src})
    target_include_directories(${name} PRIVATE
        "${CMAKE_SOURCE_DIR}" "${CMAKE_SOURCE_DIR}/../taskflow/src" "${MKL_INCLUDE_DIR}")
    # -ffp-contract=off: bit-exact pilot generation vs. the Rust plugin (no FMA fusing);
    # the only float math compiled here is setup code — kernels live in the shared .so.
    target_compile_options(${name} PRIVATE -O3 -march=native -ffp-contract=off)
    target_link_directories(${name} PRIVATE "${MKL_ROOT}/lib" "/lib/x86_64-linux-gnu" "${MIMO_LIB_DIR}")
    target_link_libraries(${name} PRIVATE
        -Wl,--start-group mkl_intel_lp64 mkl_core mkl_sequential -Wl,--end-group
        demod beamfuncs fftfuncs armadillo pthread m dl stdc++)
    target_link_options(${name} PRIVATE "-Wl,-rpath,${MIMO_LIB_DIR}" "-Wl,-rpath,${MKL_ROOT}/lib" "-Wl,-z,notext")
endfunction()

mimo_target(tf_e1 tf_e1.cpp)
target_include_directories(tf_e1 PRIVATE "${TASKFLOW_DIR}")

mimo_target(tbb_e1 tbb_e1.cpp)
target_link_libraries(tbb_e1 PRIVATE tbb)
