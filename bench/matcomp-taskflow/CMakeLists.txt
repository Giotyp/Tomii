cmake_minimum_required(VERSION 3.16)
project(tf_matcomp C CXX)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_C_STANDARD 11)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# ---------------------------------------------------------------------------
# Taskflow headers (header-only)
# Search order:
#   1. User-supplied -DTASKFLOW_DIR=...
#   2. TASKFLOW_ROOT environment variable
#   3. Vendored copy in ../anti-diag-bench/taskflow/src
# ---------------------------------------------------------------------------
if(NOT DEFINED TASKFLOW_DIR)
    if(DEFINED ENV{TASKFLOW_ROOT} AND EXISTS "$ENV{TASKFLOW_ROOT}/taskflow/taskflow.hpp")
        set(TASKFLOW_DIR "$ENV{TASKFLOW_ROOT}")
    elseif(EXISTS "${CMAKE_SOURCE_DIR}/../anti-diag-bench/taskflow/src/taskflow/taskflow.hpp")
        set(TASKFLOW_DIR "${CMAKE_SOURCE_DIR}/../anti-diag-bench/taskflow/src")
    else()
        message(FATAL_ERROR
            "Cannot find Taskflow headers. "
            "Pass -DTASKFLOW_DIR=/path/to/taskflow or set TASKFLOW_ROOT env var.")
    endif()
endif()

message(STATUS "Taskflow headers: ${TASKFLOW_DIR}")

# ---------------------------------------------------------------------------
# matcomp C kernels (compiled into the binary; same source as matrix-compute-C)
# ---------------------------------------------------------------------------
set(MATCOMP_DIR "${CMAKE_SOURCE_DIR}/../examples/matrix-compute-C")

# ---------------------------------------------------------------------------
# FFTW3 (single precision) and OpenBLAS via pkg-config
# ---------------------------------------------------------------------------
find_package(PkgConfig REQUIRED)
pkg_check_modules(FFTW3F REQUIRED fftw3f)
pkg_check_modules(OPENBLAS REQUIRED openblas)

# ---------------------------------------------------------------------------
# Binary
# ---------------------------------------------------------------------------
add_executable(tf_matcomp
    src/main.cpp
    "${MATCOMP_DIR}/src/matcomp.c"
)

target_include_directories(tf_matcomp PRIVATE
    "${TASKFLOW_DIR}"
    "${MATCOMP_DIR}/include"
    "${FFTW3F_INCLUDE_DIRS}"
    "${OPENBLAS_INCLUDE_DIRS}"
    src
)

target_compile_options(tf_matcomp PRIVATE
    -O3
    -march=native
    # Silence GCC noise in matcomp.c
    $<$<COMPILE_LANGUAGE:C>:-Wno-unused-parameter>
)

target_link_directories(tf_matcomp PRIVATE
    ${FFTW3F_LIBRARY_DIRS}
    ${OPENBLAS_LIBRARY_DIRS}
)

target_link_libraries(tf_matcomp PRIVATE
    ${FFTW3F_LIBRARIES}
    ${OPENBLAS_LIBRARIES}
    m
    pthread
)
