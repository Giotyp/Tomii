# Builds radar_rx_cpp — the all-C++ GNU Radio 3.10 baseline (radar_rx.cc).
# Requires the conda-forge GNU Radio env (see radar_rx.py docstring) and the
# CPU kernel library: make -C ../../../examples/radar-pipeline/kernels
#
# E5 (2026-09): the tuned baseline is built with OPT="-O3 -march=native". The
# GNU Radio 3.10 runtime/FFT libraries themselves are the prebuilt conda-forge
# binaries (not rebuilt); only the application blocks get the flags.
GR_ENV      ?= $(HOME)/miniconda3/envs/gnuradio
KERNELS_DIR := $(abspath ../../../examples/radar-pipeline/kernels)

CXX      ?= g++
OPT      ?= -O2
CXXFLAGS ?= $(OPT) -std=c++17 -Wall
OUT      ?= radar_rx_cpp
INCS     := -I$(GR_ENV)/include
LIBS     := -L$(GR_ENV)/lib -Wl,-rpath,$(GR_ENV)/lib \
            -lgnuradio-runtime -lgnuradio-blocks -lgnuradio-fft -lgnuradio-pmt -lfmt \
            -L$(KERNELS_DIR) -lradar_kernels -Wl,-rpath,$(KERNELS_DIR)

$(OUT): radar_rx.cc
	$(CXX) $(CXXFLAGS) $< -o $@ $(INCS) $(LIBS)

clean:
	rm -f radar_rx_cpp radar_rx_cpp.native

.PHONY: clean
