# Builds radar_rx_cpp — the all-C++ GNU Radio 3.10 baseline (radar_rx.cc).
# Requires the conda-forge GNU Radio env (see radar_rx.py docstring) and the
# CPU kernel library: make -C ../../../examples/radar-pipeline/kernels
GR_ENV      ?= $(HOME)/miniconda3/envs/gnuradio
KERNELS_DIR := $(abspath ../../../examples/radar-pipeline/kernels)

CXX      ?= g++
CXXFLAGS ?= -O2 -std=c++17 -Wall
INCS     := -I$(GR_ENV)/include
LIBS     := -L$(GR_ENV)/lib -Wl,-rpath,$(GR_ENV)/lib \
            -lgnuradio-runtime -lgnuradio-blocks -lgnuradio-fft -lgnuradio-pmt -lfmt \
            -L$(KERNELS_DIR) -lradar_kernels -Wl,-rpath,$(KERNELS_DIR)

radar_rx_cpp: radar_rx.cc
	$(CXX) $(CXXFLAGS) $< -o $@ $(INCS) $(LIBS)

clean:
	rm -f radar_rx_cpp

.PHONY: clean
