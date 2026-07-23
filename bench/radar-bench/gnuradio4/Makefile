# Builds radar_rx4 — the GNU Radio 4.0 baseline (radar_rx4.cc, C++23).
#
# Prerequisites (gcc >= 14 required; conda-forge supplies it user-space):
#   conda create -n gr4build -c conda-forge "gxx_linux-64=14" cmake ninja git
#   git clone https://github.com/fair-acc/gnuradio4 $(GR4_SRC)
#   git -C $(GR4_SRC) checkout $(GR4_COMMIT)
#   cmake -S $(GR4_SRC) -B $(GR4_SRC)/build -G Ninja -DCMAKE_BUILD_TYPE=Release \
#         -DCMAKE_CXX_COMPILER=x86_64-conda-linux-gnu-g++ \
#         -DCMAKE_C_COMPILER=x86_64-conda-linux-gnu-gcc -DWARNINGS_AS_ERRORS=OFF
#   ninja -C $(GR4_SRC)/build gnuradio-core gnuradio-blocklib-core
# Plus the CPU kernels: make -C ../../../examples/radar-pipeline/kernels
# Validated GR4 revision (2026-07-09); newer HEADs may change APIs/toolchain needs.
GR4_COMMIT  ?= 92278b628c05f01a440cf775fb6cd465c63bab4d
GR4_SRC     ?= $(HOME)/src/gnuradio4
GR4_BUILD   ?= $(GR4_SRC)/build
CONDA_ENV   ?= $(HOME)/miniconda3/envs/gr4build
KERNELS_DIR := $(abspath ../../../examples/radar-pipeline/kernels)
SYSLIB      := /usr/lib/x86_64-linux-gnu

CXX      = $(CONDA_ENV)/bin/x86_64-conda-linux-gnu-g++
CXXFLAGS ?= -O2 -std=gnu++23
INCS     := -I$(GR4_SRC)/core/include -I$(GR4_BUILD)/core/src -I$(GR4_BUILD)/core/include \
            -I$(GR4_SRC)/meta/include -I$(GR4_SRC)/algorithm/include \
            -I$(GR4_SRC)/third_party/magic_enum \
            -isystem $(GR4_BUILD)/_deps/vir-simd-src -idirafter /usr/include
LIBS     := $(GR4_BUILD)/core/libgnuradio-core.a \
            -L$(GR4_BUILD)/core -lgnuradio-blocklib-core -Wl,-rpath,$(GR4_BUILD)/core \
            -L$(GR4_BUILD)/_deps/cpr-build/cpr -lcpr -Wl,-rpath,$(GR4_BUILD)/_deps/cpr-build/cpr \
            -L$(SYSLIB) -l:libcurl.so.4 -Wl,-rpath-link,$(SYSLIB) -Wl,-rpath-link,/lib/x86_64-linux-gnu \
            -lssl -lcrypto -lpthread -lfftw3f \
            -L$(KERNELS_DIR) -lradar_kernels -Wl,-rpath,$(KERNELS_DIR)

radar_rx4: radar_rx4.cc
	$(CXX) $(CXXFLAGS) $< -o $@ $(INCS) $(LIBS)

clean:
	rm -f radar_rx4

.PHONY: clean
