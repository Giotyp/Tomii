CC      ?= cc
CFLAGS  ?= -O2 -fPIC -Wall -Wextra -std=c11
FFTW_CFLAGS := $(shell pkg-config --cflags fftw3f)
FFTW_LIBS   := $(shell pkg-config --libs fftw3f)

# GPU twin: same soname in gpu/ so RADAR_KERNELS_DIR=$(PWD)/gpu selects it.
NVCC      ?= nvcc
CUDA_ARCH ?= sm_89

all: libradar_kernels.so

libradar_kernels.so: radar_cpu.c radar.h
	$(CC) $(CFLAGS) $(FFTW_CFLAGS) -shared -o $@ radar_cpu.c $(FFTW_LIBS) -lm -lpthread

gpu: gpu/libradar_kernels.so

gpu/libradar_kernels.so: radar_gpu.cu radar.h
	mkdir -p gpu
	$(NVCC) -O2 -arch=$(CUDA_ARCH) -Xcompiler -fPIC -shared -o $@ radar_gpu.cu -lcufft -lcudart

clean:
	rm -f libradar_kernels.so gpu/libradar_kernels.so

.PHONY: all gpu clean
