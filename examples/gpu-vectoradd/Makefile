NVCC      = /usr/local/cuda-12.9/bin/nvcc
ARCH      = -arch=sm_89
NVCCFLAGS = -O2 -Xcompiler -fPIC $(ARCH) --default-stream per-thread
LDLIBS    = -lcudart

TARGET = libgpu_vadd.so
SRC    = src/gpu_vadd.cu

.PHONY: all clean

all: $(TARGET)

$(TARGET): $(SRC) include/gpu_vadd.h
	$(NVCC) $(NVCCFLAGS) -shared -Iinclude -o $@ $(SRC) $(LDLIBS)

clean:
	rm -f $(TARGET)
