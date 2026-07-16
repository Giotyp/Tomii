CC      ?= cc
CFLAGS  ?= -O2 -fPIC -Wall -Wextra -std=c11
FFTW_CFLAGS := $(shell pkg-config --cflags fftw3f)
FFTW_LIBS   := $(shell pkg-config --libs fftw3f)

all: libradar_kernels.so

libradar_kernels.so: radar_cpu.c radar.h
	$(CC) $(CFLAGS) $(FFTW_CFLAGS) -shared -o $@ radar_cpu.c $(FFTW_LIBS) -lm -lpthread

clean:
	rm -f libradar_kernels.so

.PHONY: all clean
