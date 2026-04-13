CC      = gcc
CFLAGS  = -O2 -fPIC -Wall -Wextra -Iinclude $(shell pkg-config --cflags fftw3f openblas)
LDLIBS  = $(shell pkg-config --libs fftw3f openblas) -lm

TARGET  = libmatcomp_c.so
SRC     = src/matcomp.c

.PHONY: all clean

all: $(TARGET)

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) -shared -o $@ $^ $(LDLIBS)

clean:
	rm -f $(TARGET)
