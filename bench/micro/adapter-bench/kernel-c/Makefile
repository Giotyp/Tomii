CC ?= gcc
CFLAGS ?= -O3 -march=native -fPIC -Iinclude
LDFLAGS ?= -shared

TARGET := libkernel_c.so

all: $(TARGET)

$(TARGET): src/kernel_c.c include/kernel_c.h
	$(CC) $(CFLAGS) $(LDFLAGS) -o $(TARGET) src/kernel_c.c

clean:
	rm -f $(TARGET)

.PHONY: all clean
