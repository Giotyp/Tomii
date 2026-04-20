CC      = gcc
CFLAGS  = -O2 -fPIC -Wall -Wextra -Iinclude $(shell pkg-config --cflags fftw3f openblas)
LDLIBS  = $(shell pkg-config --libs fftw3f openblas) -lm

TARGET     = libmatcomp_c.so
VALID_BIN  = validation
SRC        = src/matcomp.c

.PHONY: all validation clean

all: $(TARGET)

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) -shared -o $@ $^ $(LDLIBS)

validation: $(SRC) src/validation_main.c
	$(CC) $(CFLAGS) -o $(VALID_BIN) $^ $(LDLIBS)

clean:
	rm -f $(TARGET) $(VALID_BIN)
