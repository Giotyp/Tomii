CC      = gcc
CFLAGS  = -O2 -fPIC -Wall -Wextra -Iinclude -std=c11

TARGET  = libwordcount_c.so
SRC     = src/wordcount.c

.PHONY: all clean

all: $(TARGET)

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) -shared -o $@ $^

clean:
	rm -f $(TARGET)
