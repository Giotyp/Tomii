CXX      = g++
CXXFLAGS = -O3 -std=c++17 -march=native -I../../taskflow-bench/taskflow-lib
LDFLAGS  = -lpthread
TARGET   = bilateral_wavefront

.PHONY: all clean

all: $(TARGET)

$(TARGET): bilateral_wavefront.cpp
	$(CXX) $(CXXFLAGS) $< -o $@ $(LDFLAGS)

clean:
	rm -f $(TARGET)
