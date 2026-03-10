CXX      = g++
CXXFLAGS = -O3 -std=c++17 -march=native -Itaskflow-lib
LDFLAGS  = -lpthread
BINARIES = wavefront wavefront_block

.PHONY: all clean

all: $(BINARIES)

wavefront: src/wavefront.cpp
	$(CXX) $(CXXFLAGS) $< -o $@ $(LDFLAGS)

wavefront_block: src/wavefront_block.cpp
	$(CXX) $(CXXFLAGS) $< -o $@ $(LDFLAGS)

clean:
	rm -f $(BINARIES)
