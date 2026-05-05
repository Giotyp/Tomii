CXX      = g++
CXXFLAGS = -O3 -std=c++17 -march=native -Iinclude
LDFLAGS  = -ltbb
BINARIES = stream_bench pagerank wavefront wavefront_flow

.PHONY: all clean

all: $(BINARIES)

stream_bench: src/stream_bench.cpp include/common.h
	$(CXX) $(CXXFLAGS) $< -o $@ $(LDFLAGS)

pagerank: src/pagerank.cpp include/common.h
	$(CXX) $(CXXFLAGS) $< -o $@ $(LDFLAGS)

wavefront: src/wavefront.cpp include/common.h
	$(CXX) $(CXXFLAGS) $< -o $@ $(LDFLAGS)

wavefront_flow: src/wavefront_flow.cpp include/common.h
	$(CXX) $(CXXFLAGS) $< -o $@ $(LDFLAGS)

clean:
	rm -f $(BINARIES)
