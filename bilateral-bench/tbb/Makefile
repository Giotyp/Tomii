CXX      = g++
CXXFLAGS = -O3 -std=c++17 -march=native
LDFLAGS  = -ltbb
TARGET   = bilateral_flow

.PHONY: all clean

all: $(TARGET)

$(TARGET): bilateral_flow.cpp
	$(CXX) $(CXXFLAGS) $< -o $@ $(LDFLAGS)

clean:
	rm -f $(TARGET)
