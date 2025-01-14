CXX = g++ -std=c++17
CXXFLAGS = -O3 -march=native -shared -fPIC

TARGET = libadder.so

SRC = adder.cpp

all: $(TARGET)

$(TARGET): $(SRC)
	$(CXX) $(CXXFLAGS) -o $(TARGET) $(SRC) -Wl,--no-undefined

all: $(TARGET)

clean:
	rm -f $(TARGET)