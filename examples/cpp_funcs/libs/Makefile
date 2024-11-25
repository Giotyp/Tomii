CXX = g++ -std=c++17
CXXFLAGS = -O3 -march=native

TARGET = libadder.so

SRC = adder.cpp

all: $(TARGET)

$(TARGET): $(SRC)
	$(CXX) $(CXXFLAGS) -shared -o $(TARGET) $(SRC)

all: $(TARGET)

clean:
	rm -f $(TARGET)