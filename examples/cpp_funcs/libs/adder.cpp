#include "adder.h"
#include <iostream>

int adder(int a, int b) {
  return a + b;
}

int main(){
    std::cout << adder(1, 2) << std::endl;
}