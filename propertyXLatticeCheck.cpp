/*
  Compile using something like:
    g++ -Wall -O3 -march=native -fstrict-aliasing -std=c++14 -g propertyXCheck.cpp -lntl -o propertyXCheck

  Run as:
    ./propertyXCheck
  Give matrix on STDIN
*/

#include <cstdint>

#include <iostream>
#include <sstream>
#include <vector>
#include <string>
#include <stdexcept>

#include <NTL/mat_ZZ.h>
#include <NTL/LLL.h>

using namespace std;
using namespace NTL;

uint cols, rows;

void my_main(int argc, char** argv) {
    vector<uint8_t> input;
    cols = 0;
    string line;
    while (getline(cin, line)) {
        istringstream iss{line};
        int n;
        
        uint c = 0;
        while (iss >> n) {
            if (!(n == 0 || n == 1))
                throw(logic_error("Invalid value " + to_string(n)));
            input.emplace_back(n);
            ++c;
        }
        if (cols == 0) cols = c;
        else if (cols != c) 
            throw(logic_error("Non rectangular matrix"));
    }
    if (cols == 0) throw(logic_error("Empty matrix"));
    rows = input.size() / cols;

    // m will cxontain the transposed input
    mat_ZZ m;
    m.SetDims(cols, rows+cols);
    for (uint r=0; r<cols; ++r) {
        for (uint c=0; c<rows; ++c)
            m[r][c] = rows*4*input[c*cols+r];
        for (uint c=0; c<cols; ++c)
            m[r][c+rows] = (c == r ? -1 : 1);
    }
    cout << m;
    mat_ZZ u;
    auto rank = G_BKZ_FP(m, u);
    cout << "Rank = " << rank << "\n";
    cout << u;
    cout << m;
}

int main(int argc, char** argv) {
    try {
        my_main(argc, argv);
    } catch(exception& e) {
        cout << "Error: " << e.what() << endl;
        exit(EXIT_FAILURE);
    }
    exit(EXIT_SUCCESS);
}
