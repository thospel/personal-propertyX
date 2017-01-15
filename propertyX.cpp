/*
  Compile using something like:
    g++ -Wall -O3 -march=native -fstrict-aliasing -std=c++14 -g propertyX.cpp -pthread -lev -o propertyX

  create a file propertyX.rows.txt containing just ----
  Run as server:
    ./propertyX rows [port]
  Then connect other instantiations of the program as client:
    ./propertyX [host [port [threads]]]
  If a client is interrupted just restart it. No work will be lost (but time is)
  If the server is interrupted rename propertyX.rows.out.txt to propertyX.rows.txt, add a final ---- and restart
  client signals:
    USR1: decrease wanted number of threads. Actual decrease only happens
          when some thread finishes. Then closes connection if wanted == 0
    USR1: increase wanted number of threads. Takes effect immediately (if less)
    SYS:  show top row of bits (without the fixed first one). This gives an
          indication of how far each thread has progressed on its current column
*/
// nas  10 78s
// asus 10 247s

#include "log_buffer.hpp"
#include "propertyX.hpp"

#include <iomanip>

using namespace std;

uint max_cols_ = 1;
uint rows;
Index nr_rows, top_row;

ev::default_loop loop;

int main(int argc, char** argv) {
    timed_out << fixed << setprecision(3);
    try {
        bool is_server = false;
        if (argc > 1) {
            char* ptr;
            long i = strtol(argv[1], &ptr, 0);
            while (isspace(*ptr)) ++ptr;
            if (*ptr == 0) {
                if (i < 2) throw(range_error("Rows must be >= 2"));
                if (static_cast<unsigned long>(i) > MAX_ROWS)
                    throw(range_error("Rows must be <= " + to_string(MAX_ROWS)));
                rows = i;
                is_server = true;
            }
        }
        if (is_server) server(argc, argv);
        else           client(argc, argv);
    } catch(exception& e) {
        timed_out << "Error: " << e.what() << endl;
        exit(EXIT_FAILURE);
    }
    exit(EXIT_SUCCESS);
}
