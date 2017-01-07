#include "propertyX.hpp"

#include <cstdlib>
#include <cctype>

using namespace std;

Index nr_rows, top_row;
uint rows;
uint max_cols_ = 1;
ev::default_loop loop;

LogSink log_sink;
LogStream  log_out  {log_sink};
TimeStream timed_out{log_sink};

int main(int argc, char** argv) {
    try {
        bool is_server = false;
        if (argc > 1) {
            char* ptr;
            long i = strtol(argv[1], &ptr, 0);
            while (isspace(*ptr)) ++ptr;
            if (*ptr == 0) {
                if (i < 2) throw(range_error("Rows must be >= 2"));
                if (i > MAX_ROWS)
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
