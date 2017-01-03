#include "propertyX.hpp"

#include <cstdlib>
#include <cctype>

#include <mutex>

using namespace std;

Index nr_rows, top_row;
uint rows;
uint max_cols_ = 1;
ev::default_loop loop;

string time_string() {
    time_t t = time(NULL);
    if (t == static_cast<time_t>(-1))
        throw(system_error(errno, system_category(), "Could not get time"));
    struct tm tm;
    if (localtime_r(&t, &tm) == nullptr)
        throw(runtime_error("Could not get localtime"));
    char buffer[100];
    size_t size = strftime(buffer, sizeof(buffer), "%F %T: ", &tm);
    return string{buffer, size};
}

mutex mutex_out;
stringstream TimeBuffer::full_out;
TimeStream timed_out;

int TimeBuffer::sync() {
  if (!str().empty()) {
      {
          std::lock_guard<std::mutex> lock{mutex_out};
          full_out << time_string() << str();
      }
      str("");
  }
  return 0;
}
void TimeBuffer::flush() {
    if (full_out.str().empty()) return;
    lock_guard<mutex> lock{mutex_out};
    cout << full_out.str();
    cout.flush();
    full_out.str("");
}

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
