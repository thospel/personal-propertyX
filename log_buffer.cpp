#include "log_buffer.hpp"

using namespace std;

mutex mutex_out;
stringstream TimeBuffer::full_out;

TimeStream timed_out;

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

void TimeBuffer::flush() {
    if (full_out.str().empty()) return;
    std::lock_guard<std::mutex> lock{mutex_out};
    std::cout << full_out.str();
    std::cout.flush();
    full_out.str("");
}

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
