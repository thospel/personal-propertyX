#include "log_buffer.hpp"

using namespace std;

mutex mutex_out;
string TimeBuffer::full_out;

TimeStream timed_out;

size_t const BLOCK = 80;

TimeBuffer::TimeBuffer(std::string const& format): format_{format + '\0'} {
    buffer_.resize(BLOCK);
    setp(&buffer_[0], &buffer_[BLOCK]);
}

void TimeBuffer::flush() {
    if (full_out.empty()) return;
    std::lock_guard<std::mutex> lock{mutex_out};
    std::cout << full_out;
    std::cout.flush();
    full_out.clear();
}

int TimeBuffer::overflow(int ch) {
    if (ch == EOF) return ch;
    int offset = pptr() - pbase();
    auto size = buffer_.size() * 3 / 2;
    buffer_.resize(size);
    setp(&buffer_[0], &buffer_[size]);
    pbump(offset+1);
    buffer_[offset] = ch;
    return ch;
}

int TimeBuffer::sync() {
    int size = pbase() - pptr();
    if (size) {
        time_t t = time(NULL);
        if (t == static_cast<time_t>(-1))
            throw(system_error(errno, system_category(), "Could not get time"));
        struct tm tm;
        if (localtime_r(&t, &tm) == nullptr)
            throw(runtime_error("Could not get localtime"));
        char buffer[100];
        size_t bsize = strftime(buffer, sizeof(buffer), format_.data(), &tm);
        {
            std::lock_guard<std::mutex> lock{mutex_out};
            full_out.append(buffer, bsize);
            full_out.append(pbase(), pptr() - pbase());
        }
        pbump(size);
    }
    return 0;
}
