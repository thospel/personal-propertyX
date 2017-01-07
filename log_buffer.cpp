#include "log_buffer.hpp"

#include <cstring>
#include <ctime>

#include <stdexcept>

using namespace std;

size_t LogBuffer::BLOCK = 1;
size_t const MIN_BLOCK = 1;

LogSink::LogSink() {
    thread_ = thread(&LogSink::loop, this);
}

LogSink::~LogSink() {
    finish();
}

void LogSink::put(std::string const& prefix, char const* ptr, size_t size) {
    unique_lock<std::mutex> lock_sink(mutex_);
    if (string_[current_].empty()) cv_.notify_one();
    string_[current_].append(prefix);
    string_[current_].append(ptr, size);
}

void LogSink::loop() {
    while (true) {
        auto& str = string_[current_];
        {
            unique_lock<std::mutex> lock_sink(mutex_);
            cv_.wait(lock_sink, [this, &str]{ return !str.empty() || finished_; });
            if (str.empty()) return;
            current_ = 1 - current_;
        }
        output(str);
        str.clear();
    } 
}

void LogSink::output(std::string& str) {
    cout << str;
    cout.flush();
}

void LogSink::finish() {
    {
        unique_lock<std::mutex> lock_sink(mutex_);
        if (finished_) return;
        finished_ = true;
        cv_.notify_one();
    }
    thread_.join();
}

LogBuffer::LogBuffer(LogSink& log_sink, size_t block):
    log_sink_{log_sink} {
    buffer_.resize(min(block, MIN_BLOCK));
    setp(&*buffer_.begin(), &*buffer_.end());
}

LogBuffer::~LogBuffer() {
    sync();
}

LogBuffer::int_type LogBuffer::overflow(int_type ch) {
    if (ch == traits_type::eof()) return ch;
    size_t used = pptr() - pbase();
    buffer_.resize(2*buffer_.size());
    setp(&*buffer_.begin(), &*buffer_.end());
    buffer_[used] = ch;
    pbump(used+1);
    return ch;
}

int LogBuffer::sync() {
    return _sync();
}

TimeBuffer::TimeBuffer(LogSink& log_sink, std::string const& format) :
    LogBuffer(log_sink)
{
    format_ = format + '\0';
    out_.resize(format.size());
}

std::string TimeBuffer::time_string() {
    time_t t = time(nullptr);
    if (t == static_cast<time_t>(-1))
        throw(system_error(errno, system_category(), "Could not get time"));
    struct tm tm;
    if (localtime_r(&t, &tm) == nullptr)
        throw(runtime_error("Could not get localtime"));
    while (true) {
        size_t size = strftime(&out_[0], out_.size(), format_.data(), &tm);
        // Does not handle %l returning an empty string
        if (size || format_[0] == 0) return string{&out_[0], size};
        out_.resize(2*out_.size());
    }
}

int TimeBuffer::sync() {
    prefix(time_string());
    return _sync();
}
