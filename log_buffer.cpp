#include "log_buffer.hpp"

#include <cstring>
#include <ctime>

#include <iostream>
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

TimedBuffer::TimedBuffer(LogSink& log_sink, std::string const& format) :
    LogBuffer(log_sink)
{
    format_.resize(format.size()+2);
    format_[0] = ' ';
    memcpy(&format_[1], &format[0], format.size());
    format_[format.size()+1] = 0;
    out_.resize(format.size() + 1);
}

std::string TimedBuffer::time_string() {
    time_t t = time(nullptr);
    if (t == static_cast<time_t>(-1))
        throw(system_error(errno, system_category(), "Could not get time"));
    struct tm tm;
    if (localtime_r(&t, &tm) == nullptr)
        throw(runtime_error("Could not get localtime"));
    while (true) {
        size_t size = strftime(&out_[0], out_.size(), &format_[0], &tm);
        if (size) return string{&out_[1], size-1};
        out_.resize(2*out_.size());
    }
}

int TimedBuffer::sync() {
    prefix(time_string());
    return _sync();
}
