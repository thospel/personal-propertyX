#ifndef LOG_BUFFER_HPP
# define LOG_BUFFER_HPP
# pragma once

#include <condition_variable>
#include <iostream>
#include <mutex>
#include <streambuf>
#include <string>
#include <thread>
#include <vector>

class LogSink {
  public:
    LogSink();
    ~LogSink();
    void put(std::string const& prefix, char const* ptr, size_t size);
    void loop();
    void finish();
  protected:
    virtual void output(std::string& str);
  private:
    std::thread thread_;
    std::mutex mutex_;
    std::condition_variable cv_;
    std::string string_[2];
    int current_ = 0;
    bool finished_ = false;
};

class LogBuffer: public std::streambuf {
  public:
    static size_t BLOCK;
    LogBuffer(LogSink& log_sink, size_t block = BLOCK);
    ~LogBuffer();
    LogSink& log_sink() { return log_sink_; }
    void prefix(auto str) { prefix_ = str; }
    std::string const& prefix() const { return prefix_; }
    std::string      & prefix()       { return prefix_; }
  protected:
    int_type overflow(int_type ch);
    int sync();
    int _sync() {
        size_t used = pptr() - pbase();
        if (used) {
            log_sink_.put(prefix_, &buffer_[0], used);
            pbump(-static_cast<int>(used));
        }
        return 0;
    }
  private:
    std::vector<char> buffer_;
    std::string prefix_;
    LogSink& log_sink_;
};

class TimeBuffer: public LogBuffer {
  public:
    TimeBuffer(LogSink& log_sink, std::string const& format = "%F %T: ");
    std::string time_string();
    std::string const& format() const { return format_; }
  protected:
    int sync();
  private:
    std::string format_;
    std::vector<char> out_;
};

class LogStream: public std::ostream {
  public:
    LogStream(LogSink& log_sink, size_t block = LogBuffer::BLOCK) :
        std::ostream{&buffer_},
        buffer_{log_sink, block}
        {}
    LogSink& log_sink() { return buffer_.log_sink(); }
  private:
    LogBuffer buffer_;
};

class TimeStream: public std::ostream {
  public:
    TimeStream(LogSink& log_sink, std::string const& format = "%F %T: ") :
        std::ostream{&buffer_},
        buffer_{log_sink, format}
        {}
    std::string const& format() const { return buffer_.format(); }
    LogSink& log_sink() { return buffer_.log_sink(); }
  private:
    TimeBuffer buffer_;
};
#endif // LOG_BUFFER_HPP
