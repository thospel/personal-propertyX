#ifndef LOG_BUFFER_HPP
# define LOG_BUFFER_HPP
# pragma once

#include <condition_variable>
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

class TimedBuffer: public LogBuffer {
  public:
    TimedBuffer(LogSink& log_sink, std::string const& format = "%F %T: ");
    std::string time_string();
  protected:
    int sync();
  private:
    std::vector<char> format_;
    std::vector<char> out_;
};

#endif // LOG_BUFFER_HPP
