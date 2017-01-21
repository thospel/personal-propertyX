#ifndef LOG_BUFFER_HPP
# define LOG_BUFFER_HPP
# pragma once

#include <iostream>
#include <string>
#include <mutex>
#include <vector>

extern std::mutex mutex_out;

class TimeBuffer: public std::streambuf {
  public:
    TimeBuffer(std::string const& format = "%F %T: ");
    ~TimeBuffer() {
        sync();
        flush();
    }
  protected:
    int sync();
    int overflow(int ch);
  public:
    static std::string full_out;
    static void flush();
  private:
    std::string const format_;
    std::vector<char> buffer_;
};

class TimeStream: public std::ostream {
  public:
    TimeStream(std::string const& format = "%F %T: "): std::ostream{&buffer_}, buffer_{format} {}
  private:
    TimeBuffer buffer_;
};

extern TimeStream timed_out;

#endif // LOG_BUFFER_HPP
