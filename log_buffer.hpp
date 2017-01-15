#ifndef LOG_BUFFER_HPP
# define LOG_BUFFER_HPP
# pragma once

#include <iostream>
#include <sstream>
#include <mutex>

extern std::mutex mutex_out;
extern std::string time_string();

class TimeBuffer: public std::stringbuf {
  public:
    ~TimeBuffer() {
        sync();
        flush();
    }
  protected:
    int sync();
  public:
    static std::stringstream full_out;
    static void flush();
};

class TimeStream: public std::ostream {
  public:
    TimeStream(): std::ostream{&buffer} {}
  private:
    TimeBuffer buffer;
};

extern TimeStream timed_out;

#endif // LOG_BUFFER_HPP
