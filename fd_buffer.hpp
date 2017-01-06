#ifndef FD_BUFFER_HPP
# define FD_BUFFER_HPP
# pragma once

#include <streambuf>
#include <vector>

#include <fcntl.h>

class FdBuffer: public std::streambuf {
  public:
    static size_t BLOCK;

    FdBuffer(std::string const& name, int fd, bool tmp_file = false) :
        name_{name},
        block_{BLOCK},
        fd_{fd},
        tmp_file_{tmp_file}
        {}
    FdBuffer(bool tmp_file = false): FdBuffer{"", -1, tmp_file} {}
    ~FdBuffer() {
        if (fd_ >= 0) close();
    };
    void fd(int value) {
        fd_ = value;
    }
    int fd() const { return fd_; }
    void close();
    void open(std::string const& pathname,
              int flags = O_CREAT | O_WRONLY,
              mode_t mode = 0666);
    void fsync();
    void rename(std::string const& new_name, bool do_fsync = false, int tmp_file = -1);
  protected:
    int sync();
    int_type overflow(int_type ch) {
        if (ch == traits_type::eof()) return ch;
        if (sync() != 0) return traits_type::eof();
        *pptr() = ch;
        pbump(1);
        return ch;
    }
  private:
    std::vector<char> buffer_;
    std::string name_;
    size_t block_;
    int fd_;
    bool tmp_file_;
};

#endif // FD_BUFFER_HPP
