#ifndef FD_BUFFER_HPP
# define FD_BUFFER_HPP
# pragma once

#include <streambuf>
#include <vector>

#include <fcntl.h>

using namespace std;

class FdBuffer: public streambuf {
  public:
    static size_t BLOCK;

    FdBuffer(string const& name, int fd, bool tmp_file = false) :
        name_{name},
        block_{BLOCK},
        fd_{fd},
        tmp_file_{tmp_file}
        {}
    FdBuffer(bool tmp_file = false): FdBuffer{"", -1, tmp_file} {}
    ~FdBuffer() {
        if (fd_ >= 0) close();
    };
    int_type overflow(int_type ch) {
        if (ch == traits_type::eof()) return ch;
        if (sync() != 0) return traits_type::eof();
        *pptr() = ch;
        pbump(1);
        return ch;
    }
    void fd(int value) {
        fd_ = value;
    }
    void close();
    void open(string const& pathname,
              int flags = O_CREAT | O_WRONLY,
              mode_t mode = 0666);
    int sync();
    void fsync();
    void rename(string const& new_name, bool do_fsync = false, int tmp_file = -1);
  private:
    vector<char> buffer_;
    string name_;
    size_t block_;
    int fd_;
    bool tmp_file_;
};

#endif // FD_BUFFER_HPP
