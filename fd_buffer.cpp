#include "fd_buffer.hpp"

#include <cstdio>

#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>

size_t FdBuffer::BLOCK = 4096;
bool const FSYNC = false;

int FdBuffer::sync() {
    if (fd_ < 0)
        throw(logic_error("Flush on FdBuffer without filedescriptor"));

    char const* ptr  = pbase();
    char const* end  = pptr();

    while (ptr < end) {
        auto rc = write(fd_, ptr, end - ptr);
        if (rc > 0) {
            ptr  += rc;
            continue;
        }
        if (rc < 0) {
            if (errno == EINTR) continue;
            throw(system_error(errno, system_category(),
                               "write error to " + name_));
        }
        // rc == 0
        throw(logic_error("Zero length write to " + name_));
    }
    pbump(pbase() - end);
    return 0;
}

void FdBuffer::open(string const& pathname, int flags, mode_t mode) {
    if (fd_ >= 0)
        throw(logic_error("Open on FdBuffer with filedescriptor"));

    fd_ = ::open(pathname.c_str(), flags, mode);
    if (fd_ < 0)
        throw(system_error(errno, system_category(),
                           "Could not open file " + pathname));

    name_ = pathname;
    buffer_.resize(block_);
    setp(&*buffer_.begin(), &*buffer_.end());
}

void FdBuffer::close() {
    if (fd_ < 0)
        throw(logic_error("Close on FdBuffer without filedescriptor"));
    sync();
    ::close(fd_);
    fd_ = -1;
    if (tmp_file_)
        if (unlink(name_.c_str()) != 0)
            throw(system_error(errno, system_category(), "Could not unlink " + name_));
}

void FdBuffer::fsync() {
    if (fd_ < 0)
        throw(logic_error("Fsync on FdBuffer without filedescriptor"));

    if (!FSYNC) return;

    auto rc = ::fsync(fd_);
    if (rc < 0)
        throw(system_error(errno, system_category(), "fsync error on " + name_));
}

void FdBuffer::rename(string const& new_name, bool do_fsync, int tmp_file) {
    if (fd_ < 0)
        throw(logic_error("Rename on FdBuffer without filedescriptor"));

    if (FSYNC && do_fsync) fsync();

    if (::rename(name_.c_str(), new_name.c_str()) != 0)
        throw(system_error(errno, system_category(), "Could not rename " + name_ + " to " + new_name));
    name_ = new_name;

    if (tmp_file >= 0) tmp_file_ = tmp_file > 0;

    if (FSYNC && do_fsync) {
        auto pos = name_.rfind('/');

        string const dir = pos == string::npos ?
            "." :
            name_.substr(0, pos+1);
        int dir_fd = ::open(dir.c_str(), O_RDONLY);
        if (dir_fd < 0)
            throw(system_error(errno, system_category(), "Could not open " + dir));
        auto rc = ::fsync(dir_fd);
        if (rc < 0) {
            auto err = errno;
            ::close(dir_fd);
            throw(system_error(err, system_category(), "fsync error on " + dir));
        }
        ::close(dir_fd);
    }
}
