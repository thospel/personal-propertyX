#include "fd_buffer.hpp"

#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>

size_t FdBuffer::BLOCK = 4096;

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
}

void FdBuffer::fsync() {
    if (fd_ < 0)
        throw(logic_error("Fsync on FdBuffer without filedescriptor"));

    auto rc = ::fsync(fd_);
    if (rc < 0)
        throw(system_error(errno, system_category(), "fsync error on " + name_));
}
