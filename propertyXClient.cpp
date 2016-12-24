/*
  Compile using something like:
    g++ -Wall -O3 -march=native -fstrict-aliasing -std=c++14 -g propertyXClient.cpp -lpthread -lev -o propertyXClient

  Run as:
    ./propertyXClient [host [port [threads]]]  
*/
// asus 10 481s 471
// nas  10 103s

#include <iostream>
#include <cstring>
#include <cstdint>
#include <climits>
#include <ctgmath>
#include <cerrno>
#include <stdexcept>
#include <limits>
#include <vector>
#include <array>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <atomic>
#include <algorithm>

#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>
#include <fcntl.h>
#include <netdb.h>
#include <signal.h>

#include <ev++.h>

using namespace std;

#define RESTRICT __restrict__

char const*  HOST = "localhost";
uint const   PORT = 21453;

bool const DUMMY = false;
bool const SKIP_FRONT = true;
bool const SKIP_BACK  = true;

bool const DEBUG_SET = false;

uint8_t PROTO_VERSION = 1;
uint8_t PROGRAM_VERSION = 3;

enum {
    PROTO = 1,
    ID,
    PROGRAM,
    SIZE,
    INFO,
    FORK,
    WORK,
    RESULT,
    SOLUTION,
    IDLE,
    FINISHED,
};
enum {
    GET_PROTO = 1*256,
    GET_SIZE  = 2*256,
};

uint const ELEMENTS = 2;

using uint    = unsigned int;
using Index   = uint32_t;
using Element = uint64_t;
using Column  = array<Element, ELEMENTS>;
using Set     = vector<Column>;
using Sum     = uint8_t;
using sec = chrono::seconds;

uint const MAX_ROWS = 54;
uint const COL_FACTOR = (MAX_ROWS+1) | 1;		// 55
uint const ROW_ZERO   = COL_FACTOR/2;			// 27
uint const ROWS_PER_ELEMENT = CHAR_BIT*sizeof(Element) / log2(COL_FACTOR); // 11
uint const MAX_COLS = ROWS_PER_ELEMENT * ELEMENTS;	// 22
Element constexpr ELEMENT_FILL(Element v = ROW_ZERO, int n = ROWS_PER_ELEMENT) {
    return n ? ELEMENT_FILL(v, n-1) * COL_FACTOR + v : 0;
}
Element constexpr POWN(Element v, int n) {
    return n ? POWN(v, n-1)*v : 1;
}
Element const ELEMENT_TOP = POWN(COL_FACTOR, ROWS_PER_ELEMENT -1);
Element const ELEMENT_MAX = numeric_limits<Element>::max();
// log2(2 * ELEMENTS * sizeof(Element) * pow(3., cols) / 10)

auto nr_threads = thread::hardware_concurrency();

size_t const BLOCK = 65536;
ev_tstamp const TIMEOUT_GREETING = 10;

chrono::steady_clock::time_point time_start;
thread forked;
mutex mutex_max;
mutex mutex_out;
Element top_;
Index nr_columns, mask_columns, top_column;
uint top_offset_;
uint cols;
uint max_rows_ = 1;

class Connection;

class State {
  public:
    State(Connection* connection, uint index);
    ~State();
    void stop() { io_in_.stop(); }
    void pipe_write(void const* ptr, size_t size);
    void readable(ev::io& watcher, int revents);
    void got_work(Index col);
    void fork();
    void worker();
    uint extend(uint row);
    // Must be called with mutex_put locked
    void print(uint rows);
  private:
    Column last_;
    vector<Set> sets;
    string pipe_in_;
    Connection* const connection_;
    ev::io io_in_;
    thread thread_;
    condition_variable cv_col;
    mutex mutex_col;
    atomic<Index> input_col_;
    atomic<int>   finished_;
    Index col_, last_col_, last_col_rev_;
    uint max_row_ = 0;
    uint index_, count1_;
    int fd_[2];
    array<Sum, (MAX_ROWS + MAX_COLS - 1 + 7) / 8 * 8> side;
};

struct ColInfo {
    uint8_t min=0, max=-1;
};
vector<ColInfo> col_info;

inline void max_rows(uint rows) {
    if (rows <= max_rows_) return;
    std::lock_guard<mutex> lock(mutex_max);
    if (rows <= max_rows_) return;
    max_rows_ = rows;
}

inline int cmp(auto const& left, auto const& right) {
    for (uint i=0; i<ELEMENTS; ++i) {
        if (left[i] < right[i]) return -1;
        if (left[i] > right[i]) return  1;
    }
    return 0;
}

class Connection {
  public:
    Connection(string const& host, string const& service);
    ~Connection() {
        while (nr_states > 0) {
            auto& state = states[--nr_states];
            state.~State();
        }
        void* raw_memory = states;
        operator delete[](raw_memory);
        // states = nullptr;
        close(fd_);
        // fd_ = -1;
    }
    int fd() const { return fd_; }
    void put(uint type, void const* data, size_t size);
    void put1(uint type, uint8_t value) { put(type, &value, 1); }
    void put4(uint type, Index value) {
        uint8_t message[4];
        for (int i=0; i<4; ++i) {
            message[i] = value & 0xff;
            value >>= 8;
        }
        put(type, message, 4);
    }
    void put(uint type, string const& str) {
        put(type, str.data(), str.size());
    }
    void put(uint type) { put(type, "", 0); }
    void put_known(Index col);
    void start() {
        io_in_ .start(fd_, ev::READ);
        put1(PROTO, PROTO_VERSION);
    }
    void stop() {
        io_in_.stop();
        io_out_.stop();
        timer_greeting_.stop();
        while (nr_states > 0) {
            auto& state = states[--nr_states];
            state.~State();
        }
    }
    void idle(uint index) {
        idle_.emplace_back(index);
    }
  private:
    void readable(ev::io& watcher, int revents);
    void writable(ev::io& watcher, int revents);
    void timeout_greeting(ev::timer& timer, int revents);

    inline static Index get_index(uint8_t const*& ptr) {
        Index index = static_cast<Index>(ptr[0]) |
            static_cast<Index>(ptr[1]) <<  8 |
            static_cast<Index>(ptr[2]) << 16 |
            static_cast<Index>(ptr[3]) << 24;
        ptr += 4;
        return index;
    }

    inline void got_proto   (uint8_t const* ptr, size_t length);
    inline void got_size    (uint8_t const* ptr, size_t length);
    inline void got_info    (uint8_t const* ptr, size_t length);
    inline void got_fork    (uint8_t const* ptr, size_t length);
    inline void got_work    (uint8_t const* ptr, size_t length);
    inline void got_solution(uint8_t const* ptr, size_t length);
    inline void got_idle    (uint8_t const* ptr, size_t length);
    inline void got_finished(uint8_t const* ptr, size_t length);

    vector<uint> idle_;
    // Sigh, State doesn't work in a std::vector since it is not copy
    // constructable even though I never copy. Emulate with placement new.
    State* states = nullptr;
    ev::io io_in_;
    ev::io io_out_;
    ev::timer timer_greeting_;
    string in_;
    string out_;
    string id;
    uint nr_states = 0;
    int fd_;
    int phase = GET_PROTO;
};

ostream& operator<<(ostream& os, Column const& column) {
    for (uint i=0; i<ELEMENTS; ++i) {
        auto v = column[i];
        for (uint j=0; j<ROWS_PER_ELEMENT; ++j) {
            auto bit = v / ELEMENT_TOP;
            cout << " " << bit;
            v -= bit * ELEMENT_TOP;
            v *= COL_FACTOR;
        }
    }
    return os;
}

int64_t elapsed() {
    auto now = chrono::steady_clock::now();
    return chrono::duration_cast<sec>(now-time_start).count();
}

State::State(Connection* connection, uint index):
    connection_{connection},
    finished_{0} 
{
    index_ = index;

    auto rc = pipe(fd_);
    if (rc)
        throw(system_error(errno, system_category(), "Could not create pipe"));

    // Set read side non-blocking
    auto flags = fcntl(fd_[0], F_GETFL, 0);
    if (flags < 0) {
        auto err = errno;
        close(fd_[0]);
        close(fd_[1]);
        throw(system_error(err, system_category(), "Could not fcntl"));
    }
    flags |= O_NONBLOCK;
    flags = fcntl(fd_[0], F_SETFL, flags);
    if (flags < 0) {
        auto err = errno;
        close(fd_[0]);
        close(fd_[1]);
        throw(system_error(err, system_category(), "Could not fcntl"));
    }
    io_in_.set<State, &State::readable>(this);
    io_in_.start(fd_[0], ev::READ);

    input_col_ = nr_columns;

    sets.resize(MAX_ROWS+2);
    for (int i=0; i<2; ++i) {
        sets[i].resize(2);
        for (uint j=0; j < ELEMENTS; ++j) {
            sets[i][0][j] = ELEMENT_FILL(1);
            sets[i][1][j] = ELEMENT_MAX - ELEMENT_FILL(1);
        }
    }
    if (DEBUG_SET) {
        lock_guard<mutex> lock{mutex_out};
        for (auto const& to: sets[0]) {
            cout << to << "\n";
        }
    }
}

State::~State() {
    stop();
    if (thread_.joinable()) {
        finished_  = 1;
        input_col_ = 0;
        cv_col.notify_one();
        thread_.join();
    }
    close(fd_[0]);
    close(fd_[1]);
}

void State::readable(ev::io& watcher, int revents) {
    char buffer[BLOCK];
    auto rc = read(fd_[0], buffer, BLOCK);
    if (rc > 0) {
        pipe_in_.append(buffer, rc);
        while (pipe_in_.size()) {
            uint rows = pipe_in_[0] & 0xff;
            if (rows == 0) {
                connection_->put_known(col_);
                connection_->put4(RESULT, col_);
                connection_->idle(index_);
                pipe_in_.erase(0, 1);
                continue;
            }
            uint bytes = 1+(rows+cols-1+7) / 8;
            if (pipe_in_.size() < bytes) break;
            connection_->put(SOLUTION, pipe_in_.data(), bytes);
            pipe_in_.erase(0, bytes);
        }
        return;
    }
    // Any form of pipe closure should be impossible
    if (rc < 0)
        throw(system_error(errno, system_category(), "pipe read error"));
    // rc == 0
    throw(logic_error("Pipe close"));
}

void State::got_work(Index col) {
    if (col >= nr_columns || col == 0)
        throw(logic_error("Invalid work " + to_string(col)));
    if (false) {
        std::lock_guard<mutex> lock(mutex_out);
        cout << "Driver column " << col << endl;
    }
    std::lock_guard<mutex> lock(mutex_col);
    input_col_ = col;
    cv_col.notify_one();
}

void State::pipe_write(void const* data, size_t size) {
    auto ptr = static_cast<char const*>(data);
    while (size) {
        if (finished_) return;
        auto rc = write(fd_[1], ptr, size);
        if (rc > 0) {
            ptr  += rc;
            size -= rc;
            continue;
        }
        if (rc < 0) {
            if (errno == EINTR) continue;
            throw(system_error(errno, system_category(),
                               "Could not write to pipe"));
        }
        // rc == 0
        throw(logic_error("Zero length pipe write"));
    }
}

void State::fork() {
    if (thread_.joinable()) throw(logic_error("Second FORK"));
    thread_ = thread(&State::worker, this);
}

void State::worker() {
    {
        std::lock_guard<mutex> lock(mutex_out);
        cout << "Worker " << index_ << " ready" << endl;
    }
    std::unique_lock<std::mutex> lock(mutex_col);
    while (!finished_) {
        cv_col.wait(lock, [this]{return input_col_ < nr_columns;});
        col_ = input_col_;
        if (col_ == 0) {
            std::lock_guard<mutex> lock(mutex_out);
            cout << "Worker " << index_ << " exiting" << endl;
            return;
        }
        {
            std::lock_guard<mutex> lock(mutex_out);
            cout << "Worker " << index_ << ": Processing " << col_ << " (" << elapsed() << " s)" << endl;
        }
        {
            std::lock_guard<mutex> lock(mutex_max);
            max_row_ = max_rows_;
        }
        last_.fill(0);
        last_col_ = col_;
        last_col_rev_ = 0;
        count1_ = 0;
        uint bits = col_;
        for (uint i=0; i<cols; ++i) {
            uint bit = bits & 1;
            side[i] = bit;
            bits >>= 1;
            count1_ += bit;
            last_col_rev_ = last_col_rev_ << 1 | bit;
            Element carry = 0;
            for (uint j=0; j<ELEMENTS; ++j) {
                auto c = last_[j] % COL_FACTOR;
                last_[j] = last_[j] / COL_FACTOR + carry * ELEMENT_TOP;
                if (j == top_offset_ && bit) last_[j] += top_;
                carry = c;
            }
        }
        if (false) {
            std::lock_guard<mutex> lock(mutex_out);
            cout << "Worker " << index_ << ": rev=" << last_col_rev_ << ", last=" << last_ << endl;
        }

        if (DUMMY) {
            auto& info = col_info[col_];
            info.min     = 9;
            info.max     = 12;

            if (col_ <= MAX_ROWS) {
                uint8_t buffer[20];
                buffer[0] = col_;
                buffer[1] = 3;
                buffer[2] = 64;
                buffer[3] = 0;
                pipe_write(buffer, 1+(cols+col_-1+7)/8);
            }

            sleep(1);
        } else {
            uint m = extend(0);
            // This is a slight race, but it doesn't hurt
            // (assuming access to max doesn't tear)
            if (m < col_info[col_].max && !finished_) col_info[col_].max = m;
        }
        input_col_ = nr_columns;
        uint8_t data = 0;
        pipe_write(&data, sizeof(data));
    }
}

void indent(uint row) {
    for (uint i=0; i<row; ++i) cout << "  ";
}

uint State::extend(uint row) {
    // cout << "Extend row " << row << " " << static_cast<int>(side[cols+row-1]) << "\n";
    if (row >= MAX_ROWS) throw(range_error("row out of range"));

    if (DEBUG_SET) {
        std::lock_guard<mutex> lock(mutex_out);
        indent(row);
        cout << "last_=" <<   last_ << "\n";
    }

    if (SKIP_BACK) {
        uint m = col_info[last_col_rev_].max;
        if (m <= row) {
            // cout << "Worker " << index_ << ": Skip back " << col_ << " (" << elapsed() << " s)" << endl;
            return row;
        }
    }
    if (SKIP_FRONT) {
        uint m = col_info[last_col_].max + row;
        if (m <= max_rows_) {
            // Column cannot possibly set a new record
            if (true) {
                std::lock_guard<mutex> lock(mutex_out);
                cout << "Worker " << index_ << ": Skip front " << col_ << " (" << elapsed() << " s)" << endl;
            }
            return m;
        }
    }

    if (finished_) return 0;

    // Execute subset sum. The new column is added to set {from} giving {to}
    // {sum} is the other set.
    Column col_high;
    for (uint j=0; j<ELEMENTS; ++j) col_high[j] = last_[j] + ELEMENT_FILL();

    auto const& set_sum  = sets[row + 1];
    // Unclear if this is a win or not (no difference at n=10)
    if (binary_search(set_sum.begin()+1, set_sum.end()-1, col_high,
                              [](Column const&left, Column const& right) { return cmp(left, right) < 0; }))
        return row;

    auto const& set_from = sets[row];
    auto ranges = equal_range(set_from.begin()+1, set_from.end()-1, col_high,
                              [](Column const&left, Column const& right) { return cmp(left, right) < 0; });
    if (ranges.first != ranges.second) return row;

    auto      & set_to   = sets[row + 2];
    if (set_to.size() == 0) {
        auto size = 3 * set_from.size() - 3;
        set_to.resize(size);
        for (uint j=0; j<ELEMENTS; ++j) {
            set_to[0     ][j] = ELEMENT_FILL(1);
            set_to[size-1][j] = ELEMENT_MAX - ELEMENT_FILL(1);
        }
    }

    // 4 way merge: {set_from - last_} (twice), {set_from} and
    // {set_from + last_}
    Element const* RESTRICT ptr_sum    = &set_sum[2][0];
    Element const* RESTRICT ptr_low1   = &ranges.first[-1][0];
    Element const* RESTRICT ptr_low2   = &ranges.second[0][0];
    Element const* RESTRICT ptr_middle = &set_from[1][0];
    Element const* RESTRICT ptr_high   = &set_from[1][0];
    Column col_low1, col_low2, last0;
    for (uint j=0; j<ELEMENTS; ++j) {
        last0[j] = ELEMENT_FILL(2*ROW_ZERO) + last_[j];
        col_low1[j] = last0[j] - ptr_low1[j];
        col_low2[j] = *ptr_low2++ - last_[j];
    }
    if (false) {
        std::lock_guard<mutex> lock(mutex_out);
        indent(row);
        cout << "last0=" << last0 << "\n";
    }

    Element* ptr_end = &set_to[set_to.size()-1][0];
    Element* ptr_to  = &set_to[1][0];
    int c;
    while (ptr_to < ptr_end) {
        if (false) {
            std::lock_guard<mutex> lock(mutex_out);
            indent(row);
            cout << "col_low1" << col_low1 << "\n";
            indent(row);
            cout << "col_low2" << col_low2 << "\n";
            indent(row);
            cout << "col_high" << col_high << "\n";
        }

        c = cmp(col_low1, col_low2);
        if (c == -1) {
            c = cmp(col_low1, ptr_middle);
            if (c == -1) {
                c = cmp(col_low1, col_high);
                if (c == -1) {
                    // cout << "LOW1\n";
                    ptr_low1 -= ELEMENTS;
                    for (uint j=0; j<ELEMENTS; ++j) {
                        *ptr_to++   = col_low1[j];
                        col_low1[j] = last0[j] - ptr_low1[j];
                    }
                    goto SUM;
                }
                if (c == 1) goto HIGH;
                // low1 == high
                // cout << "low1 == high\n";
                return row;
            }
            if (c ==  1) goto MIDDLE;
            // low1 == middle
            // cout << "low1 == middle\n";
            return row;
        }
        if (c ==  1) {
            c = cmp(col_low2, ptr_middle);
            if (c == -1) {
                c = cmp(col_low2, col_high);
                if (c == -1) {
                    // cout << "LOW2\n";
                    for (uint j=0; j<ELEMENTS; ++j) {
                        *ptr_to++   = col_low2[j];
                        col_low2[j] = *ptr_low2++ - last_[j];
                    }
                    goto SUM;
                }
                if (c == 1) goto HIGH;
                // low2 == high
                // cout << "low2 == high\n";
                return row;
            }
            if (c ==  1) goto MIDDLE;
            // low2 == middle
            // cout << "low2 == middle\n";
            return row;
        }
        // low1 == low2
        // cout << "low1 == low2\n";
        return row;

      MIDDLE:
        // cout << "MIDDLE\n";
        c = cmp(ptr_middle, col_high);
        if (c == -1) {
            // cout << "MIDDLE0\n";
            for (uint j=0; j<ELEMENTS; ++j)
                *ptr_to++ = *ptr_middle++;
            // {middle} can never be in {sum} or we would already have found
            // this on the previous level
            goto DONE;
        }
        if (c ==  1) goto HIGH;
        // middle == high
        // cout << "middle == high\n";
        return row;

      HIGH:
        // cout << "HIGH0\n";
        for (uint j=0; j<ELEMENTS; ++j) {
            *ptr_to++ = col_high[j];
            col_high[j] = *ptr_high++ + last_[j];
        }
        goto SUM;

      SUM:
        for (int j=-static_cast<int>(ELEMENTS); j<0; ++j) {
            if (ptr_to[j] > ptr_sum[j]) {
                ptr_sum += ELEMENTS;
                goto SUM;
            }
            if (ptr_to[j] < ptr_sum[j]) goto DONE;
        }
        // sum == to
        return row;
      DONE:;
    }
    if (DEBUG_SET) {
        lock_guard<mutex> lock{mutex_out};
        for (auto const& to: set_to) {
            indent(row);
            cout << to << "\n";
        }
    }

    auto row1 = row+1;
    if (row1 > col_info[col_].min) col_info[col_].min = row1;
    if (false) {
        lock_guard<mutex> lock{mutex_out};
        for (uint i=0; i<row1+2; ++i) {
            cout << "Set " << i << "\n";
            auto& set = sets[i];
            for (auto& column: set)
                cout << column << "\n";
        }
    }

    if (row1 > max_row_) {
        max_row_ = row1;
        lock_guard<mutex> lock{mutex_max};

        if (max_row_ > max_rows_) {
            max_rows_ = max_row_;

            uint8_t buffer[1+(MAX_COLS+MAX_ROWS-1+7)/8];
            buffer[0] = row1;
            uint bytes = 1 + (cols+row1-1+7)/8;
            Sum const* s = &side[0];
            for (uint b=1; b<bytes; ++b, s += 8)
                buffer[b] = s[0] |
                    s[1] << 1 |
                    s[2] << 2 |
                    s[3] << 3 |
                    s[4] << 4 |
                    s[5] << 5 |
                    s[6] << 6 |
                    s[7] << 7;
            pipe_write(buffer, bytes);

            lock_guard<mutex> lock{mutex_out};
            cout << "Worker " << index_ << ": cols=" << cols << ",rows=" << row1 << " (" << elapsed() << " s)\n";
            print(row1);
        } else {
            max_row_ = max_rows_;
        }
    }

    auto last     = last_;
    Index last_col, last_col_rev;
    if (SKIP_FRONT) last_col = last_col_;
    if (SKIP_BACK)  last_col_rev = last_col_rev_;

    Element carry = 0;
    for (uint j=0; j<ELEMENTS; ++j) {
        auto c = last_[j] % COL_FACTOR;
        last_[j] = last_[j] / COL_FACTOR + carry * ELEMENT_TOP;
        carry = c;
    }
    if (SKIP_FRONT) last_col_ >>= 1;
    if (SKIP_BACK)  last_col_rev_ = last_col_rev_ << 1 & mask_columns;

    // Prefer to balance 0's and 1's
    uint rc;
    if (count1_ * 2 > cols + row) {
        side[cols + row] = 0;
        rc = extend(row1);

        if (SKIP_FRONT) last_col_     += top_column;
        if (SKIP_BACK)  last_col_rev_ += 1;
        last_[top_offset_] += top_;
        side[cols + row] = 1;
        ++count1_;
        uint m = extend(row1);
        if (m > rc) rc = m;
        --count1_;
    } else {
        if (SKIP_FRONT) last_col_     += top_column;
        if (SKIP_BACK)  last_col_rev_ += 1;
        last_[top_offset_] += top_;
        ++count1_;
        side[cols + row] = 1;
        rc = extend(row1);
        --count1_;
        last_[top_offset_] -= top_;
        if (SKIP_BACK)  last_col_rev_ -= 1;
        if (SKIP_FRONT) last_col_     -= top_column;

        side[cols + row] = 0;
        uint m = extend(row1);
        if (m > rc) rc = m;
    }

    if (SKIP_BACK)  last_col_rev_ = last_col_rev;
    if (SKIP_FRONT) last_col_     = last_col;
    last_     = last;

    if (false && row1 <= 13) {
        std::lock_guard<mutex> lock(mutex_out);
        cout << "Worker " << index_ << ":";
        for(uint i=cols; i < cols + row1; ++i)
            cout << " " << static_cast<uint>(side[i]);
        cout << ": " << rc << endl;
    }
    return rc;
}

void State::print(uint rows) {
    for (auto c=0U; c<cols;c++) {
        for (auto r=0U; r<rows;r++) {
            cout << static_cast<uint>(side[cols-1-c+r]) << " ";
        }
        cout << "\n";
    }
    cout << "----------" << endl;
}

Connection::Connection(string const& host, string const& service) {
    {
        char hostname[100];
        hostname[sizeof(hostname)-1] = 0;
        int rc = gethostname(hostname, sizeof(hostname)-1);
        if (rc) id.assign("Dunno");
        else id.assign(hostname);
        id.append(" ");
        auto pid = getpid();
        id.append(to_string(pid));
    }

    // Resolve address
    struct addrinfo hints, *results;
    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_UNSPEC;	// use IPv4 or IPv6
    hints.ai_socktype = SOCK_STREAM;
    auto rc = getaddrinfo(host.c_str(), service.c_str(), &hints, &results);
    if (rc) throw(runtime_error(gai_strerror(rc)));

    // For now the looping is a sham since we throw on any error
    for (auto res = results; res; res = res->ai_next) {
        // Create socket
        int fd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
        if (fd < 0) {
            auto err = errno;
            freeaddrinfo(results);
            throw(system_error(err, system_category(), "Could not create socket"));
        }
        int rc = connect(fd, res->ai_addr, res->ai_addrlen);
        if (rc < 0) {
            auto err = errno;
            close(fd);
            freeaddrinfo(results);
            throw(system_error(err, system_category(), "Could not connect"));
        }

        // Set non-blocking
        int flags = fcntl(fd, F_GETFL, 0);
        if (flags < 0) {
            auto err = errno;
            close(fd);
            freeaddrinfo(results);
            throw(system_error(err, system_category(), "Could not fcntl"));
        }
        flags |= O_NONBLOCK;
        flags = fcntl(fd, F_SETFL, flags);
        if (flags < 0) {
            auto err = errno;
            close(fd);
            freeaddrinfo(results);
            throw(system_error(err, system_category(), "Could not fcntl"));
        }

        // Set keepalive
        int enable = 1;
        rc = setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &enable, sizeof(enable));
        if (rc < 0) {
            auto err = errno;
            close(fd);
            freeaddrinfo(results);
            throw(system_error(err, system_category(), "Could not set SO_KEEPALIVE"));
        }

        freeaddrinfo(results);

        fd_ = fd;
        io_out_    .set<Connection, &Connection::writable>(this);
        io_in_     .set<Connection, &Connection::readable>(this);
        timer_greeting_.set<Connection, &Connection::timeout_greeting>(this);
        timer_greeting_.start(TIMEOUT_GREETING);
        return;
    }
    freeaddrinfo(results);
    throw(runtime_error("Could not make a connection"));
}

void Connection::timeout_greeting(ev::timer& timer, int revents) {
    cout << "Connection greeting timed out" << endl;
    stop();
}

void Connection::got_proto(uint8_t const* ptr, size_t length) {
    if (*ptr != PROTO_VERSION)
        throw(runtime_error("Server speaks protocol version " + to_string(static_cast<uint>(*ptr)) + " while I speak " + to_string(static_cast<uint>(PROTO_VERSION))));
    put(ID, id);
    uint8_t message[2] = { PROGRAM_VERSION, static_cast<uint8_t>(nr_threads) };
    put(PROGRAM, message, sizeof(message));
}

void Connection::got_size(uint8_t const* ptr, size_t length) {
    cols = *ptr;
    if (cols > MAX_COLS) throw(range_error("Cols " + to_string(cols) + " > MAX_COLS=" + to_string(MAX_COLS)));
    cout << "Client " << id << " starts working on " << cols << " columns" << endl;
    timer_greeting_.stop();

    nr_columns = static_cast<Index>(1) << cols;
    col_info.resize(nr_columns);
    col_info[0] = ColInfo{1, 1};
    mask_columns = nr_columns - 1;
    top_column = nr_columns / 2;

    top_ = POWN(COL_FACTOR, (cols-1) % ROWS_PER_ELEMENT);
    top_offset_ = ELEMENTS - 1 - (cols-1) / ROWS_PER_ELEMENT;

    idle_.resize(nr_threads);
    if (states) throw(logic_error("Multiple states initializations"));
    void* raw_memory = operator new[](nr_threads * sizeof(State));
    states = static_cast<State*>(raw_memory);
    while (nr_states < nr_threads) {
        new(&states[nr_states]) State(this, nr_states);
        idle_[nr_states] = nr_states;
        ++nr_states;
    }
}

void Connection::got_info(uint8_t const* ptr, size_t length) {
    Index index = get_index(ptr);
    auto& info = col_info.at(index);
    if (*ptr > info.min) {
        info.min = *ptr;
        max_rows(info.min);
    }
    ++ptr;
    if (*ptr < info.max) info.max = *ptr;
    // cout << "Got info " << index << " [" << static_cast<uint>(info.min) << ", " << static_cast<uint>(info.max) << "]\n";
}

void Connection::got_fork(uint8_t const* ptr, size_t length) {
    for (uint i=0; i<nr_states; ++i) states[i].fork();
}

void Connection::got_work(uint8_t const* ptr, size_t length) {
    Index col = get_index(ptr);
    if (!idle_.size()) throw(logic_error("Got work while not idle"));
    auto idle = idle_.back();
    idle_.pop_back();
    states[idle].got_work(col);
}

void Connection::got_solution(uint8_t const* ptr, size_t length) {
    uint rows = *ptr++;
    max_rows(rows);
    if (rows > MAX_ROWS)
        throw(logic_error("Got a solution for " + to_string(rows) + " rows"));

    array<uint8_t, (MAX_ROWS + MAX_COLS - 1 + 7) / 8 * 8> matrix;
    auto l = length - 1;
    if (l != (cols+rows-1+7)/8)
        throw(logic_error("Solution with inconsistent length"));
    for (uint i=0; l; --l) {
        uint bits = *ptr++;
        for (int j=0; j<8; ++j) {
            matrix[i++] = bits & 1;
            bits >>= 1;
        }
    }
    std::lock_guard<mutex> lock(mutex_out);
    cout << "Some computer found: cols=" << cols << ",rows=" << rows << " (" << elapsed() << " s)\n";
    for (uint c=0; c<cols; ++c) {
        auto m = &matrix[cols-1-c];
        for (uint r=0; r<rows; ++r)
            cout << static_cast<uint>(*m++) << " ";
        cout << "\n";
    }
    cout << "----------" << endl;
}

void Connection::got_idle(uint8_t const* ptr, size_t length) {
    if (idle_.size() == 0)
        throw(logic_error("Idle while working"));
    std::lock_guard<mutex> lock(mutex_out);
    cout << "Idle" << endl;
}

void Connection::got_finished(uint8_t const* ptr, size_t length) {
    if (idle_.size() != nr_threads)
        throw(logic_error("Finished while working"));
    stop();
    std::lock_guard<mutex> lock(mutex_out);
    cout << "Finished" << endl;
}

void Connection::readable(ev::io& watcher, int revents) {
    char buffer[BLOCK];
    auto rc = read(fd(), buffer, BLOCK);
    if (rc > 0) {
        in_.append(buffer, rc);
        while (in_.size()) {
            auto ptr = reinterpret_cast<uint8_t const*>(in_.data());
            size_t wanted = *ptr++;
            if (wanted > in_.size()) return;
            size_t length = wanted - 2;
            switch(*ptr++ | phase) {
                case GET_PROTO | PROTO:
                  got_proto(ptr, length);
                  phase = GET_SIZE;
                  break;
                case GET_SIZE | SIZE:
                  got_size(ptr, length);
                  time_start = chrono::steady_clock::now();
                  phase = 0;
                  break;
                case INFO:
                  got_info(ptr, length);
                  break;
                case FORK:
                  got_fork(ptr, length);
                  break;
                case WORK:
                  got_work(ptr, length);
                  break;
                case SOLUTION:
                  got_solution(ptr, length);
                  break;
                case IDLE:
                  got_idle(ptr, length);
                  break;
                case FINISHED:
                  got_finished(ptr, length);
                  break;
                default:
                  throw(range_error("Unknown message type " +
                                    to_string(static_cast<uint>(ptr[-1])) + " in phase " + to_string(phase >> 8)));
            }
            in_.erase(0, wanted);
        }
        return;
    }

    if (rc < 0) {
        if (errno == EWOULDBLOCK || errno == EAGAIN || errno == EINTR) return;
        auto err = strerror(errno);
        stop();
        std::lock_guard<mutex> lock(mutex_out);
        cout << "Could not read: " << err << endl;
    } else {
        // Close
        stop();
        std::lock_guard<mutex> lock(mutex_out);
        cout << "Peer closed connection" << endl;
    }
}

void Connection::writable(ev::io& watcher, int revents) {
    size_t size = min(out_.size(), BLOCK);
    auto rc = write(fd(), out_.data(), size);
    if (rc > 0) {
        if (static_cast<size_t>(rc) == out_.size()) {
            out_.clear();
            io_out_.stop();
        } else
            out_.erase(0, rc);
    } else if (rc < 0) {
        if (errno == EWOULDBLOCK || errno == EAGAIN || errno == EINTR) return;
        throw(system_error(errno, system_category(), "Could not write"));
    } else {
        throw(logic_error("0 length write"));
    }
}

void Connection::put(uint type, void const* data, size_t size) {
    if (type >= 256 || type == 0) throw(logic_error("Invalid type"));
    auto wanted = size+2;
    if (wanted >= 256) throw(range_error("Data too large"));
    if (out_.size() == 0) io_out_.start(fd(), ev::WRITE);
    out_.push_back(wanted);
    out_.push_back(type);
    out_.append(reinterpret_cast<char const*>(data), size);
}

void Connection::put_known(Index col) {
    if (out_.size() == 0) io_out_.start(fd(), ev::WRITE);
    auto& info = col_info[col];
    out_.push_back(8);
    out_.push_back(INFO);
    for (int i=0; i<4; ++i) {
        out_.push_back(col & 0xff);
        col >>= 8;
    }
    out_.push_back(info.min);
    out_.push_back(info.max);
}

void my_main(int argc, char** argv) {
    nr_threads = min(nr_threads, 255U);

    auto host = argc > 1 ? argv[1] : HOST;
    auto port = argc > 2 ? string{argv[2]} : to_string(PORT);
    if (argc > 3) {
        auto l = atol(argv[3]);
        if (l < 1)   throw(range_error("nr threads must be >= 1"));
        if (l > 255) throw(range_error("nr threads must be <= 255"));
        nr_threads = l;
    }

    auto rc = signal(SIGPIPE, SIG_IGN);
    if (rc == SIG_ERR)
        throw(logic_error("Could not ignore SIG PIPE"));

    Connection connection{host, port};

    ev::default_loop loop;
    connection.start();
    loop.run(0);

    cout << "Processing finished" << endl;
    return;
}

int main(int argc, char** argv) {
    try {
        my_main(argc, argv);
    } catch(exception& e) {
        cout << "Error: " << e.what() << endl;
        exit(EXIT_FAILURE);
    }
    exit(EXIT_SUCCESS);
}
