/*
  Compile using something like:
    g++ -Wall -O3 -march=native -fstrict-aliasing -std=c++14 -g propertyX.cpp -pthread -lev -o propertyX

  create a file propertyX.rows.txt containing just ----
  Run as server:
    ./propertyX rows [port]
  Then connect other instantiations of the program as client:
    ./propertyX [host [port [threads]]]
  If a client is interrupted just restart it. No work will be lost (but time is)
  If the server is interrupted rename propertyX.rows.out.txt to propertyX.rows.txt, add a final ---- and restart
  client signals:
    USR1: decrease wanted number of threads. Actual decrease only happens
          when some thread finishes. Then closes connection if wanted == 0
    USR1: increase wanted number of threads. Takes effect immediately (if less)
    SYS:  show top row of bits (without the fixed first one). This gives an
          indication of how far each thread has progressed on its current column
*/
// nas  10 78s
// asus 10 247s

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <iomanip>
#include <iostream>
#include <fstream>
#include <sstream>
#include <limits>
#include <map>
#include <mutex>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <type_traits>
#include <vector>

#include <cctype>
#include <cerrno>
#include <climits>
#include <cstdint>
#include <cstring>
#include <ctgmath>
#include <ctime>

#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>
#include <fcntl.h>
#include <netdb.h>
#include <signal.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <ev++.h>

#include "fd_buffer.hpp"

using namespace std;

#ifdef __GNUC__
# define RESTRICT __restrict__
# define NOINLINE	__attribute__((__noinline__))
# define LIKELY(x)	__builtin_expect(!!(x),true)
# define UNLIKELY(x)	__builtin_expect(!!(x),false)
# define HOT		__attribute__((__hot__))
# define COLD		__attribute__((__cold__))
#else // __GNUC__
# define RESTRICT
# define NOINLINE
# define LIKELY(x)	(x)
# define UNLIKELY(x)	(x)
# define HOT
# define COLD
#endif // __GNUC__

bool const SKIP_FRONT = true;
bool const SKIP_BACK  = true;
bool const DEBUG_SET  = false;
bool const SKIP2      = false;
bool const INSTRUMENT = false;
bool const EARLY_MIN  = false;

char const* HOST = "localhost";
uint const  PORT = 21453;

int const PERIOD = 5*60;
// int const PERIOD = 1;
int  const  BACKLOG = 5;
char const* TERMINATOR = "----";
size_t const BLOCK = 65536;
ev_tstamp const TIMEOUT_GREETING = 10;

char const* PROGRAM_NAME = "propertyX";
uint8_t PROTO_VERSION   = 2;
uint8_t PROGRAM_VERSION = 9;

enum {
    PROTO = 1,	// Communication protocol version
    ID,		// client id
    PROGRAM,	// client program version and nr threads
    SIZE,	// Number of rows to work on
    INFO,	// Info about a column (min,max)
    FORK,	// Tell client to start its threads
    WORK,	// Give client a column to work on
    RESULT,	// Tell server how many columns can be gotten starting with WORK
    SOLUTION,   // New best solution
    IDLE,	// Server running out of work, client has at least 1 idle thread
    FINISHED,	// Server completely out of work. Client must exit
    NR_THREADS,	// Change number of threads for a connection
};

enum {
    GET_PROTO   = 1*256,

    // Client states
    GET_SIZE    = 2*256,

    // Server states
    GET_ID      = 3*256,
    GET_PROGRAM = 4*256,
};

uint const ELEMENTS = 2;

using uint     = unsigned int;
using Index    = uint32_t;
using Element  = int64_t;
using uElement = uint64_t;
using Column   = array<Element, ELEMENTS>;
using Set      = vector<Column>;
using Sum      = uint8_t;
using Sec      = chrono::seconds;

uint const MAX_COLS = 54;
uint const ROW_FACTOR = (MAX_COLS+1) | 1;		// 55
uint const ROW_ZERO   = ROW_FACTOR/2;			// 27
uint const ROWS_PER_ELEMENT = CHAR_BIT*sizeof(Element) / log2(ROW_FACTOR); // 11
uint const MAX_ROWS = ROWS_PER_ELEMENT * ELEMENTS;	// 22
Element constexpr ELEMENT_FILL(Element v = ROW_ZERO, int n = ROWS_PER_ELEMENT) {
    return n ? ELEMENT_FILL(v, n-1) * ROW_FACTOR + v : 0;
}
uElement constexpr POWN(uElement v, int n) {
    return n ? POWN(v, n-1)*v : 1;
}
uElement const ELEMENT_TOP  = POWN(ROW_FACTOR, ROWS_PER_ELEMENT - 1);
Element const ELEMENT_MIN = numeric_limits<Element>::min() + ELEMENT_FILL(1);
Element const ELEMENT_MAX = numeric_limits<Element>::max() - ELEMENT_FILL(1);
// log2(2 * ELEMENTS * sizeof(Element) * pow(3., rows) / 10)

// Shared variables
Index nr_rows, mask_rows, top_row;
uint rows;
ev::default_loop loop;

// Client variables
thread forked;
mutex mutex_info;
mutex mutex_out;
Element top_;
uint top_offset_;
uint max_cols_ = 1;

// Server variables
FdBuffer out_buffer(true);
ostream out{&out_buffer};
Index nr_work;
Index done_work;
uint period = 0;
array<uint8_t,   (MAX_COLS + MAX_ROWS - 1 + 7) / 8 * 8> side;
array<uint8_t, 1+(MAX_COLS + MAX_ROWS - 1 + 7) / 8 * 8> solution;

string time_string() {
    time_t t = time(NULL);
    if (t == static_cast<time_t>(-1))
        throw(system_error(errno, system_category(), "Could not get time"));
    struct tm tm;
    if (localtime_r(&t, &tm) == nullptr)
        throw(runtime_error("Could not get localtime"));
    char buffer[100];
    size_t size = strftime(buffer, sizeof(buffer), "%F %T: ", &tm);
    return string{buffer, size};
}

class Connection;

struct Instrument {
    Instrument& operator+=(Instrument const& add) {
        calls   += add.calls;
        success += add.success;
        front   += add.front;
        back    += add.back;
        return *this;
    }
    uint64_t calls   = 0;
    uint64_t success = 0;
    uint64_t front   = 0;
    uint64_t back    = 0;
};
ostream& operator<<(ostream& os, Instrument const& instrument) {
    os << setw(12) << instrument.calls << " " << setw(12) << instrument.success << " " << setw(12) << instrument.front << " " << setw(12) << instrument.back << " " << static_cast<double>(instrument.success) / instrument.calls;
    return os;
}

struct Instruments: public array<Instrument, MAX_COLS> {
    Instruments& operator+=(Instruments const& add) {
        for (uint i=0; i<MAX_COLS; ++i) (*this)[i] += add[i];
        return *this;
    }
};
ostream& operator<<(ostream& os, Instruments const& instruments) {
    for (uint i=0; i<MAX_COLS; ++i) {
        auto const& instrument = instruments[i];
        if (instrument.calls == 0) break;
        os << setw(2) << i << ":" << instrument << "\n";
    }
    return os;
}

class TimeBuffer: public stringbuf {
  public:
    ~TimeBuffer() {
        sync();
        flush();
    };
    int sync() {
        if (!str().empty()) {
            {
                lock_guard<mutex> lock{mutex_out};
                full_out << time_string() << str();
            }
            str("");
        }
        return 0;
    }
    static stringstream full_out;
    static void flush() {
        if (full_out.str().empty()) return;
        lock_guard<mutex> lock{mutex_out};
        cout << full_out.str();
        cout.flush();
        full_out.str("");
    }
    static void flush(ev::timer &w, int revents) { flush(); }
};
stringstream TimeBuffer::full_out;

class TimeStream: public ostream {
  public:
    TimeStream(): ostream{&buffer} {}
  private:
    TimeBuffer buffer;
};
TimeStream timed_out;

class State {
  public:
    using Ptr = unique_ptr<State>;

    State(Connection* connection);
    State(State const&) = delete;
    ~State();
    void fork();
    void got_work(Index col);
    uint id() const { return id_; }
    int64_t elapsed() const {
        auto now = chrono::steady_clock::now();
        return chrono::duration_cast<Sec>(now-start_).count();
    }
    void show() { show_ = 1; }
    Instruments const& instruments() const { return instruments_; }
  private:

    class IdBuffer: public stringbuf {
      public:
        IdBuffer(uint const& id): id_{id} {}
        ~IdBuffer() { sync(); };
        int sync() {
            if (!str().empty()) {
                {
                    lock_guard<mutex> lock{mutex_out};
                    TimeBuffer::full_out << time_string() << "Worker " << id_ << ": " << str();
                }
                str("");
            }
            return 0;
        }
      private:
        uint const& id_;
    };

    class IdStream: public ostream {
      public:
        IdStream(uint const& id): ostream{&buffer}, buffer{id} {}
      private:
        IdBuffer buffer;
    };

    void stop() COLD { io_in_.stop(); }
    void pipe_write(void const* ptr, size_t size);
    void readable(ev::io& watcher, int revents);
    void worker();
    NOINLINE void skip_message(uint col, bool front) COLD;
    NOINLINE uint extend(uint row, bool zero = true, bool one = true) HOT;
    void print(uint rows);

    Column current_;
    vector<Set> sets;
    IdStream id_out;
    string pipe_in_;
    Connection* const connection_;
    ev::io io_in_;
    thread thread_;
    condition_variable cv_col;
    mutex mutex_col;
    chrono::steady_clock::time_point start_;
    atomic<Index> input_row_;
    atomic<int> finished_;
    atomic<int> show_;
    Index col_, current_col_, current_col_rev_;
    uint max_col_ = 0;
    uint processed_ = 0;
    uint id_;
    int fd_[2];
    array<Sum, (MAX_COLS + MAX_ROWS - 1 + 7) / 8 * 8> side;
    Instruments instruments_;

    static uint next_id;
};
uint State::next_id = 0;

struct ColInfo {
    uint8_t min=0, max=-1;
};
vector<ColInfo> col_info;

struct ResultInfo {
    static uint8_t const MAX = numeric_limits<uint8_t>::max();
    ResultInfo(auto mi, auto ma, auto v):
        min    {static_cast<uint8_t>(mi)},
        max    {static_cast<uint8_t>(ma)},
        version{static_cast<uint8_t>(v)} {}
    ResultInfo(): ResultInfo(0, MAX, 0) {}

    uint8_t min, max, version;
};

ostream& operator<<(ostream& os, ResultInfo const& info) {
    os << static_cast<uint>(info.min) << " " << static_cast<uint>(info.max) << " " << static_cast<uint>(info.version);
    return os;
}

vector<ResultInfo> result_info;
vector<Index>   col_known;
deque<Index>    col_work;

inline int cmp(auto const& left, auto const& right) {
    for (uint i=0; i<ELEMENTS; ++i) {
        if (left[i] < right[i]) return -1;
        if (left[i] > right[i]) return  1;
    }
    return 0;
}

void shift(Column& column, uElement carry = 0) {
    column[top_offset_] -= (column[top_offset_] + ELEMENT_FILL()) / top_ % ROW_ZERO * top_;
    carry += ROW_ZERO;
    int j = ELEMENTS;
    while (--j >= 0) {
        uElement v = column[j] + ELEMENT_FILL();
        auto c = v / ELEMENT_TOP;
        v = (v - c * ELEMENT_TOP) * ROW_FACTOR + carry;
        column[j] = v - ELEMENT_FILL();
        carry = c;
    }
}

// Server: Listen for incoming connection
class Listener {
  public:
    Listener(uint port);
    ~Listener() { stop(); }
    int fd() const { return fd_; }
    void connection(ev::io& watcher, int revents);
    void start() {
        watch_.start(fd(), ev::READ);
        timer_output_.set<TimeBuffer::flush>();
        timer_output_.start(0, 1);
    }
    void stop()  {
        if (fd_ < 0) return;
        watch_.stop();
        timer_output_.stop();
        close(fd_);
        fd_ = -1;
    }
    void start_timer() {
        start_ = chrono::steady_clock::now();
    }
    int64_t elapsed() const {
        auto now = chrono::steady_clock::now();
        return chrono::duration_cast<Sec>(now-start_).count();
    }
  private:
    ev::io watch_;
    ev::timer timer_output_;
    chrono::steady_clock::time_point start_;
    int fd_;
};

// Server: Accepted connection from client
class Accept {
  public:
    using Ptr = unique_ptr<Accept>;

    Accept(Listener* listener, int fd);
    ~Accept();
    int fd() const { return fd_; }
    // put*: Send info to client
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
    void put_known(size_t from=0);
    void put_info(Index index, ResultInfo const& info);
    void put_work();
    void peer(string const& peer) { peer_id_ = peer; }
    int64_t elapsed() const { return listener_->elapsed(); }
  private:
    void readable(ev::io& watcher, int revents);
    void writable(ev::io& watcher, int revents);
    void timeout_greeting(ev::timer& timer, int revents);

    bool update_info(ResultInfo& info, Index col, uint8_t min, uint8_t max);
    bool update_info(ResultInfo& info, Index col, uint8_t min);
    inline static Index get_index(uint8_t const*& ptr) {
        Index index = static_cast<Index>(ptr[0]) |
            static_cast<Index>(ptr[1]) <<  8 |
            static_cast<Index>(ptr[2]) << 16 |
            static_cast<Index>(ptr[3]) << 24;
        ptr += 4;
        return index;
    }

    // got_*: Received info from client
    inline void got_proto   (uint8_t const* ptr, size_t length);
    inline void got_id      (uint8_t const* ptr, size_t length);
    inline void got_program (uint8_t const* ptr, size_t length);
    inline void got_info    (uint8_t const* ptr, size_t length);
    inline void got_result  (uint8_t const* ptr, size_t length);
    inline void got_solution(uint8_t const* ptr, size_t length);
    inline void got_threads (uint8_t const* ptr, size_t length);

    set<Index> work_;
    Listener* const listener_;
    ev::io io_in_;
    ev::io io_out_;
    ev::timer timer_greeting_;
    string peer_id_;
    string in_;
    string out_;
    uint program_version_ = 0;
    uint work_max_;
    uint phase = GET_PROTO;
    int fd_;
    bool idle_ = false;
};

bool Accept::update_info(ResultInfo& info, Index col, uint8_t min, uint8_t max) {
    if (min <= info.min && max >= info.max) return false;
    if (info.version == 0)
        col_known.emplace_back(col);
    else if (info.version != program_version_) {
        out << col << " " << info << "\n";
    }
    info.version = program_version_;
    if (min > info.min) info.min = min;
    if (max < info.max) info.max = max;
    out << col << " " << info << endl;
    return true;
}

bool Accept::update_info(ResultInfo& info, Index col, uint8_t min) {
    if (min <= info.min) return false;
    if (info.version == 0)
        col_known.emplace_back(col);
    else if (info.version != program_version_) {
        out << col << " " << info << "\n";
    }
    info.version = program_version_;
    if (min > info.min) info.min = min;
    out << col << " " << info << endl;
    return true;
}

set<int> accepted_idle;
map<uint,Accept::Ptr> accepted;

void Accept::timeout_greeting(ev::timer& timer, int revents) {
    timed_out << "Accept " << fd() << " (" << peer_id_ << ") greeting timed out" << endl;
    accepted.erase(fd());
}

void Accept::got_proto(uint8_t const* ptr, size_t length) {
    if (length != 1)
        throw(logic_error("Invalid proto length " + to_string(length)));

    if (*ptr != PROTO_VERSION) {
        timed_out << "Server speaks protocol version " << static_cast<uint>(*ptr) <<  " while I speak " << static_cast<uint>(PROTO_VERSION) << endl;
        accepted.erase(fd());
        return;
    }
    put1(SIZE, rows);
    put_known();
    put(FORK);
}

void Accept::got_id(uint8_t const* ptr, size_t length) {
    peer_id_.assign(reinterpret_cast<char const*>(ptr), length);
}

void Accept::got_program(uint8_t const* ptr, size_t length) {
    if (length != 2)
        throw(logic_error("Invalid program length " + to_string(length)));

    program_version_ = *ptr++;
    work_max_        = *ptr;
    if (program_version_ == 0)
        throw(logic_error("Invalid program version 0"));
    if (work_max_ == 0)
        throw(logic_error("no threads"));
    timed_out << "Peer " << fd() << ": " << peer_id_ << " version " << program_version_ << ", " << work_max_ << " threads" << endl;
    timer_greeting_.stop();
    put_work();
}

void Accept::got_info(uint8_t const* ptr, size_t length) {
    if (length != 6)
        throw(logic_error("Invalid info length " + to_string(length)));

    Index col = get_index(ptr);
    auto& info = result_info.at(col);
    update_info(info, col, ptr[0], ptr[1]);
    for (auto& element: accepted) {
        auto& connection = *element.second;
        if (&connection == this) continue;
        connection.put_info(col, info);
    }
}

void Accept::got_result(uint8_t const* ptr, size_t length) {
    if (length != 4)
        throw(logic_error("Invalid result length " + to_string(length)));

    auto index = get_index(ptr);
    if (!work_.erase(index))
        throw(logic_error("Result that was never requested: " + to_string(index)));
    put_work();

    ++done_work;
    uint elapsed_ = elapsed();
    if (elapsed_ >= period || done_work >= nr_work) {
        timed_out << "col=" << done_work << "/" << nr_work << " (" << static_cast<uint64_t>(100*1000)*done_work/nr_work/1000. << "% " << elapsed_ << " s, avg=" << elapsed_ * 1000 / done_work / 1000. << ")" << endl;
        period = (elapsed_/PERIOD+1)*PERIOD;
    }

    // if (done_work >= nr_work) loop.unloop();
    if (done_work >= nr_work) {
        listener_->stop();
        for (auto& element: accepted) {
            auto& connection = *element.second;
            connection.put(FINISHED);
        }
        timed_out << "Finished" << endl;
    }
}

void Accept::got_solution(uint8_t const* ptr, size_t length) {
    if (length < 2)
        throw(logic_error("Invalid solution length " + to_string(length)));

    auto data = ptr;
    uint cols = *ptr++;
    if (cols <= max_cols_) return;

    if (cols > MAX_COLS)
        throw(logic_error("Got a solution for " + to_string(cols) + " cols"));
    max_cols_ = cols;
    memcpy(&solution[0], data, length);

    auto l = length - 1;
    if (l != (rows+cols-1+7)/8)
        throw(logic_error("Solution with inconsistent length"));
    for (uint i=0; l; --l) {
        uint bits = *ptr++;
        for (int j=0; j<8; ++j) {
            side[i++] = bits & 1;
            bits >>= 1;
        }
    }
    timed_out << "rows=" << rows << ",cols=" << cols << " (" << elapsed() << " s)\n";
    Index col = 0;
    for (uint r=0; r<rows; ++r) {
        auto s = &side[rows-1-r];
        col = col << 1 | *s;
        for (uint c=0; c<cols; ++c)
            timed_out << static_cast<uint>(*s++) << " ";
        timed_out << "\n";
    }
    timed_out << "----------" << endl;
    auto& info = result_info.at(col);
    update_info(info, col, cols);

    for (auto& element: accepted) {
        auto& connection = *element.second;
        if (&connection != this) connection.put(SOLUTION, data, length);
    }
}

void Accept::got_threads (uint8_t const* ptr, size_t length) {
    if (length != 1)
        throw(logic_error("Invalid threads length " + to_string(length)));

    work_max_ = *ptr;
    timed_out << "Peer " << peer_id_ << " changes number of threads to " << work_max_ << endl;
    put1(NR_THREADS, work_max_);
    put_work();
}

void Accept::readable(ev::io& watcher, int revents) {
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
                  phase = GET_ID;
                  break;
                case GET_ID | ID:
                  got_id(ptr, length);
                  phase = GET_PROGRAM;
                  break;
                case GET_PROGRAM | PROGRAM:
                  got_program(ptr, length);
                  phase = 0;
                  break;
                case INFO:
                  got_info(ptr, length);
                  break;
                case RESULT:
                  got_result(ptr, length);
                  break;
                case SOLUTION:
                  got_solution(ptr, length);
                  break;
                case NR_THREADS:
                  got_threads(ptr, length);
                  break;
                default: throw
                    (range_error("Unknown message type " +
                                 to_string(static_cast<uint>(ptr[-1])) + " in phase " + to_string(phase >> 8)));
            }
            in_.erase(0, wanted);
        }
        if (work_max_ || work_.size() || done_work >= nr_work || phase) return;
        timed_out << "No more threads on " << peer_id_ << ". Closing connection";
    } else if (rc < 0) {
        if (errno == EWOULDBLOCK || errno == EAGAIN || errno == EINTR) return;
        auto err = strerror(errno);
        timed_out << "Read error from " << peer_id_ << ":" << err;
    } else {
        // Close
        timed_out << "Accept " << fd() << " closed by " << peer_id_;
    }
    timed_out << endl;
    accepted.erase(fd());
}

void Accept::writable(ev::io& watcher, int revents) {
    size_t size = min(out_.size(), BLOCK);
    auto rc = write(fd(), out_.data(), size);
    if (rc > 0) {
        if (static_cast<size_t>(rc) == out_.size()) {
            out_.clear();
            io_out_.stop();
        } else
            out_.erase(0, rc);
        return;
    }
    if (rc < 0) {
        if (errno == EWOULDBLOCK || errno == EAGAIN || errno == EINTR) return;
        auto err = strerror(errno);
        timed_out << "Write error to " << peer_id_ << ":" << err << endl;
    } else {
        // rc == 0
        timed_out << "Zero length write to " << peer_id_ << endl;
    }
    accepted.erase(fd());
}

void Accept::put(uint type, void const* data, size_t size) {
    if (type >= 256 || type == 0) throw(logic_error("Invalid type"));
    auto wanted = size+2;
    if (wanted >= 256) throw(range_error("Data too large"));
    if (out_.size() == 0) io_out_.start(fd(), ev::WRITE);
    out_.push_back(wanted);
    out_.push_back(type);
    out_.append(reinterpret_cast<char const*>(data), size);
}

Accept::Accept(Listener* listener, int fd):
    listener_{listener},
    fd_{fd} {
    io_out_.set<Accept, &Accept::writable>(this);
    io_in_ .set<Accept, &Accept::readable>(this);
    io_in_ .start(fd_, ev::READ);
    timer_greeting_.set<Accept, &Accept::timeout_greeting>(this);
    timer_greeting_.start(TIMEOUT_GREETING);
}

Accept::~Accept() {
    io_in_.stop();
    if (out_.size()) io_out_.stop();
    close(fd_);
    accepted_idle.erase(fd_);
    for (Index work: work_)
        col_work.emplace_front(work);
    while (accepted_idle.size() && col_work.size()) {
        int fd = *accepted_idle.begin();
        Accept& accept = *accepted[fd];
        accept.put_work();
    }
    // fd_ = -1;
}

void Accept::put_known(size_t from) {
    uint cols = solution[0];
    if (cols) {
        uint bytes = 1+(rows+cols-1+7) / 8;
        put(SOLUTION, &solution[0], bytes);
    }

    if (out_.size() == 0 && col_known.size() - from)
        io_out_.start(fd(), ev::WRITE);
    out_.reserve(out_.size() + 8 * (col_known.size() - from));
    size_t to = col_known.size();
    for (size_t k = from; k < to; ++k) {
        Index index = col_known[k];
        out_.push_back(8);
        out_.push_back(INFO);
        uint32_t i = index;
        for (int j=0; j<4; ++j) {
            out_.push_back(i & 0xff);
            i >>= 8;
        }
        out_.push_back(result_info[index].min);
        out_.push_back(result_info[index].max);
    }
}

void Accept::put_info(Index index, ResultInfo const& info) {
    if (out_.size() == 0) io_out_.start(fd(), ev::WRITE);
    out_.push_back(8);
    out_.push_back(INFO);
    for (int j=0; j<4; ++j) {
        out_.push_back(index & 0xff);
        index >>= 8;
    }
    out_.push_back(info.min);
    out_.push_back(info.max);
}

void Accept::put_work() {
    while (col_work.size() && work_.size() < work_max_) {
        auto work = col_work.front();
        auto rc = work_.emplace(work);
        if (!rc.second) throw(logic_error("Duplicate work"));
        col_work.pop_front();

        if (out_.size() == 0) io_out_.start(fd(), ev::WRITE);
        out_.push_back(6);
        out_.push_back(WORK);
        for (int i=0; i<4; ++i) {
            out_.push_back(work & 0xff);
            work >>= 8;
        }
        if (!period) {
            period = PERIOD;
            listener_->start_timer();
        }
    }
    if (work_.size() >= work_max_) {
        if (idle_) {
            accepted_idle.erase(fd());
            idle_ = false;
        }
    } else {
        if (!idle_) {
            accepted_idle.emplace(fd());
            idle_ = true;
            put(IDLE);
        }
     }
}

// Client: Connection to server
class Connection {
  public:
    Connection(string const& host, string const& service, uint nr_threads = 1);
    ~Connection() {
        states.resize(0);
        close(fd_);
        // fd_ = -1;
    }
    void start() {
        io_in_ .start(fd_, ev::READ);
        put1(PROTO, PROTO_VERSION);
    }
    int fd() const { return fd_; }
    // put*: Send info to server
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
    void idle(State* state) {
        idle_.emplace_back(state);
    }
    void drop_idle();

  private:
    State* new_state();
    void id_set();
    void stop() {
        io_in_.stop();
        io_out_.stop();
        timer_greeting_.stop();
        timer_output_.stop();
        sig_usr1_.stop();
        sig_usr2_.stop();
        sig_sys_.stop();
        instruments_show();
        states.resize(0);
    }
    void readable(ev::io& watcher, int revents);
    void writable(ev::io& watcher, int revents);
    void timeout_greeting(ev::timer& timer, int revents);
    void threads_more(ev::sig& watcher, int revents);
    void threads_less(ev::sig& watcher, int revents);
    void state_show(ev::sig& watcher, int revents);
    void instruments_show() const;

    inline static Index get_index(uint8_t const*& ptr) {
        Index index = static_cast<Index>(ptr[0]) |
            static_cast<Index>(ptr[1]) <<  8 |
            static_cast<Index>(ptr[2]) << 16 |
            static_cast<Index>(ptr[3]) << 24;
        ptr += 4;
        return index;
    }

    // got_*: Received info from server
    inline void got_proto   (uint8_t const* ptr, size_t length);
    inline void got_size    (uint8_t const* ptr, size_t length);
    inline void got_info    (uint8_t const* ptr, size_t length);
    inline void got_threads (uint8_t const* ptr, size_t length);
    inline void got_fork    (uint8_t const* ptr, size_t length);
    inline void got_work    (uint8_t const* ptr, size_t length);
    inline void got_solution(uint8_t const* ptr, size_t length);
    inline void got_idle    (uint8_t const* ptr, size_t length);
    inline void got_finished(uint8_t const* ptr, size_t length);

    vector<State*> idle_;
    // Sigh, State doesn't work in a std::vector since it is not copy
    // constructable even though I never copy (but resize() implicitely can)
    vector<State::Ptr> states;
    ev::io io_in_;
    ev::io io_out_;
    ev::timer timer_greeting_;
    ev::timer timer_output_;
    ev::sig sig_usr1_, sig_usr2_, sig_sys_;
    string in_;
    string out_;
    string id;
    int fd_;
    uint phase = GET_PROTO;
    uint nr_threads_;
    uint wanted_threads_;
    bool forked_ = false;
};

ostream& operator<<(ostream& os, Column const& column) {
    for (uint i=0; i<ELEMENTS; ++i) {
        uElement v = column[i] + ELEMENT_FILL();
        for (uint j=0; j<ROWS_PER_ELEMENT; ++j) {
            uint bit = v / ELEMENT_TOP;
            os << " " << static_cast<int>(bit - ROW_ZERO);
            v -= bit * ELEMENT_TOP;
            v *= ROW_FACTOR;
        }
    }
    return os;
}

State::State(Connection* connection):
    id_out{id_},
    connection_{connection},
    finished_{0},
    show_{0}
{
    id_ = next_id++;

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

    input_row_ = nr_rows;

    sets.resize(MAX_COLS+2);
    for (int i=0; i<2; ++i) {
        sets[i].resize(2);
        for (uint j=0; j < ELEMENTS; ++j) {
            sets[i][0][j] = ELEMENT_MIN;
            sets[i][1][j] = ELEMENT_MAX;
        }
    }
    if (DEBUG_SET) {
        for (auto const& to: sets[0]) id_out << to << "\n";
        id_out.flush();
    }
}

State::~State() {
    stop();
    if (thread_.joinable()) {
        finished_  = 1;
        input_row_ = 0;
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
        bool idle = false;
        pipe_in_.append(buffer, rc);
        while (pipe_in_.size()) {
            uint cols = pipe_in_[0] & 0xff;
            if (cols == 1) {
                if (pipe_in_.size() < 1 + sizeof(Index)) break;
                Index col;
                memcpy(&col, pipe_in_.data()+1, sizeof(Index));
                pipe_in_.erase(0, 1 + sizeof(Index));
                if (EARLY_MIN && col_info[col].min >= max_cols_)
                    connection_->put_known(col);
            } else if (cols == 0) {
                ++processed_;
                connection_->put_known(col_);
                connection_->put4(RESULT, col_);
                connection_->idle(this);
                idle = true;
                pipe_in_.erase(0, 1);
            } else {
                uint bytes = 1+(rows+cols-1+7) / 8;
                if (pipe_in_.size() < bytes) break;
                connection_->put(SOLUTION, pipe_in_.data(), bytes);
                pipe_in_.erase(0, bytes);
            }
        }
        if (idle) connection_->drop_idle();
        return;
    }
    // Any form of pipe closure should be impossible
    if (rc < 0)
        throw(system_error(errno, system_category(), "pipe read error"));
    // rc == 0
    throw(logic_error("Pipe close"));
}

void State::got_work(Index col) {
    if (col >= nr_rows || col == 0)
        throw(logic_error("Invalid work " + to_string(col)));
    if (false) {
        id_out << "Driver column " << col << endl;
    }
    lock_guard<mutex> lock(mutex_col);
    input_row_ = col;
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
        lock_guard<mutex> lock(mutex_info);
        max_col_ = max_cols_;
    }
    std::unique_lock<std::mutex> lock_input(mutex_col);

    id_out << "ready" << endl;
    start_ = chrono::steady_clock::now();

    while (!finished_) {
        cv_col.wait(lock_input, [this]{return input_row_ < nr_rows;});
        col_ = input_row_;
        if (col_ == 0) break;
        auto total = elapsed();
        current_.fill(0);
        current_col_ = col_;
        current_col_rev_ = 0;
        uint bits = col_;
        char buffer[MAX_ROWS+1];
        for (uint i=0; i<rows; ++i) {
            uint bit = bits & 1;
            buffer[i] = bit ? '1' : '0';
            side[i] = bit;
            bits >>= 1;
            current_col_rev_ = current_col_rev_ << 1 | bit;
            shift(current_, bit);
        }
        buffer[rows] = 0;
        id_out << "Processing " << buffer << " (" << total << " s" << ", n=" << processed_;
        if (processed_)
            id_out << ", avg=" << total * 1000 / processed_ / 1000. << " s";
        id_out << ")";
        if (false) {
            id_out << "\nrev=" << current_col_rev_ << ", current=" << current_;
        }
        id_out << endl;

        uint m = extend(0);
        // This is a slight race, but it doesn't hurt
        // (assuming access to max doesn't tear)
        if (m < col_info[col_].max && !finished_) {
            lock_guard<mutex> lock{mutex_info};
            if (m < col_info[col_].max) col_info[col_].max = m;
        }

        input_row_ = nr_rows;
        uint8_t data = 0;
        pipe_write(&data, sizeof(data));
    }
    auto total = elapsed();
    id_out << "exiting (" << total << " s" << ", n=" << processed_;
    if (processed_)
        id_out << ", avg=" << total * 1000 / processed_ / 1000. << " s";
    id_out << ")" << endl;
}

NOINLINE Set::const_iterator lower_bound(Set::const_iterator begin,
                                         Set::const_iterator end,
                                         Column& column) {
    return lower_bound(begin, end, column,
                       [](Column const& left, Column const& right) { return cmp(left, right) < 0; });
}

void indent(uint row) {
    for (uint i=0; i<row; ++i) cout << "  ";
}

void State::skip_message(uint col, bool front) {
    char buffer[MAX_ROWS+1];
    for (uint i=0; i<rows; ++i) buffer[i] = side[col+i] ? '1' : '0';
    buffer[rows] = 0;
    id_out << "Skip front " << buffer << " (" << elapsed() << " s)" << endl;
    // id_out << (fron ? "Skip front " : "Skip back ");
    // for (uint i=0; i<rows; ++i) id_out << (side[col+i] ? '1' : '0');
    // id_out << " (" << elapsed() << " s)" << endl;
}

// The actual core of the program:
//   add one more column and see if it is independent
uint State::extend(uint col, bool zero, bool one) {
    // id_out << "Extend col " << col << " " << static_cast<int>(side[rows+col-1]) << "\n";
    if (col >= MAX_COLS) throw(range_error("col out of range"));
    auto& instrument = instruments_[col];
    if (INSTRUMENT) {
        ++instrument.calls;
    }
    if (show_) {
        show_ = 0;
        for (uint i=0; i<col; ++i) id_out << (side[rows + i] ? 1 : 0) << " ";
        id_out << endl;
    }

    if (DEBUG_SET) {
        lock_guard<mutex> lock(mutex_out);
        indent(col);
        cout << "current_=" <<   current_ << ", zero=" << (zero ? 1 : 0) << ", one=" << (one ? 1 : 0) << "\n";
    }

    if (SKIP_BACK) {
        uint m = col_info[current_col_rev_].max;
        if (UNLIKELY(m <= col)) {
            if (INSTRUMENT) ++instrument.back;
            if (false)
                skip_message(col, false);
            return col;
        }
    }
    if (SKIP_FRONT) {
        uint m = col_info[current_col_].max + col;
        if (UNLIKELY(m <= max_cols_)) {
            // Column cannot possibly set a new record
            if (INSTRUMENT) ++instrument.front;
            if (true)
                skip_message(col, true);
            return m;
        }
    }

    if (finished_) return 0;

    // Execute subset sum. The new column is added to set {from} giving {to}
    // {sum} is the other set.
    auto current = current_;

    auto const& set_sum  = sets[col + 1];
    // Unclear if this is a win or not (no difference at n=10)
    // But at least it saves me from having to check set_sum when adding
    // col_current
    if (binary_search(set_sum.begin()+1, set_sum.end()-1, current,
                      [](Column const&left, Column const& right) { return cmp(left, right) < 0; })) {
        // id_out << "Hit on sum" << endl;
        return col;
    }

    auto const& set_from = sets[col];
    auto pos_current = equal_range(set_from.cbegin()+1, set_from.cend()-1, current,
                                   [](Column const&left, Column const& right) { return cmp(left, right) < 0; });
    // Early check for in set_from was a slight win but cannot happen anymore
    // However the position is still needed for the subtraction absolute value
    if (pos_current.first != pos_current.second)
        throw(logic_error("Unexpected hit on from"));

    // Early check for sum is a loss
    // if (check_sum(set_from, pos_current.first, current0)) return col;

    auto& set_to = sets[col + 2];
    if (set_to.size() == 0) {
        auto size = 3 * set_from.size() - 3;
        set_to.resize(size);
        for (uint j=0; j<ELEMENTS; ++j) {
            set_to[0     ][j] = ELEMENT_MIN;
            set_to[size-1][j] = ELEMENT_MAX;
        }
    }

    // 4 way merge: {set_from - current} (twice), {set_from} and
    // {set_from + current}
    Element const* RESTRICT ptr_sum    = &set_sum[2][0];
    Element const* RESTRICT ptr_low1   = &pos_current.first[-1][0];
    Element const* RESTRICT ptr_low2   = &pos_current.second[0][0];
    Element const* RESTRICT ptr_middle = &set_from[1][0];
    auto from_end = set_from.cend() - 2;
    Column col_low1, col_low2, current2;
    for (uint j=0; j<ELEMENTS; ++j) {
        col_low1[j]  = current[j] - ptr_low1[j];
        col_low2[j]  = *ptr_low2++ - current[j];
        current2[j]     = current[j]*2;
    }

    int c;
    auto pos_from_low2 =
        lower_bound(pos_current.first, set_from.cend()-1, current2);
    c = cmp(current2, *pos_from_low2);
    if (c == 0) {
        // id_out << "Hit on low2 " << col << endl;
        return col;
    }
    auto skip_begin =
        (pos_from_low2 - pos_current.first) +
        (pos_current.first - set_from.cbegin() - 1) * 2;
    if (false) {
        lock_guard<mutex> lock(mutex_out);
        indent(col);
        cout << "low=" << pos_current.first - set_from.cbegin() - 1 <<
            ", low2=" << pos_from_low2  - set_from.cbegin() - 1 <<
            ", skip_begin=" << skip_begin << endl;
    }

    Element* RESTRICT ptr_to  = &set_to[1][0];
    Element* RESTRICT ptr_end = ptr_to + skip_begin * ELEMENTS;
    while (ptr_to < ptr_end) {
        if (false) {
            lock_guard<mutex> lock(mutex_out);
            indent(col);
            cout << "col_low1" << col_low1 << "\n";
            indent(col);
            cout << "col_low2" << col_low2 << "\n";
        }

        c = cmp(col_low1, col_low2);
        if (c == -1) {
            c = cmp(col_low1, ptr_middle);
            if (c == -1) {
                // cout << "LOW1\n";
                ptr_low1 -= ELEMENTS;
                for (uint j=0; j<ELEMENTS; ++j) {
                    *ptr_to++   = col_low1[j];
                    col_low1[j] = current[j] - ptr_low1[j];
                }
                goto SUM_L;
            }
            if (c ==  1) goto MIDDLE_L;
            // low1 == middle
            // cout << "low1 == middle\n";
            return col;
        }
        if (c ==  1) {
            c = cmp(col_low2, ptr_middle);
            if (c == -1) {
                // cout << "LOW2\n";
                for (uint j=0; j<ELEMENTS; ++j) {
                    *ptr_to++   = col_low2[j];
                    col_low2[j] = *ptr_low2++ - current[j];
                }
                goto SUM_L;
            }
            if (c ==  1) goto MIDDLE_L;
            // low2 == middle
            // cout << "low2 == middle\n";
            return col;
        }
        // low1 == low2
        // cout << "low1 == low2\n";
        return col;

      SUM_L:
        for (int j=-static_cast<int>(ELEMENTS); j<0; ++j) {
            if (ptr_to[j] > ptr_sum[j]) {
                ptr_sum += ELEMENTS;
                goto SUM_L;
            }
            if (ptr_to[j] < ptr_sum[j]) goto DONE_L;
        }
        // sum == to
        return col;

      MIDDLE_L:
        // cout << "MIDDLE_L\n";
        for (uint j=0; j<ELEMENTS; ++j)
            *ptr_to++ = *ptr_middle++;
        // {middle} can never be in {sum} or we would already have found
        // this on the previous level
        goto DONE_L;

      DONE_L:;
    }

    Element const* RESTRICT ptr_high   = &set_from[1][0];
    Column from_high, col_high;
    for (uint j=0; j<ELEMENTS; ++j) {
        from_high[j] = (*from_end)[j] - current[j];
        *ptr_to++ = current[j];
        col_high[j] = *ptr_high++ + current[j];
    }

    auto pos_from_high =
        lower_bound(set_from.cbegin()+1, from_end, from_high);
    if (true) {
        // Setting this to false makes the program 10% slower even though
        // the compare is never true ????
        c = cmp(from_high, *pos_from_high);
        if (c == 0) {
            // This never happens. Why ?
            throw(logic_error("Unexpected hit on high"));
            id_out << "Hit on high" << endl;
            return col;
        }
    }
    auto skip_end = set_from.cend() - 1 - pos_from_high;
    if (false) {
        lock_guard<mutex> lock(mutex_out);
        indent(col);
        cout << "end=" << pos_from_high - set_from.cbegin() - 1 <<
            ", skip_end=" << skip_end << endl;
    }
    ptr_end = &set_to[set_to.size()-1][0] - skip_end * ELEMENTS;
    if (false) {
        lock_guard<mutex> lock(mutex_out);
        indent(col);
        cout << "middle=" << (ptr_end-ptr_to)/ELEMENTS << endl;
    }
    while (ptr_to < ptr_end) {
        if (false) {
            lock_guard<mutex> lock(mutex_out);
            indent(col);
            cout << "col_low2" << col_low2 << "\n";
            indent(col);
            cout << "col_high" << col_high << "\n";
        }

        c = cmp(col_low2, ptr_middle);
        if (c == -1) {
            c = cmp(col_low2, col_high);
            if (c == -1) {
                // cout << "LOW2\n";
                for (uint j=0; j<ELEMENTS; ++j) {
                    *ptr_to++   = col_low2[j];
                    col_low2[j] = *ptr_low2++ - current[j];
                }
                goto SUM;
            }
            if (c == 1) goto HIGH;
            // low2 == high
            // cout << "low2 == high\n";
            return col;
        }
        if (c ==  1) goto MIDDLE;
        // low2 == middle
        // cout << "low2 == middle\n";
        return col;

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
        return col;

      HIGH:
        // cout << "HIGH0\n";
        for (uint j=0; j<ELEMENTS; ++j) {
            *ptr_to++ = col_high[j];
            col_high[j] = *ptr_high++ + current[j];
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
        return col;
      DONE:;
    }
    ptr_high -= ELEMENTS;

    ptr_end = &set_to[set_to.size()-1][0];
    while (ptr_to < ptr_end) {
        for (uint j=0; j<ELEMENTS; ++j)
            *ptr_to++ = *ptr_high++ + current[j];
      SUM_H:
        for (int j=-static_cast<int>(ELEMENTS); j<0; ++j) {
            if (ptr_to[j] > ptr_sum[j]) {
                ptr_sum += ELEMENTS;
                goto SUM_H;
            }
            if (ptr_to[j] < ptr_sum[j]) goto DONE_H;
        }
        // sum == to
        return col;
      DONE_H:;
    }

    // New column is OK

    if (INSTRUMENT) ++instrument.success;

    if (DEBUG_SET) {
        lock_guard<mutex> lock{mutex_out};
        for (auto const& to: set_to) {
            indent(col);
            cout << to << "\n";
        }
    }

    auto col1 = col+1;
    if (false) {
        lock_guard<mutex> lock{mutex_out};
        for (uint i=0; i<col1+2; ++i) {
            cout << "Set " << i << "\n";
            auto& set = sets[i];
            for (auto& column: set)
                cout << column << "\n";
        }
    }

    if (col1 >= max_col_) {
        max_col_ = col1;
        lock_guard<mutex> lock{mutex_info};
        if (max_col_ > max_cols_) {
            max_cols_ = max_col_;

            uint8_t buffer[1+(MAX_ROWS+MAX_COLS-1+7)/8];
            buffer[0] = col1;
            uint bytes = 1 + (rows+col1-1+7)/8;
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

            id_out << "rows=" << rows << ",cols=" << col1 << " (" << elapsed() << " s)\n";
            print(col1);
        } else {
            max_col_ = max_cols_;
            auto& info = col_info[current_col_rev_];
            if (col1 == max_col_ && col1 > info.min) {
                info.min = col1;
                static_assert(is_same<decltype(current_col_rev_), Index>::value,
                              "current_col_rev_ is not of type Index");
                uint8_t buffer[1 + sizeof(Index)];
                buffer[0] = 1;
                memcpy(&buffer[1], &current_col_rev_, sizeof(Index));
                pipe_write(buffer, sizeof(buffer));
            }
        }
    }
    if (col1 > col_info[col_].min) {
        lock_guard<mutex> lock{mutex_info};
        if (col1 > col_info[col_].min) col_info[col_].min = col1;
    }

    Index current_col, current_col_rev;
    if (SKIP_FRONT) {
        current_col = current_col_;
        current_col_ >>= 1;
    }
    current_col_rev  = current_col_rev_;
    current_col_rev_ = current_col_rev_ << 1 & mask_rows;

    shift(current_);
    current2 = current_;
    shift(current2);
    Column current22;
    if (SKIP2)
        for (uint j=0; j<ELEMENTS; ++j)
            current22[j] = current2[j] * 2;

    Set::const_iterator pos_sum, pos_to, pos_to2;
    uint rc = col;
    if (zero) {
        bool ok[2] = { true, true };
        pos_to = lower_bound(set_to.cbegin()+1, set_to.cend()-1, current2);
        c = cmp(*pos_to, current2);
        if (c == 0) {
            ok[0] = false;
            ++pos_to;
        }
        pos_sum = lower_bound(set_sum.cbegin()+1, set_sum.cend()-1, current2);
        c = cmp(*pos_sum, current2);
        if (c == 0) {
            ok[0] = false;
            ++pos_sum;
        }
        if (SKIP2) {
            pos_to2 = lower_bound(set_to.cbegin()+1, set_to.cend()-1, current22);
            c = cmp(*pos_to2, current22);
            if (c == 0) {
                ok[0] = false;
                ++pos_to2;
            }
            current22[ELEMENTS-1] += 2;
        }

        current2 [ELEMENTS-1] += 1;
        c = cmp(*pos_to, current2);
        if (c == 0) {
            ok[1] = false;
            ++pos_to;
        } else {
            c = cmp(*pos_sum, current2);
            if (c == 0)
                ok[1] = false;
            else if (SKIP2) {
                c = cmp(*pos_to2, current22);
                if (c < 0) {
                    ++pos_to2;
                    c = cmp(*pos_to2, current22);
                }
                if (c == 0) {
                    ok[1] = false;
                    ++pos_to2;
                }
            }
        }

        current2[ELEMENTS-1] +=  ROW_FACTOR-1;
        if (SKIP2) current22[ELEMENTS-1] += (ROW_FACTOR-1) * 2;
        if (false) {
            lock_guard<mutex> lock{mutex_out};
            indent(col);
            cout << "ok0 = [" << (ok[0] ? 1 : 0) << "," << (ok[1] ? 1 : 0) << "]\n";
        }

        if (ok[0] || ok[1] || col1 >= max_col_) {
            side[rows + col] = 0;
            rc = extend(col1, ok[0], ok[1]);
        }
        pos_to  = lower_bound(pos_to, min(pos_to + col1, set_to.cend()-1), current2);
        pos_sum = lower_bound(pos_sum, min(pos_sum + col1, set_sum.cend()-1), current2);
        if (SKIP2)
            pos_to2 = lower_bound(pos_to, min(pos_to + 2*col1+3, set_to.cend()-1), current22);
    } else {
        current2 [ELEMENTS-1] += ROW_FACTOR;
        pos_to  = lower_bound(set_to.cbegin()+1, set_to.cend()-1, current2);
        pos_sum = lower_bound(set_sum.cbegin()+1, set_sum.cend()-1, current2);
        if (SKIP2) {
            current22[ELEMENTS-1] += ROW_FACTOR * 2;
            pos_to2 = lower_bound(set_to.cbegin()+1, set_to.cend()-1, current22);
        }
    }

    if (one) {
        bool ok[2] = { true, true };
        c = cmp(*pos_to, current2);
        if (c == 0) {
            ok[0] = false;
            ++pos_to;
        }
        c = cmp(*pos_sum, current2);
        if (c == 0) {
            ok[0] = false;
            ++pos_sum;
        }
        if (SKIP2) {
            c = cmp(*pos_to2, current22);
            if (c == 0) {
                ok[0] = false;
                ++pos_to2;
            }
            current22[ELEMENTS-1] += 2;
        }

        current2 [ELEMENTS-1] += 1;
        c = cmp(*pos_to, current2);
        if (c == 0) ok[1] = false;
        else {
            c = cmp(*pos_sum, current2);
            if (c == 0) ok[1] = false;
            else if (SKIP2) {
                c = cmp(*pos_to2, current22);
                if (c < 0) {
                    ++pos_to2;
                    c = cmp(*pos_to2, current22);
                }
                if (c == 0) ok[1] = false;
            }
        }

        if (false) {
            lock_guard<mutex> lock{mutex_out};
            indent(col);
            cout << "ok1 = [" << (ok[0] ? 1 : 0) << "," << (ok[1] ? 1 : 0) << "]\n";
        }

        if (ok[0] || ok[1] || col1 >= max_col_) {
            if (SKIP_FRONT) current_col_     += top_row;
            current_col_rev_ += 1;
            ++current_[ELEMENTS-1];
            side[rows + col] = 1;
            uint m = extend(col1, ok[0], ok[1]);
            if (m > rc) rc = m;
        }
    }

    current_col_rev_ = current_col_rev;
    if (SKIP_FRONT) current_col_ = current_col;
    current_ = current;

    return rc;
}

void State::print(uint cols) {
    for (auto r=0U; r<rows;r++) {
        for (auto c=0U; c<cols;c++) {
            id_out << static_cast<uint>(side[rows-1-r+c]) << " ";
        }
        id_out << "\n";
    }
    id_out << "----------" << endl;
}

void Connection::id_set() {
    char hostname[100];
    hostname[sizeof(hostname)-1] = 0;
    int rc = gethostname(hostname, sizeof(hostname)-1);
    if (rc) id.assign("Dunno");
    else id.assign(hostname);
    id.append(" ");
    auto pid = getpid();
    id.append(to_string(pid));
}

State* Connection::new_state() {
    State* state = new State(this);
    states.emplace_back(state);
    idle_.emplace_back(state);
    return state;
}

Connection::Connection(string const& host, string const& service, uint nr_threads) :
    nr_threads_{nr_threads},
    wanted_threads_{nr_threads}
{
    id_set();

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
        io_out_.set<Connection, &Connection::writable>(this);
        io_in_ .set<Connection, &Connection::readable>(this);
        timer_greeting_.set<Connection, &Connection::timeout_greeting>(this);
        timer_greeting_.start(TIMEOUT_GREETING);
        timer_output_.set<TimeBuffer::flush>();
        timer_output_.start(0, 1);
        sig_usr1_.set<Connection, &Connection::threads_less>(this);
        sig_usr1_.start(SIGUSR1);
        sig_usr2_.set<Connection, &Connection::threads_more>(this);
        sig_usr2_.start(SIGUSR2);
        sig_sys_.set<Connection, &Connection::state_show>(this);
        sig_sys_.start(SIGSYS);

        return;
    }
    freeaddrinfo(results);
    throw(runtime_error("Could not make a connection"));
}

void Connection::timeout_greeting(ev::timer& timer, int revents) {
    timed_out << "Connection greeting timed out" << endl;
    stop();
}

void Connection::drop_idle() {
    while (states.size() > nr_threads_ && idle_.size()) {
        State* state = idle_.back();
        int i = states.size();
        while (--i >= 0)
            if (states[i].get() == state) {
                if (i != static_cast<int>(states.size())-1)
                    swap(states[i], states[states.size()-1]);
                idle_.pop_back();
                states.pop_back();
                timed_out << "Changed number of threads to " << states.size() << endl;
                goto CONTINUE;
            }
        throw(logic_error("idle state is not in state set"));
      CONTINUE:;
    }
}

void Connection::got_proto(uint8_t const* ptr, size_t length) {
    if (length != 1)
        throw(logic_error("Invalid proto length " + to_string(length)));

    if (*ptr != PROTO_VERSION)
        throw(runtime_error("Server speaks protocol version " + to_string(static_cast<uint>(*ptr)) + " while I speak " + to_string(static_cast<uint>(PROTO_VERSION))));
    put(ID, id);
    uint8_t message[2] = { PROGRAM_VERSION, static_cast<uint8_t>(nr_threads_) };
    put(PROGRAM, message, sizeof(message));
}

void Connection::got_size(uint8_t const* ptr, size_t length) {
    rows = *ptr;
    if (rows > MAX_ROWS) throw(range_error("Rows " + to_string(rows) + " > MAX_ROWS=" + to_string(MAX_ROWS)));
    timed_out << "Client " << id << " starts working on " << rows << " rows" << endl;
    timer_greeting_.stop();

    nr_rows = static_cast<Index>(1) << rows;
    col_info.resize(nr_rows);
    col_info[0] = ColInfo{1, 1};
    mask_rows = nr_rows - 1;
    top_row = nr_rows / 2;

    top_ = POWN(ROW_FACTOR, (rows-1) % ROWS_PER_ELEMENT);
    top_offset_ = ELEMENTS - 1 - (rows-1) / ROWS_PER_ELEMENT;

    if (states.size()) throw(logic_error("Multiple states initializations"));
    for (uint i = 0; i < nr_threads_; ++i) new_state();
}

void Connection::got_info(uint8_t const* ptr, size_t length) {
    if (length != 6)
        throw(logic_error("Invalid info length " + to_string(length)));

    Index index = get_index(ptr);
    auto& info = col_info.at(index);
    if (ptr[0] > info.min || ptr[1] < info.max) {
        lock_guard<mutex> lock{mutex_info};
        if (ptr[0] > info.min) {
            info.min = ptr[0];
            if (ptr[0] > max_cols_) max_cols_ = ptr[0];
        }
        if (ptr[1] < info.max) info.max = ptr[1];
    }
    // timed_out << "Got info " << index << " [" << static_cast<uint>(info.min) << ", " << static_cast<uint>(info.max) << "]\n";
}

void Connection::got_fork(uint8_t const* ptr, size_t length) {
    if (length != 0)
        throw(logic_error("Invalid fork length " + to_string(length)));
    if (forked_) throw(logic_error("Multiple forks"));

    for (auto& state: states) state->fork();
    forked_ = true;
    if (nr_threads_ != wanted_threads_) put1(NR_THREADS, wanted_threads_);
}

void Connection::got_work(uint8_t const* ptr, size_t length) {
    if (length != 4)
        throw(logic_error("Invalid work length " + to_string(length)));

    Index col = get_index(ptr);
    if (!idle_.size()) throw(logic_error("Got work while not idle"));
    auto state = idle_.back();
    idle_.pop_back();
    state->got_work(col);
}

void Connection::got_solution(uint8_t const* ptr, size_t length) {
    if (length < 2)
        throw(logic_error("Invalid solution length " + to_string(length)));

    uint cols = *ptr++;
    if (cols > MAX_COLS)
        throw(logic_error("Got a solution for " + to_string(cols) + " columns"));
    if (cols > max_cols_) {
        lock_guard<mutex> lock{mutex_info};
        if (cols > max_cols_) max_cols_ = cols;
    }

    array<uint8_t, (MAX_COLS + MAX_ROWS - 1 + 7) / 8 * 8> matrix;
    auto l = length - 1;
    if (l != (rows+cols-1+7)/8)
        throw(logic_error("Solution with inconsistent length"));
    for (uint i=0; l; --l) {
        uint bits = *ptr++;
        for (int j=0; j<8; ++j) {
            matrix[i++] = bits & 1;
            bits >>= 1;
        }
    }
    timed_out << "Some computer found: rows=" << rows << ",cols=" << cols << "\n";
    for (uint r=0; r<rows; ++r) {
        auto m = &matrix[rows-1-r];
        for (uint c=0; c<cols; ++c)
            timed_out << static_cast<uint>(*m++) << " ";
        timed_out << "\n";
    }
    timed_out << "----------" << endl;
}

void Connection::got_idle(uint8_t const* ptr, size_t length) {
    if (length != 0)
        throw(logic_error("Invalid idle length " + to_string(length)));

    if (idle_.size() == 0)
        throw(logic_error("Idle while working"));
    timed_out << "Idle";
    for (State* state: idle_) timed_out << " " << state->id();
    timed_out << endl;
}

void Connection::got_finished(uint8_t const* ptr, size_t length) {
    if (length != 0)
        throw(logic_error("Invalid finished length " + to_string(length)));

    if (idle_.size() != nr_threads_)
        throw(logic_error("Finished while working"));
    stop();
    timed_out << "Finished" << endl;
}

void Connection::got_threads (uint8_t const* ptr, size_t length) {
    if (length != 1)
        throw(logic_error("Invalid threads length " + to_string(length)));

    if (!forked_) throw(logic_error("Thread change without fork"));

    nr_threads_ = *ptr;
    while (states.size() < nr_threads_) {
        new_state()->fork();
        timed_out << "Changed number of threads to " << states.size() << endl;
    }
    drop_idle();
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
                case NR_THREADS:
                  got_threads(ptr, length);
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
        timed_out << "Could not read: " << err << endl;
    } else {
        // Close
        timed_out << "Peer closed connection" << endl;
    }
    stop();
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

void Connection::threads_more(ev::sig& watcher, int revents) {
    if (wanted_threads_ < 255) {
        ++wanted_threads_;
        timed_out << "Increase wanted number of threads to " << wanted_threads_;
        if (forked_) put1(NR_THREADS, wanted_threads_);
    } else
        timed_out << "Wanted number of threads already at maximum 255";
    timed_out << endl;
}

void Connection::threads_less(ev::sig& watcher, int revents) {
    if (wanted_threads_ > 0) {
        --wanted_threads_;
        timed_out << "Decrease wanted number of threads to " << wanted_threads_;
        if (forked_) put1(NR_THREADS, wanted_threads_);
    } else
        timed_out << "Wanted number of threads already at minimum 0";
    timed_out << endl;
}

void Connection::instruments_show() const {
    if (!INSTRUMENT) return;

    Instruments instruments;
    for (auto& state: states) instruments += state->instruments();
    timed_out << "\n" << instruments;
    timed_out.flush();
}

void Connection::state_show(ev::sig& watcher, int revents) {
    instruments_show();
    // This has delayed output, so put it last
    for (auto& state: states) state->show();
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

Listener::Listener(uint port) {
    if (port < 1 || port >= 65536)
        throw(out_of_range("Invalid bind port " + to_string(port)));

    // Create socket
    int fd = socket(AF_INET6, SOCK_STREAM, 0);
    if (fd < 0)
        throw(system_error(errno, system_category(), "Could not create socket"));

    // Set non-blocking
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) {
        auto err = errno;
        close(fd);
        throw(system_error(err, system_category(), "Could not fcntl"));
    }
    flags |= O_NONBLOCK;
    flags = fcntl(fd, F_SETFL, flags);
    if (flags < 0) {
        auto err = errno;
        close(fd);
        throw(system_error(err, system_category(), "Could not fcntl"));
    }

    // Reuse addr
    int enable = 1;
    int rc = setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(enable));
    if (rc < 0) {
        auto err = errno;
        close(fd);
        throw(system_error(err, system_category(), "Could not set SO_REUSEADDR"));
    }

    // Turn off IPV6 only
    enable = 0;
    rc = setsockopt(fd, IPPROTO_IPV6, IPV6_V6ONLY, &enable, sizeof(enable));
    if (rc < 0) {
        auto err = errno;
        close(fd);
        throw(system_error(err, system_category(), "Could not unset IPV6_V6ONLY"));
    }

    // Set keepalive
    enable = 1;
    rc = setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &enable, sizeof(enable));
    if (rc < 0) {
        auto err = errno;
        close(fd);
        throw(system_error(err, system_category(), "Could not set SO_KEEPALIVE"));
    }

    // Bind
    struct sockaddr_in6 addr6;
    addr6.sin6_family   = AF_INET6;
    addr6.sin6_addr     = in6addr_any;
    addr6.sin6_port     = htons(port);
    addr6.sin6_scope_id = 0;
    rc = bind(fd, reinterpret_cast<struct sockaddr *>(&addr6), sizeof(addr6));
    if (rc < 0) {
        auto err = errno;
        close(fd);
        throw(system_error(err, system_category(), "Could not bind to port " + to_string(port)));
    }

    // Listen
    rc = listen(fd, BACKLOG);
    if (rc < 0) {
        auto err = errno;
        close(fd);
        throw(system_error(err, system_category(), "Could not listen"));
    }

    // Create (but do not activate) watch
    watch_.set<Listener, &Listener::connection>(this);

    fd_ = fd;
}

void Listener::connection(ev::io& watcher, int revents) {
    struct sockaddr_in6 addr6;
    socklen_t size = sizeof(addr6);
    int fd = accept(fd_, reinterpret_cast<struct sockaddr *>(&addr6), &size);
    if (fd < 0) {
        if (errno == EWOULDBLOCK || errno == EAGAIN || errno == EINTR) return;
        throw(system_error(errno, system_category(), "Could not accept"));
    }

    if (size != sizeof(addr6)) {
        close(fd);
        throw(logic_error("Accepted IPV6 connection but struct sockaddr size does not match"));
    }

    // Set non-blocking
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) {
        auto err = errno;
        close(fd);
        throw(system_error(err, system_category(), "Could not fcntl"));
    }
    if (!(flags & O_NONBLOCK)) {
        flags |= O_NONBLOCK;
        flags = fcntl(fd, F_SETFL, flags);
        if (flags < 0) {
            auto err = errno;
            close(fd);
            throw(system_error(err, system_category(), "Could not fcntl"));
        }
    }
    char buffer[100];
    auto ip = inet_ntop(addr6.sin6_family, &addr6.sin6_addr, buffer, sizeof(buffer));
    if (ip == nullptr) {
        auto err = errno;
        close(fd);
        throw(system_error(err, system_category(), "Could not inet_ntop"));
    }
    string peer{ip};
    peer.append(" ");
    peer.append(to_string(ntohs(addr6.sin6_port)));

    auto result = accepted.emplace(fd, Accept::Ptr{new Accept(this, fd)});
    auto& connection = *result.first->second;
    connection.peer(peer);
    connection.put1(PROTO, PROTO_VERSION);
    timed_out << "Accept " << fd << " from " << peer << endl;
}

void input(string const& name) {
    ifstream in{name};
    if (!in.is_open())
        throw(system_error(errno, system_category(), "Could not open " + name));
    string line;
    while (getline(in, line)) {
        if (line == TERMINATOR) {
            sort(col_known.begin(), col_known.end());
            for (Index i: col_known)
                out << i << " " << result_info[i] << "\n";
            out.flush();
            return;
        }
        istringstream iss{line};
        int64_t index, min_cols, max_cols, version;
        iss >> index >> min_cols >> max_cols >> version;
        if (!iss) throw(runtime_error("File " + name + ": Could not parse: " + line));
        if (index <= 0 || index >= nr_rows)
            throw(runtime_error("File " + name + ": Index out of range: " + line));
        if (min_cols < 0 || min_cols > MAX_COLS)
            throw(runtime_error("File " + name + ": Min out of range: " + line));
        if (max_cols < 0 || (max_cols > MAX_COLS && max_cols != ResultInfo::MAX))
            throw(runtime_error("File " + name + ": Max out of range: " + line));
        if (version <= 0 || version >= 256)
            throw(runtime_error("File " + name + ": Version out of range: " + line));
        if (min_cols > max_cols)
            throw(runtime_error("File " + name + ": Min > Max: " + line));

        ResultInfo new_info{min_cols,max_cols,version};
        if (new_info.max > max_cols_ && new_info.max != ResultInfo::MAX)
            max_cols_ = new_info.max;
        auto& info = result_info[index];
        if (info.version == 0) {
            info = new_info;
            col_known.emplace_back(index);
        } else {
            if (info.version != new_info.version) {
                out << index << " " << info << "\n";
                info.version = new_info.version;
            }
            if (new_info.min > info.min) info.min = new_info.min;
            if (new_info.max < info.max) info.max = new_info.max;
            if (info.min > info.max)
                throw(runtime_error("File " + name + ": Update sets Min > Max: " + line));
        }
        // timed_out << "index " << index << ", min " << min_cols << ",max " << max_cols << ", version " << version << endl;
    }
    throw(runtime_error("File " + name + " does not end on " + TERMINATOR));
}

void create_work() {
    vector<uint> col1;
    col1.resize(rows);

    uint count1 = (rows+1) / 2;
    for (uint c = 0; c < count1; ++c) col1[c] = c;

    while (count1) {
        Index col = 0;
        for (uint c=0; c<count1; ++c)
            col |= top_row >> col1[c];
        if (result_info[col].version == 0)
            col_work.emplace_back(col);

        uint c = count1;
        while (c > 0) {
            --c;
            auto c1 = ++col1[c];
            if (c1 <= rows - (count1-c)) {
                while (++c < count1) col1[c] = ++c1;
                goto DONE;
            }
        }
        if (count1*2 <= rows) count1 = rows + 1 - count1;
        else count1 = rows - count1;
        for (uint c=0; c<count1; ++c) col1[c] = c;
      DONE:;
    }
    nr_work = col_work.size();
    done_work = 0;
    solution[0] = 0;
}

void server(int argc, char** argv) {
    nr_rows = static_cast<Index>(1) << rows;
    top_row = nr_rows / 2;

    uint port = PORT;
    if (argc > 2) {
        auto i = atol(argv[2]);
        if (i <= 1)
            throw(range_error("Port must be > 0"));
        if (i >= 65536)
            throw(range_error("Port must be < 65536"));
        port = i;
    }

    result_info.resize(nr_rows);
    result_info[0] = ResultInfo(1, 1, 0);

    stringstream file_in, file_out;
    file_in  << PROGRAM_NAME << "." << rows << ".txt";
    file_out << PROGRAM_NAME << "." << rows << ".out.txt";
    auto const& name_in  = file_in .str();
    auto const& name_out = file_out.str();

    out_buffer.open(name_out);
    input(name_in);
    out_buffer.rename(name_in, true, false);

    create_work();

    if (col_work.size() == 0) {
        timed_out << "Nothing to be done" << endl;
    } else {
        auto rc = signal(SIGPIPE, SIG_IGN);
        if (rc == SIG_ERR)
            throw(logic_error("Could not ignore SIG PIPE"));

        Listener listener{port};

        timed_out << "Trying " << rows << " rows" << endl;

        listener.start();
        loop.run(0);
    }

    out << TERMINATOR << endl;
    out_buffer.fsync();
    out_buffer.close();
}

void client(int argc, char** argv) {
    uint nr_threads = min(thread::hardware_concurrency(), 255U);

    auto host = argc > 1 && argv[1][0] ? argv[1] : HOST;
    auto port = argc > 2 &&
        argv[2][0] && !(argv[2][0] == '0' && argv[2][1] == 0) ?
        string{argv[2]} : to_string(PORT);
    if (argc > 3) {
        auto l = atol(argv[3]);
        if (l < 1)   throw(range_error("nr threads must be >= 1"));
        if (l > 255) throw(range_error("nr threads must be <= 255"));
        nr_threads = l;
    }

    auto rc = signal(SIGPIPE, SIG_IGN);
    if (rc == SIG_ERR)
        throw(logic_error("Could not ignore SIG PIPE"));

    Connection connection{host, port, nr_threads};

    connection.start();
    loop.run(0);

    timed_out << "Processing finished" << endl;
    return;
}

int main(int argc, char** argv) {
    try {
        bool is_server = false;
        if (argc > 1) {
            char* ptr;
            long i = strtol(argv[1], &ptr, 0);
            while (isspace(*ptr)) ++ptr;
            if (*ptr == 0) {
                if (i < 2) throw(range_error("Rows must be >= 2"));
                if (i > MAX_ROWS)
                    throw(range_error("Rows must be <= " + to_string(MAX_ROWS)));
                rows = i;
                is_server = true;
            }
        }
        if (is_server) server(argc, argv);
        else           client(argc, argv);
    } catch(exception& e) {
        timed_out << "Error: " << e.what() << endl;
        exit(EXIT_FAILURE);
    }
    exit(EXIT_SUCCESS);
}
