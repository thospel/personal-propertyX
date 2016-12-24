/*
  Compile using something like:
    g++ -Wall -O3 -march=native -fstrict-aliasing -std=c++11 -g propertyXServer.cpp -lpthread -lev -o propertyXServer

  create a file propertyX.rows.txt containing just ----
  Run as:
    ./propertyXServer rows [host [port]]
  Then connect propertyXClients
  If propertyXServer is interrupted rename propertyX.rows.out.txt to propertyX.rows.txt, add a final ---- and restart
*/

#include <iostream>
#include <fstream>
#include <sstream>
#include <cstdint>
#include <climits>
#include <ctgmath>
#include <cerrno>
#include <stdexcept>
#include <memory>
#include <array>
#include <vector>
#include <deque>
#include <algorithm>
#include <map>
#include <set>
#include <chrono>
#include <limits>

#include <ev++.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <signal.h>
#include <netdb.h>
#include <arpa/inet.h>

using namespace std;

char const* PROGRAM_NAME = "propertyX";
uint8_t PROTO_VERSION = 1;

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
    GET_PROTO   = 1*256,
    GET_ID      = 2*256,
    GET_PROGRAM = 3*256,
};

using uint    = unsigned int;
using Index   = uint32_t;
using Element = uint64_t;
using sec = chrono::seconds;

int const PERIOD = 5*60;
// int const PERIOD = 1;
uint const ELEMENTS = 2;
uint const MAX_ROWS = 54;
uint const COL_FACTOR = (MAX_ROWS+1) | 1;		// 55
uint const ROW_ZERO   = COL_FACTOR/2;			// 27
uint const ROWS_PER_ELEMENT = CHAR_BIT*sizeof(Element) / log2(COL_FACTOR); // 11
uint const MAX_COLS = ROWS_PER_ELEMENT * ELEMENTS;	// 22
uint const DEFAULT_COLS = min(MAX_COLS, 10U);

int const BACKLOG = 5;
uint const PORT = 21453;
size_t const BLOCK = 65536;
ev_tstamp const TIMEOUT_GREETING = 10;
char const* TERMINATOR = "----";

uint cols = DEFAULT_COLS;
uint max_rows_ = 1;
Index nr_columns, top_column;
Index nr_work;
Index done_work;
ofstream out;
Index col_count;
uint period = 0;
array<uint8_t, (MAX_ROWS + MAX_COLS - 1 + 7) / 8 * 8> side;
array<uint8_t, 1+(MAX_ROWS + MAX_COLS - 1 + 7) / 8 * 8> solution;
chrono::steady_clock::time_point start;

ev::default_loop loop;

int64_t elapsed() {
    auto now = chrono::steady_clock::now();
    return chrono::duration_cast<sec>(now-time_start).count();
}

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

class Listener {
  public:
    Listener(uint port);
    ~Listener() { stop(); }
    int fd() const { return fd_; }
    void connection(ev::io& watcher, int revents);
    void start() { watch_.start(fd(), ev::READ); }
    void stop()  { 
        if (fd_ < 0) return;
        watch_.stop();
        close(fd_);
        fd_ = -1;
    }
  private:
    ev::io watch_;
    int fd_;
};

class Accept {
  public:
    Accept(Listener* listener, int fd);
    ~Accept();
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
    void put_known(size_t from=0);
    void put_info(Index index, ResultInfo const& info);
    void put_work();
    void peer(string const& peer) { peer_id_ = peer; }
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
    inline void got_id      (uint8_t const* ptr, size_t length);
    inline void got_program (uint8_t const* ptr, size_t length);
    inline void got_info    (uint8_t const* ptr, size_t length);
    inline void got_result  (uint8_t const* ptr, size_t length);
    inline void got_solution(uint8_t const* ptr, size_t length);

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
    int phase = GET_PROTO;
    int fd_;
    bool idle_ = false;
};

set<int> accepted_idle;
map<uint,unique_ptr<Accept>> accepted;

void Accept::timeout_greeting(ev::timer& timer, int revents) {
    cout << "Accept " << fd() << " (" << peer_id_ << ") greeting timed out" << endl;
    accepted.erase(fd());
}

void Accept::got_proto(uint8_t const* ptr, size_t length) {
    if (*ptr != PROTO_VERSION) {
        cout << "Server speaks protocol version " << static_cast<uint>(*ptr) <<  " while I speak " << static_cast<uint>(PROTO_VERSION) << endl;
        accepted.erase(fd());
        return;
    }
    put1(SIZE, cols);
    put_known();
    put(FORK);
}

void Accept::got_id(uint8_t const* ptr, size_t length) {
    peer_id_.assign(reinterpret_cast<char const*>(ptr), length);
}

void Accept::got_program(uint8_t const* ptr, size_t length) {
    program_version_ = *ptr++;
    work_max_         = *ptr;
    if (program_version_ == 0)
        throw(logic_error("Invalid program version 0"));
    if (work_max_ == 0)
        throw(logic_error("no threads"));
    cout << "Peer " << fd() << ": " << peer_id_ << " version " << program_version_ << ", " << work_max_ << " threads" << endl;
    timer_greeting_.stop();
    put_work();
}

void Accept::got_info(uint8_t const* ptr, size_t length) {
    Index index = get_index(ptr);
    auto& info = result_info.at(index);
    if (ptr[0] > info.min || ptr[1] < info.max) {
        if (info.version == 0)
            col_known.emplace_back(index);
        else if (info.version != program_version_) {
            out << index << " " << info << endl;
        }
        info.version = program_version_;
        if (ptr[0] > info.min) info.min = ptr[0];
        if (ptr[1] < info.max) info.max = ptr[1];
        out << index << " " << info << endl;
    }
    for (auto& element: accepted) {
        auto& connection = *element.second;
        if (&connection == this) continue;
        connection.put_info(index, info);
    }
}

void Accept::got_result(uint8_t const* ptr, size_t length) {
    auto index = get_index(ptr);
    if (!work_.erase(index))
        throw(logic_error("Result that was never requested: " + to_string(index)));
    put_work();

    ++done_work;
    uint elapsed_ = elapsed();
    if (elapsed_ >= period || done_work >= nr_work) {
        cout << "col=" << done_work << "/" << nr_work << " (" << static_cast<uint64_t>(100*1000)*done_work/nr_work/1000. << "% " << elapsed_ << " s)" << endl;
        period = (elapsed_/PERIOD+1)*PERIOD;
    }

    // if (done_work >= nr_work) loop.unloop();
    if (done_work >= nr_work) {
        listener_->stop();
        for (auto& element: accepted) {
            auto& connection = *element.second;
            connection.put(FINISHED);
        }
        cout << "Finished" << endl;
    }
}

void Accept::got_solution(uint8_t const* ptr, size_t length) {
    auto data = ptr;
    uint rows = *ptr++;
    if (rows <= max_rows_) return;

    if (rows > MAX_ROWS)
        throw(logic_error("Got a solution for " + to_string(rows) + " rows"));
    max_rows_ = rows;
    memcpy(&solution[0], data, length);

    auto l = length - 1;
    if (l != (cols+rows-1+7)/8)
        throw(logic_error("Solution with inconsistent length"));
    for (uint i=0; l; --l) {
        uint bits = *ptr++;
        for (int j=0; j<8; ++j) {
            side[i++] = bits & 1;
            bits >>= 1;
        }
    }
    cout << "cols=" << cols << ",rows=" << rows << " (" << elapsed() << " s)\n";
    for (uint c=0; c<cols; ++c) {
        auto s = &side[cols-1-c];
        for (uint r=0; r<rows; ++r)
            cout << static_cast<uint>(*s++) << " ";
        cout << "\n";
    }
    cout << "----------" << endl;

    for (auto& element: accepted) {
        auto& connection = *element.second;
        if (&connection == this) continue;
        connection.put(SOLUTION, data, length);
    }
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
                default: throw
                    (range_error("Unknown message type " +
                                 to_string(static_cast<uint>(ptr[-1])) + " in phase " + to_string(phase >> 8)));
            }
            in_.erase(0, wanted);
        }
        return;
    }
    if (rc < 0) {
        if (errno == EWOULDBLOCK || errno == EAGAIN || errno == EINTR) return;
        auto err = strerror(errno);
        cout << "Read error from " << peer_id_ << ":" << err << endl;
    } else {
        // Close
        cout << "Accept " << fd() << " closed by " << peer_id_ << endl;
    }
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
        cout << "Write error to " << peer_id_ << ":" << err << endl;
    } else {
        // rc == 0
        cout << "Zero length write to " << peer_id_ << endl;
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
        Accept& connection = *accepted[fd];
        connection.put_work();
    }
    // fd_ = -1;
}

void Accept::put_known(size_t from) {
    uint rows = solution[0];
    if (rows) {
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
    if (work_.size() == work_max_)
        throw(logic_error("Accept is already full of work"));
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
            start = chrono::steady_clock::now();
        }
    }
    if (work_.size() == work_max_) {
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

    auto result = accepted.emplace(fd, std::unique_ptr<Accept>{new Accept(this, fd)});
    auto& connection = *result.first->second;
    connection.peer(peer);
    connection.put1(PROTO, PROTO_VERSION);
    cout << "Accept " << fd << " from " << peer << endl;
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
        int64_t index, min_rows, max_rows, version;
        iss >> index >> min_rows >> max_rows >> version;
        if (!iss) throw(runtime_error("File " + name + ": Could not parse: " + line));
        if (index <= 0 || index >= nr_columns)
            throw(runtime_error("File " + name + ": Index out of range: " + line));
        if (min_rows < 0 || min_rows > MAX_ROWS)
            throw(runtime_error("File " + name + ": Min out of range: " + line));
        if (max_rows < 0 || max_rows > MAX_ROWS)
            throw(runtime_error("File " + name + ": Max out of range: " + line));
        if (version <= 0 || version >= 256)
            throw(runtime_error("File " + name + ": Version out of range: " + line));
        if (min_rows > max_rows)
            throw(runtime_error("File " + name + ": Min > Max: " + line));

        ResultInfo new_info{min_rows,max_rows,version};
        if (new_info.max > max_rows_ && new_info.max != ResultInfo::MAX)
            max_rows_ = new_info.max;
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
        // cout << "index " << index << ", min " << min_rows << ",max " << max_rows << ", version " << version << endl;
    }
    throw(runtime_error("File " + name + " does not end on " + TERMINATOR));
}

void create_work() {
    vector<uint> col1;
    col1.resize(cols);

    uint count1 = (cols+1) / 2;
    for (uint c = 0; c < count1; ++c) col1[c] = c;

    while (count1) {
        Index col = 0;
        for (uint c=0; c<count1; ++c)
            col |= top_column >> col1[c];
        if (result_info[col].version == 0)
            col_work.emplace_back(col);

        uint c = count1;
        while (c > 0) {
            --c;
            auto c1 = ++col1[c];
            if (c1 <= cols - (count1-c)) {
                while (++c < count1) col1[c] = ++c1;
                goto DONE;
            }
        }
        if (count1*2 <= cols) count1 = cols + 1 - count1;
        else count1 = cols - count1;
        for (uint c=0; c<count1; ++c) col1[c] = c;
      DONE:;
    }
    nr_work = col_work.size();
    done_work = 0;
    solution[0] = 0;
}

void my_main(int argc, char** argv) {
    if (argc > 1) {
        auto i = atol(argv[1]);
        if (i < 2) throw(range_error("Column must be >= 2"));
        if (i > MAX_COLS)
            throw(range_error("Column must be <= " + to_string(MAX_COLS)));
        cols = i;
    }

    nr_columns   = static_cast<Index>(1) << cols;
    top_column   = nr_columns / 2;

    uint port = PORT;
    if (argc > 2) {
        auto i = atol(argv[mp2]);
        if (i <= 1)
            throw(range_error("Port must be > 0"));
        if (i >= 65536)
            throw(range_error("Port must be < 65536"));
        port = i;
    }

    result_info.resize(nr_columns);
    result_info[0] = ResultInfo(1, 1, 0);

    stringstream file_in, file_out;
    file_in  << PROGRAM_NAME << "." << cols << ".txt";
    file_out << PROGRAM_NAME << "." << cols << ".out.txt";
    out.open(file_out.str());
    if (!out.is_open())
        throw(system_error(errno, system_category(), "Could not create " + file_out.str()));
    out.exceptions(std::ifstream::failbit | std::ifstream::badbit);
    input(file_in.str());

    create_work();

    if (col_work.size() == 0) {
        cout << "Nothing to be done" << endl;
    } else {
        auto rc = signal(SIGPIPE, SIG_IGN);
        if (rc == SIG_ERR)
            throw(logic_error("Could not ignore SIG PIPE"));

        Listener listener{port};

        cout << "Trying " << cols << " columns" << endl;

        listener.start();
        loop.run(0);
    }

    out << TERMINATOR << "\n";
    out.close();
    {
        auto in_name  = file_in .str();
        auto out_name = file_out.str();
        auto rc = rename(out_name.c_str(), in_name.c_str());
        if (rc)
            throw(system_error(errno, system_category(), "Could not rename " + out_name + " to " + in_name));
    }
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
