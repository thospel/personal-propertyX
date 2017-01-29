#include "propertyX.hpp"
#include "log_buffer.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <iomanip>
#include <thread>

#include <fcntl.h>
#include <unistd.h>
#include <netdb.h>

using namespace std;

char const* HOST = "localhost";

bool const SKIP_FRONT = true;
bool const SKIP_BACK  = true;
bool const DEBUG_SET  = false;
bool const SKIP2      = false;
bool const INSTRUMENT = false;
bool const EARLY_MIN  = false;

Element constexpr ELEMENT_FILL(Element v = ROW_ZERO, int n = ROWS_PER_ELEMENT) {
    return n ? ELEMENT_FILL(v, n-1) * ROW_FACTOR + v : 0;
}
uElement constexpr POWN(uElement v, int n) {
    return n ? POWN(v, n-1)*v : 1;
}
uElement const ELEMENT_TOP  = POWN(ROW_FACTOR, ROWS_PER_ELEMENT - 1);
Element const ELEMENT_MIN = numeric_limits<Element>::min() + ELEMENT_FILL(1);
Element const ELEMENT_MAX = numeric_limits<Element>::max() - ELEMENT_FILL(1);

STATIC thread forked;
STATIC mutex mutex_info;
STATIC Element top_;
STATIC Index mask_rows;
STATIC uint top_offset_;

class Connection;

struct Instrument {
    Instrument& operator+=(Instrument const& add) {
        calls   += add.calls;
        success += add.success;
        merge   += add.merge;
        front   += add.front;
        back    += add.back;
        return *this;
    }
    uint64_t calls   = 0;
    uint64_t success = 0;
    uint64_t merge   = 0;
    uint64_t front   = 0;
    uint64_t back    = 0;
};
ostream& operator<<(ostream& os, Instrument const& instrument) {
    os << setw(12) << instrument.calls << " " << setw(12) << instrument.merge << " " << setw(12) << instrument.success << " " << setw(12) << instrument.front << " " << setw(12) << instrument.back << " " << static_cast<double>(instrument.success) / instrument.calls << " " << static_cast<double>(instrument.merge) / instrument.calls;
    return os;
}

struct Instruments: public array<Instrument, MAX_COLS> {
    Instruments& operator+=(Instruments const& add) {
        for (uint i=0; i<MAX_COLS; ++i) (*this)[i] += add[i];
        return *this;
    }
};
ostream& operator<<(ostream& os, Instruments const& instruments) {
    os << "rows" << setw(11) << "calls" << setw(13) << "merges" << setw(13) << "sucess" << setw(13) << "skip_front" << setw(13) << "skip_back" << setw(6) << "s/c"  << setw(6) << "m/c" << "\n";
    for (uint i=0; i<MAX_COLS; ++i) {
        auto const& instrument = instruments[i];
        if (instrument.calls == 0) break;
        os << setw(2) << i << ":" << instrument << "\n";
    }
    return os;
}

class State {
  public:
    using Ptr = unique_ptr<State>;

    State(Connection* connection);
    State(State const&) = delete;
    ~State();
    void fork();
    void got_work(Index col);
    uint id() const { return id_; }
    double elapsed() const {
        auto now = chrono::steady_clock::now();
        std::chrono::duration<double> e = now-start_;
        return e.count();
    }
    void show() { show_ = 1; }
    Instruments const& instruments() const { return instruments_; }
  private:
    void stop() COLD { io_in_.stop(); }
    void pipe_write(void const* ptr, size_t size);
    void readable(ev::io& watcher, int revents);
    void worker();
    NOINLINE void skip_message(uint col, bool front) COLD;
    NOINLINE uint extend(uint row, bool zero = true, bool one = true) HOT;
    void print(uint cols);

    Column current_;
    vector<Set> sets;
    TimeStream id_out;
    string pipe_in_;
    Connection* const connection_;
    ev::io io_in_;
    thread thread_;
    condition_variable cv_col;
    mutex mutex_col;
    chrono::steady_clock::time_point start_;
    atomic<Index> input_row_;
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
STATIC vector<ColInfo> col_info;

template <typename T1, typename T2>
inline int cmp(T1 const& left, T2 const& right) {
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
    void io_flush(ev::timer& w, int revents) {
        TimeBuffer::flush();
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
    inline void got_id      (uint8_t const* ptr, size_t length);
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
    id_out{"%F %T: Worker " + to_string(next_id) + ": "},
    connection_{connection},
    show_{0}
{
    id_ = next_id++;
    id_out << fixed << setprecision(3);

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
        {
            lock_guard<mutex> lock(mutex_col);
            input_row_ = 0;
            cv_col.notify_one();
        }
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
                // A final row that is a tie for current best
                if (pipe_in_.size() < 1 + sizeof(Index)) break;
                Index col;
                memcpy(&col, pipe_in_.data()+1, sizeof(Index));
                pipe_in_.erase(0, 1 + sizeof(Index));
                if (EARLY_MIN && col_info[col].min >= max_cols_)
                    connection_->put_known(col);
            } else if (cols == 0) {
                // Row finished
                ++processed_;
                connection_->put_known(col_);
                connection_->put4(RESULT, col_);
                connection_->idle(this);
                idle = true;
                pipe_in_.erase(0, 1);
            } else {
                // Got a new best
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
        if (input_row_ == 0) return;
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

    while (true) {
        cv_col.wait(lock_input, [this]{return input_row_ < nr_rows;});
        col_ = input_row_;
        if (col_ == 0) break;
        auto total = elapsed();
        current_.fill(0);
        current_col_ = col_;
        current_col_rev_ = 0;
        uint shifter = rows-1;
        char buffer[MAX_ROWS+1];
        for (uint i=0; i<rows; ++i, --shifter) {
            uint bit = (col_ >> shifter) & 1;
            buffer[i] = bit ? '1' : '0';
            side[i] = bit;
            current_col_rev_ |= bit << i;
            shift(current_, bit);
        }
        buffer[rows] = 0;
        id_out << "Processing " << buffer << " (" << total << " s" << ", n=" << processed_;
        if (processed_)
            id_out << ", avg=" << total / processed_ << " s";
        id_out << ")";
        if (false) {
            id_out << "\nrev=" << current_col_rev_ << ", current=" << current_;
        }
        id_out << endl;

        uint m = extend(0);
        // This is a slight race, but it doesn't hurt
        // (assuming access to max doesn't tear)
        if (m < col_info[col_].max && input_row_) {
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
                                         Column& column);
Set::const_iterator lower_bound(Set::const_iterator begin,
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
    id_out << (front ? "Skip front " : "Skip back ") << buffer << " (" << elapsed() << " s)" << endl;
    // id_out << (front ? "Skip front " : "Skip back ");
    // for (uint i=0; i<rows; ++i) id_out << (side[col+i] ? '1' : '0');
    // id_out << " (" << elapsed() << " s)" << endl;
}

// The actual core of the program:
//   add one more column and see if it is independent
uint State::extend(uint col, bool zero, bool one) {
    // id_out << "Extend col " << col << " " << static_cast<int>(side[rows+col-1]) << "\n";
    if (col >= MAX_COLS) throw(range_error("col out of range"));
    auto& instrument = instruments_[col];
    if (INSTRUMENT) ++instrument.calls;
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

    if (input_row_ == 0) return 0;

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
    auto pos_current = lower_bound(set_from.cbegin()+1, set_from.cend()-1, current);
    // Early check for in set_from was a slight win but cannot happen anymore
    // However the position is still needed for the subtraction absolute value
    if (false && cmp(*pos_current, current) == 0)
        throw(logic_error("Unexpected hit on from"));

    if (INSTRUMENT) ++instrument.merge;

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
    Element const* RESTRICT ptr_low1   = &pos_current[-1][0];
    Element const* RESTRICT ptr_low2   = &pos_current[0][0];
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
        lower_bound(pos_current, set_from.cend()-1, current2);
    c = cmp(current2, *pos_from_low2);
    if (c == 0) {
        // id_out << "Hit on low2 " << col << endl;
        return col;
    }
    auto skip_begin =
        (pos_from_low2 - pos_current) +
        (pos_current - set_from.cbegin() - 1) * 2;
    if (false) {
        lock_guard<mutex> lock(mutex_out);
        indent(col);
        cout << "low=" << pos_current - set_from.cbegin() - 1 <<
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
        current_col  = current_col_;
        current_col_ = current_col_ << 1 & mask_rows;
    }
    current_col_rev = current_col_rev_;
    current_col_rev_ >>= 1;

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
            if (SKIP_FRONT) current_col_ += 1;
            current_col_rev_ += top_row;
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
        timer_output_.set<Connection, &Connection::io_flush>(this);
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

void Connection::got_id(uint8_t const* ptr, size_t length) {
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
                  phase = GET_ID;
                  break;
                case GET_ID | ID:
                  got_id(ptr, length);
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
