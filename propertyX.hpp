#ifndef PROPERTYX_HPP
# define PROPERTYX_HPP
# pragma once

#include <array>
#include <iostream>
#include <mutex>
#include <string>
#include <sstream>
#include <vector>

#include <climits>
#include <ctgmath>

#include <ev++.h>

using uint     = unsigned int;

uint const PORT = 21453;

uint8_t const PROTO_VERSION   = 3;
uint8_t const PROGRAM_VERSION = 10;

ev_tstamp const TIMEOUT_GREETING = 10;
size_t const BLOCK = 65536;

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

// #define STATIC static
#define STATIC

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

uint const  ELEMENTS = 2;

using Index    = uint32_t;
using Element  = int64_t;
using uElement = uint64_t;
using Column   = std::array<Element, ELEMENTS>;
using Set      = std::vector<Column>;
using Sum      = uint8_t;


uint const MAX_COLS = 54;
uint const ROW_FACTOR = (MAX_COLS+1) | 1;		// 55
uint const ROW_ZERO   = ROW_FACTOR/2;			// 27
#ifdef __clang__
uint const ROWS_PER_ELEMENT = 11;
#else // __clang__
uint const ROWS_PER_ELEMENT = CHAR_BIT*sizeof(Element) / log2(ROW_FACTOR); // 11
#endif // __clang__
uint const MAX_ROWS = ROWS_PER_ELEMENT * ELEMENTS;	// 22

extern std::string time_string();

extern std::mutex mutex_out;

class TimeBuffer: public std::stringbuf {
  public:
    ~TimeBuffer() {
        sync();
        flush();
    }
  protected:
    int sync() {
        if (!str().empty()) {
            {
                std::lock_guard<std::mutex> lock{mutex_out};
                full_out << time_string() << str();
            }
            str("");
        }
        return 0;
    }
  public:
    static std::stringstream full_out;
    static void flush() {
        if (full_out.str().empty()) return;
        std::lock_guard<std::mutex> lock{mutex_out};
        std::cout << full_out.str();
        std::cout.flush();
        full_out.str("");
    }
};

class TimeStream: public std::ostream {
  public:
    TimeStream(): std::ostream{&buffer} {}
  private:
    TimeBuffer buffer;
};

extern TimeStream timed_out;
extern ev::default_loop loop;

extern uint max_cols_;
extern uint rows;
extern Index nr_rows, top_row;

extern void client(int argc, char** argv);
extern void server(int argc, char** argv);
#endif // PROPERTYX_HPP
