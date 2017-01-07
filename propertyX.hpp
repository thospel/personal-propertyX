#ifndef PROPERTYX_HPP
# define PROPERTYX_HPP
# pragma once

#include "log_buffer.hpp"

#include <climits>
#include <cstdint>
#include <ctgmath>

#include <chrono>
#include <iostream>
#include <memory>
#include <mutex>
#include <sstream>
#include <stdexcept>

#include <ev++.h>

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

#define STATIC static

uint8_t const PROTO_VERSION = 2;

using uint     = unsigned int;

uint const PORT = 21453;
size_t const BLOCK = 65536;
ev_tstamp const TIMEOUT_GREETING = 10;
uint const ELEMENTS = 2;

using Index    = uint32_t;
using Element  = int64_t;
using uElement = uint64_t;
using Sum      = uint8_t;
using Sec      = std::chrono::seconds;

uint const MAX_COLS = 54;
uint const ROW_FACTOR = (MAX_COLS+1) | 1;		// 55
uint const ROW_ZERO   = ROW_FACTOR/2;			// 27
uint const ROWS_PER_ELEMENT = CHAR_BIT*sizeof(Element) / log2(ROW_FACTOR); // 11
uint const MAX_ROWS = ROWS_PER_ELEMENT * ELEMENTS;	// 22

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

extern LogStream  log_out;
extern TimeStream timed_out;

extern ev::default_loop loop;

extern Index nr_rows, top_row;
extern uint rows;
extern uint max_cols_;

extern void client(int argc, char** argv);
extern void server(int argc, char** argv);

#endif // PROPERTYX_HPP
