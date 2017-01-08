/*
  Compile using something like:
    g++ -Wall -O3 -march=native -fstrict-aliasing -std=c++14 -g propertyXCheck2.cpp -o propertyXCheck2

  Run as:
    ./propertyXCheck2
  Give matrix on STDIN
*/

#include <cstdint>
#include <climits>
#include <ctgmath>

#include <iostream>
#include <iomanip>
#include <sstream>
#include <vector>
#include <array>
#include <limits>
#include <stdexcept>
#include <string>
#include <algorithm>

#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

using namespace std;

bool const DEBUG_MAP = false;

char const* PROGRAM_NAME = "propertyXCheck";
uint const ELEMENTS = 3;

using uint    = unsigned int;
using Element = uint64_t;
using Column  = array<Element, ELEMENTS>;
struct ColumnData {
    Column column;
    uint index;
    int  factor;
};

template<typename T>
struct Mmap {
    Mmap(string const& name, size_t items) {
        int fd = open(name.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0666);
        if (fd < 0)
            throw(system_error(errno, system_category(), "Could not open '" + name + "'"));
        map_length_ = items * sizeof(T);
        if (ftruncate(fd, map_length_)) {
            auto error = errno;
            close(fd);
            throw(system_error(error, system_category(), "Could not extend '" + name + "'"));
        }
        auto ptr = mmap(NULL, map_length_,
                        PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        if (ptr == MAP_FAILED) {
            auto error = errno;
            close(fd);
            throw(system_error(error, system_category(), "Could not mmap '" + name + "'"));
        }
        map_ = static_cast<T *>(ptr);
        fd_  = fd;
    }
    Mmap(Mmap&& mmap):
        map_length_{mmap.map_length_},
        end_{mmap.end_},
        map_{mmap.map_},
        fd_{mmap.fd_} {
            mmap.fd_ = -1;
        }
    Mmap(Mmap&)  = delete;
    ~Mmap() {
        if (fd_ >= 0) {
            munmap(map_, map_length_);
            close(fd_);
        }
    }
    size_t map_length_;
    size_t end_;
    T* map_;
    int fd_;
};
vector<Mmap<Column>> mmaps;

uint const MAX_COLS = 54;
uint const ROW_FACTOR = (MAX_COLS+1) | 1;		// 55
uint const ROW_ZERO   = ROW_FACTOR/2;			// 27
uint const ROWS_PER_ELEMENT = CHAR_BIT*sizeof(Element) / log2(ROW_FACTOR); // 11
uint const MAX_ROWS = ROWS_PER_ELEMENT * ELEMENTS;	// 33
Element constexpr ELEMENT_FILL(Element v = ROW_ZERO, int n = ROWS_PER_ELEMENT) {
    return n ? ELEMENT_FILL(v, n-1) * ROW_FACTOR + v : 0;
}
Element constexpr POWN(Element v, int n) {
    return n ? POWN(v, n-1)*v : 1;
}
Element const ELEMENT_TOP = POWN(ROW_FACTOR, ROWS_PER_ELEMENT -1);
Element const ELEMENT_MAX = numeric_limits<Element>::max();
int const COLUMN_WIDTH = ceil(log10(static_cast<double>(ROW_FACTOR)));

template <typename T1, typename T2>
inline int cmp(T1 const& left, T2 const& right) {
    for (uint i=0; i<ELEMENTS; ++i) {
        if (left[i] < right[i]) return -1;
        if (left[i] > right[i]) return  1;
    }
    return 0;
}

ostream& operator<<(ostream& os, Column const& column) {
    for (uint i=0; i<ELEMENTS; ++i) {
        auto v = column[i];
        for (uint j=0; j<ROWS_PER_ELEMENT; ++j) {
            auto bit = v % ROW_FACTOR;
            v /= ROW_FACTOR;
            cout << setw(COLUMN_WIDTH+1) << bit;
        }
    }
    return os;
}

ostream& operator<<(ostream& os, ColumnData const& column_data) {
    os << column_data.index << "[" << column_data.factor << "]:" << column_data.column;
    return os;
}

array<ColumnData, MAX_COLS> column_data;
vector<ColumnData*> result;

void decompose(Column const& target, vector<ColumnData*>& columns, int factor, bool top = false) {
    cout << "Decompose " << columns.size() << " columns\n";
    // cout << "Target:      " << target << "\n";
    if (columns.size() == 0) throw(logic_error("Empty columns"));

    mmaps[0].end_ = 1;
    mmaps[1].end_ = 1;
    for (uint j=0; j<ELEMENTS; ++j) {
        mmaps[0].map_[1][j] = ELEMENT_MAX - ELEMENT_FILL(1);
        mmaps[1].map_[1][j] = ELEMENT_MAX - ELEMENT_FILL(1);
    }
    vector<ColumnData*> split_columns[2];
    for (uint col=0; col<columns.size(); ++col) {
        split_columns[col&1].emplace_back(columns[col]);

        auto const& mmap_from = mmaps[(col+0) % 3];
        auto const& mmap_sum  = mmaps[(col+1) % 3];
        auto      & mmap_to   = mmaps[(col+2) % 3];
        mmap_to.end_ = mmap_from.end_ * 3 - 1;

        auto& column = *columns[col];
        Column const& last = column.column;
        cout << "Add Column " << setw(2) << col << ":" << last << "\n";

        auto lower_sum = lower_bound(mmap_sum.map_ + 1,
                                     mmap_sum.map_ + mmap_sum.end_,
                                     target,
                                     [](Column const&left, Column const& right) { return cmp(left, right) < 0; });
        // cout << "lower_sum=" << *lower_sum << "\n";
        if (cmp(*lower_sum, target) == 0)
            throw(logic_error("Unexpected already excluded sum target"));

        Column col_high;
        for (uint j=0; j<ELEMENTS; ++j) col_high[j] = last[j] + ELEMENT_FILL();

        auto ranges = equal_range(mmap_from.map_ + 1,
                                  mmap_from.map_ + mmap_from.end_,
                                  col_high,
                                  [](Column const&left, Column const& right) { return cmp(left, right) < 0; });
        if (ranges.first != ranges.second) {
          ZERO:
            if (!top) throw(logic_error("Unexpected zero"));
            // cout << "ZERO\n";
            decompose(target, split_columns[col & 1], factor, top);
            return;
        }
        int c = cmp(target, col_high);
        if (c == 0) {
            if (top) throw(logic_error("Input column " + to_string(col) + " is zero"));
            // cout << "Target is column " << col << "\n";
            column.factor = factor;
            result.emplace_back(columns[col]);
            return;
        }
        Column last0;
        if (c > 0)
            for (uint j=0; j<ELEMENTS; ++j)
                // last0[j] = target[j] - col_high[j] + ELEMENT_FILL();
                last0[j] = target[j] - last[j];
        else
            for (uint j=0; j<ELEMENTS; ++j)
                last0[j] = col_high[j] - target[j] + ELEMENT_FILL();
        if (binary_search(mmap_from.map_ + 1,
                          mmap_from.map_ + mmap_from.end_,
                          last0,
                          [](Column const&left, Column const& right) { return cmp(left, right) < 0; })) {
            if (top) throw(logic_error("Unexpected target match"));
            // cout << "Column match in from\n";
            decompose(target, split_columns[col & 1], factor);
            return;
        }

        // 4 way merge: {mmap_from - last} (twice), {mmap_from} and
        // {mmap_from + last}
        auto ptr_sum    = &mmap_sum.map_[2][0];
        auto ptr_low1   = &ranges.first[-1][0];
        auto ptr_low2   = &ranges.second[0][0];
        auto ptr_middle = &mmap_from.map_[1][0];
        auto ptr_high   = &mmap_from.map_[1][0];
        Column col_low1, col_low2;
        for (uint j=0; j<ELEMENTS; ++j) {
            last0[j] = ELEMENT_FILL(2*ROW_ZERO) + last[j];
            col_low1[j] = last0[j] - ptr_low1[j];
            col_low2[j] = *ptr_low2++ - last[j];
        }
        auto ptr_end = &mmap_to.map_[mmap_to.end_][0];
        auto ptr_to  = &mmap_to.map_[1][0];
        while (ptr_to < ptr_end) {
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
                      SUM:
                        for (int j=-static_cast<int>(ELEMENTS); j<0; ++j) {
                            if (ptr_to[j] > ptr_sum[j]) {
                                ptr_sum += ELEMENTS;
                                goto SUM;
                            }
                            if (ptr_to[j] < ptr_sum[j]) goto DONE;
                        }
                        // sum == to
                        if (!top) throw(logic_error("Unexpected cross zero"));
                        ptr_to -= ELEMENTS;
                        for (uint j=0; j<ELEMENTS; ++j) col_high[j] = *ptr_to++;
                        // cout << "Sum zero match\n";
                        decompose(col_high, split_columns[(col&1)], -factor);
                        decompose(col_high, split_columns[(col&1) ^ 1], factor);
                        return;
                      DONE:
                        continue;
                    }
                    if (c == 1) goto HIGH;
                    // low1 == high
                    // cout << "low1 == high\n";
                    goto ZERO;
                }
                if (c ==  1) goto MIDDLE;
                // low1 == middle
                // cout << "low1 == middle\n";
                goto ZERO;
            }
            if (c ==  1) {
                c = cmp(col_low2, ptr_middle);
                if (c == -1) {
                    c = cmp(col_low2, col_high);
                    if (c == -1) {
                        // cout << "LOW2\n";
                        for (uint j=0; j<ELEMENTS; ++j) {
                            *ptr_to++   = col_low2[j];
                            col_low2[j] = *ptr_low2++ - last[j];
                        }
                        continue;
                    }
                    if (c == 1) goto HIGH;
                    // low2 == high
                    // cout << "low2 == high\n";
                    goto ZERO;
                }
                if (c ==  1) goto MIDDLE;
                // low2 == middle
                // cout << "low2 == middle\n";
                goto ZERO;
            }
            // low1 == low2
            // cout << "low1 == low2\n";
            goto ZERO;

          MIDDLE:
            // cout << "MIDDLE\n";
            c = cmp(ptr_middle, col_high);
            if (c == -1) {
                // cout << "MIDDLE0\n";
                for (uint j=0; j<ELEMENTS; ++j)
                    *ptr_to++ = *ptr_middle++;
                // {middle} can never be in {sum} or we would already have
                // found this on the previous level
                continue;
            }
            if (c ==  1) goto HIGH;
            // middle == high
            // cout << "middle == high\n";
            goto ZERO;

          HIGH:
            // cout << "HIGH0\n";
            for (uint j=0; j<ELEMENTS; ++j) {
                *ptr_to++ = col_high[j];
                col_high[j] = *ptr_high++ + last[j];
            }
            goto SUM;
        }
        for (uint j=0; j<ELEMENTS; ++j)
            *ptr_to++ = ELEMENT_MAX - ELEMENT_FILL(1);

        if (DEBUG_MAP) {
            auto ptr_end = &mmap_to.map_[mmap_to.end_+1];
            for (auto ptr_to  = &mmap_to.map_[0]; ptr_to < ptr_end; ++ptr_to)
                cout << *ptr_to << "\n";
            cout << "-----\n";
        }

        // If target == 0 the above checks are enough
        if (top) continue;

        for (uint j=0; j<ELEMENTS; ++j) col_high[j] = last[j] + ELEMENT_FILL();
        auto lower_to = lower_bound(mmap_to.map_ + 1,
                                    mmap_to.map_ + mmap_to.end_,
                                    target,
                                    [](Column const&left, Column const& right) { return cmp(left, right) < 0; });
        // cout << "lower_to=" << *lower_to << "\n";
        if (cmp(*lower_to, target) == 0)
            throw(logic_error("Unexpected already excluded to target"));

        // check sum + to = target
        Column target0;
        for (uint j=0; j<ELEMENTS; ++j)
            target0[j] = target[j] + ELEMENT_FILL();
        // cout << "target0=" << target0 << "\n";
        ptr_sum = &lower_sum[-1][0];
        ptr_end = &lower_to[0][0];
        for (ptr_to = &mmap_to.map_[1][0]; ptr_to < ptr_end; ptr_to += ELEMENTS) {
            for (uint j=0; j<ELEMENTS; ++j)
                col_high[j] = target0[j] - ptr_to[j];
          REDO:
            // cout << "col_high=" << col_high << "\nptr_sum=" << *reinterpret_cast<Column*>(ptr_sum) << "\n";
            c = cmp(col_high, ptr_sum);
            if (c < 0) {
                ptr_sum -= ELEMENTS;
                goto REDO;
            }
            if (c == 0) {
                if (top) throw(logic_error("Unexcpected cross sum"));
                // cout << "Sum match\n";
                decompose(col_high, split_columns[(col&1) ^ 1], factor);
                for (uint j=0; j<ELEMENTS; ++j)
                    col_high[j] = target0[j] - col_high[j];
                decompose(col_high, split_columns[(col&1)], factor);
                return;
            }
        }
        for (uint j=0; j<ELEMENTS; ++j)
            target0[j] = target[j] - ELEMENT_FILL();
        // cout << "target0=" << target0 << "\n";

        // check to - sum = target
        ptr_sum = &mmap_sum.map_[1][0];
        ptr_to  = ptr_end;
        ptr_end = &mmap_to.map_[mmap_to.end_][0];
        while (ptr_to < ptr_end) {
            for (uint j=0; j<ELEMENTS; ++j)
                col_high[j] = *ptr_to++ - target0[j];
          REDO0:
            // cout << "col_high=" << col_high << "\nptr_sum=" << *reinterpret_cast<Column*>(ptr_sum) << "\n";
            c = cmp(col_high, ptr_sum);
            if (c > 0) {
                ptr_sum += ELEMENTS;
                goto REDO0;
            }
            if (c == 0) {
                // cout << "Sub0 match\n";
                decompose(col_high, split_columns[(col&1) ^ 1], -factor);
                for (uint j=0; j<ELEMENTS; ++j)
                    col_high[j] += target0[j];
                decompose(col_high, split_columns[(col&1)], factor);
                return;
            }
        }
        // check sum - to  = target
        ptr_to = &mmap_to.map_[1][0];
        ptr_sum = &lower_sum[0][0];
        ptr_end = &mmap_sum.map_[mmap_sum.end_][0];
        while (ptr_sum < ptr_end) {
            for (uint j=0; j<ELEMENTS; ++j)
                col_high[j] = *ptr_sum++ - target0[j];
          REDO1:
            // cout << "col_high=" << col_high << "\nptr_sum=" << *reinterpret_cast<Column*>(ptr_sum) << "\n";
            c = cmp(col_high, ptr_to);
            if (c > 0) {
                ptr_to += ELEMENTS;
                goto REDO1;
            }
            if (c == 0) {
                // cout << "Sub1 match\n";
                decompose(col_high, split_columns[(col&1)], -factor);
                for (uint j=0; j<ELEMENTS; ++j)
                    col_high[j] += target0[j];
                decompose(col_high, split_columns[(col&1) ^ 1], factor);
                return;
            }
        }
    }
    // if target != 0 we are analyzing a specific decomposition
    // so we ***KNOW*** it exists
    if (!top) throw(logic_error("Could not decompose"));

    // All columns added and still no decomposition: Matrix has property X
    cout << "No decomposition\n";
}

void my_main() {
    vector<uint8_t> input;
    uint cols = 0;
    string line;
    while (getline(cin, line)) {
        istringstream iss{line};
        int n;

        uint c = 0;
        while (iss >> n) {
            if (!(n == 0 || n == 1))
                throw(logic_error("Invalid value " + to_string(n)));
            input.emplace_back(n);
            ++c;
        }
        if (cols == 0) cols = c;
        else if (cols != c)
            throw(logic_error("Non rectangular matrix"));
    }
    if (cols == 0) throw(logic_error("Empty matrix"));
    uint rows = input.size() / cols;

    if (cols > MAX_COLS) throw(range_error(to_string(cols) + " columns > " + to_string(MAX_COLS)));
    if (rows > MAX_ROWS) throw(range_error(to_string(rows) + " rows > " + to_string(MAX_ROWS)));

    vector<ColumnData*> columns;
    for (uint c=0; c<cols; ++c) {
        column_data[c].index = c;
        uint r = 0;
        for (uint j=0; j<ELEMENTS; ++j) {
            Element element = 0;
            for (uint k=0; k<ROWS_PER_ELEMENT && r < rows; ++k, ++r)
                element = element * ROW_FACTOR + input[r*cols+c];
            column_data[c].column[j] = element;
        }
        columns.emplace_back(&column_data[c]);
    }

    size_t map_size = static_cast<size_t>(pow(3., static_cast<double>((cols+1)/2))+3.5);
    map_size /= 2;
    for (int i=0; i<3; ++i) {
        stringstream name;
        name << PROGRAM_NAME << "." << i << ".map";
        mmaps.emplace_back(name.str(), map_size);
        for (uint j=0; j < ELEMENTS; ++j)
            mmaps[i].map_[0][j] = ELEMENT_FILL(1);
    }

    Column target;
    for (uint j=0; j<ELEMENTS; ++j) target[j] = ELEMENT_FILL();
    decompose(target, columns, 1, true);
    if (result.size()) {
        sort(result.begin(), result.end(), [](ColumnData const* left, ColumnData const* right) { return left->index < right->index; });
        cout << "Decomposition:";
        for (auto ptr: result)
            cout << " " << (ptr->factor > 0 ? "+" : "-") << ptr->index;
        cout << "\n";
    }
}

int main() {
    try {
        my_main();
    } catch(exception& e) {
        cout << "Error: " << e.what() << endl;
        exit(EXIT_FAILURE);
    }
    exit(EXIT_SUCCESS);
}
