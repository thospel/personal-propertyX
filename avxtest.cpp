#include <iostream>

#include <immintrin.h>
#include <cstdint>
#include <cstdlib>

#include <algorithm>
#include <chrono>
#include <new>
#include <thread>
#include <vector>

using namespace std;

size_t   const LOOPS = 32;
size_t   const SIZE = 1 << 22;
// uint64_t const RANGE = 1000000000;
uint64_t const RANGE = 1000000;

# define RESTRICT __restrict__

//using Int4    = int64_t __attribute__((vector_size(4*sizeof(int64_t))));
//using Double4 = double  __attribute__((vector_size(4*sizeof(double))));
using Int4    = __m256i;
using Double4 = __m256d;

ostream& operator<<(ostream& os, Int4 val) {
    os << val[0] << " " << val[1] << " " << val[2] << " " << val[3];
    return os;
}

ostream& operator<<(ostream& os, Double4 val) {
    os << val[0] << " " << val[1] << " " << val[2] << " " << val[3];
    return os;
}


volatile int q;

inline Int4 blend(Int4 a, Int4 b, Int4 mask) __attribute__((always_inline));
Int4 blend(Int4 a, Int4 b, Int4 mask) {
    return _mm256_castpd_si256(_mm256_blendv_pd(_mm256_castsi256_pd(a),
                                                _mm256_castsi256_pd(b),
                                                _mm256_castsi256_pd(mask)));
}

inline Int4 shuffle(Int4 a, Int4 b, int imm)  __attribute__((always_inline));
Int4 shuffle(Int4 a, Int4 b, int imm) {
    return _mm256_castpd_si256(_mm256_shuffle_pd(_mm256_castsi256_pd(a),
                                                 _mm256_castsi256_pd(b),
                                                 imm));
}

inline Int4 permute(Int4 a, Int4 b, int imm)  __attribute__((always_inline));
Int4 permute(Int4 a, Int4 b, int imm) {
    return _mm256_castpd_si256(_mm256_permute2f128_pd(_mm256_castsi256_pd(a),
                                                      _mm256_castsi256_pd(b),
                                                      imm));
}

inline void rising(Int4& out_hi0, Int4& out_lo0, Int4& out_hi1, Int4& out_lo1,
                   Int4   in_hi0, Int4   in_lo0, Int4   in_hi1, Int4   in_lo1)  __attribute__((always_inline));
void rising(Int4& out_hi0, Int4& out_lo0, Int4& out_hi1, Int4& out_lo1,
                   Int4   in_hi0, Int4   in_lo0, Int4   in_hi1, Int4   in_lo1) {
    // Int4 hi_eq = _mm256_cmpeq_epi64(in_hi0, in_hi1);
    // Int4 hi_lt = _mm256_cmpgt_epi64(in_hi1, in_hi0);
    // Int4 lo_lt = _mm256_cmpgt_epi64(in_lo1, in_lo0);
    // Int4 lo    = _mm256_and_si256(hi_eq, lo_lt);
    // Int4 lt    = _mm256_or_si256(hi_lt, lo);
    Int4 hi_eq = _mm256_cmpeq_epi64(in_hi0, in_hi1);
    Int4 val0  = blend(in_hi0, in_lo0, hi_eq);
    Int4 val1  = blend(in_hi1, in_lo1, hi_eq);
    Int4 lt    = _mm256_cmpgt_epi64(val1, val0);
    out_hi0 = blend(in_hi1, in_hi0, lt);
    out_lo0 = blend(in_lo1, in_lo0, lt);
    out_hi1 = blend(in_hi0, in_hi1, lt);
    out_lo1 = blend(in_lo0, in_lo1, lt);
}

inline void reverse(Int4& out_hi, Int4& out_lo, Int4   in_hi, Int4   in_lo)  __attribute__((always_inline));
void reverse(Int4& out_hi, Int4& out_lo, Int4   in_hi, Int4   in_lo) {
    out_hi = _mm256_permute4x64_epi64(in_hi, 0b00011011);
    out_lo = _mm256_permute4x64_epi64(in_lo, 0b00011011);
}

inline void rmerge(Int4& out_hi0, Int4& out_lo0, Int4& out_rhi1, Int4& out_rlo1,
                   Int4   in_hi0, Int4   in_lo0, Int4   in_rhi1, Int4   in_rlo1) __attribute__((always_inline));
void rmerge(Int4& out_hi0, Int4& out_lo0, Int4& out_rhi1, Int4& out_rlo1,
            Int4   in_hi0, Int4   in_lo0, Int4   in_rhi1, Int4   in_rlo1) {

    // Level-1 compare
    Int4 hi10, lo10, hi11, lo11;
    rising(hi10, lo10, hi11, lo11, in_hi0, in_lo0, in_rhi1, in_rlo1);

    // Level-1 shuffles
    Int4 hi10p = permute(hi10, hi11, 0x31);
    Int4 lo10p = permute(lo10, lo11, 0x31);
    Int4 hi11p = permute(hi10, hi11, 0x20);
    Int4 lo11p = permute(lo10, lo11, 0x20);

    // Level-2 compare
    Int4 hi20, lo20, hi21, lo21;
    rising(hi20, lo20, hi21, lo21, hi10p, lo10p, hi11p, lo11p);

    // Level-2 shuffles
    Int4 hi20p = shuffle(hi20, hi21, 0b0000);
    Int4 lo20p = shuffle(lo20, lo21, 0b0000);
    Int4 hi21p = shuffle(hi20, hi21, 0b1111);
    Int4 lo21p = shuffle(lo20, lo21, 0b1111);

    // Level-3 compare
    Int4 hi30, lo30, hi31, lo31;
    rising(hi30, lo30, hi31, lo31, hi20p, lo20p, hi21p, lo21p);

    // Level-3 shuffles
    Int4 hi30p = permute(hi30, hi31, 0x20);
    Int4 lo30p = permute(lo30, lo31, 0x20);
    Int4 hi31p = permute(hi30, hi31, 0x31);
    Int4 lo31p = permute(lo30, lo31, 0x31);

    out_hi0 = _mm256_permute4x64_epi64(hi30p, 0b11011000);
    out_lo0 = _mm256_permute4x64_epi64(lo30p, 0b11011000);
    out_rhi1 = _mm256_permute4x64_epi64(hi31p, 0b00100111);
    out_rlo1 = _mm256_permute4x64_epi64(lo31p, 0b00100111);
}

inline void merge(Int4& out_hi0, Int4& out_lo0, Int4& out_hi1, Int4& out_lo1,
                  Int4   in_hi0, Int4   in_lo0, Int4  in_rhi1, Int4  in_rlo1)  __attribute__((always_inline));
void merge(Int4& out_hi0, Int4& out_lo0, Int4& out_hi1, Int4& out_lo1,
           Int4   in_hi0, Int4   in_lo0, Int4  in_rhi1, Int4  in_rlo1) {

    // Level-1 compare
    Int4 hi10, lo10, hi11, lo11;
    rising(hi10, lo10, hi11, lo11, in_hi0, in_lo0, in_rhi1, in_rlo1);

    // Level-1 shuffles
    Int4 hi10p = permute(hi10, hi11, 0x31);
    Int4 lo10p = permute(lo10, lo11, 0x31);
    Int4 hi11p = permute(hi10, hi11, 0x20);
    Int4 lo11p = permute(lo10, lo11, 0x20);

    // Level-2 compare
    Int4 hi20, lo20, hi21, lo21;
    rising(hi20, lo20, hi21, lo21, hi10p, lo10p, hi11p, lo11p);

    // Level-2 shuffles
    Int4 hi20p = shuffle(hi20, hi21, 0b0000);
    Int4 lo20p = shuffle(lo20, lo21, 0b0000);
    Int4 hi21p = shuffle(hi20, hi21, 0b1111);
    Int4 lo21p = shuffle(lo20, lo21, 0b1111);

    // Level-3 compare
    Int4 hi30, lo30, hi31, lo31;
    rising(hi30, lo30, hi31, lo31, hi20p, lo20p, hi21p, lo21p);

    // Level-3 shuffles
    Int4 hi30p = shuffle(hi30, hi31, 0b1111);
    Int4 lo30p = shuffle(lo30, lo31, 0b1111);
    Int4 hi31p = shuffle(hi30, hi31, 0b0000);
    Int4 lo31p = shuffle(lo30, lo31, 0b0000);

    out_hi0 = permute(hi30p, hi31p, 0x02);
    out_lo0 = permute(lo30p, lo31p, 0x02);
    out_hi1 = permute(hi30p, hi31p, 0x13);
    out_lo1 = permute(lo30p, lo31p, 0x13);
}

__attribute__((__noinline__)) void calc(Int4 a[2], Int4 b[2]);
void calc(Int4 a[2], Int4 b[2]) {
    Int4 Rev[2];
    reverse(Rev[0], Rev[1], b[0], b[1]);
    cout << "Rev: " << Rev[0] << " | " << Rev[1] << "\n";

    Int4 Lo[2];
    Int4 Hi[2];

    asm("");
    rmerge(Lo[0], Lo[1], Hi[0], Hi[1], a[0], a[1], Rev[0], Rev[1]);
    asm("");

    cout << "Lo: " << Lo[0] << " | " << Lo[1] << "\n";
    cout << "Hi: " << Hi[0] << " | " << Hi[1] << "\n";
}

// http://xoroshiro.di.unimi.it/xoroshiro128plus.c
uint64_t s[2] = { 0, 1 };

static inline uint64_t rotl(const uint64_t x, int k) {
	return (x << k) | (x >> (64 - k));
}

uint64_t rand64() {
	const uint64_t s0 = s[0];
	uint64_t s1 = s[1];
	const uint64_t result = s0 + s1;

	s1 ^= s0;
	s[0] = rotl(s0, 55) ^ s1 ^ (s1 << 14); // a, b
	s[1] = rotl(s1, 36); // c

	return result;
}

struct Element {
    Int4 hi, lo;
};
template <typename T>
struct Vector {
    Vector(size_t size) : size_{size} {
        void* p = aligned_alloc(64, (size+1) * sizeof(*ptr_));
        if (p == nullptr)
            throw bad_alloc();
        ptr_ = static_cast<T*>(p);
    }
    ~Vector() {
        void *p = static_cast<void *>(ptr_);
        free(p);
    }
    size_t size() const { return size_; }
    T& operator[](size_t i) { return ptr_[i]; }
    T const& operator[](size_t i) const { return ptr_[i]; }
    T* begin() { return &ptr_[0]; }
    T* end()   { return &ptr_[size_]; }
    T const* begin() const { return &ptr_[0]; }
    T const* end()   const { return &ptr_[size_]; }
    T* ptr_;
    size_t size_;
};

void classic_merge(Vector<Int4>& c_hi, Vector<Int4>& c_lo,
                   Vector<Int4> const& a_hi, Vector<Int4> const& a_lo,
                   Vector<Int4> const& b_hi, Vector<Int4> const& b_lo) {
    long long int const* RESTRICT a_hi_ptr = &(*a_hi.begin())[0];
    long long int const* RESTRICT a_lo_ptr = &(*a_lo.begin())[0];
    long long int const* RESTRICT b_hi_ptr = &(*b_hi.begin())[0];
    long long int const* RESTRICT b_lo_ptr = &(*b_lo.begin())[0];
    long long int      * RESTRICT c_hi_ptr = &(*c_hi.begin())[0];
    long long int      * RESTRICT c_lo_ptr = &(*c_lo.begin())[0];
    long long int      * RESTRICT c_hi_end = &(*c_hi.end())[0];

    while (c_hi_ptr < c_hi_end) {
        if (*a_hi_ptr < *b_hi_ptr) {
          LOW:
            *c_hi_ptr++ = *a_hi_ptr++;
            *c_lo_ptr++ = *a_lo_ptr++;
        } else if (*a_hi_ptr > *b_hi_ptr) {
          HIGH:
            *c_hi_ptr++ = *b_hi_ptr++;
            *c_lo_ptr++ = *b_lo_ptr++;
        } else if (*a_lo_ptr < *b_lo_ptr) goto LOW;
        else goto HIGH;
    }
} 

void vector_merge(Vector<Int4>& c_hi, Vector<Int4>& c_lo,
                  Vector<Int4> const& a_hi, Vector<Int4> const& a_lo,
                  Vector<Int4> const& b_hi, Vector<Int4> const& b_lo) {
    Int4 const* RESTRICT a_hi_ptr = a_hi.begin();
    Int4 const* RESTRICT a_lo_ptr = a_lo.begin();
    Int4 const* RESTRICT b_hi_ptr = b_hi.begin();
    Int4 const* RESTRICT b_lo_ptr = b_lo.begin();
    Int4      * RESTRICT c_hi_ptr = c_hi.begin();
    Int4      * RESTRICT c_lo_ptr = c_lo.begin();
    Int4      * RESTRICT c_hi_end = c_hi.end();
    
    Int4 hi0 = *a_hi_ptr++;
    Int4 lo0 = *a_lo_ptr++;
    Int4 rhi1, rlo1;
    reverse(rhi1, rlo1, *b_hi_ptr++, *b_lo_ptr++);
    while (c_hi_ptr < c_hi_end) {
        rmerge(hi0, lo0, rhi1, rlo1, hi0, lo0, rhi1, rlo1);
        *c_hi_ptr++ = hi0;
        *c_lo_ptr++ = lo0;
        if ((*a_hi_ptr)[0] < (*b_hi_ptr)[0]) {
          LOW:
            hi0 = *a_hi_ptr++;
            lo0 = *a_lo_ptr++;
        } else if ((*a_hi_ptr)[0] > (*b_hi_ptr)[0]) {
          HIGH:
            hi0 = *b_hi_ptr++;
            lo0 = *b_lo_ptr++;
        } else if ((*a_lo_ptr)[0] < (*b_lo_ptr)[0]) goto LOW;
        else goto HIGH;
    }
}

int main() {
    Int4 a1[2] = {{ 0, 2, 4, 6}, { 4, 5, 6, 7 }};
    Int4 a2[2] = {{ 1, 3, 5, 7 },{ 8, 5, 5, 8 }};
    calc(a1, a2);

    uint nr_threads = thread::hardware_concurrency();
    cout << "nr_threads=" << nr_threads << endl;

    Vector<Int4> a_hi(SIZE);
    Vector<Int4> a_lo(SIZE);
    Vector<Int4> b_hi(SIZE);
    Vector<Int4> b_lo(SIZE);

    for (auto& v: a_hi)
        for (int i=0; i<4; ++i)
            v[i] = (rand64() >> 1) % (RANGE * RANGE);
    for (auto& v: b_hi)
        for (int i=0; i<4; ++i)
            v[i] = (rand64() >> 1) % (RANGE * RANGE);

    auto s = &(*a_hi.begin())[0];
    auto e = &(*a_hi.end())[0];
    e[0] = RANGE;
    sort(s, e);
    auto t = &(*a_lo.begin())[0];
    while (s < e) {
        *t++ = *s % RANGE;
        *s = *s / RANGE;
        ++s;
    }

    for (int i=0; i<10; i++)
        cout << a_hi[i] << " | " << a_lo[i] << "\n";
    cout << "\n";

    s = &(*b_hi.begin())[0];
    e = &(*b_hi.end())[0];
    e[0] = RANGE;
    sort(s, e);
    t = &(*b_lo.begin())[0];
    while (s < e) {
        *t++ = *s % RANGE;
        *s = *s / RANGE;
        ++s;
    }

    for (int i=0; i<10; i++)
        cout << b_hi[i] << " | " << b_lo[i] << "\n";
    cout << "\n";

    vector<Vector<Int4>> c_hi;
    c_hi.reserve(nr_threads);
    vector<Vector<Int4>> c_lo;
    c_lo.reserve(nr_threads);
    for (uint t=0; t<nr_threads; ++t) {
        c_hi.emplace_back(SIZE*2);
        c_lo.emplace_back(SIZE*2);
    }
    cout << "hi size=" << c_hi.size() << "\n";
    cout << "lo size=" << c_lo.size() << "\n";

    vector<thread> threads;
    threads.reserve(nr_threads);
    auto start = chrono::steady_clock::now();
    for (uint t=0; t<nr_threads; ++t) {
        auto fun = [&, t]() {
            for (uint i=0; i<LOOPS; ++i)
                vector_merge(c_hi[t], c_lo[t], a_hi, a_lo, b_hi, b_lo);
        };
        threads.emplace_back(fun);
    }
    for (auto& t: threads) t.join();
    threads.clear();
    auto end = chrono::steady_clock::now();
    std::chrono::duration<double> duration = end - start;

    for (int i=0; i<10; i++)
        cout << c_hi[0][i] << " | " << c_lo[0][i] << "\n";
    cout << "\n";

    cout << "Per element: " << duration.count() / SIZE / 2 / LOOPS << "\n";

    threads.reserve(nr_threads);
    start = chrono::steady_clock::now();
    for (uint t=0; t<nr_threads; ++t) {
        auto fun = [&, t]() {
            for (uint i=0; i<LOOPS; ++i)
                classic_merge(c_hi[t], c_lo[t], a_hi, a_lo, b_hi, b_lo);
        };
        threads.emplace_back(fun);
    }
    for (auto& t: threads) t.join();
    threads.clear();
    end = chrono::steady_clock::now();
    duration = end - start;

    for (int i=0; i<10; i++)
        cout << c_hi[0][i] << " | " << c_lo[0][i] << "\n";
    cout << "\n";

    cout << "Per element: " << duration.count() / SIZE / 2 / LOOPS << "\n";
}
