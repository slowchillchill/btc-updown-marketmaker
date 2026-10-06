#pragma once
#include <stdint.h>
#include <cuda_runtime.h>

// 容量只描述实际分配。字索引、位长、ABI均用int64，不限制业务数值域。
#ifdef __CUDACC__
namespace public_gpu {
__device__ inline int64_t length(const uint32_t* x, int64_t width) {
  while (width && !x[width - 1]) --width;
  return width;
}
__device__ inline int compare(const uint32_t* a, int64_t na,
                              const uint32_t* b, int64_t nb) {
  na = length(a, na);
  nb = length(b, nb);
  if (na != nb) return na < nb ? -1 : 1;
  for (int64_t j = na; j-- > 0;) {
    if (a[j] != b[j]) return a[j] < b[j] ? -1 : 1;
  }
  return 0;
}
__device__ inline void zero(uint32_t* x, int64_t n) {
  for (int64_t i = 0; i < n; ++i) x[i] = 0;
}
__device__ inline void copy(uint32_t* o, int64_t no,
                            const uint32_t* x, int64_t nx) {
  for (int64_t i = 0; i < no; ++i) o[i] = i < nx ? x[i] : 0;
}
__device__ inline void add(uint32_t* o, int64_t no, const uint32_t* a,
                           int64_t na, const uint32_t* b, int64_t nb) {
  uint64_t carry = 0;
  for (int64_t j = 0; j < no; ++j) {
    uint64_t v = carry + (j < na ? a[j] : 0ULL) + (j < nb ? b[j] : 0ULL);
    o[j] = uint32_t(v);
    carry = v >> 32;
  }
}
// 调用者保证a>=b；允许o==a，供长除法原地处理余数。
__device__ inline void subtract(uint32_t* o, int64_t no, const uint32_t* a,
                                int64_t na, const uint32_t* b, int64_t nb) {
  uint64_t borrow = 0;
  for (int64_t j = 0; j < no; ++j) {
    uint64_t av = j < na ? a[j] : 0;
    uint64_t bv = (j < nb ? b[j] : 0ULL) + borrow;
    o[j] = uint32_t(av - bv);
    borrow = av < bv;
  }
}
__device__ inline void multiply(uint32_t* o, int64_t no, const uint32_t* a,
                                int64_t na, const uint32_t* b, int64_t nb) {
  zero(o, no);
  for (int64_t i = 0; i < na; ++i) {
    uint64_t carry = 0;
    for (int64_t j = 0; j < nb; ++j) {
      uint64_t v = uint64_t(a[i]) * b[j] + o[i + j] + carry;
      o[i + j] = uint32_t(v);
      carry = v >> 32;
    }
    o[i + nb] = uint32_t(carry);
  }
}
__device__ inline int64_t bit_length_from_top(uint32_t top, int64_t width) {
  return width ? (width - 1) * int64_t(32) + 32 - __clz(top) : 0;
}
__device__ inline int64_t bit_length(const uint32_t* a, int64_t na) {
  na = length(a, na);
  return bit_length_from_top(na ? a[na - 1] : 0, na);
}
__device__ inline void shift_copy(uint32_t* out, int64_t width,
                                  const uint32_t* a, int64_t na, int64_t bits) {
  zero(out, width);
  int64_t whole = bits / 32;
  int part = bits % 32;
  for (int64_t j = 0; j < na; ++j) {
    out[j + whole] |= a[j] << part;
    if (part) out[j + whole + 1] |= a[j] >> (32 - part);
  }
}
// 商容量>=分子容量；余数容量>=分母容量+1。除零只返回fault。
__device__ inline bool divide(uint32_t* q, int64_t nq, uint32_t* r,
                              int64_t nr, const uint32_t* a, int64_t na,
                              const uint32_t* b, int64_t nb) {
  zero(q, nq);
  zero(r, nr);
  nb = length(b, nb);
  if (!nb) return false;
  for (int64_t bit = bit_length(a, na); bit-- > 0;) {
    uint32_t carry = (a[bit / 32] >> (bit % 32)) & 1;
    for (int64_t j = 0; j < nr; ++j) {
      uint32_t next = r[j] >> 31;
      r[j] = (r[j] << 1) | carry;
      carry = next;
    }
    if (compare(r, nr, b, nb) >= 0) {
      subtract(r, nr, r, nr, b, nb);
      q[bit / 32] |= uint32_t(1) << (bit % 32);
    }
  }
  return true;
}

// 共享Euclid归约。调用者提供10*cap独占临时字；输入/结果容量证明归调用模块。
__device__ inline bool floor_sum_value(const uint32_t* counts, int64_t wn,
    const uint32_t* moduli, int64_t wm, const uint32_t* slopes, int64_t wa,
    const uint32_t* offsets, int64_t wb, uint32_t* out, int64_t wo,
    uint32_t* work, int64_t cap, int64_t& required) {
  uint32_t* n = work;
  uint32_t* m = n + cap;
  uint32_t* a = m + cap;
  uint32_t* b = a + cap;
  uint32_t* q = b + cap;
  uint32_t* r = q + cap;
  uint32_t* product = r + cap;
  uint32_t* term = product + cap;
  uint32_t* saved = term + cap;
  uint32_t* result = saved + cap;
  copy(n, cap, counts, wn);
  copy(m, cap, moduli, wm);
  copy(a, cap, slopes, wa);
  copy(b, cap, offsets, wb);
  zero(result, cap);
  bool valid = length(m, cap) != 0;
  while (valid && length(n, cap)) {
    if (compare(a, cap, m, cap) >= 0) {
      divide(q, cap, r, cap, a, cap, m, cap);
      copy(saved, cap, q, cap);
      copy(a, cap, r, cap);
      zero(term, cap); term[0] = 1;
      subtract(term, cap, n, cap, term, cap);
      multiply(product, cap, n, length(n, cap), term, length(term, cap));
      zero(term, cap); term[0] = 2;
      divide(q, cap, r, cap, product, cap, term, 1);
      multiply(term, cap, q, length(q, cap), saved, length(saved, cap));
      add(result, cap, result, cap, term, cap);
    }
    if (compare(b, cap, m, cap) >= 0) {
      divide(q, cap, r, cap, b, cap, m, cap);
      copy(b, cap, r, cap);
      multiply(term, cap, n, length(n, cap), q, length(q, cap));
      add(result, cap, result, cap, term, cap);
    }
    multiply(product, cap, a, length(a, cap), n, length(n, cap));
    add(product, cap, product, cap, b, cap);
    if (compare(product, cap, m, cap) < 0) break;
    divide(q, cap, r, cap, product, cap, m, cap);
    copy(n, cap, q, cap);
    copy(b, cap, r, cap);
    uint32_t* old_m = m; m = a; a = old_m;
  }
  required = length(result, cap);
  copy(out, wo, result, cap);
  return valid;
}
}
#endif
extern "C" void public_gpu_integer(int, const uint32_t*, int64_t, const int8_t*,
    const uint32_t*, int64_t, const int8_t*, uint32_t*, int64_t, int8_t*,
    uint32_t*, int64_t, int8_t*, int32_t*, uint32_t*, int64_t, int64_t*,
    int64_t, cudaStream_t);
extern "C" void public_gpu_project(int, const uint32_t*, int64_t, const int8_t*,
    const uint32_t*, int64_t, float*, int32_t*, uint32_t*, int64_t, int64_t, cudaStream_t);
extern "C" void public_gpu_index_probe(const int64_t*, const uint32_t*, int64_t*,
    int64_t, cudaStream_t);
extern "C" void public_gpu_gcd(const uint32_t*, int64_t, const uint32_t*,
    int64_t, uint32_t*, uint32_t*, int64_t, int64_t, cudaStream_t);
extern "C" void public_gpu_floor_sum(const uint32_t*, int64_t, const uint32_t*, int64_t,
    const uint32_t*, int64_t, const uint32_t*, int64_t, uint32_t*, int64_t,
    int32_t*, uint32_t*, int64_t, int64_t*, int64_t, cudaStream_t);
