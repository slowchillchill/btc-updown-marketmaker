#include "exact.cuh"
#include <cmath>
using namespace public_gpu;

__global__ void integer_kernel(int op, const uint32_t* aw, int64_t wa,
    const int8_t* as, const uint32_t* bw, int64_t wb, const int8_t* bs,
    uint32_t* ow, int64_t wo, int8_t* os, uint32_t* rw, int64_t wr,
    int8_t* rs, int32_t* fault, int64_t rows) {
  int64_t row = int64_t(blockIdx.x) * blockDim.x + threadIdx.x;
  if (row >= rows) return;
  const auto* a = aw + row * wa;
  const auto* b = bw + row * wb;
  auto* o = ow + row * wo;
  auto* r = rw + row * wr;
  int64_t na = length(a, wa), nb = length(b, wb);
  int sa = na ? as[row] : 0, sb = nb ? bs[row] : 0;
  zero(o, wo);
  zero(r, wr);
  fault[row] = 0;
  rs[row] = 0;
  if (op == 0 || op == 1) {
    if (op == 1) sb = -sb;
    if (sa == sb) {
      add(o, wo, a, na, b, nb);
      os[row] = sa;
    } else if (compare(a, na, b, nb) >= 0) {
      subtract(o, wo, a, na, b, nb);
      os[row] = sa;
    } else {
      subtract(o, wo, b, nb, a, na);
      os[row] = sb;
    }
  } else if (op == 2) {
    multiply(o, wo, a, na, b, nb);
    os[row] = sa * sb;
  } else if (op == 3) {
    fault[row] = !divide(o, wo, r, wr, a, na, b, nb);
    os[row] = sa * sb;
    rs[row] = length(r, wr) ? sa : 0;
  } else {
    os[row] = sa == sb ? sa * compare(a, na, b, nb) : (sa < sb ? -1 : 1);
  }
  if (op != 4 && !length(o, wo)) os[row] = 0;
}

// 高位尾数和指数避免任意长整数转double时溢出。
__device__ inline double log_ratio(const uint32_t* n, int64_t nn,
                                   const uint32_t* d, int64_t nd) {
  int64_t bn = bit_length(n, nn), bd = bit_length(d, nd);
  uint64_t sn = 0, sd = 0;
  int64_t kn = bn > 53 ? bn - 53 : 0, kd = bd > 53 ? bd - 53 : 0;
  for (int64_t i = bn; i-- > kn;) sn = (sn << 1) | ((n[i / 32] >> (i % 32)) & 1);
  for (int64_t i = bd; i-- > kd;) sd = (sd << 1) | ((d[i / 32] >> (i % 32)) & 1);
  return log(double(sn) / double(sd)) + double(kn - kd) * 0.69314718055994530942;
}
__device__ inline float ratio_f32(const uint32_t* n, int64_t nn,
    const uint32_t* d, int64_t nd, int8_t sign, uint32_t* scratch, int64_t cap) {
  uint32_t* a = scratch;
  uint32_t* b = a + cap;
  uint32_t* q = b + cap;
  uint32_t* r = q + cap;
  int64_t e = bit_length(n, nn) - bit_length(d, nd);
  if (e >= 0) {
    shift_copy(b, cap, d, nd, e);
    if (compare(n, nn, b, cap) < 0) --e;
  } else {
    shift_copy(a, cap, n, nn, -e);
    if (compare(a, cap, d, nd) < 0) --e;
  }
  uint32_t signbit = sign < 0 ? 0x80000000u : 0;
  if (e > 127) return __uint_as_float(signbit | 0x7f800000u);
  if (e < -150) return __uint_as_float(signbit);
  int64_t shift = e >= -126 ? 23 - e : 149;
  if (shift >= 0) {
    shift_copy(a, cap, n, nn, shift);
    copy(b, cap, d, nd);
  } else {
    copy(a, cap, n, nn);
    shift_copy(b, cap, d, nd, -shift);
  }
  divide(q, cap, r, cap, a, cap, b, cap);
  subtract(a, cap, b, cap, r, cap);
  int order = compare(r, cap, a, cap);
  uint32_t m = q[0];
  if (order > 0 || (order == 0 && (m & 1))) ++m;
  if (e >= -126) {
    if (m == 0x1000000u) { m >>= 1; ++e; }
    if (e > 127) return __uint_as_float(signbit | 0x7f800000u);
    return __uint_as_float(signbit | (uint32_t(e + 127) << 23) | (m & 0x7fffffu));
  }
  return __uint_as_float(signbit | m);
}
__global__ void project_kernel(int op, const uint32_t* nw, int64_t wn,
    const int8_t* ns, const uint32_t* dw, int64_t wd, float* out,
    int32_t* fault, uint32_t* work, int64_t cap, int64_t rows) {
  int64_t row = int64_t(blockIdx.x) * blockDim.x + threadIdx.x;
  if (row >= rows) return;
  const auto* n = nw + row * wn;
  const auto* d = dw + row * wd;
  int64_t nn = length(n, wn), nd = length(d, wd);
  fault[row] = !nd;
  if (!nd || !nn) { out[row] = 0; return; }
  if (op == 0) {
    out[row] = ratio_f32(n, nn, d, nd, ns[row], work + row * 4 * cap, cap);
    return;
  }
  double exponent = log_ratio(n, nn, d, nd);
  if (op == 2) exponent *= 0.5;
  double projected = exponent > 0 ? exponent + log1p(exp(-exponent)) : log1p(exp(exponent));
  out[row] = float(ns[row] < 0 ? -projected : projected);
}

// 验证与实际bit_length共用的位长/字位索引，不分配或长除256MiB整数。
__global__ void index_probe_kernel(const int64_t* widths, const uint32_t* tops,
                                   int64_t* out, int64_t rows) {
  int64_t row = int64_t(blockIdx.x) * blockDim.x + threadIdx.x;
  if (row >= rows) return;
  int64_t bits = bit_length_from_top(tops[row], widths[row]);
  out[row * 3] = bits;
  out[row * 3 + 1] = (bits - 1) / 32;
  out[row * 3 + 2] = (bits - 1) % 32;
}
extern "C" void public_gpu_integer(int op, const uint32_t* a, int64_t wa,
    const int8_t* sa, const uint32_t* b, int64_t wb, const int8_t* sb,
    uint32_t* o, int64_t wo, int8_t* so, uint32_t* r, int64_t wr,
    int8_t* sr, int32_t* f, int64_t rows, cudaStream_t stream) {
  integer_kernel<<<(rows + 255) / 256, 256, 0, stream>>>(
      op, a, wa, sa, b, wb, sb, o, wo, so, r, wr, sr, f, rows);
}
extern "C" void public_gpu_project(int op, const uint32_t* n, int64_t wn,
    const int8_t* sn, const uint32_t* d, int64_t wd, float* o, int32_t* f,
    uint32_t* work, int64_t cap, int64_t rows, cudaStream_t stream) {
  project_kernel<<<(rows + 255) / 256, 256, 0, stream>>>(
      op, n, wn, sn, d, wd, o, f, work, cap, rows);
}
extern "C" void public_gpu_index_probe(const int64_t* widths, const uint32_t* tops,
    int64_t* out, int64_t rows, cudaStream_t stream) {
  index_probe_kernel<<<(rows + 255) / 256, 256, 0, stream>>>(widths, tops, out, rows);
}
