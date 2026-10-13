#include "exact.cuh"
#include <cmath>
using namespace public_gpu;

// SIM阶段行安装：禁用行只检查掩码，不读取或复制未准备的scratch字节。
__global__ void selected_rows_kernel(uint8_t* target, const uint8_t* incoming,
    const int64_t* rows, const bool* enabled, int64_t bytes) {
  const int64_t source = blockIdx.x;
  if (!enabled[source]) return;
  const int64_t destination = rows[source];
  for (int64_t byte = threadIdx.x; byte < bytes; byte += blockDim.x)
    target[destination * bytes + byte] = incoming[source * bytes + byte];
}
extern "C" void public_gpu_selected_rows(void* target, const void* incoming,
    const int64_t* rows, const bool* enabled, int64_t bytes, int64_t count, cudaStream_t stream) {
  selected_rows_kernel<<<count,256,0,stream>>>(static_cast<uint8_t*>(target),
      static_cast<const uint8_t*>(incoming),rows,enabled,bytes);
}

// 固定图只绑定描述向量；实际进入态地址在原阶段开始时刷新。
__global__ void bound_rows_kernel(uint8_t* value, const int64_t* addresses,
    int64_t field, const int64_t* rows, const bool* enabled, int64_t bytes, bool scatter) {
  const int64_t selected = blockIdx.x;
  if (!enabled[selected]) return;
  auto* bound = reinterpret_cast<uint8_t*>(addresses[field]);
  auto* destination = scatter ? bound + rows[selected] * bytes : value + selected * bytes;
  const auto* source = scatter ? value + selected * bytes : bound + rows[selected] * bytes;
  for (int64_t byte = threadIdx.x; byte < bytes; byte += blockDim.x)
    destination[byte] = source[byte];
}
extern "C" void public_gpu_bound_rows(void* value, const int64_t* addresses,
    int64_t field, const int64_t* rows, const bool* enabled, int64_t bytes,
    int64_t count, bool scatter, cudaStream_t stream) {
  bound_rows_kernel<<<count,256,0,stream>>>(static_cast<uint8_t*>(value),addresses,
      field,rows,enabled,bytes,scatter);
}

__global__ void integer_kernel(int op, const uint32_t* aw, int64_t wa,
    const int8_t* as, const uint32_t* bw, int64_t wb, const int8_t* bs,
    uint32_t* ow, int64_t wo, int8_t* os, uint32_t* rw, int64_t wr,
    int8_t* rs, int32_t* fault, uint32_t* work, int64_t cap,
    int64_t* required, int64_t rows) {
  int64_t row = int64_t(blockIdx.x) * blockDim.x + threadIdx.x;
  if (row >= rows) return;
  const auto* a = aw + row * wa;
  const auto* b = bw + row * wb;
  auto* output = ow + row * wo;
  auto* remainder = rw + row * wr;
  auto* o = work + row * 2 * cap;
  auto* r = o + cap;
  int64_t na = length(a, wa), nb = length(b, wb);
  int sa = na ? as[row] : 0, sb = nb ? bs[row] : 0;
  zero(o, cap);
  zero(r, cap);
  fault[row] = 0;
  required[row] = 0;
  rs[row] = 0;
  if (op == 0 || op == 1) {
    if (op == 1) sb = -sb;
    if (sa == sb) {
      add(o, cap, a, na, b, nb);
      os[row] = sa;
    } else if (compare(a, na, b, nb) >= 0) {
      subtract(o, cap, a, na, b, nb);
      os[row] = sa;
    } else {
      subtract(o, cap, b, nb, a, na);
      os[row] = sb;
    }
  } else if (op == 2) {
    multiply(o, cap, a, na, b, nb);
    os[row] = sa * sb;
  } else if (op == 3) {
    fault[row] = !divide(o, cap, r, cap, a, na, b, nb);
    os[row] = sa * sb;
    rs[row] = length(r, cap) ? sa : 0;
  } else {
    os[row] = sa == sb ? sa * compare(a, na, b, nb) : (sa < sb ? -1 : 1);
  }
  int64_t used = length(o, cap);
  int64_t used_rem = length(r, cap);
  if (used > wo || used_rem > wr) {
    // 独立资源信号，整批扩容重试；不是业务拒绝、截断数值或NoTrade。
    fault[row] |= 2;
    required[row] = used > used_rem ? used : used_rem;
    zero(output, wo);
    zero(remainder, wr);
    os[row] = rs[row] = 0;
    return;
  }
  copy(output, wo, o, used);
  copy(remainder, wr, r, used_rem);
  if (op != 4 && !used) os[row] = 0;
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
    int8_t* sr, int32_t* f, uint32_t* work, int64_t cap,
    int64_t* required, int64_t rows, cudaStream_t stream) {
  integer_kernel<<<(rows + 255) / 256, 256, 0, stream>>>(
      op, a, wa, sa, b, wb, sb, o, wo, so, r, wr, sr, f, work, cap, required, rows);
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

__global__ void gcd_kernel(const uint32_t* a, int64_t wa, const uint32_t* b,
    int64_t wb, uint32_t* out, uint32_t* work, int64_t cap, int64_t rows) {
  int64_t row = int64_t(blockIdx.x) * blockDim.x + threadIdx.x;
  if (row >= rows) return;
  int64_t scratch_cap = cap + 1;
  uint32_t* x = work + row * 4 * scratch_cap;
  uint32_t* y = x + scratch_cap;
  uint32_t* q = y + scratch_cap;
  uint32_t* r = q + scratch_cap;
  copy(x, scratch_cap, a + row * wa, wa);
  copy(y, scratch_cap, b + row * wb, wb);
  while (length(y, scratch_cap)) {
    divide(q, scratch_cap, r, scratch_cap, x, scratch_cap, y, scratch_cap);
    // 旋转独占临时区；不用CPU条件判断或回传余数。
    uint32_t* old_x = x;
    x = y;
    y = r;
    r = old_x;
  }
  copy(out + row * cap, cap, x, cap);
}
extern "C" void public_gpu_gcd(const uint32_t* a, int64_t wa, const uint32_t* b,
    int64_t wb, uint32_t* out, uint32_t* work, int64_t cap, int64_t rows,
    cudaStream_t stream) {
  gcd_kernel<<<(rows + 255) / 256, 256, 0, stream>>>(a, wa, b, wb, out, work, cap, rows);
}

// sum_{0<=i<n} floor((a*i+b)/m)，非负整数；Euclid循环全在设备。
// 输入每个最多w字，结果 < n*(a*n+b)故3w+4临时字足够，不设业务位宽。
__global__ void floor_sum_kernel(const uint32_t* counts, int64_t wn,
    const uint32_t* moduli, int64_t wm, const uint32_t* slopes, int64_t wa,
    const uint32_t* offsets, int64_t wb, uint32_t* out, int64_t wo,
    int32_t* fault, uint32_t* work, int64_t cap, int64_t* required, int64_t rows) {
  int64_t row = int64_t(blockIdx.x) * blockDim.x + threadIdx.x;
  if (row >= rows) return;
  bool valid = floor_sum_value(counts + row * wn, wn, moduli + row * wm, wm,
      slopes + row * wa, wa, offsets + row * wb, wb, out + row * wo, wo,
      work + row * 10 * cap, cap, required[row]);
  fault[row] = valid ? 0 : 1;
  if (required[row] > wo) fault[row] |= 2;
}
extern "C" void public_gpu_floor_sum(const uint32_t* n, int64_t wn,
    const uint32_t* m, int64_t wm, const uint32_t* a, int64_t wa,
    const uint32_t* b, int64_t wb, uint32_t* out, int64_t wo,
    int32_t* fault, uint32_t* work, int64_t cap, int64_t* required, int64_t rows,
    cudaStream_t stream) {
  floor_sum_kernel<<<(rows + 255) / 256, 256, 0, stream>>>(
      n, wn, m, wm, a, wa, b, wb, out, wo, fault, work, cap, required, rows);
}
