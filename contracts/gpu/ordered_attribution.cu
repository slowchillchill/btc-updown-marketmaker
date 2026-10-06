#include "exact.cuh"
using namespace public_gpu;

namespace {
struct Ratio { uint32_t* n; uint32_t* d; };

// 每根独占临时区。业务数值均为非负任意精度有理数；cap仅是当前物理容量。
struct Arithmetic {
  int64_t cap;
  uint32_t* work;
  int32_t fault = 0;
  int64_t required = 0;

  __device__ void product(uint32_t* out, const uint32_t* a, const uint32_t* b) {
    int64_t na = length(a, cap), nb = length(b, cap);
    if (na + nb >= cap) {
      fault |= 2;
      required = max(required, na + nb + 1);
      zero(out, cap);
      return;
    }
    multiply(out, cap, a, na, b, nb);
  }

  __device__ void assign(Ratio out, Ratio in) {
    copy(out.n, cap, in.n, cap);
    copy(out.d, cap, in.d, cap);
  }

  __device__ void normalize(Ratio value) {
    auto* a = work + 4 * cap;
    auto* b = work + 5 * cap;
    auto* q = work + 6 * cap;
    auto* r = work + 7 * cap;
    if (!length(value.d, cap)) { fault |= 1; return; }
    if (!length(value.n, cap)) {
      zero(value.d, cap); value.d[0] = 1;
      return;
    }
    copy(a, cap, value.n, cap);
    copy(b, cap, value.d, cap);
    while (length(b, cap)) {
      divide(q, cap, r, cap, a, cap, b, cap);
      copy(a, cap, b, cap);
      copy(b, cap, r, cap);
    }
    divide(q, cap, r, cap, value.n, cap, a, cap);
    copy(value.n, cap, q, cap);
    divide(q, cap, r, cap, value.d, cap, a, cap);
    copy(value.d, cap, q, cap);
  }

  __device__ int order(Ratio a, Ratio b) {
    product(work, a.n, b.d);
    product(work + cap, b.n, a.d);
    return compare(work, cap, work + cap, cap);
  }

  __device__ void combine(Ratio out, Ratio a, Ratio b, bool minus = false) {
    product(work, a.n, b.d);
    product(work + cap, b.n, a.d);
    product(work + 2 * cap, a.d, b.d);
    if (minus) {
      if (compare(work, cap, work + cap, cap) < 0) {
        fault |= 1; return;
      }
      subtract(work + 3 * cap, cap, work, cap, work + cap, cap);
    } else {
      int64_t needed = max(length(work, cap), length(work + cap, cap)) + 1;
      if (needed > cap) { fault |= 2; required = max(required, needed); return; }
      add(work + 3 * cap, cap, work, cap, work + cap, cap);
    }
    copy(out.n, cap, work + 3 * cap, cap);
    copy(out.d, cap, work + 2 * cap, cap);
    normalize(out);
  }

  __device__ void multiply_ratio(Ratio out, Ratio a, Ratio b, bool inverse = false) {
    product(work, a.n, inverse ? b.d : b.n);
    product(work + cap, a.d, inverse ? b.n : b.d);
    copy(out.n, cap, work, cap);
    copy(out.d, cap, work + cap, cap);
    normalize(out);
  }

  __device__ void round_scale12(Ratio out, Ratio value) {
    auto* scale = work + 2 * cap;
    auto* q = work + 3 * cap;
    auto* r = work + 4 * cap;
    auto* other = work + 5 * cap;
    zero(scale, cap);
    uint64_t quantum = 1000000000000ULL;
    scale[0] = uint32_t(quantum); scale[1] = uint32_t(quantum >> 32);
    product(work, value.n, scale);
    if (!divide(q, cap, r, cap, work, cap, value.d, cap)) { fault |= 1; return; }
    // 比较r与den-r，避免2r额外溢出；平局按商奇偶最近偶数。
    subtract(other, cap, value.d, cap, r, cap);
    int relation = compare(r, cap, other, cap);
    if (relation > 0 || (relation == 0 && (q[0] & 1))) {
      zero(other, cap); other[0] = 1;
      if (length(q, cap) == cap) { fault |= 2; required = max(required, cap + 1); return; }
      add(q, cap, q, cap, other, 1);
    }
    copy(out.n, cap, q, cap);
    copy(out.d, cap, scale, cap);
    normalize(out);
  }
};

__global__ void ordered_kernel(const uint32_t* tn, const uint32_t* td,
    const int8_t* ts, const uint32_t* cn, const uint32_t* cd, const int8_t* cs,
    int64_t width, int64_t columns, const bool* enabled, uint32_t* on,
    uint32_t* od, int32_t* fault, int64_t* required, uint32_t* scratch,
    int64_t cap, int64_t roots) {
  int64_t root = int64_t(blockIdx.x) * blockDim.x + threadIdx.x;
  if (root >= roots) return;
  fault[root] = 0; required[root] = 0;
  if (!enabled[root]) {
    for (int64_t column = 0; column < columns; ++column) {
      int64_t at = (root * columns + column) * width;
      copy(on + at, width, cn + at, width);
      copy(od + at, width, cd + at, width);
    }
    return;
  }
  auto* storage = scratch + root * 30 * cap;
  Arithmetic math{cap, storage};
  Ratio values[11];
  for (int i = 0; i < 11; ++i) values[i] = {storage + (8 + 2*i)*cap, storage + (9 + 2*i)*cap};
  auto sum = values[0], remaining = values[1], item = values[2], future = values[3];
  auto low = values[4], high = values[5], proposed = values[6], temporary = values[7];
  auto total = values[8], empty = values[9], scale_ratio = values[10];
  for (auto value : values) { zero(value.n, cap); zero(value.d, cap); value.d[0] = 1; }
  copy(total.n, cap, tn + root*width, width);
  copy(total.d, cap, td + root*width, width);
  if (ts[root] < 0 || !length(total.d, cap)) math.fault |= 1;
  math.assign(remaining, total);
  for (int64_t column = 0; column < columns; ++column) {
    int64_t at = (root*columns + column)*width;
    copy(item.n, cap, cn + at, width); copy(item.d, cap, cd + at, width);
    if (cs[root*columns + column] < 0 || !length(item.d, cap)) math.fault |= 1;
    if (!math.fault) math.combine(sum, sum, item);
  }
  if (!math.fault && math.order(total, sum) > 0) math.fault |= 1;
  for (int64_t column = 0; column < columns && !math.fault; ++column) {
    int64_t at = (root*columns + column)*width;
    copy(item.n, cap, cn + at, width); copy(item.d, cap, cd + at, width);
    math.combine(future, sum, item, true);
    if (math.order(remaining, future) > 0) math.combine(low, remaining, future, true);
    else math.assign(low, empty);
    math.assign(high, math.order(item, remaining) <= 0 ? item : remaining);
    if (!length(item.n, cap) || !length(remaining.n, cap)) math.assign(proposed, empty);
    else if (math.order(item, sum) == 0) math.assign(proposed, remaining);
    else {
      math.multiply_ratio(temporary, remaining, item);
      math.multiply_ratio(scale_ratio, temporary, sum, true);
      math.round_scale12(proposed, scale_ratio);
      if (math.order(proposed, low) < 0) math.assign(proposed, low);
      if (math.order(proposed, high) > 0) math.assign(proposed, high);
    }
    int64_t needed = max(length(proposed.n, cap), length(proposed.d, cap));
    if (needed > width) { math.fault |= 2; math.required = max(math.required, needed); break; }
    copy(on + at, width, proposed.n, cap); copy(od + at, width, proposed.d, cap);
    math.combine(remaining, remaining, proposed, true);
    math.assign(sum, future);
  }
  if (!math.fault && length(remaining.n, cap)) math.fault |= 1;
  fault[root] = math.fault; required[root] = math.required;
}
}

extern "C" void public_gpu_ordered_attribution(const uint32_t* tn, const uint32_t* td,
    const int8_t* ts, const uint32_t* cn, const uint32_t* cd, const int8_t* cs,
    int64_t width, int64_t columns, const bool* enabled, uint32_t* on,
    uint32_t* od, int32_t* fault, int64_t* required, uint32_t* scratch,
    int64_t cap, int64_t roots, cudaStream_t stream) {
  ordered_kernel<<<(roots+127)/128,128,0,stream>>>(tn,td,ts,cn,cd,cs,width,columns,
      enabled,on,od,fault,required,scratch,cap,roots);
}
