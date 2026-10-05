#pragma once

// #181 有限诊断：12 位最近偶数分配；此容量不是生产合法数值域。
#include <stdint.h>
#include <cuda_runtime.h>
#include <gmp.h>
#include "cgbn/cgbn.h"

namespace finite_attribution_gpu_sample {
constexpr uint32_t bits = 256;
constexpr uint32_t threads_per_integer = 8;
using Context = cgbn_context_t<threads_per_integer>;
using Env = cgbn_env_t<Context, bits>;
using Number = Env::cgbn_t;
using Wide = Env::cgbn_wide_t;
using Memory = cgbn_mem_t<bits>;

// 官方 load 的指针所指类型不含 const；局部复制保留输入只读语义。
__device__ inline void load(Env &env, Number &out, const Memory &input) {
  Memory local = input;
  cgbn_load(env, out, &local);
}
__device__ inline void multiply(Env &env, Number &out, const Number &a,
                                const Number &b, uint32_t &fault) {
  Wide product;
  cgbn_mul_wide(env, product, a, b);
  fault |= cgbn_compare_ui32(env, product._high, 0) != 0;
  cgbn_set(env, out, product._low);
}
__device__ inline void add(Env &env, Number &out, const Number &a,
                           const Number &b, uint32_t &fault) {
  fault |= cgbn_add(env, out, a, b) != 0;
}

// numerator 是完整 512 位乘积。除法仅在商可装入本诊断 256 位时执行。
__device__ inline void divide_rne(Env &env, Number &out, const Wide &numerator,
                                  const Number &denominator, uint32_t &fault) {
  if (cgbn_compare_ui32(env, denominator, 0) == 0 ||
      cgbn_compare(env, numerator._high, denominator) >= 0) {
    fault = 1;
    cgbn_set_ui32(env, out, 0);
    return;
  }
  Number remainder, distance;
  cgbn_div_rem_wide(env, out, remainder, numerator, denominator);
  cgbn_sub(env, distance, denominator, remainder);
  const int order = cgbn_compare(env, remainder, distance);
  if (order > 0 || (order == 0 && (cgbn_get_ui32(env, out) & 1)))
    fault |= cgbn_add_ui32(env, out, out, 1) != 0;
}

// 所有金额以源尺度 S>=12 的整数表示；factor=10^(S-12)。不预先压缩源精度。
__device__ inline void round_scale12(Env &env, Number &out, const Number &magnitude,
                                     const Number &factor, uint32_t &fault) {
  Wide numerator;
  cgbn_set(env, numerator._low, magnitude);
  cgbn_set_ui32(env, numerator._high, 0);
  Number rounded;
  divide_rne(env, rounded, numerator, factor, fault);
  multiply(env, out, rounded, factor, fault);
}

__device__ inline void split_scale12(Env &env, Number &allocated, Number &remaining,
                                     const Number &total, const Number &part,
                                     const Number &whole, const Number &factor,
                                     uint32_t &fault) {
  if (cgbn_compare_ui32(env, whole, 0) == 0 || cgbn_compare(env, part, whole) > 0) {
    fault = 1;
    cgbn_set_ui32(env, allocated, 0);
    cgbn_set(env, remaining, total);
    return;
  }
  if (cgbn_compare_ui32(env, part, 0) == 0) {
    cgbn_set_ui32(env, allocated, 0);
  } else if (cgbn_compare(env, part, whole) == 0) {
    cgbn_set(env, allocated, total);
  } else {
    Wide numerator;
    Number denominator, rounded;
    cgbn_mul_wide(env, numerator, total, part);
    multiply(env, denominator, whole, factor, fault);
    divide_rne(env, rounded, numerator, denominator, fault);
    multiply(env, allocated, rounded, factor, fault);
    // 更细源金额可能小于一次舍入单位，使用精确上边界避免负余量。
    if (cgbn_compare(env, allocated, total) > 0) cgbn_set(env, allocated, total);
  }
  fault |= cgbn_sub(env, remaining, total, allocated) != 0;
  Number restored;
  add(env, restored, allocated, remaining, fault);
  fault |= cgbn_compare(env, restored, total) != 0;
}

// 容量候选：稳定顺序、剩余总额/容量二分；边界保证本项和后续项都可容纳。
__device__ inline void allocate_capacity(Env &env, Number &allocated,
                                        Number &remaining_total, Number &remaining_capacity,
                                        const Number &capacity, const Number &factor,
                                        bool last, uint32_t &fault) {
  Number future, lower, upper, ignored;
  fault |= cgbn_sub(env, future, remaining_capacity, capacity) != 0;
  if (cgbn_compare_ui32(env, capacity, 0) == 0) {
    cgbn_set_ui32(env, allocated, 0);
  } else if (last) {
    cgbn_set(env, allocated, remaining_total);
  } else {
    split_scale12(env, allocated, ignored, remaining_total, capacity,
                  remaining_capacity, factor, fault);
    if (cgbn_compare(env, remaining_total, future) > 0)
      cgbn_sub(env, lower, remaining_total, future);
    else
      cgbn_set_ui32(env, lower, 0);
    cgbn_set(env, upper, cgbn_compare(env, capacity, remaining_total) < 0 ? capacity : remaining_total);
    if (cgbn_compare(env, allocated, lower) < 0) cgbn_set(env, allocated, lower);
    if (cgbn_compare(env, allocated, upper) > 0) cgbn_set(env, allocated, upper);
  }
  fault |= cgbn_compare(env, allocated, capacity) > 0;
  fault |= cgbn_sub(env, remaining_total, remaining_total, allocated) != 0;
  cgbn_set(env, remaining_capacity, future);
}
} // namespace finite_attribution_gpu_sample
