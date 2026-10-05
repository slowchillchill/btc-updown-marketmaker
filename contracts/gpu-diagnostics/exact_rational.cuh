#pragma once

// #181 有限诊断：对应 EconomicRational；256 位仅覆盖随附样本，不是生产域上限。
#include <stdint.h>
#include <cuda_runtime.h>
#include <gmp.h>
#include "cgbn/cgbn.h"

namespace rational_gpu_sample {
constexpr uint32_t bits = 256;
constexpr uint32_t threads_per_integer = 8;
using Context = cgbn_context_t<threads_per_integer>;
using Env = cgbn_env_t<Context, bits>;
using Number = Env::cgbn_t;
using Wide = Env::cgbn_wide_t;
using Memory = cgbn_mem_t<bits>;
struct Stored {
  Memory numerator, denominator;
  uint32_t numerator_negative, denominator_negative;
};
struct Rational { Number numerator, denominator; uint32_t negative; };

// 官方接口 load 接受非 const 指针；只复制局部输入，不修改已传入的材料。
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
__device__ inline void normalize(Env &env, Rational &value, uint32_t &fault) {
  if (cgbn_compare_ui32(env, value.denominator, 0) == 0) {
    fault = 1;
    cgbn_set_ui32(env, value.denominator, 1);
    cgbn_set_ui32(env, value.numerator, 0);
  }
  if (cgbn_compare_ui32(env, value.numerator, 0) == 0) {
    value.negative = 0;
    cgbn_set_ui32(env, value.denominator, 1);
    return;
  }
  Number divisor, n, d;
  cgbn_gcd(env, divisor, value.numerator, value.denominator);
  cgbn_div(env, n, value.numerator, divisor);
  cgbn_div(env, d, value.denominator, divisor);
  cgbn_set(env, value.numerator, n);
  cgbn_set(env, value.denominator, d);
}
__device__ inline void from_stored(Env &env, Rational &out, const Stored &input,
                                   uint32_t &fault) {
  load(env, out.numerator, input.numerator);
  load(env, out.denominator, input.denominator);
  out.negative = input.numerator_negative ^ input.denominator_negative;
  normalize(env, out, fault);
}
__device__ inline void store(Env &env, Stored &out, const Rational &value) {
  cgbn_store(env, &out.numerator, value.numerator);
  cgbn_store(env, &out.denominator, value.denominator);
  if (threadIdx.x == 0) {
    out.numerator_negative = value.negative;
    out.denominator_negative = 0;
  }
}
__device__ inline void add_sub(Env &env, Rational &out, const Rational &a,
                               const Rational &b, bool subtract, uint32_t &fault) {
  Number left, right;
  Rational result;
  multiply(env, left, a.numerator, b.denominator, fault);
  multiply(env, right, b.numerator, a.denominator, fault);
  multiply(env, result.denominator, a.denominator, b.denominator, fault);
  const uint32_t right_negative = b.negative ^ uint32_t(subtract);
  if (a.negative == right_negative) {
    fault |= cgbn_add(env, result.numerator, left, right) != 0;
    result.negative = a.negative;
  } else if (cgbn_compare(env, left, right) >= 0) {
    cgbn_sub(env, result.numerator, left, right);
    result.negative = a.negative;
  } else {
    cgbn_sub(env, result.numerator, right, left);
    result.negative = right_negative;
  }
  normalize(env, result, fault);
  out = result;
}
__device__ inline void mul_div(Env &env, Rational &out, const Rational &a,
                               const Rational &b, bool divide, uint32_t &fault) {
  Rational result;
  multiply(env, result.numerator, a.numerator, divide ? b.denominator : b.numerator, fault);
  multiply(env, result.denominator, a.denominator, divide ? b.numerator : b.denominator, fault);
  result.negative = a.negative ^ b.negative;
  normalize(env, result, fault);
  out = result;
}
__device__ inline int compare(Env &env, const Rational &a, const Rational &b,
                              uint32_t &fault) {
  if (a.negative != b.negative) return a.negative ? -1 : 1;
  Number left, right;
  multiply(env, left, a.numerator, b.denominator, fault);
  multiply(env, right, b.numerator, a.denominator, fault);
  const int order = cgbn_compare(env, left, right);
  return a.negative ? -order : order;
}
__device__ inline void shift_left(Env &env, Number &out, const Number &value,
                                  uint32_t shift, uint32_t &fault) {
  fault |= shift > cgbn_clz(env, value);
  cgbn_shift_left(env, out, value, shift);
}

// 与 native-inputs::exact::round_f32 相同的最近偶数规则，整个投影在整数域形成位模式。
__device__ inline uint32_t round_f32(Env &env, const Rational &value, uint32_t &fault) {
  const uint32_t sign = value.negative << 31;
  if (cgbn_compare_ui32(env, value.numerator, 0) == 0) return sign;
  int exponent = int(cgbn_clz(env, value.denominator)) - int(cgbn_clz(env, value.numerator));
  Number shifted;
  if (exponent >= 0) {
    shift_left(env, shifted, value.denominator, uint32_t(exponent), fault);
    if (cgbn_compare(env, value.numerator, shifted) < 0) --exponent;
  } else {
    shift_left(env, shifted, value.numerator, uint32_t(-exponent), fault);
    if (cgbn_compare(env, shifted, value.denominator) < 0) --exponent;
  }
  if (exponent > 127) return sign | 0x7f800000;
  if (exponent < -150) return sign;
  const int shift = exponent - 23 > -149 ? exponent - 23 : -149;
  Number a, b, quotient, remainder, distance;
  if (shift >= 0) {
    cgbn_set(env, a, value.numerator);
    shift_left(env, b, value.denominator, uint32_t(shift), fault);
  } else {
    shift_left(env, a, value.numerator, uint32_t(-shift), fault);
    cgbn_set(env, b, value.denominator);
  }
  cgbn_div_rem(env, quotient, remainder, a, b);
  cgbn_sub(env, distance, b, remainder);
  const int order = cgbn_compare(env, remainder, distance);
  if (order > 0 || (order == 0 && (cgbn_get_ui32(env, quotient) & 1)))
    fault |= cgbn_add_ui32(env, quotient, quotient, 1) != 0;
  fault |= cgbn_clz(env, quotient) < bits - 25;
  uint32_t q = cgbn_get_ui32(env, quotient);
  if (q == 0) return sign;
  if (shift == -149 && q < (1u << 23)) return sign | q;
  exponent = shift + 23;
  if (q == (1u << 24)) { q >>= 1; ++exponent; }
  if (exponent > 127) return sign | 0x7f800000;
  return sign | (uint32_t(exponent + 127) << 23) | (q - (1u << 23));
}
} // namespace rational_gpu_sample
