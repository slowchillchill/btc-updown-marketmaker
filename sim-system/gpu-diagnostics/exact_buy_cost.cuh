#pragma once

// #181 有限技术样板：256 位操作数、512 位乘积；仅指数 1、普通 GTC。
// 公共 CPU 参考为 sim-system/src/buy_cost.rs::limit_buy_cost_v1。
// 不接入生产，不证明全部规则、Decimal/BigUint 中间域或实际成交费用。
#include <stdint.h>
#include <cuda_runtime.h>
#include <gmp.h>
#include "cgbn/cgbn.h"

namespace sim_gpu_sample {
constexpr uint32_t bits = 256;
constexpr uint32_t threads_per_integer = 8;
using Context = cgbn_context_t<threads_per_integer>;
using Env = cgbn_env_t<Context, bits>;
using Number = Env::cgbn_t;
using Wide = Env::cgbn_wide_t;
using Memory = cgbn_mem_t<bits>;

struct CostInput {
  Memory quantity_centishares, rate_numerator, rate_denominator;
  uint32_t tick_denominator, price_index;
};
struct CostOutput {
  Memory principal_micro, fee_1e5_units;
  uint32_t arithmetic_fault;
};

// CGBN 的 load 接口要求非 const 指针；复制到线程局部存储，保持输入只读。
__device__ inline void load_read_only(Env &env, Number &out, const Memory &input) {
  Memory local = input;
  cgbn_load(env, out, &local);
}

// fault 只使本诊断失败，不定义生产订单拒绝策略。保留完整乘积后核对本样本位宽。
__device__ inline void multiply(Env &env, Number &out, const Number &a,
                                const Number &b, uint32_t &fault) {
  Wide product;
  cgbn_mul_wide(env, product, a, b);
  fault |= cgbn_compare_ui32(env, product._high, 0) != 0;
  cgbn_set(env, out, product._low);
}
__device__ inline void scale(Env &env, Number &out, const Number &a,
                             uint32_t factor, uint32_t &fault) {
  Number b;
  cgbn_set_ui32(env, b, factor);
  multiply(env, out, a, b, fault);
}
__device__ inline void half_up(Env &env, Number &out, const Number &numerator,
                               const Number &denominator, uint32_t &fault) {
  Number remainder, distance;
  cgbn_div_rem(env, out, remainder, numerator, denominator);
  // r >= d-r 等价于 2r >= d，避免倍增余数造成中间溢出。
  cgbn_sub(env, distance, denominator, remainder);
  if (cgbn_compare(env, remainder, distance) >= 0)
    fault |= cgbn_add_ui32(env, out, out, 1) != 0;
}
__device__ inline void limit_buy_cost_e1(Env &env, const CostInput &input,
                                         Number &principal, Number &fee,
                                         uint32_t &fault) {
  Number quantity, rate_numerator, rate_denominator, numerator, denominator;
  load_read_only(env, quantity, input.quantity_centishares);
  load_read_only(env, rate_numerator, input.rate_numerator);
  load_read_only(env, rate_denominator, input.rate_denominator);
  // 本样板 tick 为 1/100 或 1/10000；本金沿公共微单位整数入口。
  scale(env, numerator, quantity, input.price_index, fault);
  scale(env, numerator, numerator, 10000, fault);
  cgbn_set_ui32(env, denominator, input.tick_denominator);
  cgbn_div(env, principal, numerator, denominator);
  cgbn_set_ui32(env, fee, 0);
  if (cgbn_compare_ui32(env, principal, 1000000) < 0)
    return; // 普通 LIMIT 即时最低本金为 1 pUSD；沿已有公共分支。
  const uint32_t p = input.price_index < input.tick_denominator / 2
                         ? input.price_index : input.tick_denominator / 2;
  multiply(env, numerator, quantity, rate_numerator, fault);
  scale(env, numerator, numerator, p, fault);
  scale(env, numerator, numerator, input.tick_denominator - p, fault);
  scale(env, numerator, numerator, 1000, fault);
  scale(env, denominator, rate_denominator, input.tick_denominator, fault);
  scale(env, denominator, denominator, input.tick_denominator, fault);
  half_up(env, fee, numerator, denominator, fault);
}
} // namespace sim_gpu_sample
