// #181 有限标准数学函数诊断；不接入公共生产类型或规定生产位宽。
#pragma once
#include <cmath>
#include <cstdint>

#ifdef __CUDACC__
#define STANDARD_PROJECTION_HD __host__ __device__
#else
#define STANDARD_PROJECTION_HD
#endif

namespace standard_projection_sample {
struct LeadingInteger {
  uint64_t significand;
  int exponent;
};

// 取最多53个有效高位。低位直接截去；这不是整数或整体比值的正确舍入算法。
// 单独缩放两个整数，避免先把整个分子、分母转为double而产生inf/inf。
STANDARD_PROJECTION_HD inline LeadingInteger leading_integer(const uint32_t *words, uint32_t count) {
  uint32_t high = words[count - 1];
  int high_bits = 0;
  while (high) { ++high_bits; high >>= 1; }
  const int bits = static_cast<int>((count - 1) * 32) + high_bits;
  const int shift = bits > 53 ? bits - 53 : 0;
  uint64_t significand = 0;
  for (int bit = bits - 1; bit >= shift; --bit)
    significand = (significand << 1) | ((words[bit / 32] >> (bit % 32)) & 1u);
  return {significand, shift};
}

STANDARD_PROJECTION_HD inline double scaled_ratio(const uint32_t *numerator, uint32_t numerator_count,
                                                  const uint32_t *denominator, uint32_t denominator_count,
                                                  bool negative) {
  const auto n = leading_integer(numerator, numerator_count);
  const auto d = leading_integer(denominator, denominator_count);
  if (n.significand == 0) return 0.0;
  const double magnitude = ldexp(static_cast<double>(n.significand) / static_cast<double>(d.significand),
                                 n.exponent - d.exponent);
  return negative ? -magnitude : magnitude;
}
}  // namespace standard_projection_sample

#undef STANDARD_PROJECTION_HD
