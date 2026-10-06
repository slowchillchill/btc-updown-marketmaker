#include "../../contracts/gpu/exact.cuh"

// 输入为已归一化ExactDecimal文本；原source token另存，不能用本结果覆盖原证据。
__global__ void decimal_kernel(const uint8_t* bytes, int64_t size,
    const int64_t* offsets, const bool* present, uint32_t* coefficient,
    uint32_t* denominator, int8_t* signs, int32_t* scales,
    int32_t* faults, int64_t rows) {
  int64_t row = int64_t(blockIdx.x) * blockDim.x + threadIdx.x;
  if (row >= rows) return;
  auto* c = coefficient + row * 3;
  auto* d = denominator + row * 3;
  public_gpu::zero(c, 3);
  public_gpu::zero(d, 3);
  d[0] = 1;
  signs[row] = 0;
  scales[row] = faults[row] = 0;
  if (!present[row]) return;
  int64_t start = offsets[row], end = offsets[row + 1];
  if (start < 0 || end <= start || end > size) { faults[row] = 1; return; }
  int sign = 1;
  if (bytes[start] == '-' || bytes[start] == '+') {
    sign = bytes[start] == '-' ? -1 : 1;
    ++start;
  }
  bool point = false, digit = false, last_digit = false;
  int32_t scale = 0;
  for (int64_t i = start; i < end; ++i) {
    uint8_t b = bytes[i];
    if (b == '.' && !point && digit) { point = true; last_digit = false; continue; }
    if (b < '0' || b > '9') { faults[row] = 1; return; }
    digit = last_digit = true;
    uint64_t carry = b - '0';
    for (int j = 0; j < 3; ++j) {
      uint64_t next = uint64_t(c[j]) * 10 + carry;
      c[j] = uint32_t(next);
      carry = next >> 32;
    }
    if (carry) { faults[row] = 1; return; }
    if (point && ++scale > 28) { faults[row] = 1; return; }
  }
  if (!digit || !last_digit) { faults[row] = 1; return; }
  for (int32_t i = 0; i < scale; ++i) {
    uint64_t carry = 0;
    for (int j = 0; j < 3; ++j) {
      uint64_t next = uint64_t(d[j]) * 10 + carry;
      d[j] = uint32_t(next);
      carry = next >> 32;
    }
  }
  signs[row] = public_gpu::length(c, 3) ? sign : 0;
  scales[row] = scale;
}
extern "C" void public_gpu_decimal(const uint8_t* bytes, int64_t size,
    const int64_t* offsets, const bool* present, uint32_t* coefficient,
    uint32_t* denominator, int8_t* signs, int32_t* scales,
    int32_t* faults, int64_t rows, cudaStream_t stream) {
  decimal_kernel<<<(rows + 255) / 256, 256, 0, stream>>>(bytes, size, offsets,
      present, coefficient, denominator, signs, scales, faults, rows);
}
