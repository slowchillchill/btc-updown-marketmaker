#include <cuda_runtime.h>
#include <stdint.h>

// 只查询原组前缀内每个事实列的末项；列按target/原步骤序排序，CPU不推进业务分支。
__global__ void prefix_lookup_kernel(const int64_t* rows, int64_t count,
    const int64_t* offsets, int64_t targets, const int64_t* prefixes,
    int64_t roots, int64_t* output, int32_t* fault) {
  int64_t item = int64_t(blockIdx.x) * blockDim.x + threadIdx.x;
  int64_t total = roots * targets;
  if (item >= total) return;
  int64_t target = item % targets, root = item / targets;
  int64_t begin = offsets[target], end = offsets[target + 1];
  output[item] = -1;
  fault[item] = 0;
  if (begin < 0 || end < begin || end > count) { fault[item] = 1; return; }
  int64_t low = begin, high = end;
  while (low < high) {
    int64_t middle = low + (high - low) / 2;
    if (rows[middle * 4 + 2] <= prefixes[root]) low = middle + 1;
    else high = middle;
  }
  if (low != begin) output[item] = low - 1;
}
extern "C" void public_gpu_prefix_lookup(const int64_t* rows, int64_t count,
    const int64_t* offsets, int64_t targets, const int64_t* prefixes,
    int64_t roots, int64_t* output, int32_t* fault, cudaStream_t stream) {
  int64_t total = roots * targets;
  if (total) prefix_lookup_kernel<<<(total + 255) / 256, 256, 0, stream>>>(
      rows, count, offsets, targets, prefixes, roots, output, fault);
}
