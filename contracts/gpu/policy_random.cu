#include <cuda_runtime.h>
#include <curand_kernel.h>
#include <cstdint>

// cuRAND Philox原seed/subsequence/offset。纯候选，不改游标或全局Torch RNG。
// https://docs.nvidia.com/cuda/archive/12.9.0/curand/device-api-overview.html
__global__ void policy_uniform_kernel(const int64_t* seed,const int64_t* counter,const bool* enabled,
    double* output,int64_t rows) {
  const int64_t row=int64_t(blockIdx.x)*blockDim.x+threadIdx.x;
  if (row>=rows) return;
  if (!enabled[row]) {output[row]=0.5;return;}
  const auto* words=counter+row*4;
  const uint64_t offset=uint64_t(uint32_t(words[0]))|(uint64_t(uint32_t(words[1]))<<32);
  const uint64_t subsequence=uint64_t(uint32_t(words[2]))|(uint64_t(uint32_t(words[3]))<<32);
  curandStatePhilox4_32_10_t state;
  curand_init(uint64_t(seed[row]),subsequence,offset,&state);
  output[row]=(double(curand(&state))+0.5)/4294967296.0;
}
extern "C" void public_gpu_policy_uniform(const int64_t* seed,const int64_t* counter,const bool* enabled,
    double* output,int64_t rows,cudaStream_t stream) {
  policy_uniform_kernel<<<(rows+255)/256,256,0,stream>>>(seed,counter,enabled,output,rows);
}
