#include <cuda_runtime.h>
#include <stdint.h>

// 原SIM source_trade_id为"sim-"+完整u64十进制；同账户/同Trade命名空间的
// MemoryLogicalIdentity顺序等价此十进制字节序，不能用数值fill_sequence顺序替代。
__global__ void sim_trade_key_kernel(const uint32_t* words,const int8_t* signs,
    int64_t width,uint8_t* keys,int32_t* fault,int64_t rows) {
  int64_t row=int64_t(blockIdx.x)*blockDim.x+threadIdx.x;
  if(row>=rows) return;
  const uint32_t* value=words+row*width;
  int32_t error=(signs[row]<0 || signs[row]>1) ? 1 : 0;
  for(int64_t column=2;column<width;++column) error |= value[column]!=0 ? 1 : 0;
  uint64_t number=value[0];
  if(width>1) number |= uint64_t(value[1])<<32;
  error |= ((number==0)!=(signs[row]==0)) ? 1 : 0;
  uint8_t reversed[20];
  int size=0;
  do { reversed[size++]=uint8_t('0'+number%10);number/=10; } while(number);
  for(int column=0;column<20;++column)
    keys[row*20+column]=column<size ? reversed[size-column-1] : 0;
  fault[row]=error;
}

extern "C" void noe_gpu_sim_trade_key(const uint32_t* words,const int8_t* signs,
    int64_t width,uint8_t* keys,int32_t* fault,int64_t rows,cudaStream_t stream) {
  sim_trade_key_kernel<<<(rows+255)/256,256,0,stream>>>(words,signs,width,keys,fault,rows);
}
