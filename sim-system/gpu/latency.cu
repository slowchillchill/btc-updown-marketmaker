#include <cuda_runtime.h>
#include <cstdint>

namespace {
// SHA-256按原SIM的五段u64大端长度前缀字节流执行，非新随机算法。
__constant__ uint32_t constants[64] = {
  0x428a2f98,0x71374491,0xb5c0fbcf,0xe9b5dba5,0x3956c25b,0x59f111f1,0x923f82a4,0xab1c5ed5,
  0xd807aa98,0x12835b01,0x243185be,0x550c7dc3,0x72be5d74,0x80deb1fe,0x9bdc06a7,0xc19bf174,
  0xe49b69c1,0xefbe4786,0x0fc19dc6,0x240ca1cc,0x2de92c6f,0x4a7484aa,0x5cb0a9dc,0x76f988da,
  0x983e5152,0xa831c66d,0xb00327c8,0xbf597fc7,0xc6e00bf3,0xd5a79147,0x06ca6351,0x14292967,
  0x27b70a85,0x2e1b2138,0x4d2c6dfc,0x53380d13,0x650a7354,0x766a0abb,0x81c2c92e,0x92722c85,
  0xa2bfe8a1,0xa81a664b,0xc24b8b70,0xc76c51a3,0xd192e819,0xd6990624,0xf40e3585,0x106aa070,
  0x19a4c116,0x1e376c08,0x2748774c,0x34b0bcb5,0x391c0cb3,0x4ed8aa4a,0x5b9cca4f,0x682e6ff3,
  0x748f82ee,0x78a5636f,0x84c87814,0x8cc70208,0x90befffa,0xa4506ceb,0xbef9a3f7,0xc67178f2
};
__device__ uint32_t rotate(uint32_t x, int bits) {
  return (x >> bits) | (x << (32 - bits));
}
struct Hash {
  uint32_t state[8] = {0x6a09e667,0xbb67ae85,0x3c6ef372,0xa54ff53a,
                       0x510e527f,0x9b05688c,0x1f83d9ab,0x5be0cd19};
  uint8_t block[64];
  int used = 0;
  uint64_t size = 0;

  __device__ void compress() {
    uint32_t words[64];
    for (int i = 0; i < 16; ++i) {
      words[i] = (uint32_t(block[4*i]) << 24) | (uint32_t(block[4*i+1]) << 16)
          | (uint32_t(block[4*i+2]) << 8) | block[4*i+3];
    }
    for (int i = 16; i < 64; ++i) {
      uint32_t a = words[i-15], b = words[i-2];
      words[i] = words[i-16] + (rotate(a,7)^rotate(a,18)^(a>>3)) + words[i-7]
          + (rotate(b,17)^rotate(b,19)^(b>>10));
    }
    uint32_t a=state[0],b=state[1],c=state[2],d=state[3];
    uint32_t e=state[4],f=state[5],g=state[6],h=state[7];
    for (int i = 0; i < 64; ++i) {
      uint32_t t1 = h + (rotate(e,6)^rotate(e,11)^rotate(e,25))
          + ((e&f)^(~e&g)) + constants[i] + words[i];
      uint32_t t2 = (rotate(a,2)^rotate(a,13)^rotate(a,22))
          + ((a&b)^(a&c)^(b&c));
      h=g; g=f; f=e; e=d+t1; d=c; c=b; b=a; a=t1+t2;
    }
    state[0]+=a;state[1]+=b;state[2]+=c;state[3]+=d;
    state[4]+=e;state[5]+=f;state[6]+=g;state[7]+=h;
  }
  __device__ void byte(uint8_t value) {
    block[used++] = value;
    ++size;
    if (used == 64) { compress(); used=0; }
  }
  __device__ void length(uint64_t value) {
    for (int i = 7; i >= 0; --i) byte(uint8_t(value >> (8*i)));
  }
  __device__ void segment(const uint8_t* bytes, int64_t size) {
    length(uint64_t(size));
    for (int64_t i = 0; i < size; ++i) byte(bytes[i]);
  }
  __device__ void finish(uint8_t* digest) {
    uint64_t bits = size * 8;
    byte(0x80);
    while (used != 56) byte(0);
    length(bits);
    for (int i = 0; i < 32; ++i) digest[i] = uint8_t(state[i/4] >> (24-8*(i%4)));
  }
};

__device__ bool range(const int64_t* offsets, int64_t row, int64_t count,
                      int64_t bytes, int64_t& begin, int64_t& end) {
  if (row < 0 || row >= count) return false;
  begin=offsets[row]; end=offsets[row+1];
  return begin >= 0 && end >= begin && end <= bytes;
}
__global__ void sample(const uint8_t* prefix, int64_t prefix_size,
    const uint8_t* seeds, int64_t seed_size, const int64_t* seed_offsets, int64_t seed_count,
    const int64_t* roots, const uint8_t* identities, int64_t identity_size,
    const int64_t* identity_offsets, int64_t identity_count, const int64_t* identity_indices,
    const uint32_t* sequences, const int8_t* signs,
    int64_t sequence_width, const int64_t* domains, bool decimal_identity,
    const uint64_t* cumulative, const uint64_t* delays, int64_t bins, uint64_t count,
    uint32_t* output, int32_t* fault, uint8_t* digests, int64_t rows) {
  int64_t row = int64_t(blockIdx.x)*blockDim.x + threadIdx.x;
  if (row >= rows) return;
  output[2*row]=0; output[2*row+1]=0; fault[row]=0;
  for (int i=0;i<32;++i) digests[32*row+i]=0;
  int64_t begin,end,id_begin=0,id_end=0;
  int64_t domain = domains[row];
  if (!range(seed_offsets,roots[row],seed_count,seed_size,begin,end) || domain<0 || domain>2) {
    fault[row]=1; return;
  }
  uint8_t decimal[20];
  int decimal_size=0;
  if (decimal_identity) {
    const uint32_t* words=sequences+row*sequence_width;
    if (signs[row]<0 || signs[row]>1) {fault[row]=1;return;}
    for (int64_t i=2;i<sequence_width;++i) if (words[i]) {fault[row]=1;return;}
    uint64_t value=uint64_t(words[0]) | (sequence_width>1 ? uint64_t(words[1])<<32 : 0);
    if (value && !signs[row]) {fault[row]=1;return;}
    do { decimal[decimal_size++]=uint8_t('0'+value%10);value/=10; } while(value);
  } else if (!range(identity_offsets,identity_indices[row],identity_count,identity_size,id_begin,id_end)) {
    fault[row]=1; return;
  }
  // 可分配张量的字节索引为int64；SHA长度是u64位数，先防止总长度溢出。
  uint64_t total=24+29;
  uint64_t lengths[3]={uint64_t(prefix_size),uint64_t(end-begin),
      uint64_t(decimal_identity ? decimal_size : id_end-id_begin)};
  for(int i=0;i<3;++i) {
    if(lengths[i]>UINT64_MAX/8-total) {fault[row]=1;return;}
    total+=lengths[i];
  }
  Hash hash;
  for (int64_t i=0;i<prefix_size;++i) hash.byte(prefix[i]);
  hash.segment(seeds+begin,end-begin);
  if (decimal_identity) {
    hash.length(decimal_size);
    for (int i=decimal_size-1;i>=0;--i) hash.byte(decimal[i]);
  } else hash.segment(identities+id_begin,id_end-id_begin);
  const char* identifier = domain==0 ? "REQUEST_OUTBOUND_V1" :
      (domain==1 ? "DIRECT_RESPONSE_INBOUND_V1" : "LIFECYCLE_FEEDBACK_INBOUND_V1");
  // strlen保持原标识长度，避免把终止符算作身份。
  int domain_size=0;
  while (identifier[domain_size]) ++domain_size;
  hash.segment(reinterpret_cast<const uint8_t*>(identifier),domain_size);
  hash.finish(digests+32*row);
  uint64_t word=0;
  for (int i=0;i<8;++i) word=(word<<8)|digests[32*row+i];
  uint64_t rank=__umul64hi(word,count);
  int64_t low=0,high=bins;
  while(low<high) {
    int64_t middle=low+(high-low)/2;
    if(cumulative[middle]<=rank) low=middle+1; else high=middle;
  }
  if(low==bins) {fault[row]=1;return;}
  uint64_t delay=delays[low];
  output[2*row]=uint32_t(delay);output[2*row+1]=uint32_t(delay>>32);
}
}

extern "C" void sim_gpu_latency(const uint8_t* prefix,int64_t prefix_size,
    const uint8_t* seeds,int64_t seed_size,const int64_t* seed_offsets,int64_t seed_count,
    const int64_t* roots,const uint8_t* identities,int64_t identity_size,
    const int64_t* identity_offsets,int64_t identity_count,const int64_t* identity_indices,
    const uint32_t* sequences,const int8_t* signs,
    int64_t sequence_width,const int64_t* domains,bool decimal_identity,
    const uint64_t* cumulative,const uint64_t* delays,int64_t bins,uint64_t count,
    uint32_t* output,int32_t* fault,uint8_t* digests,int64_t rows,cudaStream_t stream) {
  sample<<<(rows+127)/128,128,0,stream>>>(prefix,prefix_size,seeds,seed_size,seed_offsets,
      seed_count,roots,identities,identity_size,identity_offsets,identity_count,identity_indices,
      sequences,signs,sequence_width,
      domains,decimal_identity,cumulative,delays,bins,count,output,fault,digests,rows);
}
