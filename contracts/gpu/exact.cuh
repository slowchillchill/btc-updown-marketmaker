#pragma once
#include <stdint.h>
#include <cuda_runtime.h>

// 公共精确整数基础。宽度是当前分配容量，不是业务数值上限。
#ifdef __CUDACC__
namespace public_gpu {
__device__ inline int length(const uint32_t* x, int width) {
  while (width && !x[width-1]) --width;
  return width;
}
__device__ inline int compare(const uint32_t* a, int na, const uint32_t* b, int nb) {
  na=length(a,na); nb=length(b,nb);
  if (na!=nb) return na<nb?-1:1;
  for (int j=na-1;j>=0;--j) if (a[j]!=b[j]) return a[j]<b[j]?-1:1;
  return 0;
}
__device__ inline void zero(uint32_t* x,int n) { for(int i=0;i<n;++i)x[i]=0; }
__device__ inline void copy(uint32_t* o,int no,const uint32_t* x,int nx) {
  for(int i=0;i<no;++i)o[i]=i<nx?x[i]:0;
}
__device__ inline void add(uint32_t* o,int no,const uint32_t* a,int na,const uint32_t* b,int nb) {
  uint64_t c=0;
  for(int j=0;j<no;++j){uint64_t v=c+(j<na?a[j]:0ULL)+(j<nb?b[j]:0ULL);o[j]=uint32_t(v);c=v>>32;}
}
// 调用者先确保a>=b。支持o==a，便于长除法复用余数空间。
__device__ inline void subtract(uint32_t* o,int no,const uint32_t* a,int na,const uint32_t* b,int nb) {
  uint64_t borrow=0;
  for(int j=0;j<no;++j){uint64_t av=j<na?a[j]:0,bv=(j<nb?b[j]:0ULL)+borrow;o[j]=uint32_t(av-bv);borrow=av<bv;}
}
__device__ inline void multiply(uint32_t* o,int no,const uint32_t* a,int na,const uint32_t* b,int nb) {
  zero(o,no);
  for(int i=0;i<na;++i){uint64_t c=0;
    for(int j=0;j<nb;++j){uint64_t v=uint64_t(a[i])*b[j]+o[i+j]+c;o[i+j]=uint32_t(v);c=v>>32;}
    o[i+nb]=uint32_t(c);
  }
}
__device__ inline int bit_length(const uint32_t* a,int na){na=length(a,na);return na?(na-1)*32+32-__clz(a[na-1]):0;}
__device__ inline void shift_copy(uint32_t* out,int width,const uint32_t* a,int na,int bits){
  zero(out,width);int whole=bits/32,part=bits%32;
  for(int j=0;j<na;++j){out[j+whole]|=a[j]<<part;if(part)out[j+whole+1]|=a[j]>>(32-part);}
}
// 输出商容量>=分子容量，余数容量>=分母容量+1。除零只返回fault，不提交业务状态。
__device__ inline bool divide(uint32_t* q,int nq,uint32_t* r,int nr,const uint32_t* a,int na,const uint32_t* b,int nb){
  zero(q,nq);zero(r,nr);nb=length(b,nb);if(!nb)return false;
  for(int bit=bit_length(a,na)-1;bit>=0;--bit){uint32_t carry=(a[bit/32]>>(bit%32))&1;
    for(int j=0;j<nr;++j){uint32_t next=r[j]>>31;r[j]=(r[j]<<1)|carry;carry=next;}
    if(compare(r,nr,b,nb)>=0){subtract(r,nr,r,nr,b,nb);q[bit/32]|=uint32_t(1)<<(bit%32);}
  }
  return true;
}
}
#endif
extern "C" void public_gpu_integer(int op,const uint32_t*,int,const int8_t*,const uint32_t*,int,const int8_t*,uint32_t*,int,int8_t*,uint32_t*,int,int8_t*,int32_t*,int64_t,cudaStream_t);
extern "C" void public_gpu_project(int op,const uint32_t*,int,const int8_t*,const uint32_t*,int,float*,int32_t*,uint32_t*,int,int64_t,cudaStream_t);
