#include "exact.cuh"
#include <cmath>
using namespace public_gpu;

__global__ void integer_kernel(int op,const uint32_t* aw,int wa,const int8_t* as,const uint32_t* bw,int wb,const int8_t* bs,uint32_t* ow,int wo,int8_t* os,uint32_t* rw,int wr,int8_t* rs,int32_t* fault,int64_t rows){
  int64_t row=int64_t(blockIdx.x)*blockDim.x+threadIdx.x;if(row>=rows)return;
  const auto* a=aw+row*wa;const auto* b=bw+row*wb;auto* o=ow+row*wo;auto* r=rw+row*wr;
  int na=length(a,wa),nb=length(b,wb),sa=na?as[row]:0,sb=nb?bs[row]:0;
  zero(o,wo);zero(r,wr);fault[row]=0;rs[row]=0;
  if(op==0 || op==1){if(op==1)sb=-sb;
    if(sa==sb){add(o,wo,a,na,b,nb);os[row]=sa;}
    else{int cmp=compare(a,na,b,nb);if(cmp>=0){subtract(o,wo,a,na,b,nb);os[row]=sa;}else{subtract(o,wo,b,nb,a,na);os[row]=sb;}}
  }else if(op==2){multiply(o,wo,a,na,b,nb);os[row]=sa*sb;}
  else if(op==3){fault[row]=!divide(o,wo,r,wr,a,na,b,nb);os[row]=sa*sb;rs[row]=length(r,wr)?sa:0;}
  else{int cmp=sa==sb?sa*compare(a,na,b,nb):(sa<sb?-1:1);os[row]=cmp;}
  if(op!=4&&!length(o,wo))os[row]=0;
}
// 高位尾数+指数，避免将任意长整数直接变成溢出的double。
__device__ inline double log_ratio(const uint32_t* n,int nn,const uint32_t* d,int nd){
  int bn=bit_length(n,nn),bd=bit_length(d,nd);uint64_t sn=0,sd=0;
  int kn=bn>53?bn-53:0,kd=bd>53?bd-53:0;
  for(int i=bn-1;i>=kn;--i)sn=(sn<<1)|((n[i/32]>>(i%32))&1);
  for(int i=bd-1;i>=kd;--i)sd=(sd<<1)|((d[i/32]>>(i%32))&1);
  return log(double(sn)/double(sd))+double(kn-kd)*0.69314718055994530942;
}
__device__ inline float ratio_f32(const uint32_t* n,int nn,const uint32_t* d,int nd,int8_t sign,uint32_t* scratch,int cap){
  uint32_t* a=scratch;uint32_t* b=a+cap;uint32_t* q=b+cap;uint32_t* r=q+cap;
  int e=bit_length(n,nn)-bit_length(d,nd);
  if(e>=0){shift_copy(b,cap,d,nd,e);if(compare(n,nn,b,cap)<0)--e;}
  else{shift_copy(a,cap,n,nn,-e);if(compare(a,cap,d,nd)<0)--e;}
  uint32_t signbit=sign<0?0x80000000u:0;
  if(e>127)return __uint_as_float(signbit|0x7f800000u);
  if(e<-150)return __uint_as_float(signbit);
  int shift=e>=-126?23-e:149;
  if(shift>=0){shift_copy(a,cap,n,nn,shift);copy(b,cap,d,nd);}else{copy(a,cap,n,nn);shift_copy(b,cap,d,nd,-shift);}
  divide(q,cap,r,cap,a,cap,b,cap);
  // r与d-r比较，不用额外倍乘造成容量溢出；借用a为距离。
  subtract(a,cap,b,cap,r,cap);int order=compare(r,cap,a,cap);uint32_t m=q[0];
  if(order>0||(order==0&&(m&1)))++m;
  if(e>=-126){if(m==0x1000000u){m>>=1;++e;}if(e>127)return __uint_as_float(signbit|0x7f800000u);
    return __uint_as_float(signbit|(uint32_t(e+127)<<23)|(m&0x7fffffu));}
  return __uint_as_float(signbit|m);
}
__global__ void project_kernel(int op,const uint32_t* nw,int wn,const int8_t* ns,const uint32_t* dw,int wd,float* out,int32_t* fault,uint32_t* work,int cap,int64_t rows){
  int64_t row=int64_t(blockIdx.x)*blockDim.x+threadIdx.x;if(row>=rows)return;
  const auto* n=nw+row*wn;const auto* d=dw+row*wd;int nn=length(n,wn),nd=length(d,wd);fault[row]=!nd;
  if(!nd){out[row]=0;return;}if(!nn){out[row]=0;return;}
  if(op==0){out[row]=ratio_f32(n,nn,d,nd,ns[row],work+row*4*cap,cap);return;}
  double exponent=log_ratio(n,nn,d,nd);if(op==2)exponent*=0.5;
  double projected=exponent>0?exponent+log1p(exp(-exponent)):log1p(exp(exponent));
  out[row]=float(ns[row]<0?-projected:projected);
}
extern "C" void public_gpu_integer(int op,const uint32_t* a,int wa,const int8_t* sa,const uint32_t* b,int wb,const int8_t* sb,uint32_t* o,int wo,int8_t* so,uint32_t* r,int wr,int8_t* sr,int32_t* f,int64_t rows,cudaStream_t stream){integer_kernel<<<(rows+255)/256,256,0,stream>>>(op,a,wa,sa,b,wb,sb,o,wo,so,r,wr,sr,f,rows);}
extern "C" void public_gpu_project(int op,const uint32_t* n,int wn,const int8_t* sn,const uint32_t* d,int wd,float* o,int32_t* f,uint32_t* work,int cap,int64_t rows,cudaStream_t stream){project_kernel<<<(rows+255)/256,256,0,stream>>>(op,n,wn,sn,d,wd,o,f,work,cap,rows);}
