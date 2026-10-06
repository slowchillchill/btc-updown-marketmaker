#include <torch/extension.h>
#include <c10/cuda/CUDAGuard.h>
#include <c10/cuda/CUDAStream.h>
#include <c10/cuda/CUDAException.h>
#include "../../contracts/gpu/exact.cuh"

void words_check(const torch::Tensor& x){TORCH_CHECK(x.is_cuda()&&x.scalar_type()==torch::kInt32&&x.is_contiguous()&&x.dim()==2,"精确整数需CUDA连续int32[batch,words]");TORCH_CHECK(x.size(1)>0,"需至少一个物理字");}
void sign_check(const torch::Tensor& x,const torch::Tensor& words){TORCH_CHECK(x.is_cuda()&&x.scalar_type()==torch::kInt8&&x.is_contiguous()&&x.dim()==1&&x.size(0)==words.size(0)&&x.device()==words.device(),"符号需同设备int8[batch]");}
std::vector<torch::Tensor> integer_op(int op,torch::Tensor a,torch::Tensor sa,torch::Tensor b,torch::Tensor sb){
  words_check(a);words_check(b);sign_check(sa,a);sign_check(sb,b);TORCH_CHECK(a.device()==b.device()&&a.size(0)==b.size(0),"整数批次和设备不一致");TORCH_CHECK(op>=0&&op<=4,"未知精确整数操作");
  const c10::cuda::CUDAGuard guard(a.device());int wa=a.size(1),wb=b.size(1),wo=op==2?wa+wb:(op==3?wa:std::max(wa,wb)+1),wr=wb+1;auto rows=a.size(0);
  auto out=torch::empty({rows,wo},a.options()),sign=torch::empty({rows},sa.options()),rem=torch::empty({rows,wr},a.options()),rs=torch::empty({rows},sa.options()),fault=torch::empty({rows},a.options());
  if(rows)public_gpu_integer(op,(uint32_t*)a.data_ptr<int32_t>(),wa,sa.data_ptr<int8_t>(),(uint32_t*)b.data_ptr<int32_t>(),wb,sb.data_ptr<int8_t>(),(uint32_t*)out.data_ptr<int32_t>(),wo,sign.data_ptr<int8_t>(),(uint32_t*)rem.data_ptr<int32_t>(),wr,rs.data_ptr<int8_t>(),fault.data_ptr<int32_t>(),rows,c10::cuda::getCurrentCUDAStream().stream());
  C10_CUDA_KERNEL_LAUNCH_CHECK();return {out,sign,rem,rs,fault};
}
std::vector<torch::Tensor> project(int op,torch::Tensor n,torch::Tensor sign,torch::Tensor d){
  words_check(n);words_check(d);sign_check(sign,n);TORCH_CHECK(n.device()==d.device()&&n.size(0)==d.size(0),"比率批次和设备不一致");TORCH_CHECK(op>=0&&op<=2,"未知投影");const c10::cuda::CUDAGuard guard(n.device());
  auto rows=n.size(0);int cap=std::max(n.size(1),d.size(1))+6;auto out=torch::empty({rows},n.options().dtype(torch::kFloat32)),fault=torch::empty({rows},n.options());auto work=torch::empty({rows,op==0?4*cap:1},n.options());
  if(rows)public_gpu_project(op,(uint32_t*)n.data_ptr<int32_t>(),n.size(1),sign.data_ptr<int8_t>(),(uint32_t*)d.data_ptr<int32_t>(),d.size(1),out.data_ptr<float>(),fault.data_ptr<int32_t>(),(uint32_t*)work.data_ptr<int32_t>(),cap,rows,c10::cuda::getCurrentCUDAStream().stream());
  C10_CUDA_KERNEL_LAUNCH_CHECK();return {out,fault};
}
PYBIND11_MODULE(TORCH_EXTENSION_NAME,m){m.def("integer_op",&integer_op);m.def("project",&project);}
