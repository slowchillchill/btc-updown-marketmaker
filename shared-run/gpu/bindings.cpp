#include <torch/extension.h>
#include <c10/cuda/CUDAGuard.h>
#include <c10/cuda/CUDAStream.h>
#include <c10/cuda/CUDAException.h>
#include <limits>
#include "../../contracts/gpu/exact.cuh"
extern "C" cudaError_t public_graph_condition_node(cudaGraphNode_t*, cudaGraph_t,
    cudaGraphNode_t, cudaGraphConditionalHandle, const bool*);
extern "C" void public_gpu_selected_rows(void*,const void*,const int64_t*,const bool*,
    int64_t,int64_t,cudaStream_t);
extern "C" void public_gpu_bound_rows(void*,const int64_t*,int64_t,const int64_t*,const bool*,
    int64_t,int64_t,bool,cudaStream_t);

void copy_bound_rows(torch::Tensor value,torch::Tensor addresses,int64_t field,
    torch::Tensor rows,torch::Tensor enabled,bool scatter) {
  // Stage构造同布局连续行和固定地址描述；不增加业务接纳门。
  const c10::cuda::CUDAGuard guard(value.device());
  if(rows.numel())public_gpu_bound_rows(value.data_ptr(),addresses.data_ptr<int64_t>(),field,
      rows.data_ptr<int64_t>(),enabled.data_ptr<bool>(),value.numel()*value.element_size()/rows.numel(),
      rows.numel(),scatter,c10::cuda::getCurrentCUDAStream().stream());
  C10_CUDA_KERNEL_LAUNCH_CHECK();
}

void copy_selected_rows(torch::Tensor target,torch::Tensor incoming,torch::Tensor rows,torch::Tensor enabled) {
  // 仅由SIM阶段缓冲的reshape和原int64行/启用掩码调用；布局由该构造入口保证。
  const c10::cuda::CUDAGuard guard(target.device());
  if(rows.numel())public_gpu_selected_rows(target.data_ptr(),incoming.data_ptr(),rows.data_ptr<int64_t>(),
      enabled.data_ptr<bool>(),target.size(1)*target.element_size(),rows.numel(),
      c10::cuda::getCurrentCUDAStream().stream());
  C10_CUDA_KERNEL_LAUNCH_CHECK();
}

class DeviceConditionalLoop {
  torch::Tensor ready_;
  cudaGraph_t graph_ = nullptr;
  cudaGraphExec_t executable_ = nullptr;
 public:
  DeviceConditionalLoop(uint64_t entry, uint64_t body, torch::Tensor ready) : ready_(ready) {
    TORCH_CHECK(ready.is_cuda() && ready.scalar_type() == torch::kBool && ready.is_contiguous()
        && ready.dim() == 0, "条件循环须持有设备bool标量");
    const c10::cuda::CUDAGuard guard(ready.device());
    try {
      C10_CUDA_CHECK(cudaGraphCreate(&graph_, 0));
      cudaGraphConditionalHandle handle;
      C10_CUDA_CHECK(cudaGraphConditionalHandleCreate(&handle, graph_, 0, 0));
      cudaGraphNode_t entry_node, setter, loop;
      C10_CUDA_CHECK(cudaGraphAddChildGraphNode(&entry_node, graph_, nullptr, 0,
          reinterpret_cast<cudaGraph_t>(entry)));
      C10_CUDA_CHECK(public_graph_condition_node(&setter, graph_, entry_node, handle, ready.data_ptr<bool>()));
      cudaGraphNodeParams params{};
      params.type = cudaGraphNodeTypeConditional;
      params.conditional.handle = handle;
      params.conditional.type = cudaGraphCondTypeWhile;
      params.conditional.size = 1;
      C10_CUDA_CHECK(cudaGraphAddNode(&loop, graph_, &setter, 1, &params));
      auto loop_body = params.conditional.phGraph_out[0];
      cudaGraphNode_t child, tail;
      C10_CUDA_CHECK(cudaGraphAddChildGraphNode(&child, loop_body, nullptr, 0,
          reinterpret_cast<cudaGraph_t>(body)));
      C10_CUDA_CHECK(public_graph_condition_node(&tail, loop_body, child, handle, ready.data_ptr<bool>()));
      C10_CUDA_CHECK(cudaGraphInstantiateWithFlags(&executable_, graph_, 0));
    } catch (...) {
      if (executable_) cudaGraphExecDestroy(executable_);
      if (graph_) cudaGraphDestroy(graph_);
      throw;
    }
  }
  DeviceConditionalLoop(const DeviceConditionalLoop&) = delete;
  ~DeviceConditionalLoop() {
    if (executable_) cudaGraphExecDestroy(executable_);
    if (graph_) cudaGraphDestroy(graph_);
  }
  void replay() {
    const c10::cuda::CUDAGuard guard(ready_.device());
    C10_CUDA_CHECK(cudaGraphLaunch(executable_, c10::cuda::getCurrentCUDAStream().stream()));
  }
};
// 按设备许可执行静态阶段图，再由设备决定是否继续。只持有图，不调度业务或训练梯度。
class DeviceConditionalSequenceLoop {
  torch::Tensor ready_;
  std::vector<torch::Tensor> conditions_;
  cudaGraph_t graph_ = nullptr;
  cudaGraphExec_t executable_ = nullptr;
 public:
  DeviceConditionalSequenceLoop(uint64_t entry, std::vector<uint64_t> bodies,
      std::vector<torch::Tensor> conditions, torch::Tensor ready)
      : ready_(ready), conditions_(std::move(conditions)) {
    TORCH_CHECK(ready.is_cuda() && ready.scalar_type()==torch::kBool && ready.dim()==0
        && ready.is_contiguous(), "循环条件须为连续CUDA bool标量");
    TORCH_CHECK(bodies.size()==conditions_.size() && !bodies.empty(), "阶段图与许可须一一对应");
    for (const auto& flag : conditions_)
      TORCH_CHECK(flag.device()==ready.device() && flag.scalar_type()==torch::kBool
          && flag.dim()==0 && flag.is_contiguous(), "阶段许可须同设备连续bool标量");
    const c10::cuda::CUDAGuard guard(ready.device());
    try {
      C10_CUDA_CHECK(cudaGraphCreate(&graph_,0));
      cudaGraphConditionalHandle handle;
      C10_CUDA_CHECK(cudaGraphConditionalHandleCreate(&handle,graph_,0,0));
      cudaGraphNode_t start,setter,loop;
      C10_CUDA_CHECK(cudaGraphAddChildGraphNode(&start,graph_,nullptr,0,reinterpret_cast<cudaGraph_t>(entry)));
      C10_CUDA_CHECK(public_graph_condition_node(&setter,graph_,start,handle,ready.data_ptr<bool>()));
      cudaGraphNodeParams params{};
      params.type=cudaGraphNodeTypeConditional;params.conditional.handle=handle;
      params.conditional.type=cudaGraphCondTypeWhile;params.conditional.size=1;
      C10_CUDA_CHECK(cudaGraphAddNode(&loop,graph_,&setter,1,&params));
      auto body=params.conditional.phGraph_out[0];
      cudaGraphNode_t previous=nullptr;
      for (size_t i=0;i<bodies.size();++i) {
        cudaGraphConditionalHandle selected;
        C10_CUDA_CHECK(cudaGraphConditionalHandleCreate(&selected,graph_,0,0));
        cudaGraphNode_t condition,branch,child;
        C10_CUDA_CHECK(public_graph_condition_node(&condition,body,previous,selected,
            conditions_[i].data_ptr<bool>()));
        cudaGraphNodeParams stage{};
        stage.type=cudaGraphNodeTypeConditional;stage.conditional.handle=selected;
        stage.conditional.type=cudaGraphCondTypeIf;stage.conditional.size=1;
        C10_CUDA_CHECK(cudaGraphAddNode(&branch,body,&condition,1,&stage));
        C10_CUDA_CHECK(cudaGraphAddChildGraphNode(&child,stage.conditional.phGraph_out[0],nullptr,0,
            reinterpret_cast<cudaGraph_t>(bodies[i])));
        previous=branch;
      }
      cudaGraphNode_t tail;
      C10_CUDA_CHECK(public_graph_condition_node(&tail,body,previous,handle,ready.data_ptr<bool>()));
      C10_CUDA_CHECK(cudaGraphInstantiateWithFlags(&executable_,graph_,0));
    } catch (...) {
      if (executable_) cudaGraphExecDestroy(executable_);
      if (graph_) cudaGraphDestroy(graph_);
      throw;
    }
  }
  DeviceConditionalSequenceLoop(const DeviceConditionalSequenceLoop&)=delete;
  ~DeviceConditionalSequenceLoop() {
    if (executable_) cudaGraphExecDestroy(executable_);
    if (graph_) cudaGraphDestroy(graph_);
  }
  void replay() {
    const c10::cuda::CUDAGuard guard(ready_.device());
    C10_CUDA_CHECK(cudaGraphLaunch(executable_,c10::cuda::getCurrentCUDAStream().stream()));
  }
};
extern "C" void public_gpu_policy_uniform(const int64_t*,const int64_t*,const bool*,double*,int64_t,cudaStream_t);
torch::Tensor policy_uniform(torch::Tensor seed,torch::Tensor counter,torch::Tensor enabled) {
  TORCH_CHECK(seed.is_cuda() && seed.scalar_type()==torch::kInt64 && seed.dim()==1 && seed.is_contiguous()
      && counter.device()==seed.device() && counter.scalar_type()==torch::kInt64
      && counter.is_contiguous() && counter.dim()==2 && counter.size(0)==seed.size(0)
      && counter.size(1)==4,"随机源需要同设备seed[R]/128位游标[R,4]低字在前");
  TORCH_CHECK(enabled.device()==seed.device() && enabled.scalar_type()==torch::kBool
      && enabled.sizes()==seed.sizes() && enabled.is_contiguous(),"随机许可须为同根连续bool");
  TORCH_CHECK(seed.numel()<=int64_t(INT32_MAX)*256,"随机物理人口超过CUDA grid.x容量");
  const c10::cuda::CUDAGuard guard(seed.device());
  auto output=torch::empty_like(seed,seed.options().dtype(torch::kFloat64));
  if (seed.numel()) public_gpu_policy_uniform(seed.data_ptr<int64_t>(),counter.data_ptr<int64_t>(),
      enabled.data_ptr<bool>(),output.data_ptr<double>(),seed.numel(),c10::cuda::getCurrentCUDAStream().stream());
  C10_CUDA_KERNEL_LAUNCH_CHECK();
  return output;
}
extern "C" void public_gpu_decimal(const uint8_t*, int64_t, const int64_t*,
    const bool*, uint32_t*, uint32_t*, int8_t*, int32_t*, int32_t*, int64_t, cudaStream_t);
extern "C" void sim_gpu_snapshot_gather(const int64_t*, const int64_t*, const bool*,
    const int64_t*, const int64_t*, const int64_t*, const int64_t*, const int64_t*,
    const int64_t*, const uint32_t*, int64_t, const int8_t*, const uint32_t*, int64_t,
    const int32_t*, const uint32_t*, int64_t, const int8_t*, const uint32_t*, int64_t,
    const int32_t*, const bool*, int64_t*, int64_t*, int32_t*, uint32_t*,
    int64_t, int64_t, int64_t, cudaStream_t);

void words_check(const torch::Tensor& x) {
  TORCH_CHECK(x.is_cuda() && x.scalar_type() == torch::kInt32 &&
      x.is_contiguous() && x.dim() == 2, "需CUDA连续int32[batch,words]");
  TORCH_CHECK(x.size(1) > 0 && x.size(1) <= INT64_MAX / 32 - 6,
      "字容量无法用int64表示位长及临时空间");
  TORCH_CHECK(x.size(0) <= int64_t(INT32_MAX) * 256, "批次超过设备grid.x容量");
}
void sign_check(const torch::Tensor& x, const torch::Tensor& words) {
  TORCH_CHECK(x.is_cuda() && x.scalar_type() == torch::kInt8 && x.is_contiguous()
      && x.dim() == 1 && x.size(0) == words.size(0)
      && x.device() == words.device(), "需同设备int8[batch]符号");
}
void capacity_check(int64_t rows, int64_t width) {
  TORCH_CHECK(width > 0 && (!rows || width <= INT64_MAX / rows / 4),
      "实际字节容量超出int64可分配范围");
}
std::vector<torch::Tensor> hidden_snapshot_gather(std::vector<torch::Tensor> x) {
  TORCH_CHECK(x.size() == 18, "快照gather需完整源/运行列");
  for (const auto& column : x)
    TORCH_CHECK(column.is_cuda() && column.is_contiguous() && column.device() == x[0].device(),
        "快照gather列须同设备且连续");
  for (int i : {0,1,3,4,5,6,7,8}) TORCH_CHECK(x[i].scalar_type() == torch::kInt64, "快照坐标须int64");
  TORCH_CHECK(x[2].scalar_type() == torch::kBool && x[17].scalar_type() == torch::kBool,
      "快照人口/关闭掩码须bool");
  words_check(x[9]); words_check(x[11]); words_check(x[13]); words_check(x[15]);
  sign_check(x[10],x[9]); sign_check(x[14],x[13]);
  TORCH_CHECK(x[12].scalar_type() == torch::kInt32 && x[16].scalar_type() == torch::kInt32,
      "快照精确故障须int32");
  int64_t roots=x[0].numel(), levels=x[5].numel();
  TORCH_CHECK(x[0].dim()==1 && x[1].sizes()==x[0].sizes() && x[2].sizes()==x[0].sizes()
      && x[3].sizes()==x[4].sizes() && x[17].sizes()==x[3].sizes()
      && x[6].sizes()==x[5].sizes() && x[7].dim()==1 && x[7].numel()>0
      && x[8].dim()==2 && x[8].size(1)==3 && x[9].size(0)==levels
      && x[11].size(0)==levels && x[12].numel()==levels
      && x[13].size(0)==x[15].size(0) && x[16].numel()==x[13].size(0), "快照源/运行布局不一致");
  const c10::cuda::CUDAGuard guard(x[0].device());
  int64_t cap=std::max(x[9].size(1)+x[15].size(1),x[13].size(1)+x[11].size(1));
  capacity_check(roots,2*cap);
  auto ids=torch::zeros({levels},x[0].options());
  auto counts=torch::zeros_like(ids);
  auto fault=torch::zeros({roots},x[12].options());
  auto work=torch::empty({roots,2*cap},x[9].options());
  sim_gpu_snapshot_gather(x[0].data_ptr<int64_t>(),x[1].data_ptr<int64_t>(),x[2].data_ptr<bool>(),
      x[3].data_ptr<int64_t>(),x[4].data_ptr<int64_t>(),x[5].data_ptr<int64_t>(),x[6].data_ptr<int64_t>(),
      x[7].data_ptr<int64_t>(),x[8].data_ptr<int64_t>(),(uint32_t*)x[9].data_ptr<int32_t>(),x[9].size(1),
      x[10].data_ptr<int8_t>(),(uint32_t*)x[11].data_ptr<int32_t>(),x[11].size(1),x[12].data_ptr<int32_t>(),
      (uint32_t*)x[13].data_ptr<int32_t>(),x[13].size(1),x[14].data_ptr<int8_t>(),
      (uint32_t*)x[15].data_ptr<int32_t>(),x[15].size(1),x[16].data_ptr<int32_t>(),x[17].data_ptr<bool>(),
      ids.data_ptr<int64_t>(),counts.data_ptr<int64_t>(),fault.data_ptr<int32_t>(),
      (uint32_t*)work.data_ptr<int32_t>(),cap,roots,levels,c10::cuda::getCurrentCUDAStream().stream());
  C10_CUDA_KERNEL_LAUNCH_CHECK();
  return {ids,counts,fault};
}
std::vector<torch::Tensor> integer_op(int op, torch::Tensor a, torch::Tensor sa,
                                    torch::Tensor b, torch::Tensor sb) {
  words_check(a); words_check(b); sign_check(sa, a); sign_check(sb, b);
  TORCH_CHECK(a.device() == b.device() && a.size(0) == b.size(0), "批次/设备不一致");
  TORCH_CHECK(op >= 0 && op <= 4, "未知整数操作");
  const c10::cuda::CUDAGuard guard(a.device());
  int64_t wa = a.size(1), wb = b.size(1), rows = a.size(0);
  int64_t wo = std::max(wa, wb), wr = wo;
  int64_t cap = wa + wb + 1;
  capacity_check(rows, wo); capacity_check(rows, wr);
  capacity_check(rows, 2 * cap);
  auto out = torch::empty({rows, wo}, a.options());
  auto sign = torch::empty({rows}, sa.options());
  auto rem = torch::empty({rows, wr}, a.options());
  auto rs = torch::empty({rows}, sa.options());
  auto fault = torch::empty({rows}, a.options());
  auto required = torch::empty({rows}, a.options().dtype(torch::kInt64));
  auto work = torch::empty({rows, 2 * cap}, a.options());
  if (rows) public_gpu_integer(op, (uint32_t*)a.data_ptr<int32_t>(), wa,
      sa.data_ptr<int8_t>(), (uint32_t*)b.data_ptr<int32_t>(), wb,
      sb.data_ptr<int8_t>(), (uint32_t*)out.data_ptr<int32_t>(), wo,
      sign.data_ptr<int8_t>(), (uint32_t*)rem.data_ptr<int32_t>(), wr,
      rs.data_ptr<int8_t>(), fault.data_ptr<int32_t>(),
      (uint32_t*)work.data_ptr<int32_t>(), cap, required.data_ptr<int64_t>(), rows,
      c10::cuda::getCurrentCUDAStream().stream());
  C10_CUDA_KERNEL_LAUNCH_CHECK();
  return {out, sign, rem, rs, fault, required};
}
std::vector<torch::Tensor> project(int op, torch::Tensor n,
                                 torch::Tensor sign, torch::Tensor d) {
  words_check(n); words_check(d); sign_check(sign, n);
  TORCH_CHECK(n.device() == d.device() && n.size(0) == d.size(0), "批次/设备不一致");
  TORCH_CHECK(op >= 0 && op <= 2, "未知投影");
  const c10::cuda::CUDAGuard guard(n.device());
  int64_t rows = n.size(0), cap = std::max(n.size(1), d.size(1)) + 6;
  capacity_check(rows, 4 * cap);
  auto out = torch::empty({rows}, n.options().dtype(torch::kFloat32));
  auto fault = torch::empty({rows}, n.options());
  auto work = torch::empty({rows, op == 0 ? 4 * cap : 1}, n.options());
  if (rows) public_gpu_project(op, (uint32_t*)n.data_ptr<int32_t>(), n.size(1),
      sign.data_ptr<int8_t>(), (uint32_t*)d.data_ptr<int32_t>(), d.size(1),
      out.data_ptr<float>(), fault.data_ptr<int32_t>(),
      (uint32_t*)work.data_ptr<int32_t>(), cap, rows,
      c10::cuda::getCurrentCUDAStream().stream());
  C10_CUDA_KERNEL_LAUNCH_CHECK();
  return {out, fault};
}
torch::Tensor index_probe(torch::Tensor widths, torch::Tensor tops) {
  TORCH_CHECK(widths.is_cuda() && widths.scalar_type() == torch::kInt64
      && widths.is_contiguous() && widths.dim() == 1, "需int64宽度元数据");
  TORCH_CHECK(tops.is_cuda() && tops.scalar_type() == torch::kInt32
      && tops.is_contiguous() && tops.dim() == 1 && tops.sizes() == widths.sizes()
      && tops.device() == widths.device(), "需同设备int32最高字元数据");
  const c10::cuda::CUDAGuard guard(widths.device());
  int64_t rows = widths.size(0);
  TORCH_CHECK(rows <= int64_t(INT32_MAX) * 256, "批次超过grid容量");
  auto out = torch::empty({rows, 3}, widths.options());
  if (rows) public_gpu_index_probe(widths.data_ptr<int64_t>(),
      (uint32_t*)tops.data_ptr<int32_t>(), out.data_ptr<int64_t>(), rows,
      c10::cuda::getCurrentCUDAStream().stream());
  C10_CUDA_KERNEL_LAUNCH_CHECK();
  return out;
}
torch::Tensor gcd(torch::Tensor a, torch::Tensor b) {
  words_check(a); words_check(b);
  TORCH_CHECK(a.device() == b.device() && a.size(0) == b.size(0), "GCD批次/设备不一致");
  const c10::cuda::CUDAGuard guard(a.device());
  int64_t rows = a.size(0), cap = std::max(a.size(1), b.size(1));
  capacity_check(rows, 4 * (cap + 1));
  auto out = torch::empty({rows, cap}, a.options());
  auto work = torch::empty({rows, 4 * (cap + 1)}, a.options());
  if (rows) public_gpu_gcd((uint32_t*)a.data_ptr<int32_t>(), a.size(1),
      (uint32_t*)b.data_ptr<int32_t>(), b.size(1), (uint32_t*)out.data_ptr<int32_t>(),
      (uint32_t*)work.data_ptr<int32_t>(), cap, rows,
      c10::cuda::getCurrentCUDAStream().stream());
  C10_CUDA_KERNEL_LAUNCH_CHECK();
  return out;
}
std::vector<torch::Tensor> floor_sum(torch::Tensor n, torch::Tensor m,
                                     torch::Tensor a, torch::Tensor b) {
  for (const auto& value : {n,m,a,b}) {
    words_check(value);
    TORCH_CHECK(value.device() == n.device() && value.size(0) == n.size(0), "floor_sum批次/设备不一致");
  }
  const c10::cuda::CUDAGuard guard(n.device());
  int64_t width = std::max({n.size(1),m.size(1),a.size(1),b.size(1)});
  TORCH_CHECK(width <= (INT64_MAX/10-4)/3, "floor_sum临时字索引溢出");
  int64_t rows = n.size(0), cap = 3*width+4;
  capacity_check(rows,10*cap);
  auto out = torch::empty({rows,width},n.options());
  auto fault = torch::empty({rows},n.options());
  auto required = torch::empty({rows},n.options().dtype(torch::kInt64));
  auto work = torch::empty({rows,10*cap},n.options());
  if (rows) public_gpu_floor_sum((uint32_t*)n.data_ptr<int32_t>(),n.size(1),
      (uint32_t*)m.data_ptr<int32_t>(),m.size(1),(uint32_t*)a.data_ptr<int32_t>(),a.size(1),
      (uint32_t*)b.data_ptr<int32_t>(),b.size(1),(uint32_t*)out.data_ptr<int32_t>(),width,
      fault.data_ptr<int32_t>(),(uint32_t*)work.data_ptr<int32_t>(),cap,
      required.data_ptr<int64_t>(),rows,c10::cuda::getCurrentCUDAStream().stream());
  C10_CUDA_KERNEL_LAUNCH_CHECK();
  return {out,fault,required};
}
extern "C" void sim_gpu_limit_segment(const uint32_t*, const int8_t*, int64_t,
    uint32_t*, uint32_t*, bool*, int32_t*, int64_t*, uint32_t*, int64_t, int64_t, cudaStream_t);
std::vector<torch::Tensor> limit_segment(torch::Tensor inputs, torch::Tensor signs) {
  TORCH_CHECK(inputs.is_cuda() && inputs.scalar_type() == torch::kInt32 &&
      inputs.is_contiguous() && inputs.dim() == 3 && inputs.size(1) == 8,
      "需CUDA连续int32[batch,8,words]原段和订单精确量");
  int64_t rows = inputs.size(0), width = inputs.size(2);
  TORCH_CHECK(rows <= int64_t(INT32_MAX) * 128 && width > 0 &&
      width <= (INT64_MAX / 138 - 8) / 6 && width <= INT64_MAX / 32 - 6,
      "SIM搜索临时字索引或grid容量溢出");
  TORCH_CHECK(signs.is_cuda() && signs.device() == inputs.device() &&
      signs.scalar_type() == torch::kInt8 && signs.is_contiguous() &&
      signs.dim() == 2 && signs.size(0) == rows && signs.size(1) == 8,
      "需同设备int8[batch,8]原符号");
  const c10::cuda::CUDAGuard guard(inputs.device());
  int64_t cap = 6 * width + 8;
  capacity_check(rows, 138 * cap);
  auto base = torch::empty({rows, width}, inputs.options());
  auto quote = torch::empty_like(base);
  auto present = torch::empty({rows}, inputs.options().dtype(torch::kBool));
  auto fault = torch::empty({rows}, inputs.options());
  auto required = torch::empty({rows}, inputs.options().dtype(torch::kInt64));
  auto work = torch::empty({rows, 138 * cap}, inputs.options());
  if (rows) sim_gpu_limit_segment((uint32_t*)inputs.data_ptr<int32_t>(),
      signs.data_ptr<int8_t>(), width, (uint32_t*)base.data_ptr<int32_t>(),
      (uint32_t*)quote.data_ptr<int32_t>(), present.data_ptr<bool>(),
      fault.data_ptr<int32_t>(), required.data_ptr<int64_t>(),
      (uint32_t*)work.data_ptr<int32_t>(), cap, rows,
      c10::cuda::getCurrentCUDAStream().stream());
  C10_CUDA_KERNEL_LAUNCH_CHECK();
  return {base, quote, present, fault, required};
}
std::vector<torch::Tensor> decimal(torch::Tensor bytes, torch::Tensor offsets,
                                  torch::Tensor present) {
  TORCH_CHECK(bytes.is_cuda() && bytes.scalar_type() == torch::kUInt8
      && bytes.is_contiguous() && bytes.dim() == 1, "需设备连续字节字典");
  TORCH_CHECK(offsets.is_cuda() && offsets.scalar_type() == torch::kInt64
      && offsets.is_contiguous() && offsets.dim() == 1 && offsets.numel() >= 1
      && offsets.device() == bytes.device(), "需同设备int64[N+1]偏移");
  int64_t rows = offsets.numel() - 1;
  TORCH_CHECK(rows <= int64_t(INT32_MAX) * 256, "批次超grid容量");
  TORCH_CHECK(present.is_cuda() && present.scalar_type() == torch::kBool
      && present.is_contiguous() && present.dim() == 1 && present.numel() == rows
      && present.device() == bytes.device(), "需同设备bool[N]缺失mask");
  const c10::cuda::CUDAGuard guard(bytes.device());
  auto options = bytes.options().dtype(torch::kInt32);
  auto c = torch::empty({rows, 3}, options), d = torch::empty({rows, 3}, options);
  auto signs = torch::empty({rows}, options.dtype(torch::kInt8));
  auto scale = torch::empty({rows}, options), fault = torch::empty({rows}, options);
  if (rows) public_gpu_decimal(bytes.data_ptr<uint8_t>(), bytes.numel(),
      offsets.data_ptr<int64_t>(), present.data_ptr<bool>(),
      (uint32_t*)c.data_ptr<int32_t>(), (uint32_t*)d.data_ptr<int32_t>(),
      signs.data_ptr<int8_t>(), scale.data_ptr<int32_t>(), fault.data_ptr<int32_t>(),
      rows, c10::cuda::getCurrentCUDAStream().stream());
  C10_CUDA_KERNEL_LAUNCH_CHECK();
  return {c, d, signs, scale, fault};
}
extern "C" void public_gpu_prefix_lookup(const int64_t*, int64_t, const int64_t*,
    int64_t, const int64_t*, int64_t, int64_t*, int32_t*, cudaStream_t);
std::vector<torch::Tensor> prefix_lookup(torch::Tensor rows, torch::Tensor offsets,
                                        torch::Tensor prefixes) {
  TORCH_CHECK(rows.is_cuda() && rows.scalar_type() == torch::kInt64 &&
      rows.is_contiguous() && rows.dim() == 2 && rows.size(1) == 4,
      "需CUDA连续int64[N,4]公共事实更新列");
  for (const auto& tensor : {offsets, prefixes}) {
    TORCH_CHECK(tensor.device() == rows.device() && tensor.scalar_type() == torch::kInt64 &&
        tensor.is_contiguous() && tensor.dim() == 1, "需同设备连续int64索引列");
  }
  TORCH_CHECK(offsets.numel() >= 1, "目标偏移必须包含末端");
  int64_t roots = prefixes.numel(), targets = offsets.numel() - 1;
  TORCH_CHECK(!roots || targets <= (int64_t(INT32_MAX) * 256) / roots,
      "公共查询批次超grid容量，需按资源分块");
  const c10::cuda::CUDAGuard guard(rows.device());
  auto output = torch::empty({roots, targets}, prefixes.options());
  auto fault = torch::empty({roots, targets}, prefixes.options().dtype(torch::kInt32));
  public_gpu_prefix_lookup(rows.data_ptr<int64_t>(), rows.size(0), offsets.data_ptr<int64_t>(),
      targets, prefixes.data_ptr<int64_t>(), roots, output.data_ptr<int64_t>(),
      fault.data_ptr<int32_t>(), c10::cuda::getCurrentCUDAStream().stream());
  C10_CUDA_KERNEL_LAUNCH_CHECK();
  return {output, fault};
}
extern "C" void sim_gpu_latency(const uint8_t*,int64_t,const uint8_t*,int64_t,
    const int64_t*,int64_t,const int64_t*,const uint8_t*,int64_t,const int64_t*,
    int64_t,const int64_t*,const uint32_t*,const int8_t*,int64_t,const int64_t*,bool,const uint64_t*,
    const uint64_t*,int64_t,uint64_t,uint32_t*,int32_t*,uint8_t*,int64_t,cudaStream_t);

std::vector<torch::Tensor> sample_latency(torch::Tensor prefix,torch::Tensor seeds,
    torch::Tensor seed_offsets,torch::Tensor roots,torch::Tensor identities,
    torch::Tensor identity_offsets,torch::Tensor sequences,torch::Tensor signs,
    torch::Tensor domains,torch::Tensor cumulative,torch::Tensor delays,
    uint64_t count,bool decimal_identity,torch::Tensor identity_indices) {
  const auto device=prefix.device();
  auto check=[&](const torch::Tensor& value,torch::ScalarType type,int dimensions) {
    TORCH_CHECK(value.is_cuda() && value.device()==device && value.scalar_type()==type
        && value.dim()==dimensions && value.is_contiguous(),"SIM时延设备列dtype/shape不符");
  };
  check(prefix,torch::kUInt8,1);check(seeds,torch::kUInt8,1);
  check(seed_offsets,torch::kInt64,1);check(roots,torch::kInt64,1);
  check(identities,torch::kUInt8,1);check(identity_offsets,torch::kInt64,1);
  check(identity_indices,torch::kInt64,1);
  check(sequences,torch::kInt32,2);check(signs,torch::kInt8,1);
  check(domains,torch::kInt64,1);check(cumulative,torch::kInt64,1);check(delays,torch::kInt64,1);
  int64_t rows=roots.numel();
  TORCH_CHECK(seed_offsets.numel()>=1 && domains.numel()==rows && signs.numel()==rows
      && sequences.size(0)==rows && sequences.size(1)>0 && identity_offsets.numel()>=1
      && identity_indices.numel()==rows
      && cumulative.numel()>0 && delays.numel()==cumulative.numel() && count>0
      && rows<=int64_t(INT32_MAX)*128 && rows<=INT64_MAX/32,"SIM时延列容量/人口不符");
  const c10::cuda::CUDAGuard guard(device);
  auto words=torch::empty({rows,2},sequences.options());
  auto fault=torch::empty({rows},sequences.options());
  auto digest=torch::empty({rows,32},prefix.options());
  if(rows) sim_gpu_latency(prefix.data_ptr<uint8_t>(),prefix.numel(),seeds.data_ptr<uint8_t>(),
      seeds.numel(),seed_offsets.data_ptr<int64_t>(),seed_offsets.numel()-1,
      roots.data_ptr<int64_t>(),identities.data_ptr<uint8_t>(),identities.numel(),
      identity_offsets.data_ptr<int64_t>(),identity_offsets.numel()-1,
      identity_indices.data_ptr<int64_t>(),(uint32_t*)sequences.data_ptr<int32_t>(),
      signs.data_ptr<int8_t>(),sequences.size(1),domains.data_ptr<int64_t>(),
      decimal_identity,(uint64_t*)cumulative.data_ptr<int64_t>(),
      (uint64_t*)delays.data_ptr<int64_t>(),cumulative.numel(),count,
      (uint32_t*)words.data_ptr<int32_t>(),fault.data_ptr<int32_t>(),digest.data_ptr<uint8_t>(),
      rows,c10::cuda::getCurrentCUDAStream().stream());
  C10_CUDA_KERNEL_LAUNCH_CHECK();
  return {words,fault,digest};
}

extern "C" void noe_gpu_sim_trade_key(const uint32_t*,const int8_t*,int64_t,uint8_t*,int32_t*,int64_t,cudaStream_t);

std::vector<torch::Tensor> sim_trade_key(torch::Tensor words,torch::Tensor signs) {
  words_check(words);
  sign_check(signs,words);
  int64_t rows=words.size(0),width=words.size(1);
  capacity_check(rows,width);
  TORCH_CHECK(rows<=INT64_MAX/20,"NOE原实体字节坐标溢出");
  const c10::cuda::CUDAGuard guard(words.device());
  auto keys=torch::empty({rows,20},words.options().dtype(torch::kUInt8));
  auto fault=torch::empty({rows},words.options());
  if(rows) noe_gpu_sim_trade_key((uint32_t*)words.data_ptr<int32_t>(),signs.data_ptr<int8_t>(),width,
      keys.data_ptr<uint8_t>(),fault.data_ptr<int32_t>(),rows,c10::cuda::getCurrentCUDAStream().stream());
  C10_CUDA_KERNEL_LAUNCH_CHECK();
  return {keys,fault};
}

extern "C" void public_gpu_ordered_attribution(const uint32_t*,const uint32_t*,const int8_t*,
    const uint32_t*,const uint32_t*,const int8_t*,int64_t,int64_t,const bool*,uint32_t*,
    uint32_t*,int32_t*,int64_t*,uint32_t*,int64_t,int64_t,cudaStream_t);

std::vector<torch::Tensor> ordered_attribution(torch::Tensor tn,torch::Tensor td,torch::Tensor ts,
    torch::Tensor cn,torch::Tensor cd,torch::Tensor cs,torch::Tensor enabled,int64_t columns) {
  words_check(tn); words_check(td); words_check(cn); words_check(cd);
  sign_check(ts,tn); sign_check(cs,cn);
  int64_t roots=tn.size(0),width=tn.size(1);
  TORCH_CHECK(roots<=int64_t(INT32_MAX)*128,"归属批次超过设备grid.x容量");
  TORCH_CHECK(columns>=0 && (!roots || columns<=INT64_MAX/roots),"归属人口坐标溢出");
  for(auto value:{td,cn,cd}) {
    TORCH_CHECK(value.device()==tn.device() && value.size(1)==width,"归属值字容量/设备不一致");
  }
  TORCH_CHECK(td.size(0)==roots && cn.size(0)==roots*columns && cd.size(0)==roots*columns,
      "归属根/人口形状不一致");
  TORCH_CHECK(enabled.device()==tn.device() && enabled.scalar_type()==torch::kBool &&
      enabled.is_contiguous() && enabled.dim()==1 && enabled.size(0)==roots,"归属门须同设备bool[roots]");
  // 规范化后的累计分母至多源分母乘积；为跨乘、scale12及除法保留额外字。
  // 此界只分配工作区，不限制数值或人口。核中仍检查每次乘法/输出有效字数。
  TORCH_CHECK(columns<=(INT64_MAX-32)/8,"归属工作区人口溢出");
  int64_t factor=8*columns+32;
  TORCH_CHECK(width<=(INT64_MAX/30-16)/factor,"归属工作区字索引溢出");
  int64_t cap=factor*width+16;
  capacity_check(roots,30*cap);
  const c10::cuda::CUDAGuard guard(tn.device());
  auto on=torch::zeros_like(cn),od=torch::zeros_like(cd);
  auto fault=torch::zeros({roots},tn.options());
  auto required=torch::zeros({roots},tn.options().dtype(torch::kInt64));
  auto scratch=torch::empty({roots,30*cap},tn.options());
  if(roots) public_gpu_ordered_attribution((uint32_t*)tn.data_ptr<int32_t>(),(uint32_t*)td.data_ptr<int32_t>(),ts.data_ptr<int8_t>(),
      (uint32_t*)cn.data_ptr<int32_t>(),(uint32_t*)cd.data_ptr<int32_t>(),cs.data_ptr<int8_t>(),width,columns,
      enabled.data_ptr<bool>(),(uint32_t*)on.data_ptr<int32_t>(),(uint32_t*)od.data_ptr<int32_t>(),fault.data_ptr<int32_t>(),
      required.data_ptr<int64_t>(),(uint32_t*)scratch.data_ptr<int32_t>(),cap,roots,c10::cuda::getCurrentCUDAStream().stream());
  C10_CUDA_KERNEL_LAUNCH_CHECK();
  return {on,od,fault,required};
}

PYBIND11_MODULE(TORCH_EXTENSION_NAME, m) {
  m.def("copy_selected_rows",&copy_selected_rows);
  m.def("copy_bound_rows",&copy_bound_rows);
  m.def("hidden_snapshot_gather", &hidden_snapshot_gather);
  pybind11::class_<DeviceConditionalLoop>(m, "DeviceConditionalLoop")
      .def(pybind11::init<uint64_t, uint64_t, torch::Tensor>())
      .def("replay", &DeviceConditionalLoop::replay);
  pybind11::class_<DeviceConditionalSequenceLoop>(m,"DeviceConditionalSequenceLoop")
      .def(pybind11::init<uint64_t,std::vector<uint64_t>,std::vector<torch::Tensor>,torch::Tensor>())
      .def("replay",&DeviceConditionalSequenceLoop::replay);
  m.def("policy_uniform",&policy_uniform);
  m.def("ordered_attribution", &ordered_attribution);
  m.def("sim_trade_key", &sim_trade_key);
  m.def("integer_op", &integer_op);
  m.def("project", &project);
  m.def("index_probe", &index_probe);
  m.def("gcd", &gcd);
  m.def("floor_sum", &floor_sum);
  m.def("limit_segment", &limit_segment);
  m.def("sample_latency", &sample_latency);
  m.def("decimal", &decimal);
  m.def("prefix_lookup", &prefix_lookup);
}
