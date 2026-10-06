#include <torch/extension.h>
#include <c10/cuda/CUDAGuard.h>
#include <c10/cuda/CUDAStream.h>
#include <c10/cuda/CUDAException.h>
#include <limits>
#include "../../contracts/gpu/exact.cuh"
extern "C" void public_gpu_decimal(const uint8_t*, int64_t, const int64_t*,
    const bool*, uint32_t*, uint32_t*, int8_t*, int32_t*, int32_t*, int64_t, cudaStream_t);

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
PYBIND11_MODULE(TORCH_EXTENSION_NAME, m) {
  m.def("integer_op", &integer_op);
  m.def("project", &project);
  m.def("index_probe", &index_probe);
  m.def("gcd", &gcd);
  m.def("floor_sum", &floor_sum);
  m.def("limit_segment", &limit_segment);
  m.def("decimal", &decimal);
  m.def("prefix_lookup", &prefix_lookup);
}
