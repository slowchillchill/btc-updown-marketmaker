#include <torch/extension.h>
#include <c10/cuda/CUDAGuard.h>
#include <c10/cuda/CUDAStream.h>
#include <c10/cuda/CUDAException.h>
#include <limits>
#include "../../contracts/gpu/exact.cuh"

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
  int64_t wo = op == 2 ? wa + wb : (op == 3 ? wa : std::max(wa, wb) + 1);
  int64_t wr = wb + 1;
  capacity_check(rows, wo); capacity_check(rows, wr);
  auto out = torch::empty({rows, wo}, a.options());
  auto sign = torch::empty({rows}, sa.options());
  auto rem = torch::empty({rows, wr}, a.options());
  auto rs = torch::empty({rows}, sa.options());
  auto fault = torch::empty({rows}, a.options());
  if (rows) public_gpu_integer(op, (uint32_t*)a.data_ptr<int32_t>(), wa,
      sa.data_ptr<int8_t>(), (uint32_t*)b.data_ptr<int32_t>(), wb,
      sb.data_ptr<int8_t>(), (uint32_t*)out.data_ptr<int32_t>(), wo,
      sign.data_ptr<int8_t>(), (uint32_t*)rem.data_ptr<int32_t>(), wr,
      rs.data_ptr<int8_t>(), fault.data_ptr<int32_t>(), rows,
      c10::cuda::getCurrentCUDAStream().stream());
  C10_CUDA_KERNEL_LAUNCH_CHECK();
  return {out, sign, rem, rs, fault};
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
PYBIND11_MODULE(TORCH_EXTENSION_NAME, m) {
  m.def("integer_op", &integer_op);
  m.def("project", &project);
  m.def("index_probe", &index_probe);
}
