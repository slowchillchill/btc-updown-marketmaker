#include <cuda_runtime.h>

// 条件来自设备状态；每次入口及body完成后重新设置，绝不回读CPU选业务分支。
__global__ void set_public_graph_condition(cudaGraphConditionalHandle handle, const bool* ready) {
  cudaGraphSetConditional(handle, *ready ? 1u : 0u);
}

extern "C" cudaError_t public_graph_condition_node(cudaGraphNode_t* node, cudaGraph_t graph,
    cudaGraphNode_t predecessor, cudaGraphConditionalHandle handle, const bool* ready) {
  void* args[] = {&handle, &ready};
  cudaKernelNodeParams params{};
  params.func = reinterpret_cast<void*>(set_public_graph_condition);
  params.gridDim = dim3(1);
  params.blockDim = dim3(1);
  params.kernelParams = args;
  return cudaGraphAddKernelNode(node, graph, predecessor ? &predecessor : nullptr,
      predecessor ? 1 : 0, &params);
}
