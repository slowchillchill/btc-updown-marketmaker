"""公共模块CUDA算子的批量PyTorch接线；不复制业务归约。"""
from pathlib import Path
from functools import lru_cache

@lru_cache(maxsize=1)
def load_extension():
    import torch
    from torch.utils.cpp_extension import load
    root=Path(__file__).resolve().parents[2]
    return load(name="public_gpu_181",sources=[str(root/"shared-run/gpu/bindings.cpp"),str(root/"shared-run/gpu/conditional_graph.cu"),str(root/"contracts/gpu/exact.cu"),str(root/"contracts/gpu/ordered_attribution.cu"),str(root/"sim-system/gpu/limit_segment.cu"),str(root/"sim-system/gpu/latency.cu"),str(root/"execution-normalization/gpu/identity.cu"),str(root/"data-pipeline/gpu/decimal.cu"),str(root/"data-pipeline/gpu/public_state.cu")],
                extra_cflags=["-O3"],extra_cuda_cflags=["-O3","--fmad=false"],with_cuda=True)
