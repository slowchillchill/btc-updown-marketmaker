"""模型只消费contracts公共GPU数值；不在训练模块复制经济运算。"""
from functools import lru_cache
import importlib.util
from pathlib import Path
import sys


@lru_cache(maxsize=1)
def _numbers():
    path = Path(__file__).resolve().parents[2] / 'contracts/gpu/numbers.py'
    name = 'public_gpu_numbers'
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


ExactInt = _numbers().ExactInt
ExactComparison = _numbers().ExactComparison
ExactRational = _numbers().ExactRational
public_extension = _numbers().public_extension
run_exact_batch = _numbers().run_exact_batch
