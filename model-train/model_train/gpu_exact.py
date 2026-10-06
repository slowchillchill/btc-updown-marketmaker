"""公共精确CUDA整数和有理投影的模型消费。CPU仅在材料/报告批次装箱。"""
from dataclasses import dataclass
from pathlib import Path
from functools import lru_cache
import importlib.util
import torch

@lru_cache(maxsize=1)
def public_extension():
    path=Path(__file__).resolve().parents[2]/"shared-run/gpu/extension.py"
    spec=importlib.util.spec_from_file_location("public_gpu_extension",path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    return module.load_extension()

@dataclass(frozen=True)
class ExactInt:
    words: torch.Tensor
    sign: torch.Tensor
    fault: torch.Tensor

    @classmethod
    def from_values(cls, values, *, device="cuda"):
        """显式批次输入边界；不能用于训练逐条件往返。"""
        values=list(values);width=max(1,max((abs(v).bit_length()+31)//32 for v in values)) if values else 1
        words=[]
        for value in values:
            row=[]
            for i in range(width):
                w=(abs(value)>>(32*i))&0xffffffff;row.append(w if w<0x80000000 else w-0x100000000)
            words.append(row)
        return cls(torch.tensor(words,dtype=torch.int32,device=device).reshape(len(values),width),
                   torch.tensor([0 if v==0 else (-1 if v<0 else 1) for v in values],dtype=torch.int8,device=device),
                   torch.zeros(len(values),dtype=torch.int32,device=device))

    def _operate(self, other, op):
        w,s,r,rs,f=public_extension().integer_op(op,self.words,self.sign,other.words,other.sign)
        f=f|self.fault|other.fault
        return ExactInt(w,s,f),ExactInt(r,rs,f)

    def __add__(self, other):return self._operate(other,0)[0]
    def __sub__(self, other):return self._operate(other,1)[0]
    def __mul__(self, other):return self._operate(other,2)[0]
    def divrem(self, other):return self._operate(other,3)
    def compare(self, other):return self._operate(other,4)[0].sign

    def project(self, denominator, *, operation=0):
        output,fault=public_extension().project(operation,self.words,self.sign*denominator.sign,denominator.words)
        return output,fault|self.fault|denominator.fault

    def report_values(self):
        """阶段报告/独立验证边界，生产条件循环不调用。"""
        words=self.words.cpu().tolist();signs=self.sign.cpu().tolist()
        return [s*sum((w&0xffffffff)<<(32*i) for i,w in enumerate(row)) for row,s in zip(words,signs)]
