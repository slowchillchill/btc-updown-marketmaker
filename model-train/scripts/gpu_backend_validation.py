#!/usr/bin/env python3
"""#181实际公共GPU后端必要验证；不是旧CPU全窗诊断或正式训练。"""
import argparse
import json
import math
import random
import struct
import subprocess
import time
from decimal import Decimal, localcontext
from pathlib import Path
import torch
from model_train.gpu_exact import ExactInt, public_extension


def nearest_bits(n,d):
    if not n:return 0
    sign=0x80000000 if n<0 else 0;n=abs(n)
    e=n.bit_length()-d.bit_length()
    if (n < (d<<e)) if e>=0 else ((n<<-e)<d):e-=1
    if e>127:return sign|0x7f800000
    if e<-150:return sign
    shift=23-e if e>=-126 else 149
    a,b=(n<<shift,d) if shift>=0 else (n,d<<-shift)
    q,r=divmod(a,b)
    if 2*r>b or (2*r==b and q%2):q+=1
    if e>=-126:
        if q==1<<24:q>>=1;e+=1
        if e>127:return sign|0x7f800000
        return sign|((e+127)<<23)|(q&0x7fffff)
    return sign|q


def log(message):print(message,flush=True)


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--output',type=Path,required=True);parser.add_argument('--cases',type=int,default=4096)
    args=parser.parse_args();started=time.monotonic();log('阶段=编译/加载公共CUDA后端')
    extension=public_extension();compile_seconds=time.monotonic()-started
    rng=random.Random(181003);a=[0,1,-1,(1<<96)-1,((1<<96)-1)//10000,(1<<512)+7,1<<2048,-((1<<3072)-1)];b=[1,1,1,10**28,100,10**28+3,(1<<1024)+1,(1<<64)+1]
    for _ in range(max(0,args.cases-len(a))):
        av=rng.getrandbits(rng.choice([1,32,96,128,256,513,1024,2048]));bv=rng.getrandbits(rng.choice([1,32,96,128,256,513,1024])) or 1
        a.append(-av if rng.randrange(2) else av);b.append(bv)
    log(f'阶段=精确整数批量验证 工作量={len(a)} 耗时={time.monotonic()-started:.3f}s')
    ga,gb=ExactInt.from_values(a),ExactInt.from_values(b);records=[]
    for name,value,reference in [('add',ga+gb,[x+y for x,y in zip(a,b)]),('sub',ga-gb,[x-y for x,y in zip(a,b)]),('mul',ga*gb,[x*y for x,y in zip(a,b)])]:
        assert value.report_values()==reference,name
        assert not bool(value.fault.any()),name
        records.append(name);log(f'完成={name} 耗时={time.monotonic()-started:.3f}s')
    q,r=ga.divrem(gb);expected_q=[(abs(x)//y)*(-1 if x<0 else 1) for x,y in zip(a,b)]
    assert q.report_values()==expected_q,'division';assert r.report_values()==[x-y*z for x,y,z in zip(a,b,expected_q)],'remainder'
    comparison = ga.compare(gb)
    assert comparison.order.cpu().tolist()==[(x>y)-(x<y) for x,y in zip(a,b)],'compare'
    assert bool(comparison.valid_mask().all()), 'compare valid mask'
    assert not bool(q.fault.any());records.extend(['divrem','compare'])
    log(f'阶段=精确f32及标准末端投影 耗时={time.monotonic()-started:.3f}s')
    pn=[0,1,3,5,2**24+1,2**24+3,2**25-1,2**128-2**103,2**128-2**104]
    pd=[1,2**150,2**150,2**150,2**24,2**24,2**24,1,1]
    pn+=a;pd+=b
    gn,gd=ExactInt.from_values(pn),ExactInt.from_values(pd);values,fault=gn.project(gd)
    bits=[struct.unpack('<I',struct.pack('<f',v))[0] for v in values.cpu().tolist()]
    assert bits==[nearest_bits(n,d) for n,d in zip(pn,pd)],'ratio f32 RNE'
    assert not bool(fault.any());records.append('round_f32')
    # 只测实际后端的末端投影；独立Decimal依据批量一次，报告有限误差，不宣称统一误差界。
    logs,lf=gn.project(gd,operation=1);positive=ExactInt.from_values([abs(n) for n in pn]);roots,rf=positive.project(gd,operation=2)
    with localcontext() as ctx:
        ctx.prec=90
        expected_log=[float((Decimal(abs(n))/Decimal(d)+1).ln())*(-1 if n<0 else 1) for n,d in zip(pn,pd)]
        expected_root=[float(((Decimal(abs(n))/Decimal(d)).sqrt()+1).ln()) for n,d in zip(pn,pd)]
    errors={}
    for name,actual,expected in [('signed_log',logs,expected_log),('sqrt_log',roots,expected_root)]:
        assert bool(torch.isfinite(actual).all()),name
        reference=torch.tensor(expected,dtype=torch.float32,device='cuda');difference=(actual-reference).abs();errors[name]={'max_absolute':float(difference.max()),'different':int((actual!=reference).sum())}
    assert not bool(lf.any()|rf.any())
    _,zero_fault=ga.project(ExactInt.from_values([0]*len(a)));assert bool((zero_fault==1).all()),'zero denominator'
    _,_,_,_,df=extension.integer_op(3,ga.words,ga.sign,ExactInt.from_values([0]*len(a)).words,gb.sign);assert bool((df==1).all()),'division zero'
    bad, _ = ExactInt.from_values([1]).divrem(ExactInt.from_values([0]))
    compared_bad = bad.compare(ExactInt.from_values([1]))
    assert int(compared_bad.fault[0]) == 1 and not bool(compared_bad.valid_mask()[0]), 'compare fault propagation'
    widths = [1, (1 << 26), (1 << 26) + 1, (1 << 30) + 1]
    tops = [1, -1, 1, -2147483648]
    probe = extension.index_probe(torch.tensor(widths, dtype=torch.int64, device='cuda'), torch.tensor(tops, dtype=torch.int32, device='cuda'))
    expected_bits = [(w-1)*32+(top & 0xffffffff).bit_length() for w,top in zip(widths,tops)]
    assert probe.cpu().tolist() == [[v,(v-1)//32,(v-1)%32] for v in expected_bits], '64-bit device bit indexes'
    records.extend(['compare_fault', 'index64'])
    torch.cuda.synchronize();elapsed=time.monotonic()-started
    report={'scope':'实际公共GPU整数/末端投影；不是完整环境/PPO/训练容量','source_commit':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),'cases':len(a),'passed':records,'projection_errors':errors,'compile_seconds':compile_seconds,'elapsed_seconds':elapsed,'device':torch.cuda.get_device_name(),'peak_allocated_bytes':torch.cuda.max_memory_allocated()}
    args.output.parent.mkdir(parents=True,exist_ok=True);args.output.write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n');log(f'完成 精确检查={records} 投影有限误差={errors} 耗时={elapsed:.3f}s')

if __name__=='__main__':main()
