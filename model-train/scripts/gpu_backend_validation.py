#!/usr/bin/env python3
"""#181实际公共GPU后端必要验证；不是旧CPU全窗诊断或正式训练。"""
import argparse
import json
import math
import random
import struct
import subprocess
import importlib.util
import sys
import time
from decimal import Decimal, localcontext
from fractions import Fraction
from pathlib import Path
import torch
from model_train.gpu_exact import ExactInt, ExactRational, public_extension, run_exact_batch


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
    _,_,_,_,df,_=extension.integer_op(3,ga.words,ga.sign,ExactInt.from_values([0]*len(a)).words,gb.sign);assert bool((df==1).all()),'division zero'
    bad, _ = ExactInt.from_values([1]).divrem(ExactInt.from_values([0]))
    compared_bad = bad.compare(ExactInt.from_values([1]))
    assert int(compared_bad.fault[0]) == 1 and not bool(compared_bad.valid_mask()[0]), 'compare fault propagation'
    widths = [1, (1 << 26), (1 << 26) + 1, (1 << 30) + 1]
    tops = [1, -1, 1, -2147483648]
    probe = extension.index_probe(torch.tensor(widths, dtype=torch.int64, device='cuda'), torch.tensor(tops, dtype=torch.int32, device='cuda'))
    expected_bits = [(w-1)*32+(top & 0xffffffff).bit_length() for w,top in zip(widths,tops)]
    assert probe.cpu().tolist() == [[v,(v-1)//32,(v-1)%32] for v in expected_bits], '64-bit device bit indexes'
    records.extend(['compare_fault', 'index64'])
    log(f'阶段=公共精确有理数 工作量={len(a)} 耗时={time.monotonic()-started:.3f}s')
    ra = ExactRational.from_values(a, b)
    rc = ExactRational.from_values(b, [3] * len(b))
    fa = [Fraction(n, d) for n, d in zip(a, b)]
    fc = [Fraction(n, 3) for n in b]
    for name, actual, reference in [
        ('rational_add', ra+rc, [x+y for x,y in zip(fa,fc)]),
        ('rational_sub', ra-rc, [x-y for x,y in zip(fa,fc)]),
        ('rational_mul', ra*rc, [x*y for x,y in zip(fa,fc)]),
        ('rational_div', ra.divided_by(rc), [x/y for x,y in zip(fa,fc)]),
    ]:
        assert actual.numerator.report_values() == [x.numerator for x in reference], name
        assert actual.denominator.report_values() == [x.denominator for x in reference], name
        assert not bool(actual.fault.any()), name
        records.append(name)
        log(f'完成={name} 耗时={time.monotonic()-started:.3f}s')
    assert ra.floor().report_values() == [x.numerator//x.denominator for x in fa], 'rational_floor'
    bad_r = ExactRational.from_values([1], [0])
    assert not bool(bad_r.compare(ExactRational.from_values([1], [1])).valid_mask()[0]), 'rational zero denominator fault'
    canonical = ExactRational.from_values([0, -6, 6], [-7, -8, -8])
    assert canonical.numerator.report_values() == [0, 3, -3], 'normalized numerator sign'
    assert canonical.denominator.report_values() == [1, 4, 4], 'normalized denominator and zero'
    assert not bool(canonical.fault.any()), 'valid normalized rationals'
    records.extend(['rational_floor', 'rational_fault'])
    log(f'阶段=原SIM经济成本GPU消费者 耗时={time.monotonic()-started:.3f}s')
    module_path = Path(__file__).resolve().parents[2] / 'sim-system/gpu/buy_cost.py'
    spec = importlib.util.spec_from_file_location('sim_gpu_buy_cost', module_path)
    buy_cost = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = buy_cost
    spec.loader.exec_module(buy_cost)
    quantities = [500] * 6 + [((1<<96)-1)//10000]
    prices = [Fraction(k,100) for k in [25,10,20,25,50,90]] + [Fraction(9999,10000)]
    rates = [Fraction(7,100)] * 6 + [Fraction((1<<96)-1)]
    post = [True, False, False, False, False, False, False]
    nums = ExactInt.from_values(quantities)
    price_r = ExactRational.from_values([p.numerator for p in prices], [p.denominator for p in prices])
    rate_r = ExactRational.from_values([r.numerator for r in rates], [r.denominator for r in rates])
    cost = run_exact_batch(lambda: buy_cost.limit_buy_cost(nums, price_r, rate_r, 1,
        post_only=torch.tensor(post, dtype=torch.bool, device='cuda'), minimum_principal_micro=nums.constant(1_000_000)))
    principal_expected = [n * int(p*1_000_000)//100 for n,p in zip(quantities,prices)]
    def half_up(f):
        q,r = divmod(f.numerator, f.denominator)
        return q + int(2*r >= f.denominator)
    fees_expected = [0 if po or principal < 1_000_000 else half_up(n*1000*r*min(p,Fraction(1,2))*(1-min(p,Fraction(1,2))))
        for n,p,r,po,principal in zip(quantities,prices,rates,post,principal_expected)]
    assert fees_expected[:6] == [0,0,5600,6563,8750,8750], 'existing Rust production cost evidence'
    assert fees_expected[-1] > 1<<128, 'valid fee exceeds u128'
    assert cost.principal_micro.report_values() == principal_expected, 'SIM principal micro'
    assert cost.fee_1e5_units.report_values() == fees_expected, 'SIM H5 fee'
    assert not bool((cost.principal_micro.fault | cost.fee_1e5_units.fault).any()), 'SIM cost valid'
    threshold_rate = ExactRational.from_values([4,39999], [1_000_000,10_000_000_000])
    threshold_n = ExactInt.from_values([500,500])
    threshold_p = ExactRational.from_values([1,1], [2,2])
    threshold_fee = run_exact_batch(lambda: buy_cost.market_fee_estimate(threshold_n, threshold_p, threshold_rate, 1))
    assert threshold_fee.report_values() == [1,0], 'strict fee half unit'
    for exponent in [95, 0xffffffff]:
        tiny_fee = run_exact_batch(lambda: buy_cost.market_fee_estimate(nums, price_r, rate_r, exponent), initial_word_capacity=128)
        assert tiny_fee.report_values() == [0]*len(quantities), 'large exponent exact zero'
        assert not bool(tiny_fee.fault.any())
    records.extend(['sim_limit_cost', 'sim_fee_half', 'sim_fee_large_exponent'])
    log(f'阶段=连续容量复用 耗时={time.monotonic()-started:.3f}s')
    def constant_chain():
        value = ExactRational.from_values([1], [1])
        capacities = []
        for _ in range(64):
            value = value * value
            capacities.append((value.numerator.words.shape[1], value.denominator.words.shape[1]))
        return value, capacities
    torch.cuda.synchronize()
    resident_before = torch.cuda.memory_allocated()
    torch.cuda.reset_peak_memory_stats()
    stable, capacities = run_exact_batch(constant_chain)
    torch.cuda.synchronize()
    constant_peak_growth = torch.cuda.max_memory_allocated() - resident_before
    assert stable.numerator.report_values() == [1] and stable.denominator.report_values() == [1]
    assert set(capacities) == {(4,4)}, 'constant rational storage must remain stable'
    assert constant_peak_growth < 16*1024*1024, 'bounded continuous temporary storage'
    def growth_chain():
        value = ExactInt.from_values([1<<126])
        return value * value
    grown = run_exact_batch(growth_chain)
    assert grown.report_values() == [1<<252] and not bool(grown.fault.any()), 'real growth must expand capacity and retry'
    assert grown.words.shape[1] == 8, 'resource growth uses actual required width'
    records.extend(['continuous_capacity', 'true_growth_retry'])
    log(f'阶段=DPT批量转换/原键稳定排序/独立归约 耗时={time.monotonic()-started:.3f}s')
    path = Path(__file__).resolve().parents[2] / 'data-pipeline/gpu/prepare.py'
    spec = importlib.util.spec_from_file_location('dpt_gpu_prepare', path)
    dpt = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = dpt
    spec.loader.exec_module(dpt)
    tokens = ['0', '-1.25', '79228162514264337593543950335',
              '0.0000000000000000000000000001', '12.000000000000000000000000001', '', '1..2']
    payload = ''.join(tokens).encode('ascii')
    offsets = [0]
    for token in tokens:
        offsets.append(offsets[-1] + len(token))
    columns = dpt.decimal_columns(torch.tensor(list(payload), dtype=torch.uint8, device='cuda'),
        torch.tensor(offsets, dtype=torch.int64, device='cuda'),
        torch.tensor([True]*5+[False,True], dtype=torch.bool, device='cuda'))
    assert columns.coefficient.report_values()[:5] == [0,-125,(1<<96)-1,1,12000000000000000000000000001], 'GPU decimal coefficients'
    assert columns.scale.cpu().tolist()[:6] == [0,2,0,28,27,0], 'GPU decimal scales and missing'
    assert columns.coefficient.fault.cpu().tolist() == [0,0,0,0,0,0,1], 'GPU malformed decimal fault'
    keys = [b'',b'\x00',b'\x00\x00',b'\x00\xff',b'\xff',b'\x00\x00']
    key_bytes = torch.tensor([list(k)+[0]*(2-len(k)) for k in keys], dtype=torch.uint8, device='cuda')
    order, key_fault = dpt.stable_source_order(key_bytes, torch.tensor([len(k) for k in keys], dtype=torch.int64, device='cuda'))
    assert order.cpu().tolist() == sorted(range(len(keys)), key=lambda i:keys[i]), 'GPU original byte-key stable order'
    assert not bool(key_fault.any())
    assert dpt.independent_tag_counts(torch.tensor([0,2,2,1], device='cuda'), 4).cpu().tolist() == [1,1,2,0]
    records.extend(['dpt_decimal', 'dpt_stable_keys', 'dpt_tag_counts'])
    torch.cuda.synchronize();elapsed=time.monotonic()-started
    report={'scope':'实际公共GPU整数/有理数/SIM费用/连续容量；不是完整环境/PPO/训练容量','source_commit':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),'cases':len(a),'passed':records,'projection_errors':errors,'compile_seconds':compile_seconds,'elapsed_seconds':elapsed,'device':torch.cuda.get_device_name(),'constant_chain_peak_growth_bytes':constant_peak_growth,'constant_chain_words':capacities[-1],'peak_after_constant_stage_reset_bytes':torch.cuda.max_memory_allocated()}
    args.output.parent.mkdir(parents=True,exist_ok=True);args.output.write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n');log(f'完成 精确检查={records} 投影有限误差={errors} 耗时={elapsed:.3f}s')

if __name__=='__main__':main()
