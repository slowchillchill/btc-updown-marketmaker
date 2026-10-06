"""原Rust SIM采样器与实际设备算子对照；只在AutoDL运行。"""
import hashlib
import json
from pathlib import Path
import sys
import time
import torch
from latency import EmpiricalDelaySampler, byte_column
from queues import ExactInt


def values(value):
    rows=value.words.cpu().tolist()
    return [sum((word & 0xffffffff) << (32*i) for i,word in enumerate(row)) for row in rows]


def main():
    source=json.load(sys.stdin)
    profile_path=Path(__file__).resolve().parents[1]/'config/latency-profiles'/source['profile_sha256']/'profile.json'
    profile_bytes=profile_path.read_bytes()
    sampler=EmpiricalDelaySampler.build(profile_bytes,source['profile_sha256'],
                                       [bytes(seed) for seed in source['seeds']])
    requests=source['requests']
    roots=torch.tensor([row['root'] for row in requests],device='cuda')
    identities,offsets=byte_column([row['identity'].encode('utf-8') for row in requests])
    domains=torch.tensor([row['domain'] for row in requests],device='cuda')
    started=time.perf_counter()
    output=sampler.sample_requests(roots,identities,offsets,domains)
    assert output.milliseconds.fault.cpu().tolist()==[0]*len(requests)
    assert values(output.milliseconds)==[row['delay'] for row in requests]
    # 完整digest对SHA padding、原五段长度前缀提供独立逐字节参考。
    for index,row in enumerate(requests):
        segments=[b'sha256_multiply_high_empirical_rank_v1',source['profile_sha256'].encode(),
                  bytes(source['seeds'][row['root']]),row['identity'].encode(),
                  [b'REQUEST_OUTBOUND_V1',b'DIRECT_RESPONSE_INBOUND_V1'][row['domain']]]
        encoded=b''.join(len(value).to_bytes(8,'big')+value for value in segments)
        assert bytes(output.digest[index].cpu().tolist())==hashlib.sha256(encoded).digest()
    feedback=source['feedback']
    roots=torch.tensor([row['root'] for row in feedback],device='cuda')
    sequences=ExactInt.from_values([row['sequence'] for row in feedback])
    result=sampler.sample_feedback(roots,sequences)
    assert values(result.milliseconds)==[row['delay'] for row in feedback]
    assert not result.milliseconds.fault.any().cpu().item()
    at,fault=result.arrival_at(torch.full_like(roots,1200))
    assert not fault.any().cpu().item()
    assert at.cpu().tolist()==[1200+row['delay'] for row in feedback]
    _,fault=result.arrival_at(torch.full_like(roots,(1<<63)-1))
    assert fault.cpu().tolist()==[1]*len(feedback)
    bad=ExactInt.from_values([1<<64])
    assert sampler.sample_feedback(roots[:1],bad).milliseconds.fault.cpu().tolist()==[1]
    upstream=ExactInt(sequences.words,sequences.sign,torch.ones_like(sequences.fault))
    assert sampler.sample_feedback(roots,upstream).milliseconds.fault.cpu().tolist()==[1]*len(feedback)
    # 仅验证原u64容量边界，profile SHA不冒用正式版本。
    large=EmpiricalDelaySampler(sampler.prefix,sampler.seeds,sampler.seed_offsets,
        torch.tensor([1,-(1<<63),-1],device='cuda'),
        torch.tensor([0,-(1<<63),-1],device='cuda'),(1<<64)-1,sampler.profile_sha256)
    sampled=large.sample_feedback(roots,sequences)
    assert values(sampled.milliseconds)==source['large_expected']
    _,fault=sampled.arrival_at(torch.zeros_like(roots))
    assert fault.cpu().tolist()==[int(delay >= 1<<63) for delay in source['large_expected']]
    torch.cuda.synchronize()
    print(json.dumps({'original_request_cases':len(requests),'original_feedback_u64_cases':len(feedback),
        'full_SHA256_digest_checked':True,'u64_histogram_and_delay_checked':True,
        'elapsed_seconds_with_reference_reads':time.perf_counter()-started,
        'device_peak_allocated_bytes':torch.cuda.max_memory_allocated(),
        'runtime_feedback_NOE_Ledger_delivered':False}))


if __name__=='__main__':
    main()
