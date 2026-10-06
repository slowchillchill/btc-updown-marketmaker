"""SIM原经验时延的设备采样；profile/seed一次冷构建，动态身份保持在设备。

返回精确u64毫秒及独立fault。不是发送、公开可见或经济提交。
"""
from dataclasses import dataclass
import hashlib
import json
import torch
from queues import Numbers, ExactInt

SAMPLER_VERSION = b'sha256_multiply_high_empirical_rank_v1'


def byte_column(values, *, device='cuda'):
    """冷字典一次装箱；热调用直接传已有设备bytes/offsets。"""
    offsets = [0]
    parts = []
    for value in values:
        parts.append(value)
        offsets.append(offsets[-1] + len(value))
    raw = b''.join(parts)
    return (torch.tensor(list(raw), dtype=torch.uint8, device=device),
            torch.tensor(offsets, dtype=torch.int64, device=device))


def _signed_bits(value):
    return value if value < 1 << 63 else value - (1 << 64)


@dataclass(frozen=True)
class DelaySample:
    milliseconds: ExactInt
    digest: torch.Tensor

    def arrival_at(self, sent_at):
        """沿原u64→i64 delay及checked_add；结果故障不能提交到延迟队列。"""
        words = self.milliseconds.words.to(torch.int64) & 0xffffffff
        delay = words[:, 0] | (words[:, 1] << 32)
        invalid = (words[:, 1] > 0x7fffffff) | (sent_at > 0x7fffffffffffffff-delay)
        fault = self.milliseconds.fault | invalid.to(torch.int32)
        return sent_at+delay, fault


@dataclass(frozen=True)
class EmpiricalDelaySampler:
    prefix: torch.Tensor
    seeds: torch.Tensor
    seed_offsets: torch.Tensor
    cumulative: torch.Tensor
    delays: torch.Tensor
    sample_count: int
    profile_sha256: str

    @classmethod
    def build(cls, profile_bytes, expected_sha256, session_seeds, *, device='cuda'):
        actual = hashlib.sha256(profile_bytes).hexdigest()
        if actual != expected_sha256:
            raise ValueError('SIM时延profile SHA不符')
        bins = json.loads(profile_bytes)['histogram']
        cumulative = []
        delays = []
        count = 0
        for item in bins:
            delay, population = item['delay_ms'], item['count']
            if type(delay) is not int or type(population) is not int or not (
                    0 <= delay < 1 << 64 and 0 <= population < 1 << 64):
                raise ValueError('SIM时延profile需u64直方图')
            count += population
            if count >= 1 << 64:
                raise ValueError('SIM时延profile人口溢出')
            cumulative.append(_signed_bits(count))
            delays.append(_signed_bits(delay))
        if not count:
            raise ValueError('SIM时延profile没有可采样人口')
        prefix = b''.join(len(value).to_bytes(8, 'big') + value
                          for value in [SAMPLER_VERSION, actual.encode('ascii')])
        seeds, offsets = byte_column(session_seeds, device=device)
        return cls(torch.tensor(list(prefix), dtype=torch.uint8, device=device), seeds,
                   offsets, torch.tensor(cumulative, dtype=torch.int64, device=device),
                   torch.tensor(delays, dtype=torch.int64, device=device), count, actual)

    def _sample(self, roots, identities, offsets, sequences, domains, *, decimal):
        words, fault, digest = Numbers.public_extension().sample_latency(
            self.prefix, self.seeds, self.seed_offsets, roots.contiguous(), identities,
            offsets, sequences.words.contiguous(), sequences.sign.contiguous(),
            domains.contiguous(), self.cumulative, self.delays, self.sample_count, decimal)
        fault = fault | sequences.fault
        sign = words.ne(0).any(dim=1).to(torch.int8)
        return DelaySample(ExactInt(words, sign, fault), digest)

    def sample_requests(self, roots, identities, offsets, domains):
        """domain=0出站，1直接响应入站；稳定requestID为原UTF8设备字节。"""
        empty = ExactInt(torch.zeros((roots.numel(), 1), dtype=torch.int32, device=roots.device),
                         torch.zeros_like(roots, dtype=torch.int8),
                         torch.zeros_like(roots, dtype=torch.int32))
        result = self._sample(roots, identities, offsets, empty, domains, decimal=False)
        invalid = (domains != 0) & (domains != 1)
        value = result.milliseconds
        return DelaySample(ExactInt(value.words, value.sign,
                                   value.fault | invalid.to(torch.int32)), result.digest)

    def sample_feedback(self, roots, private_sequences):
        """原private反馈u64序号在CUDA转十进制，不能转有符号int64或CPU字符串。"""
        rows = roots.numel()
        return self._sample(roots, torch.empty(0, dtype=torch.uint8, device=roots.device),
                            torch.zeros(rows+1, dtype=torch.int64, device=roots.device),
                            private_sequences, torch.full_like(roots, 2), decimal=True)
