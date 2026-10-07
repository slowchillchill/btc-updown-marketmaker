"""原Rust登记/响应时点对照与完整u64/目录扩容设备边界；非完整运行宿主。"""
import json
from pathlib import Path
import sys
import time
import torch
from latency import EmpiricalDelaySampler, byte_column
from mutation_requests import MutationRequests
from queues import ExactInt, Numbers


def value(number):
    return [int(sign)*sum((word & 0xffffffff) << (32*i) for i, word in enumerate(row))
            for sign, row in zip(number.sign.cpu().tolist(), number.words.cpu().tolist())]


def main():
    source = json.load(sys.stdin)
    started = time.perf_counter()
    profile = Path(__file__).resolve().parents[1]/'config/latency-profiles'/source['profile_sha256']/'profile.json'
    sampler = EmpiricalDelaySampler.build(profile.read_bytes(), source['profile_sha256'], [bytes(source['seed'])])
    identities, offsets = byte_column([source['identity'].encode(), b'other-request', b'high-sequence'])
    tensor = lambda values, dtype=torch.int64: torch.tensor(values, dtype=dtype, device='cuda')
    sequence = ExactInt.from_values([source['input_sequence']])
    original = MutationRequests.empty(sequence, capacity=1)
    inputs = [sampler, identities, offsets, tensor([0]), tensor([1]), sequence,
        tensor([source['sent_at']]), tensor([[0, 1]]), tensor([[7, 1]]), tensor([[True, True]], torch.bool),
        tensor([[False, True]], torch.bool), tensor([source['taker_delay_ms']]), tensor([True], torch.bool), tensor([0], torch.int32)]
    state, receipt = original.run_registration(*inputs)
    assert receipt.fault.cpu().tolist() == [0] and receipt.registered.cpu().tolist() == [True]
    assert value(receipt.input_sequence) == [source['receipt']['input_sequence']]
    assert receipt.arrived_at.cpu().tolist() == [source['receipt']['arrived_at']]
    assert receipt.scheduled_count.cpu().tolist() == [source['receipt']['scheduled_count']]
    # 重投换时刻、序号、损坏新正文，必须先命中原身份并返回旧回执。
    retry = list(inputs)
    retry[5] = ExactInt.from_values([1 << 80])
    retry[6] = tensor([(1 << 63)-1])
    retry[8] = tensor([[-1, -1]])
    retry[-1] = tensor([1], torch.int32)
    same, cached = state.run_registration(*retry)
    assert cached.duplicate.cpu().tolist() == [True] and cached.fault.cpu().tolist() == [0]
    assert value(cached.input_sequence) == [source['input_sequence']]
    assert torch.equal(same.item_refs, state.item_refs)
    for ordinal, expected in enumerate(source['pending']):
        pending = state.next_pending()
        assert pending.ordinal.cpu().tolist() == [expected['ordinal']]
        assert pending.effective_at.cpu().tolist() == [expected['effective_at']]
        assert value(pending.input_sequence) == [expected['input_sequence']]
        state, completed = Numbers.run_exact_batch(lambda: state.complete(pending, tensor([10+ordinal]),
            tensor([True], torch.bool), tensor([0], torch.int32)))
        assert completed.emitted.cpu().tolist() == [ordinal == len(source['pending'])-1]
    assert completed.responded_at.cpu().tolist() == [source['response']['responded_at']]
    assert completed.received_at.cpu().tolist() == [source['response']['received_at']]
    assert completed.ordinals.cpu().tolist() == [[1, 7]]
    assert completed.result_refs.cpu().tolist() == [[11, 10]]
    assert not state.next_pending().exists.cpu().item()
    # 新请求增长目录：旧首次意图、序号和封口保持；原全输入序号无符号比较。
    later = list(inputs)
    later[3] = tensor([1]); later[5] = ExactInt.from_values([(1 << 63)+7])
    state, later_receipt = state.run_registration(*later)
    assert state.capacity == 2 and later_receipt.registered.cpu().item()
    assert state.sealed[:, 0].cpu().tolist() == [True]
    third = list(inputs)
    third[3] = tensor([2]); third[5] = ExactInt.from_values([(1 << 63)+6])
    # 先取得各自实际采样延迟，再令发送时刻对齐同一处理前沿，测试明确标工程控制。
    first_arrival = int(later_receipt.arrived_at.cpu()[0])
    probe, probed = state.run_registration(*third)
    third[6] = tensor([source['sent_at']+first_arrival-int(probed.arrived_at.cpu()[0])])
    state, third_receipt = state.run_registration(*third)
    assert state.capacity == 4 and third_receipt.registered.cpu().item()
    assert state.next_pending().request_key.cpu().tolist() == [2]
    assert value(state.next_pending().input_sequence) == [(1 << 63)+6]
    # 已有源故障不能变成空pending或正常零回执。
    broken = list(inputs); broken[3] = tensor([-1])
    bad, denied = original.run_registration(*broken)
    assert denied.fault.cpu().tolist() == [1] and not bad.occupied.any().cpu().item()
    torch.cuda.synchronize()
    print(json.dumps({'original_request_receipt_and_pending_checked': True,
        'changed_body_retry_keeps_first_terms_and_receipt_checked': True,
        'original_response_after_all_items_checked': True,
        'controlled_same_time_full_u64_input_order_checked': True,
        'physical_growth_keeps_old_request_and_completion_checked': True,
        'elapsed_seconds_with_reference_reads': time.perf_counter()-started,
        'device_peak_allocated_bytes': torch.cuda.max_memory_allocated(),
        'SDK_admission_full_root_and_order_execution_delivered': False}))


if __name__ == '__main__':
    main()
