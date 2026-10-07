"""既有合法Native100ms场景的固定图、资源退出和旧冻结生命周期核对。"""
from dataclasses import fields, is_dataclass, replace
import torch
from prepare import _numbers


def freeze_with_resources(encoder, inputs, *, word_capacity=4):
    """图外协调五个人口和原精确字需求；重试只重算本观察，不重消费来源。"""
    numbers=_numbers(); retries=0
    while True:
        candidate=numbers.device_exact_batch(lambda:encoder.freeze(*inputs),
            word_capacity=word_capacity,device=encoder.shared.slots.device)
        fit=candidate.fits & candidate.value.resources_fit.all()
        if bool(fit.cpu()): return candidate.value.snapshot(),word_capacity,retries
        requests=torch.cat((candidate.required_words.reshape(1),candidate.value.required.max(dim=0).values)).cpu().tolist()
        word_capacity=1<<(max(4,requests[0])-1).bit_length()
        encoder.grow_resources(requests[1:]); retries+=1


def _owned(value, words):
    numbers=_numbers()
    if isinstance(value,numbers.ExactInt):
        value=value.with_capacity(words)
        return numbers.ExactInt(value.words.clone(),value.sign.clone(),value.fault.clone())
    if isinstance(value,torch.Tensor): return value.clone()
    if is_dataclass(value): return replace(value,**{field.name:_owned(getattr(value,field.name),words) for field in fields(value)})
    if value is None: return None
    raise TypeError('固定冻结输入只允许实际记忆与张量')


def _width(value):
    if isinstance(value,_numbers().ExactInt): return value.words.size(1)
    if is_dataclass(value): return max((_width(getattr(value,field.name)) for field in fields(value)),default=4)
    return 4


def _copy(target, source):
    if isinstance(target,torch.Tensor):
        assert target.shape==source.shape and target.dtype==source.dtype
        target.copy_(source)
    elif is_dataclass(target):
        for field in fields(target): _copy(getattr(target,field.name),getattr(source,field.name))
    else: assert target is None and source is None


def _same(left, right):
    if isinstance(left,_numbers().ExactInt):
        width=max(left.words.size(1),right.words.size(1))
        assert torch.equal(left.with_capacity(width).words,right.with_capacity(width).words)
        assert torch.equal(left.sign,right.sign) and torch.equal(left.fault,right.fault)
    elif isinstance(left,torch.Tensor): assert torch.equal(left,right)
    elif is_dataclass(left):
        for field in fields(left): _same(getattr(left,field.name),getattr(right,field.name))
    else: assert left is None and right is None


def _same_freeze(left, right):
    depth=max(left.book_levels.size(2),right.book_levels.size(2))
    def padded(value):
        return replace(value,
            book_levels=torch.nn.functional.pad(value.book_levels,(0,0,0,depth-value.book_levels.size(2))),
            book_mask=torch.nn.functional.pad(value.book_mask,(0,depth-value.book_mask.size(2))))
    _same(padded(left),padded(right))


def validate_freeze_replay(encoder, first_inputs, next_inputs):
    first,first_words,_=freeze_with_resources(encoder,first_inputs)
    following,next_words,_=freeze_with_resources(encoder,next_inputs)
    assert first.role.cpu().tolist()==[1] and following.role.cpu().tolist()==[1]
    assert first.book_mask.any().cpu().item() and following.book_mask.any().cpu().item()
    assert not torch.equal(first.fast,following.fast), '两次重放必须是不同的真实合法观察'
    words=max(first_words,next_words,*(_width(value) for value in (*first_inputs,*next_inputs)))
    inputs=tuple(_owned(value,words) for value in first_inputs)
    following_inputs=tuple(_owned(value,words) for value in next_inputs)
    numbers=_numbers()
    warm=numbers.device_exact_batch(lambda:encoder.freeze(*inputs),word_capacity=words,device='cuda')
    assert (warm.fits & warm.value.resources_fit.all()).cpu().item()
    graph=torch.cuda.CUDAGraph()
    with torch.cuda.graph(graph):
        candidate=numbers.device_exact_batch(lambda:encoder.freeze(*inputs),word_capacity=words,device='cuda')
    graph.replay()
    old=candidate.value.snapshot(); saved=old.snapshot()
    _same_freeze(old,first)
    for target,source in zip(inputs,following_inputs): _copy(target,source)
    graph.replay()
    assert (candidate.fits & candidate.value.resources_fit.all()).cpu().item()
    _same_freeze(candidate.value.snapshot(),following)
    _same(old,saved)
    # 相同未提交观察故意只留一个成员；真实21条BBA窗口产生实际资源退出。
    encoder.packer.capacity=encoder.binance.book.capacity=1
    encoder.events.window.fact_capacity=encoder.events.window.source_capacity=encoder.trades.window.capacity=1
    warm=numbers.device_exact_batch(lambda:encoder.freeze(*inputs),word_capacity=words,device='cuda')
    assert warm.fits.cpu().item() and not warm.value.resources_fit.all().cpu().item()
    insufficient=torch.cuda.CUDAGraph()
    with torch.cuda.graph(insufficient):
        blocked=numbers.device_exact_batch(lambda:encoder.freeze(*inputs),word_capacity=words,device='cuda')
    insufficient.replay()
    assert not blocked.value.resources_fit.all().cpu().item()
    assert not blocked.value.fault.any().cpu().item(), '物理容量退出不成为业务fault'
    for actual,expected in zip(inputs,following_inputs): _same(actual,expected)
    _same(old,saved)
    recovered,_,retries=freeze_with_resources(encoder,inputs,word_capacity=words)
    assert retries>0
    _same_freeze(recovered,following)
    _same(old,saved)
    return {'same_graph_distinct_active_observations':[int(first_inputs[0].cpu()[0]),int(next_inputs[0].cpu()[0])],
        'actual_required_population':recovered.required.cpu().tolist(),'resource_retries':retries,
        'complete_frozen_tensors_Exact_history_endpoint_stable_after_replay_and_growth':True,
        'failed_capacity_keeps_inputs_and_retry_equals_sufficient_candidate':True}
