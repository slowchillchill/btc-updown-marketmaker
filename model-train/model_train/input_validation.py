"""真实 NativeGroup 的输入持有验证；从完整打包独立对照共享路径和延后反传。"""
import copy

import torch
from .graphs import CONFIGURATIONS, TwoGraphs
from .native_inputs import NativeComponentConsumer


def retained_inputs(native_streams):
    shared = [[group.features(shared_books=True) for group in stream] for stream in native_streams]
    plain = [[group.features() for group in stream] for stream in native_streams]
    model = TwoGraphs(CONFIGURATIONS[2])
    other = copy.deepcopy(model)
    results = []
    empty = dict(rows=[], pre_frame_hidden_ops=[], post_frame_hidden_ops=[], cancel=[])
    for streams in (shared, plain):
        streams[1].insert(0, empty)
    for grad_enabled in (True, False):
        consumers = [NativeComponentConsumer(model), NativeComponentConsumer(other)]
        saved = [[], []]
        losses = [None, None]
        handles = []
        for index, graph in enumerate((model, other)):
            def capture(module, args, index=index):
                saved[index].append((tuple(v.detach() for v in args), tuple(v.detach().clone() for v in args)))
            handles.append(graph.state.register_forward_pre_hook(capture))
        with torch.set_grad_enabled(grad_enabled):
            for step in range(max(map(len, shared))):
                for index, streams in enumerate((shared, plain)):
                    groups = [s[step] if step < len(s) else empty for s in streams]
                    groups_out = consumers[index].consume_batch(groups, prepare_parameters=[False] * len(groups))
                    if losses[index] is None and any(len(side) > 1 for g in groups for r in g['rows'] for side in r['book_levels']):
                        z = next(groups_out[g].states[r] for g, group in enumerate(groups)
                                 for r, row in enumerate(group['rows'])
                                 if any(len(side) > 1 for side in row['book_levels']))
                        losses[index] = z[:, 0].sum()
                for key, state in consumers[0].hidden.items():
                    torch.testing.assert_close(state, consumers[1].hidden[key], atol=0, rtol=0)
        for handle in handles:
            handle.remove()
        assert losses[0] is not None and len(saved[0]) > 2
        assert len(saved[0]) == len(saved[1])
        for (actual, frozen), (expected, _) in zip(*saved):
            for value, old, reference in zip(actual, frozen, expected):
                # 对原始位比较，包含有符号零；detach 本身不复制存储。
                assert torch.equal(value.contiguous().view(torch.uint8), old.contiguous().view(torch.uint8))
                assert torch.equal(value.contiguous().view(torch.uint8), reference.contiguous().view(torch.uint8))
        compared = 0
        book_gradients = 0
        if grad_enabled:
            # 两帧以上前向与退役完成后，才对较早帧反传。
            left = torch.autograd.grad(losses[0], tuple(model.parameters()), allow_unused=True)
            right = torch.autograd.grad(losses[1], tuple(other.parameters()), allow_unused=True)
            for (name, _), a, b in zip(model.named_parameters(), left, right):
                assert (a is None) == (b is None)
                if a is not None:
                    torch.testing.assert_close(a, b, atol=0, rtol=0)
                    assert torch.isfinite(a).all()
                    compared += a.numel()
                    if name.startswith('state.book.'):
                        book_gradients += int(a.count_nonzero())
            assert compared > 0
            assert book_gradients > 0
        held_books = {value[2].untyped_storage().data_ptr(): value[2].untyped_storage().nbytes()
                      for value, _ in saved[0]}
        held_masks = {value[3].untyped_storage().data_ptr(): value[3].untyped_storage().nbytes()
                      for value, _ in saved[0]}
        results.append(dict(grad_enabled=grad_enabled, state_calls=len(saved[0]),
                            gradient_scalars=compared, nonzero_book_gradients=book_gradients,
                            held_book_versions=len(held_books), held_book_storage_bytes=sum(held_books.values()),
                            held_mask_versions=len(held_masks), held_mask_storage_bytes=sum(held_masks.values()),
                            retained_inputs_unchanged=True))
    return results
