"""有限真实原生组的三端证据入口；不启动训练或长期运行。"""
import torch
from .graphs import TwoGraphs, CONFIGURATIONS, ACTION_INPUTS, STATE_INPUTS
from .native_inputs import NativeComponentConsumer
from .actor import consume_actor, conditional_choice
from .validate_graphs import call_ort, session, compare


def assert_close(reference, actual, *, rtol=1e-4, atol=1e-5):
    """沿原数值口径拒绝任一端非有限；精确重算另保留零容差。"""
    assert reference.shape == actual.shape
    if reference.numel():
        compare((reference.detach().reshape(-1),), (actual.detach().reshape(-1),), {}, {})
    if rtol == atol == 0:
        torch.testing.assert_close(reference, actual, rtol=0, atol=0)


def compare_actor_steps(reference_path, reference_steps, actual_path, actual_steps, context, stats):
    """逐步核对真实支持/前缀、当前可选分数，然后才核对严格选择。"""
    for step, (reference, actual) in enumerate(zip(reference_steps, actual_steps)):
        label = {**context, 'step': step}
        assert reference[:3] == actual[:3], {**label, 'failure': 'support/state/prefix mismatch',
                                            'reference': reference[:3], 'actual': actual[:3]}
        mask = torch.tensor(reference[0], dtype=torch.bool)
        left, right = torch.tensor(reference[3]), torch.tensor(actual[3])
        assert left.shape == right.shape == mask.shape, label
        try:
            compare((left[mask],), (right[mask],), label, stats)
        except AssertionError as error:
            detail = error.args[0]
            detail['original_head_index'] = int(mask.nonzero().flatten()[detail['index'][0]])
            detail['index_space'] = 'supported scores; original_head_index names the full head coordinate'
            raise
        a, b = reference_path[step], actual_path[step]
        assert a == b, {**label, 'failure': 'strict choice mismatch', 'reference_choice': a,
                       'actual_choice': b, 'reference_scores_at_choices': [float(left[a]), float(left[b])],
                       'actual_scores_at_choices': [float(right[a]), float(right[b])]}
    assert len(reference_steps) == len(actual_steps) == len(reference_path) == len(actual_path), context


def _continuation(model, consumer, ort_consumer, state_forward, action_forward,
                  object_forward, ort_state, ort_action, config):
    """保留两端已递推隐藏状态与会话；执行后只追加真实后继组。"""
    def advance(native_group):
        traces = [[], []]
        def state_call(call, index):
            def run(*args):
                z, hidden = call(*args)
                traces[index].append((z.flatten().tolist(), hidden.flatten().tolist()))
                return z, hidden
            return run
        def ort_objects(features):
            batch, slots, _ = features.shape
            return ort_action(torch.zeros(batch, config.hidden), torch.zeros(batch, 30), torch.zeros(batch, 60),
                              features, torch.ones(batch, slots, dtype=torch.bool), torch.zeros(batch, 3 * config.hidden // 2),
                              torch.zeros(batch, dtype=torch.bool), torch.zeros(batch, 9999))[6]
        results = []
        with torch.no_grad():
            for index, current in enumerate((consumer, ort_consumer)):
                model.state.forward = state_call(state_forward if index == 0 else ort_state, index)
                model.action.forward = action_forward if index == 0 else ort_action
                model.action.object_encoder.forward = object_forward if index == 0 else ort_objects
                group = current.consume(native_group.features(shared_books=True))
                frozen = {key: value.clone() for key, value in current.hidden.items()}
                state_calls = len(traces[index])
                decisions = []
                for row, role in enumerate(group.roles):
                    if role != 'DecisionRequired':
                        continue
                    actor = native_group.actor(row)
                    result = consume_actor(current, group, row, actor, deterministic=True)
                    probe = native_group.actor(row)
                    steps = []
                    for choice in result.actor.path():
                        outputs = current.parameters(group, row, probe.prefix_features(), price_fee_features=probe.price_fee_features())
                        head = probe.head_index()
                        scores = (torch.cat([block[head][0] for block in outputs])[:len(probe.support())]
                                  if head == 7 else outputs[0][head][0])
                        steps.append((probe.support(), probe.validation_state(), probe.prefix_features(), scores.tolist()))
                        probe.choose(choice)
                    decisions.append((row, actor.path(), steps))
                assert len(traces[index]) == state_calls
                for key, value in frozen.items():
                    assert_close(value, current.hidden[key], rtol=0, atol=0)
                results.append(decisions)
            assert len(traces[0]) == len(traces[1])
            for left, right in zip(traces[0], traces[1]):
                for a, b in zip(left, right):
                    assert_close(torch.tensor(a), torch.tensor(b))
            assert len(results[0]) == len(results[1])
            for left, right in zip(results[0], results[1]):
                assert left[0] == right[0]
                compare_actor_steps(left[1], left[2], right[1], right[2], {'row': left[0]}, {})
        model.state.forward = state_forward
        model.action.forward = action_forward
        model.action.object_encoder.forward = object_forward
        return traces[0], results[0]
    return advance


def _scores(consumer, group, actor):
    outputs = consumer.parameters(group, 0, actor.prefix_features(),
                                  price_fee_features=actor.price_fee_features())
    head = actor.head_index()
    if head == 7:
        return torch.cat([block[head][0] for block in outputs])[:len(actor.support())]
    return outputs[0][head][0]


def validate_python(native_group, history, directory, replay, market_probe=True):
    """每端独立递推；定向探针与实际贪心路径分别保留。"""
    torch.set_num_threads(1)
    config = next(c for c in CONFIGURATIONS if c.name == directory.name)
    model = TwoGraphs(config).eval()
    model.load_state_dict(torch.load(directory / 'initial_weights.pt', weights_only=True))
    traces = []
    original_state = model.state.forward

    def traced_state(*args):
        z, hidden = original_state(*args)
        traces.append((z.flatten().tolist(), hidden.flatten().tolist()))
        return z, hidden

    model.state.forward = traced_state
    consumer = NativeComponentConsumer(model)
    with torch.no_grad():
        for prior in history:
            consumer.consume(prior)
        group = consumer.consume(native_group.features(shared_books=True))
        frozen = {key: value.clone() for key, value in consumer.hidden.items()}
        state_calls = len(traces)
        actor = native_group.actor(0)
        result = consume_actor(consumer, group, 0, actor, deterministic=True)
        same = consume_actor(consumer, group, 0, actor.restart(), replay=actor.path())
        assert_close(result.log_prob, same.log_prob, rtol=0, atol=0)

        def records(current_consumer, current_group, path):
            probe = native_group.actor(0)
            values = []
            for choice in path:
                support = probe.support()
                logits = _scores(current_consumer, current_group, probe)
                values.append((support, probe.validation_state(), probe.prefix_features(), logits.tolist()))
                if sum(support) == 1:
                    rng = torch.random.get_rng_state().clone()
                    only, logp, entropy = conditional_choice(logits, support)
                    assert only == choice and float(logp) == float(entropy) == 0
                    assert torch.equal(rng, torch.random.get_rng_state())
                probe.choose(choice)
            assert probe.complete()
            return values

        actual_records = records(consumer, group, actor.path())
        probe_records = records(consumer, group, replay)
        # 同一真实冻结组的Family(不适用)、A/B Price(不同tick)混合批次。
        probe = native_group.actor(0)
        fee_cases = []
        for choice in replay:
            if not fee_cases or probe.head_index() == 3:
                fee_cases.append((probe.prefix_features(), probe.price_fee_features(), probe.support()))
            probe.choose(choice)
        assert len(fee_cases) == 3
        assert not any(fee_cases[0][1])
        if market_probe:
            assert any(fee_cases[1][1]) and any(fee_cases[2][1])
            assert fee_cases[1][1] != fee_cases[2][1]
        else:
            assert not any(fee_cases[1][1]) and not any(fee_cases[2][1])
        mixed_args = (group.states[0].expand(3, -1), group.rules[0].expand(3, -1),
                      torch.tensor([case[0] for case in fee_cases]), torch.zeros(3, 1, 184),
                      torch.zeros(3, 1, dtype=torch.bool), torch.zeros(3, 3 * config.hidden // 2),
                      torch.zeros(3, dtype=torch.bool), torch.tensor([case[1] for case in fee_cases]))
        mixed_scores = model.action(*mixed_args)[3]
        assert len(traces) == state_calls
        for key, value in frozen.items():
            assert_close(consumer.hidden[key], value, rtol=0, atol=0)

        ort_action = call_ort(session(directory / 'action_decoder.onnx', 1), ACTION_INPUTS)
        ort_state = call_ort(session(directory / 'state_encoder.onnx', 1), STATE_INPUTS)
        mixed_ort = ort_action(*mixed_args)[3]
        assert_close(mixed_scores, mixed_ort, rtol=1e-4, atol=1e-5)
        for index in (1, 2):
            support = torch.tensor(fee_cases[index][2])
            assert mixed_scores[index].masked_fill(~support, -torch.inf).argmax() == mixed_ort[index].masked_fill(~support, -torch.inf).argmax()
        original_action = model.action.forward
        model.action.forward = lambda *args: ort_action(*args)
        ort_traces = []

        def traced_ort_state(*args):
            z, hidden = ort_state(*args)
            ort_traces.append((z.flatten().tolist(), hidden.flatten().tolist()))
            return z, hidden

        model.state.forward = traced_ort_state
        ort_consumer = NativeComponentConsumer(model)
        for prior in history:
            ort_consumer.consume(prior)
        ort_group = ort_consumer.consume(native_group.features(shared_books=True))
        assert len(traces) == len(ort_traces)
        for expected, actual in zip(traces, ort_traces):
            for a, b in zip(expected, actual):
                assert_close(torch.tensor(a), torch.tensor(b), rtol=1e-4, atol=1e-5)
        ort_frozen = {key: value.clone() for key, value in ort_consumer.hidden.items()}
        actual = consume_actor(ort_consumer, ort_group, 0, native_group.actor(0), deterministic=True)
        compare_actor_steps(actor.path(), actual_records, actual.actor.path(),
                            records(ort_consumer, ort_group, actual.actor.path()), {'row': 0}, {})
        for path, expected in [(actor.path(), actual_records), (replay, probe_records)]:
            for want, got in zip(expected, records(ort_consumer, ort_group, path)):
                assert want[:3] == got[:3]
                assert_close(torch.tensor(want[2]), torch.tensor(got[2]), rtol=0, atol=0)
                mask = torch.tensor(want[0], dtype=torch.bool)
                assert_close(torch.tensor(want[3])[mask], torch.tensor(got[3])[mask], rtol=1e-4, atol=1e-5)
        assert len(ort_traces) == state_calls
        for key, value in ort_frozen.items():
            assert_close(ort_consumer.hidden[key], value, rtol=0, atol=0)
        model.action.forward = original_action
        model.state.forward = original_state
    # 同一真实冻结上下文上的采样与原路径重算，保留全部梯度。
    torch.manual_seed(742001)
    sampled = consume_actor(consumer, group, 0, native_group.actor(0))
    recomputed = consume_actor(consumer, group, 0, native_group.actor(0), replay=sampled.actor.path())
    assert_close(sampled.log_prob, recomputed.log_prob, rtol=0, atol=0)
    parameters = tuple(model.action.parameters())
    left = torch.autograd.grad(sampled.log_prob, parameters, allow_unused=True)
    right = torch.autograd.grad(recomputed.log_prob, parameters, allow_unused=True)
    assert any(value is not None and torch.count_nonzero(value) for value in left)
    for a, b in zip(left, right):
        assert (a is None) == (b is None)
        if a is not None:
            assert_close(a, b, rtol=0, atol=0)
    continuation = _continuation(model, consumer, ort_consumer, original_state, original_action,
                                 model.action.object_encoder.forward, ort_state, ort_action, config)
    return traces, actor.path(), actual_records, probe_records, mixed_scores.tolist(), config.layers, config.hidden, continuation


def main():
    import argparse, hashlib, json, os, subprocess, sys, sysconfig
    from pathlib import Path
    import onnxruntime
    parser=argparse.ArgumentParser()
    parser.add_argument('--graphs',type=Path,required=True)
    parser.add_argument('--old-graphs',type=Path,help='可选历史权重，仅记录SHA；v5重初始化不要求跨版本逐值一致')
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--configs',nargs='+',default=[c.name for c in CONFIGURATIONS])
    args=parser.parse_args();args.output.mkdir(parents=True,exist_ok=True)
    root=Path(__file__).resolve().parents[1]
    library=Path(onnxruntime.__file__).parent/'capi/libonnxruntime.so.1.28.0'
    def sha(path):
        with path.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
    evidence={'scope':'finite actual actor; full C/P remains separate','ort_library_sha256':sha(library),'configurations':{}}
    for config in CONFIGURATIONS:
        current=args.graphs/config.name/'initial_weights.pt'
        new=torch.load(current,weights_only=True,map_location='cpu')
        initialized=TwoGraphs(config).state_dict()
        assert set(new)==set(initialized)
        assert all(torch.equal(value,new[name]) for name,value in initialized.items())
        evidence['configurations'][config.name]={'seed':config.seed,
            'input_contract_version':'native_inputs_v5',
            'feature_schema_sha256':sha(root/'native-inputs/feature_schema.json'),
            'same_seed_initialized_parameters':len(new),'new_weights_sha256':sha(current)}
        if args.old_graphs:
            previous=args.old_graphs/config.name/'initial_weights.pt'
            if not previous.exists():previous=args.old_graphs/'resumed'/config.name/'initial_weights.pt'
            evidence['configurations'][config.name]['historical_weights_sha256']=sha(previous)
            evidence['configurations'][config.name]['cross_version_equality']='不适用：v5盘口前端与市场宽度改变同种子初始化消耗'
        del initialized,new
    for name in args.configs:
        env=os.environ.copy()
        env.update(PYTHONHOME=sys.base_prefix,PYO3_PYTHON=sys.executable,
            PYTHONPATH=os.pathsep.join([str(root),sysconfig.get_path('purelib')]),
            LD_LIBRARY_PATH=os.pathsep.join([str(Path(sys.base_prefix)/'lib'),env.get('LD_LIBRARY_PATH','')]),
            ORT_DYLIB_PATH=str(library),ACTOR_GRAPH_DIRECTORY=str(args.graphs/name))
        command=['cargo','test','--manifest-path',str(root/'native-inputs/Cargo.toml'),'--features','python,ort','--locked','--offline','--lib','same_process','--','--ignored','--nocapture','--test-threads=1']
        log=args.output/(name+'.log')
        with log.open('w') as f:result=subprocess.run(command,env=env,stdout=f,stderr=subprocess.STDOUT)
        evidence['configurations'][name]['exit_code']=result.returncode
        evidence['configurations'][name]['log_sha256']=sha(log)
        (args.output/'report.json').write_text(json.dumps(evidence,indent=2))
        if result.returncode:raise RuntimeError(f'{name} failed; see {log}')
    (args.output/'report.json').write_text(json.dumps(evidence,indent=2))



def validate_cancel(native_group, history, directory):
    """真实259对象人口的三撤单族；尾块分数不能省略。"""
    torch.set_num_threads(1)
    config = next(c for c in CONFIGURATIONS if c.name == directory.name)
    model = TwoGraphs(config).eval()
    model.load_state_dict(torch.load(directory / 'initial_weights.pt', weights_only=True))
    def mixed_blocks(consumer, group, *, gradients=False):
        actors = [native_group.actor(0) for _ in range(3)]
        actors[0].choose(3)
        actors[2].choose(4)
        calls = []
        hook = model.action.register_forward_hook(
            lambda module, inputs, outputs: calls.append(tuple(inputs[4].sum(dim=1).tolist())))
        outputs = consumer.parameters_batch([(group, 0, actor) for actor in actors])
        hook.remove()
        assert calls == [(256, 256, 256), (3, 3)]
        assert list(map(len, outputs)) == [2, 1, 2]
        batched_loss = single_loss = 0
        for actor, blocks in zip(actors, outputs):
            actual = (torch.cat([block[7][0] for block in blocks])[:259]
                      if actor.head_index() == 7 else blocks[0][actor.head_index()][0])
            single = _scores(consumer, group, actor)
            assert_close(actual, single, rtol=1e-4, atol=1e-5)
            if gradients:
                batched_loss = batched_loss + actual.square().mean()
                single_loss = single_loss + single.square().mean()
        assert consumer.parameters_batch([]) == []
        if gradients:
            parameters = tuple(model.parameters())
            batched = torch.autograd.grad(batched_loss, parameters, allow_unused=True, retain_graph=True)
            singles = torch.autograd.grad(single_loss, parameters, allow_unused=True, retain_graph=True)
            for left, right in zip(batched, singles):
                assert (left is None) == (right is None)
                if left is not None:
                    assert_close(left, right, rtol=1e-4, atol=1e-5)

    consumer = NativeComponentConsumer(model)
    traces = []
    forward = model.state.forward
    def traced(*args):
        z, h = forward(*args)
        traces.append((z.flatten().tolist(), h.flatten().tolist()))
        return z, h
    model.state.forward = traced
    with torch.no_grad():
        for prior in history:
            consumer.consume(prior)
        group = consumer.consume(native_group.features(shared_books=True))
        mixed_blocks(consumer, group)
        result = consume_actor(consumer, group, 0, native_group.actor(0), deterministic=True)
        paths = [result.actor.path()]
        for family in (3, 4, 5):
            actor = native_group.actor(0)
            actor.choose(family)
            choice = _scores(consumer, group, actor).masked_fill(~torch.tensor(actor.support()), -torch.inf).argmax().item()
            paths.append([family, choice])
        records = []
        for path in paths:
            actor = native_group.actor(0)
            steps = []
            for choice in path:
                steps.append((actor.support(), actor.validation_state(), actor.prefix_features(), _scores(consumer, group, actor).tolist()))
                actor.choose(choice)
            records.append(steps)
        ort_action = call_ort(session(directory / 'action_decoder.onnx', 1), ACTION_INPUTS)
        ort_state = call_ort(session(directory / 'state_encoder.onnx', 1), STATE_INPUTS)
        ort_traces = []
        def traced_ort(*args):
            z, h = ort_state(*args)
            ort_traces.append((z.flatten().tolist(), h.flatten().tolist()))
            return z, h
        model.state.forward = traced_ort
        model.action.forward = lambda *args: ort_action(*args)
        original_encoder = model.action.object_encoder.forward
        # 第一遍嵌入同样由ORT图计算，不能暗中复用PyTorch归约输入。
        def ort_objects(features):
            batch, slots, _ = features.shape
            return ort_action(torch.zeros(batch, config.hidden), torch.zeros(batch, 30), torch.zeros(batch, 60),
                              features, torch.ones(batch, slots, dtype=torch.bool), torch.zeros(batch, 3 * config.hidden // 2),
                              torch.zeros(batch, dtype=torch.bool), torch.zeros(batch, 9999))[6]
        model.action.object_encoder.forward = ort_objects
        ort_consumer = NativeComponentConsumer(model)
        for prior in history:
            ort_consumer.consume(prior)
        ort_group = ort_consumer.consume(native_group.features(shared_books=True))
        mixed_blocks(ort_consumer, ort_group)
        for left, right in zip(traces, ort_traces):
            for a, b in zip(left, right):
                assert_close(torch.tensor(a), torch.tensor(b), rtol=1e-4, atol=1e-5)
        actual = consume_actor(ort_consumer, ort_group, 0, native_group.actor(0), deterministic=True)
        for path_index, (path, steps) in enumerate(zip(paths, records)):
            actor = native_group.actor(0)
            for choice, expected in zip(path, steps):
                assert (actor.support(), actor.validation_state(), actor.prefix_features()) == expected[:3]
                assert_close(torch.tensor(expected[2]), torch.tensor(actor.prefix_features()), rtol=0, atol=0)
                scores = _scores(ort_consumer, ort_group, actor)
                mask = torch.tensor(actor.support(), dtype=torch.bool)
                assert_close(torch.tensor(expected[3])[mask], scores[mask], rtol=1e-4, atol=1e-5)
                if path_index == 0:
                    assert actual.actor.path()[len(actor.path())] == choice
                if actor.head_index() == 7:
                    assert scores.numel() == len(native_group.features()['cancel'])
                    assert scores.masked_fill(~torch.tensor(actor.support()), -torch.inf).argmax().item() == choice
                actor.choose(choice)
        assert len(traces) == len(ort_traces)
    model.state.forward = forward
    # 删除实例forward覆盖，恢复nn.Module原forward绑定。
    del model.action.forward
    model.action.object_encoder.forward = original_encoder
    training = NativeComponentConsumer(model)
    for prior in history:
        training.consume(prior)
    current = training.consume(native_group.features(shared_books=True))
    mixed_blocks(training, current, gradients=True)
    # 真实Market对象头只有一项，完整consume_actor路径不耗随机数。
    singleton = native_group.actor(0)
    singleton.choose(5)
    rng = torch.random.get_rng_state().clone()
    only = consume_actor(training, current, 0, singleton)
    assert float(only.log_prob.detach()) == float(only.conditional_entropy_sum.detach()) == 0
    assert torch.equal(rng, torch.random.get_rng_state())
    # 真实Paired全人口条件分布采样，重算保留编码器与全局归约梯度。
    torch.manual_seed(742006)
    sampled_actor = native_group.actor(0)
    sampled_actor.choose(4)
    sampled = consume_actor(training, current, 0, sampled_actor)
    replay_actor = native_group.actor(0)
    replay_actor.choose(4)
    replayed = consume_actor(training, current, 0, replay_actor, replay=sampled_actor.path()[1:])
    assert_close(sampled.log_prob, replayed.log_prob, rtol=0, atol=0)
    parameters = tuple(model.parameters())
    left = torch.autograd.grad(sampled.log_prob, parameters, allow_unused=True, retain_graph=True)
    right = torch.autograd.grad(replayed.log_prob, parameters, allow_unused=True)
    for a, b in zip(left, right):
        assert (a is None) == (b is None)
        if a is not None:
            assert_close(a, b, rtol=0, atol=0)
    gradients = dict(zip((name for name, _ in model.named_parameters()), left))
    assert any(value is not None and torch.count_nonzero(value) for name, value in gradients.items() if 'object_encoder' in name)
    continuation = _continuation(model, consumer, ort_consumer, forward, model.action.forward,
                                 original_encoder, ort_state, ort_action, config)
    return traces, paths, records, config.layers, config.hidden, continuation


if __name__ == "__main__":
    main()
