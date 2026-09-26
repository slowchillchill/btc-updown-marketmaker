"""有限真实连续组的长寿命三端宿主；此模块不定义市场或账户语义。"""
import time
from pathlib import Path
import torch
from .graphs import TwoGraphs, CONFIGURATIONS, STATE_INPUTS, ACTION_INPUTS
from .native_inputs import NativeComponentConsumer
from .validate_graphs import session, call_ort, compare
from .actor import conditional_choice
from .actor_validation import compare_actor_steps


class ContinuousPython:
    def __init__(self, directory, threads):
        started = time.perf_counter()
        directory = Path(directory)
        torch.set_num_threads(threads)
        torch.set_num_interop_threads(1)
        self.config = next(c for c in CONFIGURATIONS if c.name == directory.name)
        self.model = TwoGraphs(self.config).eval()
        self.model.load_state_dict(torch.load(directory / 'initial_weights.pt', weights_only=True))
        self.state_forward = self.model.state.forward
        self.action_forward = self.model.action.forward
        self.object_forward = self.model.action.object_encoder.forward
        self.state_session = session(directory / 'state_encoder.onnx', threads)
        self.action_session = session(directory / 'action_decoder.onnx', threads)
        self.ort_state = call_ort(self.state_session, STATE_INPUTS)
        self.ort_action = call_ort(self.action_session, ACTION_INPUTS)
        self.consumers = [NativeComponentConsumer(self.model), NativeComponentConsumer(self.model)]
        self.hidden_operations = [0, 0]
        self.parameter_gru_calls = [0, 0]
        self.population_auditing = False
        for index, consumer in enumerate(self.consumers):
            original = consumer._operations
            def operations(values, *, original=original, index=index):
                self.hidden_operations[index] += len(values)
                return original(values)
            consumer._operations = operations
        self.loading_seconds = time.perf_counter() - started
        self.state_batches = {}
        self.action_batches = {}
        self.durations = {name: [] for name in ('state_torch', 'state_ort', 'action_torch', 'action_ort', 'support_torch', 'support_ort', 'objects_torch', 'objects_ort')}
        self.comparisons = {}
        self.keep_population = False
        self.population_groups = []
        self.provenance = self._provenance()
        from .validate_graphs import sha
        self.provenance['graphs'] = {str(directory / name): sha(directory / name) for name in ('initial_weights.pt', 'state_encoder.onnx', 'state_encoder.onnx.data', 'action_decoder.onnx', 'action_decoder.onnx.data')}
        self.provenance['sources'] = {str(path): sha(path) for path in sorted(Path(__file__).parent.glob('*.py'))}
        self.provenance['native_extension_sha256'] = sha(self.provenance['native_module'])
        self.provenance['input_contract_version'] = 'native_inputs_v5'
        self.provenance['feature_schema_sha256'] = sha(Path(__file__).resolve().parents[1] / 'native-inputs' / 'feature_schema.json')
        self.provenance['threads'] = threads
        self.provenance['ort_sessions'] = {
            'providers': [self.state_session.get_providers(), self.action_session.get_providers()],
            'optimization': 'ALL', 'execution': 'SEQUENTIAL', 'intra_threads': threads,
            'inter_threads': 1, 'memory_pattern': False, 'spinning': False, 'cpu_arena': True,
            'native_settings': 'same explicit builder settings; CPU only',
        }
        self.provenance['input_parameters'] = {'generator': 'random.Random', 'seed': '742000+j', 'field': 'initial public book bid/ask depth', 'inclusive_range': [100, 200], 'order': 'Trade, Orphan, Rebate; then independent root index; repeat/config/thread reuse'}

    def _objects_ort(self, features):
        batch, slots, _ = features.shape
        hidden = self.config.hidden
        if self.population_auditing:
            self.population_report['calls'].append({'runtime': 1, 'phase': 'object_encoding', 'batch': batch,
                'slots': slots, 'valid_rows': [slots] * batch})
        return self.ort_action(torch.zeros(batch, hidden), torch.zeros(batch, 30), torch.zeros(batch, 60),
            features, torch.ones(batch, slots, dtype=torch.bool), torch.zeros(batch, 3 * hidden // 2),
            torch.zeros(batch, dtype=torch.bool), torch.zeros(batch, 9999))[6]

    def advance(self, native_groups, autonomous):
        selected = [autonomous] * len(native_groups) if isinstance(autonomous, bool) else autonomous
        assert len(selected) == len(native_groups)
        features = [group.features() for group in native_groups]
        traces = [[], []]
        state_inputs = [[], []]
        finished_actors = []
        step_heads = []
        results = []
        self.population_groups.clear()
        for runtime, consumer in enumerate(self.consumers):
            label = 'torch' if runtime == 0 else 'ort'
            def state(*args):
                state_inputs[runtime].append(tuple(value.detach() for value in args))
                started = time.perf_counter()
                output = (self.state_forward if runtime == 0 else self.ort_state)(*args)
                self.durations['state_' + label].append(time.perf_counter() - started)
                batch = args[0].shape[0]
                self.state_batches[batch] = self.state_batches.get(batch, 0) + 1
                traces[runtime].append(tuple(value.detach().clone() for value in output))
                return output
            def action(*args):
                started = time.perf_counter()
                output = (self.action_forward if runtime == 0 else self.ort_action)(*args)
                self.durations['action_' + label].append(time.perf_counter() - started)
                batch = args[0].shape[0]
                self.action_batches[batch] = self.action_batches.get(batch, 0) + 1
                return output
            def objects(*args):
                started = time.perf_counter()
                output = (self.object_forward if runtime == 0 else self._objects_ort)(*args)
                self.durations['objects_' + label].append(time.perf_counter() - started)
                return output
            self.model.state.forward = state
            self.model.action.forward = action
            self.model.action.object_encoder.forward = objects
            with torch.no_grad():
                action_calls_before = len(self.durations['action_' + label])
                object_calls_before = len(self.durations['objects_' + label])
                groups = consumer.consume_batch(features, prepare_parameters=selected)
                if not any(selected[i] and any(row['role'] == 'DecisionRequired' for row in group['rows']) for i, group in enumerate(features)):
                    assert len(self.durations['action_' + label]) == action_calls_before
                    assert len(self.durations['objects_' + label]) == object_calls_before
                if self.keep_population: self.population_groups.append(groups)
                frozen = {k: v.clone() for k, v in consumer.hidden.items()}
                state_count = len(traces[runtime])
                actors = [(index, row, native_groups[index].actor(row))
                          for index, group in enumerate(groups) for row, role in enumerate(group.roles)
                          if role == 'DecisionRequired' and selected[index]]
                records = [[] for _ in actors]
                heads = [[] for _ in actors]
                while any(not actor.complete() for _, _, actor in actors):
                    active = [i for i, (_, _, actor) in enumerate(actors) if not actor.complete()]
                    entries = [(groups[actors[i][0]], actors[i][1], actors[i][2]) for i in active]
                    supports = []
                    for _, _, actor in entries:
                        started = time.perf_counter()
                        supports.append(actor.support())
                        self.durations['support_' + label].append(time.perf_counter() - started)
                    outputs = consumer.parameters_batch(entries)
                    for i, blocks, support in zip(active, outputs, supports):
                        actor = actors[i][2]
                        head = actor.head_index()
                        scores = torch.cat([block[head][0] for block in blocks])[:len(support)] if head == 7 else blocks[0][head][0]
                        choice, _, _ = conditional_choice(scores, support, deterministic=True)
                        heads[i].append(head)
                        records[i].append((support, actor.validation_state(), actor.prefix_features(), scores.tolist()))
                        actor.choose(choice)
                self.parameter_gru_calls[runtime] += len(traces[runtime]) - state_count
                assert len(traces[runtime]) == state_count, 'parameter graph advanced GRU'
                for key, value in frozen.items():
                    assert torch.equal(value, consumer.hidden[key]), 'parameter step mutated hidden'
                results.append([(index, row, actor.path(), record) for (index, row, actor), record in zip(actors, records)])
                finished_actors.append(actors)
                step_heads.append(heads)
        self.model.state.forward = self.state_forward
        self.model.action.forward = self.action_forward
        self.model.action.object_encoder.forward = self.object_forward
        try:
            assert len(traces[0]) == len(traces[1])
            for frame, (reference, actual) in enumerate(zip(traces[0], traces[1])):
                compare(reference, actual, {'frame': frame}, self.comparisons)
            assert len(results[0]) == len(results[1])
            for left, right in zip(results[0], results[1]):
                assert left[:2] == right[:2], 'actor root/row mismatch'
                compare_actor_steps(left[2], left[3], right[2], right[3],
                    {'root': left[0], 'row': left[1], 'cutoff': features[left[0]]['cutoff'],
                     'runtime': 'PythonORT'}, self.comparisons)
        except AssertionError as error:
            # 首错沿原观察产物目录保存当前帧；不提交失败意图或推进后继。
            import json, os
            destination = Path(os.environ['CONTINUOUS_OUTPUT']) / 'failed-comparison'
            destination.mkdir(parents=True, exist_ok=True)
            detail = {'error': str(error), 'features': features, 'results': results,
                'step_heads': step_heads,
                'cancel_candidates': [group.validation_cancel_objects() for group in native_groups],
                'intents': [[actor.validation_intent() for _, _, actor in actors] for actors in finished_actors],
                'provenance': self.provenance}
            (destination / 'frame.json').write_text(json.dumps(detail, indent=2) + '\n')
            torch.save({'state_inputs': state_inputs, 'state_outputs': traces,
                        'hidden': [consumer.hidden for consumer in self.consumers]}, destination / 'state.pt')
            raise
        return [(z.flatten().tolist(), hidden.flatten().tolist()) for z, hidden in traces[0]], results[0]

    def _provenance(self):
        import os
        import sys
        import importlib.metadata
        from .validate_graphs import sha
        import model_native_inputs
        import onnxruntime
        maps = Path('/proc/self/maps').read_text().splitlines()
        libraries = sorted({line.split()[-1] for line in maps if 'libonnxruntime.so' in line})
        return {'mapped_ort': {path: sha(path) for path in libraries},
                'native_module': model_native_inputs.__file__, 'python_module': __file__,
                'python': sys.version, 'pid': os.getpid(),
                'versions': {name: importlib.metadata.version(name) for name in ('torch', 'onnxruntime', 'numpy')}}

    def release_observation(self):
        # 宿主已停止这一观察根，释放模型持有的引用；不伪造业务 Retire。
        self.population_groups.clear()
        before = [len(c.hidden) for c in self.consumers]
        for consumer in self.consumers:
            consumer.hidden.clear()
        return before

    def sample(self):
        import ctypes
        class Mallinfo2(ctypes.Structure):
            _fields_ = [(name, ctypes.c_size_t) for name in ('arena', 'ordblks', 'smblks', 'hblks', 'hblkhd', 'usmblks', 'fsmblks', 'uordblks', 'fordblks', 'keepcost')]
        libc = ctypes.CDLL(None)
        allocator = None
        if hasattr(libc, 'mallinfo2'):
            libc.mallinfo2.restype = Mallinfo2
            data = libc.mallinfo2()
            allocator = {name: getattr(data, name) for name, _ in data._fields_}
        status = Path('/proc/self/status').read_text().splitlines()
        rss = next(int(line.split()[1]) * 1024 for line in status if line.startswith('VmRSS:'))
        return {'rss_bytes': rss, 'allocator_mallinfo2': allocator,
                'parameter_gru_calls': self.parameter_gru_calls.copy(),
                'hidden_operations_since_statistics': self.hidden_operations.copy(),
                'object_encoding_calls_since_statistics': {k: len(v) for k, v in self.durations.items() if k.startswith('objects_')},
                'state_calls_since_statistics': {k: len(v) for k, v in self.durations.items() if k.startswith('state_')},
                'action_block_calls_since_statistics': {k: len(v) for k, v in self.durations.items() if k.startswith('action_')},
                'state_batches_since_statistics': self.state_batches.copy(), 'action_batches_since_statistics': self.action_batches.copy(),
                'hidden_rows': [len(c.hidden) for c in self.consumers],
                'hidden_elements': [sum(v.numel() for v in c.hidden.values()) for c in self.consumers]}

    def statistics(self, reset=False):
        import numpy as np
        result = {name: {'count': len(values), 'median_seconds': float(np.median(values)),
                  'p95_seconds': float(np.quantile(values, .95)), 'max_seconds': max(values)}
                  for name, values in self.durations.items() if values}
        result.update(state_batches=self.state_batches.copy(), action_batches=self.action_batches.copy(), comparisons=self.comparisons.copy(), hidden_operations=self.hidden_operations.copy())
        if reset:
            self.hidden_operations[:] = [0, 0]
            for values in self.durations.values(): values.clear()
            self.state_batches.clear()
            self.action_batches.clear()
            self.comparisons.clear()
        return result

    def audit_population(self, native_groups, seed):
        from dataclasses import replace
        from .actor import consume_actor
        from .actor_validation import assert_close
        state_calls_before = [len(self.durations['state_' + label]) for label in ('torch', 'ort')]
        frozen_hidden = [{key: value.clone() for key, value in consumer.hidden.items()} for consumer in self.consumers]
        def forbidden_state(*args): raise AssertionError('P parameter path invoked state graph')
        self.model.state.forward = forbidden_state
        records = []
        self.population_report = {'calls': [], 'active_original_rows': [], 'layouts': []}
        self.population_auditing = True
        runtime_records = []
        for runtime, consumer in enumerate(self.consumers):
            def measured_action(*args):
                self.population_report['calls'].append({'runtime': runtime, 'batch': args[0].shape[0],
                    'slots': args[3].shape[1], 'valid_rows': args[4].sum(dim=1).tolist()})
                return (self.action_forward if runtime == 0 else self.ort_action)(*args)
            self.model.action.forward = measured_action
            self.model.action.object_encoder.forward = self.object_forward if runtime == 0 else self._objects_ort
            local = []
            groups = self.population_groups[runtime]
            entries = [(g, next(i for i, role in enumerate(g.roles) if role == 'DecisionRequired'), n)
                       for g, n in zip(groups, native_groups)]
            mixed = [(g, row, n.actor(row)) for g, row, n in entries]
            for root, (g, row, native) in enumerate(entries):
                count = len(native.features()['cancel'])
                if count == 0:
                    assert not g.cancel_exists.item() and torch.count_nonzero(g.cancel_reduced).item() == 0
                layouts = ([256, 1], [128, 129]) if count == 257 else ([min(256, count), count - 256] if count > 256 else [count],)
                for family in (0, 3, 4, 5):
                    actor = native.actor(row)
                    if not actor.support()[family]: continue
                    actor.choose(family)
                    if actor.complete():
                        assert family == 0
                        continue
                    variants = []
                    for lengths in layouts:
                        self.population_report['layouts'].append({'runtime': runtime, 'root': root, 'row': row, 'family': family, 'valid_lengths': lengths})
                        with torch.no_grad():
                            features, reduced, exists = consumer.objects(native.features()['cancel'], lengths=lengths)
                            frozen = replace(g, cancel_features=features, cancel_reduced=reduced, cancel_exists=exists)
                            blocks = consumer.parameters(frozen, row, actor.prefix_features(), lengths=lengths)
                            scores = torch.cat([b[7][0, :length] for b, length in zip(blocks, lengths)])
                            variants.append(scores)
                    support = actor.support()
                    legal = torch.tensor(support, dtype=torch.bool)
                    for actual in variants[1:]: assert_close(variants[0][legal], actual[legal], rtol=1e-4, atol=1e-5)
                    tied, _, _ = conditional_choice(torch.zeros_like(variants[0]), support, deterministic=True)
                    assert tied == next(i for i, legal in enumerate(support) if legal)
                    choice = variants[0].masked_fill(~torch.tensor(support), -torch.inf).argmax().item()
                    actor.choose(choice)
                    local.append((root, family, variants[0].tolist(), support, actor.path()))
                # 原生真实行同时包含空人口和非空人口；对完整输出按有效长度核对独立B1。
            with torch.no_grad():
                batched = consumer.parameters_batch(mixed)
                for (g, row, actor), output in zip(mixed, batched):
                    single = consumer.parameters_batch([(g, row, actor)])[0]
                    assert len(output) >= len(single)
                    head = actor.head_index()
                    legal = torch.tensor(actor.support(), dtype=torch.bool)
                    assert_close(output[0][head][0][legal], single[0][head][0][legal], rtol=1e-4, atol=1e-5)
            if len(mixed) == 2:
                # P7定向完成行探针：原B2族步骤后，空人口行完成，另一原身份独立续走。
                mixed[0][2].choose(0)
                mixed[1][2].choose(4)
                active = [i for i, (_, _, actor) in enumerate(mixed) if not actor.complete()]
                self.population_report['active_original_rows'].append({'runtime': runtime, 'steps': [[0,1], active, []]})
                assert active == [1]
                completed_prefix = mixed[0][2].path()
                with torch.no_grad():
                    outputs = consumer.parameters_batch([mixed[i] for i in active])
                    actor = mixed[1][2]
                    scores = torch.cat([block[7][0] for block in outputs[0]])[:len(actor.support())]
                    expected = next(record for record in local if record[0] == 1 and record[1] == 4)
                    legal = torch.tensor(actor.support(), dtype=torch.bool)
                    assert_close(scores[legal], scores.new_tensor(expected[2])[legal], rtol=1e-4, atol=1e-5)
                    actor.choose(expected[4][-1])
                assert mixed[0][2].path() == completed_prefix and all(actor.complete() for _, _, actor in mixed)
                def forbidden(*args): raise AssertionError('empty active set invoked parameter graph')
                self.model.action.forward = forbidden
                assert consumer.parameters_batch([]) == []
                self.model.action.forward = self.action_forward if runtime == 0 else self.ort_action
            runtime_records.append(local)
        for a, b in zip(*runtime_records):
            assert a[:2] == b[:2] and a[3] == b[3]
            legal = torch.tensor(a[3], dtype=torch.bool)
            assert_close(torch.tensor(a[2])[legal], torch.tensor(b[2])[legal], rtol=1e-4, atol=1e-5)
            assert a[4] == b[4]
        self.model.action.forward = self.action_forward
        self.model.action.object_encoder.forward = self.object_forward
        # 同一当前真实257人口，重新编码两种分块，完整条件路径采样再重算联合log_prob/梯度。
        torch.manual_seed(seed)
        for g, native in zip(self.population_groups[0], native_groups):
            if len(native.features()['cancel']) != 257: continue
            row = next(i for i, role in enumerate(g.roles) if role == 'DecisionRequired')
            trained = []
            for lengths in ([256, 1], [128, 129]):
                features, reduced, exists = self.consumers[0].objects(native.features()['cancel'], lengths=lengths)
                trained.append(replace(g, cancel_features=features, cancel_reduced=reduced, cancel_exists=exists))
            a = native.actor(row); a.choose(4)
            sampled = consume_actor(self.consumers[0], trained[0], row, a)
            b = native.actor(row); b.choose(4)
            replay = consume_actor(self.consumers[0], trained[1], row, b, replay=a.path()[1:])
            assert_close(sampled.log_prob, replay.log_prob, rtol=1e-4, atol=1e-5)
            params = tuple(self.model.parameters())
            left = torch.autograd.grad(sampled.log_prob, params, allow_unused=True)
            right = torch.autograd.grad(replay.log_prob, params, allow_unused=True)
            for a, b in zip(left, right):
                assert (a is None) == (b is None)
                if a is not None: assert_close(a, b, rtol=1e-4, atol=1e-5)
            singleton = native.actor(row); singleton.choose(5)
            before = torch.random.get_rng_state().clone()
            result = consume_actor(self.consumers[0], trained[0], row, singleton)
            assert result.log_prob.item() == 0 and torch.equal(before, torch.random.get_rng_state())
        assert [len(self.durations['state_' + label]) for label in ('torch', 'ort')] == state_calls_before
        for consumer, frozen in zip(self.consumers, frozen_hidden):
            assert consumer.hidden.keys() == frozen.keys()
            for key, value in frozen.items(): assert torch.equal(consumer.hidden[key], value)
        self.model.state.forward = self.state_forward
        self.population_auditing = False
        self.population_report['parameter_gru_calls'] = [0, 0]
        return runtime_records[0]
