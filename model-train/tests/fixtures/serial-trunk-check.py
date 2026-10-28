"""Finite CPU artificial tensors/stubs only. No Policy, real checkpoint, GRU or Adam.step."""
import ast
import copy
import io
import json
import os
from pathlib import Path
import struct
import subprocess
import sys
import tempfile
import time
import types
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'python'))
import torch
from learning_engine import LearningEngine
from model_protocol import read_packet
from performance_capture import (CaptureObserver, CaptureWriter, WorkCounter,
                                 projection_feature_evidence)
from serial_trunk_validation import packet, first_block, count_gates, PipeClient, compare_artifacts

torch.set_num_threads(2)
torch.set_num_interop_threads(1)


class ToyGRU(torch.nn.Module):
    """An explicit small arithmetic stub, deliberately not torch.nn.GRU."""
    def __init__(self):
        super().__init__()
        self.calls = 0
        self.hidden_values = []

    def forward(self, inputs, hidden):
        self.calls += 1
        self.hidden_values.append(hidden.detach().clone())
        result = inputs[..., :2] * 0 + hidden.transpose(0, 1) + 1
        return result, result[:, -1].unsqueeze(0)


class ToyValue(torch.nn.Module):
    def forward(self, latent):
        return latent.sum().reshape(1)


class ToyModel(torch.nn.Module):
    def __init__(self):
        super().__init__()
        self.config = {"layers": 1, "width": 2}
        self.input = torch.nn.Identity()
        self.gru = ToyGRU()
        self.value = ToyValue()
        self.field, self.category, self.kind, self.numeric = (torch.nn.Identity() for _ in range(4))
        self.option, self.bit, self.decoder = (torch.nn.Identity() for _ in range(3))
        self.encodes = 0

    def encode(self, sequences, device):
        self.encodes += 1
        tokens = [token for sequence in sequences for token in sequence]
        self.field(torch.tensor([v[0] for v in tokens], dtype=torch.int64))
        self.category(torch.tensor([v[1] for v in tokens], dtype=torch.int64))
        self.kind(torch.tensor([v[3] for v in tokens], dtype=torch.int64))
        self.numeric(torch.tensor([v[2] for v in tokens], dtype=torch.float32).reshape(-1, 1))
        return torch.tensor([[1., 2.] for _ in sequences], requires_grad=True), 0


class ToyService:
    def __init__(self):
        self.model = ToyModel()
        self.device = torch.device('cpu')
        self.config = {'width': 2, 'layers': 1}
        self.metrics = {'recompute_heads': 0}
        self.performance = {'mode': 'device_v1'}
        self.identity = {'numerical_runtime': {'parameter_dtype': 'torch.float32'}}
        self.accounts = {}
        self.learning = LearningEngine(self)

    def check_cancel(self):
        pass

    def choose(self, *args):
        raise AssertionError('artificial check must not sample')

    begin = decode = choose


def fixture_engine():
    service = ToyService()
    engine = service.learning
    engine.config = {'money_scale_pusd': 1.0, 'epsilon': .2, 'value_weight': .5, 'tbptt_steps': 32}
    engine.weights = {'artificial': .2}
    frames = [{'account': 'artificial', 'call': i + 1,
               'global': [[1, 2, -0., 3]], 'markets': [[[4, 5, 1., 2]]]} for i in range(32)]
    labels = [{'old_joint_logp': -.5, 'advantage': 1., 'critic_target': 3.,
               'full_h_sentinel': {'j_end_exact': '0.123456789012345678901'}} for _ in frames]
    engine.cache = types.SimpleNamespace(schedule=tuple(('artificial', i) for i in range(32)),
                                         labels={'artificial': labels}, frame=lambda key, index: frames[index],
                                         frame_for_teacher=lambda frame: frame, prefetch_block=lambda frames: None)
    calls = []
    def heads(frame, encoded, latent, **kwargs):
        calls.append(frame['call'])
        return latent.sum().double() * 0 - .5, []
    engine._teacher_heads = heads
    engine.epoch = 1
    return service, frames, labels, calls


class Checks(unittest.TestCase):
    def test_actual_serial_forward_does_not_cross_backward_boundary(self):
        service, frames, labels, heads = fixture_engine()
        engine = service.learning
        engine._prepare_device_labels()
        before_backward = torch.Tensor.backward
        def forbidden(*args, **kwargs):
            self.fail('forward-only helper called backward')
        torch.Tensor.backward = forbidden
        try:
            account, following = engine._device_forward(0, 32)
        finally:
            torch.Tensor.backward = before_backward
        self.assertEqual(account, 'artificial')
        self.assertEqual(service.model.encodes, 32)
        self.assertEqual(service.model.gru.calls, 32)
        self.assertEqual(heads, list(range(1, 33)))
        self.assertEqual(engine.counts['artificial'], 32)
        self.assertEqual(len(engine.pending_loss['artificial']), 32)
        self.assertTrue(torch.equal(following, torch.full((1, 1, 2), 32.)))
        self.assertTrue(service.model.training and torch.is_grad_enabled())
        self.assertEqual([float(value[0, 0, 0]) for value in service.model.gru.hidden_values], list(range(32)))
        self.assertIsNotNone(engine.pending_loss['artificial'][-1].grad_fn)
        engine.pending_loss.clear()
        engine.hidden.clear()

    def test_shared_trunk_and_unchanged_loss_expressions(self):
        current = ast.parse((ROOT / 'python/learning_engine.py').read_text())
        original = ast.parse(Path(os.environ['SERIAL_TRUNK_BEFORE']).read_text())
        def method(tree, name):
            cls = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == 'LearningEngine')
            return next(node for node in cls.body if isinstance(node, ast.FunctionDef) and node.name == name)
        old_teacher, trunk = method(original, 'teacher'), method(current, '_teacher_trunk')
        # All actual mathematical trunk statements are reused verbatim as AST, ending at value.
        stop = next(i for i, node in enumerate(old_teacher.body) if isinstance(node, ast.Assign)
                    and isinstance(node.value, ast.Call) and isinstance(node.value.func, ast.Attribute)
                    and node.value.func.attr == '_teacher_heads')
        self.assertEqual([ast.dump(node) for node in old_teacher.body[1:stop]],
                         [ast.dump(node) for node in trunk.body[1:-1]])
        def assigned(tree, name, targets):
            return {node.targets[0].id: ast.dump(node.value) for node in ast.walk(method(tree, name))
                    if isinstance(node, ast.Assign) and isinstance(node.targets[0], ast.Name)
                    and node.targets[0].id in targets}
        targets = {'ratio', 'actor', 'critic', 'loss'}
        self.assertEqual(assigned(original, '_device_segment', targets), assigned(current, '_device_forward', targets))
        forward_source = ast.unparse(method(current, '_device_forward'))
        self.assertNotIn('.backward(', forward_source)
        self.assertNotIn('.step(', forward_source)
        self.assertIn('self._device_forward(start, stop)', ast.unparse(method(current, '_device_segment')))

    def test_projection_alias_one_value_and_clone_two(self):
        projected = torch.tensor([1., -0., 3., 4.], requires_grad=True)
        actual = projected.reshape(1, 1, -1)
        one = projection_feature_evidence(projected, actual)
        two = projection_feature_evidence(projected, actual.clone())
        self.assertTrue(one['same_contiguous_view'])
        self.assertEqual(one['stored_value_objects'], 1)
        self.assertFalse(two['same_contiguous_view'])
        self.assertEqual(two['stored_value_objects'], 2)

    def test_real_observer_hooks_artificial_modules(self):
        service, frames, labels, _ = fixture_engine()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            writer = CaptureWriter(torch, root / 'captures', 1 << 20)
            counter = WorkCounter(root, time.monotonic() + 10, 32, 0, 'artificial', time.monotonic())
            observer = CaptureObserver(torch, service, writer, counter, 'L65')
            observer.observe_projection_features = True
            try:
                observer.begin('learn/1/L65/000')
                observer.active_frame = frames[0]
                service.learning.teacher(frames[0], service.learning.zeros())
                self.assertIn(('projection_output', 'activation'), observer.values)
                self.assertNotIn(('gru_features', 'activation'), observer.values)
                observer.finish()
                self.assertEqual(counter.counts['main_gru_completed'], 1)
                evidence = json.loads((root / 'captures/projection-features.jsonl').read_text())
                self.assertTrue(evidence['same_contiguous_view'])
                self.assertEqual(evidence['call'], 1)
                self.assertTrue(evidence['train'] and evidence['grad_enabled'])
            finally:
                observer.close()
                writer.close()
                counter.close()

    def test_packet_real_decoder_preserves_bits_and_json_precision(self):
        label = {'negative_zero': -0., 'exponent': 1.2345678901234567e-123, 'text': 'a\\b"\n'}
        request = {'phase': 'learn_cache_frame', 'account': 'artificial', 'call': 1,
                   'global': [[4294967295, 3000000000, -0., 255]], 'markets': [[], [[1, 2, 1.25, 3]]],
                   'label': label}
        restored = read_packet(io.BytesIO(packet(request)))['batch'][0]
        self.assertEqual(restored['label'], label)
        self.assertEqual(bytes(restored['global'].fields), struct.pack('<I', 4294967295))
        self.assertEqual(bytes(restored['global'].numeric), struct.pack('<f', -0.))
        self.assertEqual(bytes(restored['markets'][1].numeric), struct.pack('<f', 1.25))
        with self.assertRaises(EOFError):
            read_packet(io.BytesIO(packet(request)[:-1]))

    def test_prefix_only_and_wrong_identity(self):
        import hashlib
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'artificial.jsonl'
            records = [{'ordinal': i, 'expected_call': i + 1, 'frame': {'account': 'x', 'call': i + 1},
                        'label': {'full_H': 'not_recomputed', 'v': 1.2345678901234567}} for i in range(33)]
            raw = ''.join(json.dumps(row) + '\n' for row in records).encode()
            path.write_bytes(raw)
            binding = {'path': str(path), 'max_bytes': len(raw), 'sha256': hashlib.sha256(raw).hexdigest()}
            selected = first_block(binding)
            self.assertEqual(len(selected), 32)
            self.assertEqual(selected[-1]['label'], records[31]['label'])
            records[3]['frame']['call'] = 9
            raw = ''.join(json.dumps(row) + '\n' for row in records).encode()
            path.write_bytes(raw)
            binding.update(max_bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest())
            with self.assertRaises(ValueError):
                first_block(binding)

    def test_pipe_uses_production_decoder_and_waits(self):
        script = ('import sys,json\nfrom model_protocol import read_packet\n'
                  'r=read_packet(sys.stdin.buffer)\n'
                  'print(json.dumps({"batch":[{"phase":r["batch"][0]["phase"],"ok":True}]}),flush=True)\n')
        env = dict(os.environ, PYTHONPATH=str(ROOT / 'python'))
        process = subprocess.Popen([sys.executable, '-c', script], env=env, stdin=subprocess.PIPE,
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            client = PipeClient(process, time.monotonic() + 5, 1 << 20)
            result = client.request({'phase': 'artificial'})
            self.assertEqual(result, {'phase': 'artificial', 'ok': True})
            process.stdin.close()
            self.assertEqual(process.wait(timeout=5), 0)
        finally:
            if process.poll() is None:
                process.kill()
                process.wait()
            process.stdout.close()
            process.stderr.close()

    def test_counts_and_missing_capture_cannot_pass(self):
        counts = {key: 1 for key in ('bounded_drivers', 'policy_constructions', 'model_loads', 'formal_learn_open',
                                     'checkpoint_state_deserializations', 'service_model_deserializations')}
        counts.update(main_gru_frames=32, main_gru_calls=32, main_gru_completed=32)
        self.assertTrue(all(count_gates(counts).values()))
        counts['actual_adam'] = 1
        self.assertFalse(count_gates(counts)['actual_adam'])
        reference = {'driver': {'variant': 'A'}, 'finish': {'counts': counts}, 'initial': {}, 'records': {}}
        candidate = copy.deepcopy(reference)
        candidate['driver']['variant'] = 'B-serial-trunk'
        with self.assertRaises(ValueError):
            compare_artifacts(torch, reference, candidate)


class FullCorrectnessChecks(unittest.TestCase):
    def test_real_frame_columns_and_prefetch_with_cpu_cuda_stubs(self):
        import contextlib
        import input_cache as cache_module
        from input_cache import GroupInputCache, TokenSequence
        events, facts = [], []
        class Stream:
            def wait_stream(self, other): events.append('wait_stream')
            def wait_event(self, other): events.append('wait_event')
        compute = Stream()
        class Event:
            def record(self, stream): events.append('record_event')
            def synchronize(self): events.append('host_completion')
        cuda = types.SimpleNamespace(Stream=lambda **kwargs: Stream(), Event=Event,
                                     current_stream=lambda *args: compute,
                                     stream=lambda stream: contextlib.nullcontext())
        class TorchStub:
            def __getattr__(self, name):
                return cuda if name == 'cuda' else getattr(torch, name)
            @staticmethod
            def empty(*args, **kwargs):
                kwargs.pop('device', None)
                kwargs.pop('pin_memory', None)
                return torch.empty(*args, **kwargs)
        def frame(call, value):
            return {'account': 'L', 'call': call,
                    'global': TokenSequence([[4294967295, 3, value, 1], [4, 5, -0., 2]]),
                    'markets': [TokenSequence([[6, 7, 1.25, 3]])]}
        first, second = frame(1, 2.), frame(2, 9.)
        service = types.SimpleNamespace(device=types.SimpleNamespace(type='cuda'), metrics={})
        cache = GroupInputCache(service)
        cache.cpu_pool = types.SimpleNamespace(allocated=0, payload=78)
        cache.streaming_capacity = 16
        cache.buffer_observer = lambda record: facts.append(record)
        original_torch, original_record_stream = cache_module.torch, torch.Tensor.record_stream
        cache_module.torch = TorchStub()
        torch.Tensor.record_stream = lambda tensor, stream: events.append('record_stream')
        try:
            held = cache.frame_for_teacher(first)
            columns = [part.columns(service.device) for part in [held['global'], *held['markets']]]
            old_numeric = columns[0][2].clone()
            charged = service.metrics['h2d_bytes']
            self.assertTrue(charged > 0)
            self.assertEqual(sum(part[4] for part in columns), 0)
            self.assertEqual(charged, sum(row['bytes'] for row in facts if row['event'] == 'h2d_submitted'))
            self.assertEqual(columns[0][0].tolist(), [4294967295, 4])
            self.assertEqual(bytes(columns[0][2][1].view(torch.uint8)), struct.pack('<f', -0.))
            cache.prefetch_block([second])
            with self.assertRaises(RuntimeError):
                cache.prefetch_block([second])
            following = cache.frame_for_teacher(second)
            self.assertEqual(following['global'].columns(service.device)[2][0].item(), 9.)
            self.assertTrue(torch.equal(old_numeric, held['global'].columns(service.device)[2]))
            self.assertIsNone(cache.prefetched)
            self.assertEqual(len(cache.streaming_slots), 2)
            self.assertNotEqual(cache.streaming_slots[0]['raw'].untyped_storage().data_ptr(),
                                cache.streaming_slots[1]['raw'].untyped_storage().data_ptr())
            self.assertTrue(any(row['event'] == 'h2d_submitted' and row['prefetch'] for row in facts))
            self.assertTrue(any(row['event'] == 'owned_copy_submitted' and row['prefetch'] for row in facts))
            self.assertIn('host_completion', events)
            self.assertIn('wait_event', events)
            self.assertEqual(service.metrics['h2d_bytes'], sum(row['bytes'] for row in facts if row['event'] == 'h2d_submitted'))
            self.assertTrue(cache.owned_live_bytes > 0)
            cache.release_account_columns('L')
            self.assertEqual(cache.owned_live_bytes, 0)
        finally:
            torch.Tensor.record_stream = original_record_stream
            cache_module.torch = original_torch

    def test_two_epochs_tbptt_tails_and_stop_call_order_with_stubs(self):
        service, _, _, _ = fixture_engine()
        engine = service.learning
        engine.epoch = 0
        accounts = ['L', 'F-N', 'F-P', 'F-R', 'F-S']
        frames = {key: [{'account': key, 'call': i + 1, 'global': [[1, 2, 0., 1]],
                         'markets': [[[3, 4, 1., 2]]]} for i in range(65 if key == 'L' else 1)] for key in accounts}
        labels = {key: [{'old_joint_logp': -.5, 'advantage': 1., 'critic_target': 3.} for _ in values]
                  for key, values in frames.items()}
        schedule = tuple((key, i) for key in accounts for i in range(len(frames[key])))
        released, backwards, steps = [], [], []
        engine.cache = types.SimpleNamespace(schedule=schedule, labels=labels,
                         frame=lambda key, index: frames[key][index], frame_for_teacher=lambda frame: frame,
                         prefetch_block=lambda frames: None, release_account_columns=lambda key: released.append(key))
        service.device = types.SimpleNamespace(type='cuda')  # only selects control flow; ToyModel ignores device
        engine.zeros = lambda: torch.zeros(1, 1, 2)
        engine.weights = {key: .2 for key in accounts}
        engine._prepare_device_labels = lambda: setattr(engine, 'device_labels', torch.tensor(
            [[labels[key][index]['old_joint_logp'], labels[key][index]['advantage']]
             for key, index in schedule], dtype=torch.float64))
        service.model.token = torch.nn.Identity()
        service.model.option_bias = torch.nn.Parameter(torch.zeros(1))
        engine.optimizer = types.SimpleNamespace(zero_grad=lambda **kwargs: None,
                                                 step=lambda: steps.append(engine.epoch))
        original_backward = torch.Tensor.backward
        torch.Tensor.backward = lambda tensor, *args, **kwargs: backwards.append((engine.epoch, dict(engine.counts)))
        try:
            engine.run_device_epoch()
            engine.run_device_epoch()
            self.assertEqual(steps, [1, 2])
            self.assertEqual(len(backwards), 14)
            self.assertEqual(released, ['L', 'L', 'L', 'F-N', 'F-P', 'F-R', 'F-S'] * 2)
            self.assertEqual(backwards[0][1], {'L': 32})
            self.assertEqual(backwards[1][1], {'L': 64})
            self.assertEqual(service.model.gru.calls, 138)
            # Own-baseline stop occurs only after the original first backward and pending clear.
            backwards.clear()
            def stop():
                if engine.epoch == 1 and engine.counts.get('L') == 32 and not engine.pending_loss['L']:
                    raise InterruptedError('artificial safe-point stop')
            engine.check_cancel = stop
            engine.epoch = 0
            with self.assertRaises(InterruptedError):
                engine.run_device_epoch()
            self.assertEqual(len(backwards), 1)
            self.assertEqual(steps, [1, 2])
        finally:
            torch.Tensor.backward = original_backward

    def test_full_observer_learning_only_and_all_phase_completed_counter(self):
        service, frames, _, _ = fixture_engine()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            writer = CaptureWriter(torch, root / 'captures', 1 << 20)
            counter = WorkCounter(root, time.monotonic() + 10, 32, 0, 'artificial-all-phase', time.monotonic())
            observer = CaptureObserver(torch, service, writer, counter, 'L65')
            observer.observe_projection_features = True
            observer.step_keys = {('artificial', 1): 'L65/000'}
            try:
                observer.begin('collect/L65/000')
                service.learning.teacher(frames[0], service.learning.zeros())
                self.assertNotIn(('projection_output', 'activation'), observer.values)
                observer.finish()
                self.assertFalse((root / 'captures/projection-features.jsonl').exists())
                observer.begin('learn/1/L65/000')
                observer.active_frame = frames[0]
                service.learning.teacher(frames[0], service.learning.zeros())
                self.assertIn(('projection_output', 'activation'), observer.values)
                observer.finish()
                self.assertEqual(counter.counts['main_gru_completed'], 2)
                self.assertEqual(counter.counts['main_gru_calls_completed'], 2)
                row = json.loads((root / 'captures/projection-features.jsonl').read_text())
                self.assertEqual((row['epoch'], row['sample']), (1, 'L65/000'))
            finally:
                observer.close()
                writer.close()
                counter.close()

    def test_actual_constructor_deserialization_instrumentation_with_plain_stubs(self):
        from performance_validation import LoadObservations, full_work_counts
        calls = []
        fake_torch = types.SimpleNamespace(load=lambda path, **kwargs: calls.append(str(path)) or {'plain': True})
        fake_module = types.SimpleNamespace(Policy=lambda config: {'artificial_config': config})
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            counter = WorkCounter(root, time.monotonic() + 10, 272, 2, 'artificial-loads', time.monotonic(),
                                  additional_limits=full_work_counts(1))
            model, checkpoint = root / 'package', root / 'checkpoint.pt'
            observer = LoadObservations(fake_torch, fake_module, model, 'C65', counter)
            observer.install()
            try:
                fake_torch.load(model)
                fake_module.Policy({'not_a_production_Policy': True})
                self.assertEqual(counter.counts['consumer_seq3_deserializations_completed'], 1)
                observer.phase = 'L65'
                fake_torch.load(model)
                observer.checkpoint_path = checkpoint
                fake_torch.load(checkpoint)
                self.assertEqual(counter.counts['learner_bootstrap_deserializations_completed'], 1)
                self.assertEqual(counter.counts['checkpoint_state_deserializations_completed'], 1)
                self.assertEqual(len(calls), 3)
                with self.assertRaises(RuntimeError):
                    fake_torch.load(root / 'unbound')
            finally:
                observer.close()
                counter.close()
        normal, stop, cold = full_work_counts(1), full_work_counts(10), full_work_counts(12)
        self.assertEqual((normal['model_loads'], normal['production_backward']), (2, 14))
        self.assertEqual((stop['actual_adam'], stop['production_backward']), (0, 1))
        self.assertEqual((cold['model_loads'], cold['checkpoint_state_deserializations']), (3, 2))

    def test_projection_sidecar_semantics_and_missing_coordinate(self):
        from analytic_moment_compare import projection_semantics, learning_projection_records, NotApplicable
        layout = {'shape': [2], 'stride': [1], 'dtype': 'torch.float32', 'device': 'cuda:0',
                  'storage_pointer': 123, 'storage_offset': 0, 'numel': 2, 'contiguous': True, 'ordinary_tensor': True}
        row = {'account': 'L', 'call': 1, 'batch': 0, 'time': 0, 'epoch': 1, 'phase': 'L65',
               'sample': 'L65/000', 'same_contiguous_view': True, 'stored_value_objects': 1,
               'projection': layout, 'gru_features': dict(layout, shape=[1, 1, 2], stride=[2, 2, 1]),
               'train': True, 'grad_enabled': True,
               'precision': {'parameter_dtype': 'torch.float32', 'torch_intra_threads': 4}}
        other = copy.deepcopy(row)
        other['projection']['storage_pointer'] = 999
        other['precision']['torch_intra_threads'] = 2
        self.assertEqual(projection_semantics(row), projection_semantics(other))
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'captures').mkdir()
            path = root / 'captures/projection-features.jsonl'
            path.write_text(json.dumps(row) + '\n')
            slot = types.SimpleNamespace(slot=1, workers={'L65': {'directory': root,
                      'identity': {'service_identity': {'config': {'width': 2}}}}},
                      index={'records': {'learn/1/L65/000': []}}, cost={'additional_metadata_bytes_read': 0},
                      learning_coordinates={'L65/000': ('L', 1)},
                      learning_observed_schema={'learn/1/L65/000': {'fields': {'projection_output'},
                                                                  'kinds': {'projection_output': 'activation'}}})
            self.assertEqual(set(learning_projection_records(slot)), {'learn/1/L65/000'})
            slot.learning_observed_schema.clear()
            with self.assertRaises(NotApplicable):
                learning_projection_records(slot)

    def test_exact_source_contract_closure_and_wrong_ast_rejected(self):
        import hashlib
        from analytic_moment_compare import read_contract, NotApplicable
        path = Path(os.environ['FULL_CORRECTNESS_CONTRACT'])
        contract, binding = read_contract(path)
        self.assertEqual(binding['comparison_contract_sha256'], hashlib.sha256(path.read_bytes()).hexdigest())
        self.assertEqual(set(binding), {'comparison_contract_sha256', 'method', 'source_constraints_sha256'})
        constraints = json.loads(Path(contract['source_constraints']['path']).read_text())
        constraints['functions']['observer.performance_validation.py']['serve'] = '0' * 64
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            wrong = root / 'wrong-constraints.json'
            wrong.write_text(json.dumps(constraints))
            changed = copy.deepcopy(contract)
            changed['source_constraints'] = {'path': str(wrong), 'max_bytes': wrong.stat().st_size,
                                             'sha256': hashlib.sha256(wrong.read_bytes()).hexdigest()}
            changed_path = root / 'wrong-contract.json'
            changed_path.write_text(json.dumps(changed))
            with self.assertRaises(NotApplicable):
                read_contract(changed_path)

    def test_projection_normal_stop_prefix_normal_and_missing_rejected(self):
        from analytic_moment_compare import compare_learning_projection, NotApplicable
        layout = {'shape': [2], 'stride': [1], 'dtype': 'torch.float32', 'device': 'cuda:0',
                  'storage_pointer': 123, 'storage_offset': 0, 'numel': 2,
                  'contiguous': True, 'ordinary_tensor': True}
        coordinates = [('L65/%03d' % i, 'L', i + 1) for i in range(65)]
        coordinates += [(name, name, 1) for name in ('F-N', 'F-P', 'F-R', 'F-S')]
        normal = []
        for epoch in (1, 2):
            for sample, account, call in coordinates:
                normal.append({'account': account, 'call': call, 'batch': 0, 'time': 0,
                    'epoch': epoch, 'phase': 'L65', 'sample': sample, 'same_contiguous_view': True,
                    'stored_value_objects': 1, 'projection': layout,
                    'gru_features': dict(layout, shape=[1, 1, 2], stride=[2, 2, 1]),
                    'train': True, 'grad_enabled': True, 'precision': {'parameter_dtype': 'torch.float32'}})
        def key(row): return 'learn/%d/%s' % (row['epoch'], row['sample'])
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            def slot(name, number, rows, loaded):
                here = root / name
                (here / 'captures').mkdir(parents=True)
                path = here / 'captures/projection-features.jsonl'
                path.write_text(''.join(json.dumps(row) + '\n' for row in rows))
                return types.SimpleNamespace(slot=number,
                    workers={'L65': {'directory': here, 'identity': {'service_identity': {'config': {'width': 2}}}}},
                    index={'records': {key(row): [] for row in rows}}, cost={'additional_metadata_bytes_read': 0},
                    learning_coordinates={sample: (account, call) for sample, account, call in coordinates},
                    learning_observed_schema={key(row): {'fields': {'projection_output'},
                                              'kinds': {'projection_output': 'activation'}} for row in loaded})
            # Precisely reproduce the missing interface: full138 index/sidecar,
            # but only first32 schemas were loaded by original normal->stop gates.
            reference = slot('normal-prefix-loaded', 1, normal, normal[:32])
            stop = slot('stop', 9, normal[:32], normal[:32])
            report = {'comparisons': [], 'scope_exclusions': []}
            compare_learning_projection(torch, reference, stop, report)
            self.assertTrue(report['comparisons'] and all(row['pass'] for row in report['comparisons']))
            self.assertFalse(any('L65/032' in row['path'] or '/learn/2/' in row['path'] for row in report['comparisons']))
            second_stop = slot('second-stop', 10, normal[:32], normal[:32])
            report = {'comparisons': [], 'scope_exclusions': []}
            compare_learning_projection(torch, stop, second_stop, report)
            self.assertTrue(all(row['pass'] for row in report['comparisons']))
            full_left = slot('full-left', 1, normal, normal)
            full_right = slot('full-right', 6, normal, normal)
            report = {'comparisons': [], 'scope_exclusions': []}
            compare_learning_projection(torch, full_left, full_right, report)
            self.assertTrue(all(row['pass'] for row in report['comparisons']))
            self.assertTrue(any('/learn/2/F-S/' in row['path'] for row in report['comparisons']))
            path = reference.workers['L65']['directory'] / 'captures/projection-features.jsonl'
            path.write_text(''.join(json.dumps(row) + '\n' for row in normal[1:]))
            with self.assertRaises(NotApplicable):
                compare_learning_projection(torch, reference, stop, {'comparisons': [], 'scope_exclusions': []})
            wrong = copy.deepcopy(normal)
            wrong[0]['call'] = 999
            path.write_text(''.join(json.dumps(row) + '\n' for row in wrong))
            with self.assertRaises(NotApplicable):
                compare_learning_projection(torch, reference, stop, {'comparisons': [], 'scope_exclusions': []})


if __name__ == '__main__':
    unittest.main(verbosity=2)
