"""真实 Rust 公共链产物的慢支路/对象消费；不替代完整两图业务验收。"""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

import torch
from model_train.graphs import CONFIGURATIONS, TwoGraphs, reduce_objects
from model_train.native_inputs import NativeComponentConsumer
from model_train.book import pad_books


class NativeInputTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.temp.cleanup)
        root = Path(__file__).resolve().parents[1]
        groups = Path(cls.temp.name) / "groups.json"
        populations = Path(cls.temp.name) / "populations.json"
        overlap = Path(cls.temp.name) / "overlap.json"
        books = Path(cls.temp.name) / "book-groups.json"
        env = dict(os.environ, NATIVE_INPUT_FIXTURE_OUTPUT=str(groups),
                   NATIVE_OBJECT_FIXTURE_OUTPUT=str(populations), NATIVE_OVERLAP_FIXTURE_OUTPUT=str(overlap),
                   NATIVE_BOOK_FIXTURE_OUTPUT=str(books))
        subprocess.run(["cargo", "test", "--manifest-path", str(root / "native-inputs/Cargo.toml"),
                        "--locked", "--offline", "--lib"], env=env, check=True)
        cls.small_groups = json.loads(groups.read_text())
        cls.book_groups = json.loads(books.read_text())
        cls.groups = cls.small_groups + json.loads(overlap.read_text()) + cls.book_groups
        cls.populations = json.loads(populations.read_text())
        cls.evidence = Path(os.environ["NATIVE_VALIDATION_OUTPUT"]) if os.environ.get("NATIVE_VALIDATION_OUTPUT") else None
        if cls.evidence:
            cls.evidence.mkdir(parents=True, exist_ok=True)
            (cls.evidence / "groups.json").write_text(json.dumps(cls.groups))
            (cls.evidence / "populations.json").write_text(populations.read_text())
        cls.graphs = TwoGraphs(CONFIGURATIONS[2]).eval()
        # 原验收权重可由调用方明确传入；默认复用原构造器相同冻结种子。
        if path := os.environ.get("NATIVE_COMPONENT_WEIGHTS"):
            cls.graphs.load_state_dict(torch.load(path, map_location="cpu", weights_only=True))

    def test_real_complete_state_and_frozen_parameter_consumption(self):
        for config in (CONFIGURATIONS[1], CONFIGURATIONS[2]):
            graphs = self.graphs if not config.baseline else TwoGraphs(config).eval()
            consumer = NativeComponentConsumer(graphs)
            expected_hidden = {}
            calls = []
            handle = graphs.state.register_forward_hook(lambda *args: calls.append(1))
            previous_cash = {}
            for group in self.groups:
                # 独立顺序重放全部层状态；同一原生真实特征，不用回调替身。
                oracle = NativeComponentConsumer(graphs)
                oracle.hidden = expected_hidden
                oracle._operations(group["pre_frame_hidden_ops"])
                expected = []
                from model_train.native_inputs import hidden_key
                for row in group["rows"]:
                    key = hidden_key(row["hidden_state_key"])
                    with torch.no_grad():
                        z, h = graphs.state(torch.tensor([row["fast"]]),
                                            torch.tensor([row["slow"]]),
                                            *pad_books([row["book_levels"]], like=torch.empty(0)), oracle.hidden[key])
                    oracle.hidden[key] = h
                    expected.append(z)
                    self.assertEqual(row["fast"][566:], row["rule"])
                    if row["role"] == "DecisionRequired":
                        cash = row["slow"][110]
                        root_key=key[:2]
                        if root_key in previous_cash and cash != previous_cash[root_key]:
                            self.assertAlmostEqual(cash, torch.log1p(torch.tensor(80.)).item(), places=6)
                        previous_cash[root_key] = cash
                oracle._operations(group["post_frame_hidden_ops"])
                expected_hidden = oracle.hidden
                calls.clear()
                with torch.no_grad():
                    result = consumer.consume(group)
                self.assertEqual(len(calls), len(group["rows"]))
                for actual, want in zip(result.states, expected):
                    torch.testing.assert_close(actual, want, rtol=0, atol=0)
                for key, state in expected_hidden.items():
                    torch.testing.assert_close(consumer.hidden[key], state, rtol=0, atol=0)
                saved = {k:v.clone() for k,v in consumer.hidden.items()}
                for i, row in enumerate(group["rows"]):
                    if row["role"] != "DecisionRequired":
                        continue
                    for prefix in row["fixture_prefixes"]:
                        with torch.no_grad():
                            scores = consumer.parameters(result, i, prefix)
                        self.assertTrue(torch.isfinite(scores[0][0]).all())
                self.assertEqual(len(calls), len(group["rows"]))
                for key, h in saved.items():
                    torch.testing.assert_close(consumer.hidden[key], h, rtol=0, atol=0)
            handle.remove()
            self.assertEqual(len(consumer.hidden), 1)  # 下一市场继续，开市不重置。

    def test_real_different_depth_roots_batch_without_reordering_or_extra_state_updates(self):
        graph = self.graphs
        independent = [NativeComponentConsumer(graph), NativeComponentConsumer(graph)]
        batched = NativeComponentConsumer(graph)
        streams = [self.small_groups, self.book_groups]
        absent = dict(rows=[], pre_frame_hidden_ops=[], post_frame_hidden_ops=[], cancel=[])
        frame_counts = [0,0]
        for step in range(max(map(len, streams))):
            groups = [stream[step] if step < len(stream) else absent for stream in streams]
            with torch.no_grad():
                expected = [consumer.consume(group) for consumer,group in zip(independent,groups)]
                actual = batched.consume_batch(groups)
            for index,(reference,value) in enumerate(zip(expected,actual)):
                self.assertEqual(reference.roles,value.roles)
                self.assertEqual(len(value.states),len(groups[index]['rows']))
                frame_counts[index] += len(value.states)
                for z,want in zip(value.states,reference.states):
                    torch.testing.assert_close(z,want,atol=1e-5,rtol=1e-4)
            wanted = {k:v for consumer in independent for k,v in consumer.hidden.items()}
            self.assertEqual(set(batched.hidden),set(wanted))
            for key,state in wanted.items():
                torch.testing.assert_close(batched.hidden[key],state,atol=1e-5,rtol=1e-4)
        self.assertTrue(all(frame_counts))
        self.assertEqual(independent[1].hidden,{})  # 深盘口根经过ContextOnly、DecisionRequired到原Retire。

    @unittest.skipUnless(os.environ.get("NATIVE_MAIN_GRAPH") and os.environ.get("NATIVE_BASELINE_GRAPH"), "requires original frozen graph archive")
    def test_real_native_sequence_in_original_three_runtimes(self):
        import onnxruntime
        import numpy as np
        from model_train.validate_graphs import session, call_ort
        from model_train.graphs import ACTION_INPUTS, encode_all, object_blocks
        from model_train.native_inputs import hidden_key
        root = Path(__file__).resolve().parents[1]
        library = next((Path(onnxruntime.__file__).parent / "capi").glob("libonnxruntime.so.*"))

        def put(records, label, values):
            records[label] = [v.detach().numpy().reshape(-1) if isinstance(v, torch.Tensor)
                              else np.asarray(v).reshape(-1) for v in values]

        def head(prefix):
            return (0, 1, 2, 4, 5, 3, 7)[prefix[7:14].index(1.)]

        def compare(actual, expected):
            self.assertEqual(actual.keys(), expected.keys())
            for label, values in actual.items():
                self.assertEqual(len(values), len(expected[label]))
                for a, b in zip(values, expected[label]):
                    self.assertTrue(np.isfinite(a).all(), label)
                    self.assertTrue(np.isfinite(b).all(), label)
                    np.testing.assert_allclose(a, b, rtol=1e-4, atol=1e-5, err_msg=label)

        for config in (CONFIGURATIONS[1], CONFIGURATIONS[2]):
            archive = Path(os.environ["NATIVE_BASELINE_GRAPH" if config.baseline else "NATIVE_MAIN_GRAPH"])
            graphs = TwoGraphs(config).eval()
            graphs.load_state_dict(torch.load(archive / "initial_weights.pt", map_location="cpu", weights_only=True))
            consumer = NativeComponentConsumer(graphs)
            expected = {}
            frame_outputs = []
            handle = graphs.state.register_forward_hook(lambda module, inputs, output: frame_outputs.append(output))
            with torch.no_grad():
                for g, group in enumerate(self.groups):
                    frame_outputs.clear()
                    result = consumer.consume(group)
                    for r, (row, values) in enumerate(zip(group["rows"], frame_outputs)):
                        label = f"native/{g}/{r}"
                        put(expected, label + "/state", values)
                        if row["role"] != "DecisionRequired":
                            continue
                        count = len(group["cancel"])
                        lengths = [min(256, count - i) for i in range(0, count, 256)] or [0]
                        offset = 0
                        for part, length in enumerate(lengths):
                            put(expected, f"{label}/embedding{part}", [graphs.action.object_encoder(result.cancel_features[:, offset:offset+length])])
                            offset += length
                        put(expected, label + "/reduced", [result.cancel_reduced, result.cancel_exists.float()])
                        before = consumer.hidden[hidden_key(row["hidden_state_key"])].clone()
                        for p, prefix in enumerate(row["fixture_prefixes"]):
                            for part, outputs in enumerate(consumer.parameters(result, r, prefix)):
                                put(expected, f"{label}/prefix{p}/part{part}", [outputs[head(prefix)]])
                        self.assertEqual(len(frame_outputs), len(group["rows"]))
                        torch.testing.assert_close(consumer.hidden[hidden_key(row["hidden_state_key"])], before, rtol=0, atol=0)
                        put(expected, label + "/hidden_after_parameters", [before])
            handle.remove()

            # Python ORT 的状态、z、对象和全局归约全部在本端产生。
            state_session = session(archive / "state_encoder.onnx", 1)
            action = call_ort(session(archive / "action_decoder.onnx", 1), ACTION_INPUTS)
            operations = NativeComponentConsumer(graphs)
            ort_values = {}
            for g, group in enumerate(self.groups):
                operations._operations(group["pre_frame_hidden_ops"])
                states = []
                for r, row in enumerate(group["rows"]):
                    key = hidden_key(row["hidden_state_key"])
                    book, book_mask = pad_books([row["book_levels"]], like=torch.empty(0))
                    z, h = state_session.run(None, {
                        "fast_features":np.asarray([row["fast"]], dtype=np.float32),
                        "slow_features":np.asarray([row["slow"]], dtype=np.float32),
                        "book_levels":book.numpy(), "book_mask":book_mask.numpy(),
                        "h_prev":operations.hidden[key].numpy()})
                    operations.hidden[key] = torch.from_numpy(h)
                    states.append(torch.from_numpy(z))
                    put(ort_values, f"native/{g}/{r}/state", [z, h])
                for r, row in enumerate(group["rows"]):
                    if row["role"] != "DecisionRequired":
                        continue
                    label = f"native/{g}/{r}"
                    z, rule = states[r], torch.tensor([row["rule"]])
                    count = len(group["cancel"])
                    features = torch.tensor(group["cancel"]).reshape(1, count, 184)
                    mask = torch.ones((1, count), dtype=torch.bool)
                    lengths = [min(256, count-i) for i in range(0, count, 256)] or [0]
                    embeddings = []
                    def first_pass(*args):
                        outputs = action(*args)
                        embeddings.append(outputs[6])
                        return outputs
                    reduced, exists = encode_all(first_pass, z, rule, features, mask, lengths, empty=count == 0)
                    for part, (values, length) in enumerate(zip(embeddings, lengths)):
                        put(ort_values, f"{label}/embedding{part}", [values[:, :length]])
                    put(ort_values, label + "/reduced", [reduced, exists.float()])
                    before = operations.hidden[hidden_key(row["hidden_state_key"])].clone()
                    for p, prefix in enumerate(row["fixture_prefixes"]):
                        for part, (f, m, _) in enumerate(object_blocks(features, mask, lengths, empty=count == 0)):
                            if part and head(prefix) != 7:
                                break
                            outputs = action(z, rule, torch.tensor([prefix]), f, m, reduced, exists)
                            put(ort_values, f"{label}/prefix{p}/part{part}", [outputs[head(prefix)]])
                    torch.testing.assert_close(operations.hidden[hidden_key(row["hidden_state_key"])], before, rtol=0, atol=0)
                    put(ort_values, label + "/hidden_after_parameters", [before])
                operations._operations(group["post_frame_hidden_ops"])
            compare(ort_values, expected)
            fixture = Path(self.temp.name) / f"native-{config.name}.json"
            fixture.write_text(json.dumps({"layers":config.layers, "hidden":config.hidden, "groups":self.groups}))
            process = subprocess.run(["cargo", "run", "--manifest-path", str(root / "rust-ort-validation/Cargo.toml"),
                                      "--locked", "--offline", "--", str(library), str(archive), "1", "--native", str(fixture)],
                                     capture_output=True, text=True)
            if self.evidence:
                (self.evidence / f"{config.name}-native.json").write_text(fixture.read_text())
                (self.evidence / f"{config.name}-rust.jsonl").write_text(process.stdout)
                (self.evidence / f"{config.name}-rust.stderr").write_text(process.stderr)
                for name, values in (("pytorch", expected), ("python-ort", ort_values)):
                    (self.evidence / f"{config.name}-{name}.json").write_text(
                        json.dumps({key:[v.tolist() for v in row] for key,row in values.items()}))
            process.check_returncode()
            actual = {item["label"]:item["values"] for item in map(json.loads, process.stdout.splitlines())
                      if item.get("label", "").startswith("native/")}
            compare(actual, expected)

    def test_actual_complete_objects_and_two_257_partitions(self):
        empty = next(group for group in self.groups if not group["cancel"])
        for group in [empty, *self.populations]:
            count = len(group["cancel"])
            self.assertIn(count, (0, 2, 256, 257, 259))
            _, actual_reduced, actual_exists = NativeComponentConsumer(self.graphs).objects(group["cancel"])
            features = torch.tensor(group["cancel"], dtype=torch.float32).reshape(1, count, 184)
            expected = self.graphs.action.object_encoder(features)
            mask = torch.ones((1, count), dtype=torch.bool)
            reduced, exists = reduce_objects([(expected, mask)])
            torch.testing.assert_close(actual_reduced, reduced, rtol=2e-5, atol=2e-6)
            self.assertEqual(actual_exists.item(), exists.item())
            if count == 257:
                _, other_reduced, _ = NativeComponentConsumer(self.graphs).objects(group["cancel"], lengths=[128, 129])
                torch.testing.assert_close(actual_reduced, other_reduced,
                                           rtol=2e-5, atol=2e-6)


if __name__ == "__main__":
    unittest.main()
