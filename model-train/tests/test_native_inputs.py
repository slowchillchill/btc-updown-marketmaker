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


class NativeInputTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.temp.cleanup)
        root = Path(__file__).resolve().parents[1]
        groups = Path(cls.temp.name) / "groups.json"
        populations = Path(cls.temp.name) / "populations.json"
        env = dict(os.environ, NATIVE_INPUT_FIXTURE_OUTPUT=str(groups),
                   NATIVE_OBJECT_FIXTURE_OUTPUT=str(populations))
        subprocess.run(["cargo", "test", "--manifest-path", str(root / "native-inputs/Cargo.toml"),
                        "--locked", "--offline", "--lib"], env=env, check=True)
        cls.groups = json.loads(groups.read_text())
        cls.populations = json.loads(populations.read_text())
        cls.graphs = TwoGraphs(CONFIGURATIONS[2]).eval()
        # 原验收权重可由调用方明确传入；默认复用原构造器相同冻结种子。
        if path := os.environ.get("NATIVE_COMPONENT_WEIGHTS"):
            cls.graphs.load_state_dict(torch.load(path, map_location="cpu", weights_only=True))

    def test_real_slow_branch_and_mechanical_hidden_operations(self):
        consumer = NativeComponentConsumer(self.graphs)
        calls = []
        def advance(row, encoded, previous):
            calls.append(row["hidden_state_key"])
            self.assertEqual(encoded.shape, (1, 128))
            # 状态宿主调用计数探针；不填造836槽或把探针当实际GRU前向。
            return previous + 1
        for group in self.groups:
            result = consumer.consume(group, on_frame=advance)
            for row, actual in zip(group["rows"], result.slow_embeddings):
                expected = self.graphs.state.slow_encoder(torch.tensor([row["slow"]]))
                torch.testing.assert_close(actual, expected, rtol=0, atol=0)
            before = len(calls)
            # 参数消费只读已编码结果，不再次消费组，不触发隐藏推进。
            result.cancel_reduced.sum().item()
            result.cancel_embeddings.sum().item()
            self.assertEqual(len(calls), before)
        self.assertEqual(len(calls), sum(len(group["rows"]) for group in self.groups))
        self.assertEqual(consumer.hidden, {})

    def test_actual_complete_objects_and_two_257_partitions(self):
        empty = next(group for group in self.groups if not group["cancel"])
        for group in [empty, *self.populations]:
            count = len(group["cancel"])
            self.assertIn(count, (0, 2, 256, 257, 259))
            result = NativeComponentConsumer(self.graphs).consume(group)
            features = torch.tensor(group["cancel"], dtype=torch.float32).reshape(1, count, 129)
            expected = self.graphs.action.object_encoder(features)
            torch.testing.assert_close(result.cancel_embeddings, expected, rtol=2e-5, atol=2e-6)
            mask = torch.ones((1, count), dtype=torch.bool)
            reduced, exists = reduce_objects([(expected, mask)])
            torch.testing.assert_close(result.cancel_reduced, reduced, rtol=2e-5, atol=2e-6)
            self.assertEqual(result.cancel_exists.item(), exists.item())
            if count == 257:
                other = NativeComponentConsumer(self.graphs).consume(group, lengths=[128, 129])
                torch.testing.assert_close(result.cancel_reduced, other.cancel_reduced,
                                           rtol=2e-5, atol=2e-6)


if __name__ == "__main__":
    unittest.main()
