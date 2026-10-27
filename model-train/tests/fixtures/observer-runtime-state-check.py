"""Finite CPU-only observation fixtures; no Policy, forward, Adam.step or CUDA access."""
import argparse
import ast
import copy
import hashlib
import json
import os
from pathlib import Path
import sys
import time
from types import SimpleNamespace
import unittest

PARSER = argparse.ArgumentParser()
PARSER.add_argument("--evidence-root", required=True, type=Path)
ARGS = PARSER.parse_args()
ROOT = ARGS.evidence_root.resolve()
ROOT.mkdir(parents=True, exist_ok=False)
PYTHON = Path(__file__).resolve().parents[2] / "python"
sys.path.insert(0, str(PYTHON))

import torch
import performance_capture as capture
import performance_validation as validation

torch.set_num_threads(2)
torch.set_num_interop_threads(1)
EVENTS = []


class TinyModule(torch.nn.Module):
    def __init__(self):
        super().__init__()
        self.p = torch.nn.Parameter(torch.tensor([-99.0, -99.0]))
        self.q = torch.nn.Parameter(torch.tensor([4.0]))
        self.p_alias = self.p
        self.register_buffer("buffer", torch.tensor([7], dtype=torch.int64))
        self.q.grad = None
        self.p.grad = torch.zeros_like(self.p)

    def forward(self, *args, **kwargs):
        raise AssertionError("fixture must not call forward")


class FakeOptimizer:
    step_calls = 0

    def __init__(self, model):
        self.param_groups = [{"params": [model.q, model.p]}]
        self.defaults = {"lr": 0.003, "betas": (0.9, 0.999), "eps": 1e-8,
                         "weight_decay": 0, "amsgrad": False, "maximize": False,
                         "foreach": None, "fused": None}
        self.identifiers = [0, 1]
        self.state = {}

    def load_artificial_state(self):
        self.identifiers = [73, 11]
        self.state = {73: {"exp_avg": torch.tensor([3.0]), "step": torch.tensor(4.0)}}

    def state_dict(self):
        return {"state": self.state,
                "param_groups": [{**self.defaults, "params": list(self.identifiers)}]}

    def step(self, *args, **kwargs):
        type(self).step_calls += 1
        raise AssertionError("fixture must not call any optimizer step")


class FakeCuda:
    def __init__(self, initialized):
        self.initialized = initialized
        self.calls = []
        self.state = torch.tensor([4, 5, 6], dtype=torch.uint8)

    def is_initialized(self):
        self.calls.append("is_initialized")
        return self.initialized

    def current_device(self):
        self.calls.append("current_device")
        return 0

    def get_rng_state(self, device):
        self.calls.append(["get_rng_state", device])
        if not self.initialized:
            raise AssertionError("an unused CUDA generator must not be initialized")
        return self.state


class TorchFacade:
    def __init__(self, initialized=False):
        self.cuda = FakeCuda(initialized)

    def __getattr__(self, name):
        return getattr(torch, name)


class FakeGenerator:
    def __init__(self):
        self.state = torch.tensor([1, 2, 3], dtype=torch.uint8)

    def get_state(self):
        return self.state


class RecordingWriter(capture.CaptureWriter):
    def write_initial_observation(self, snapshot, metadata):
        EVENTS.append("record")
        return super().write_initial_observation(snapshot, metadata)


def make_service(device=None):
    model = TinyModule()
    engine = SimpleNamespace(optimizer=None, consumed={}, sequence=7, group={"id": "artificial-group"})
    return SimpleNamespace(model=model, learning=engine, device=device or torch.device("cpu"),
                           accounts={}, identity={"checkpoint_sha256": "artificial-only"})


def make_session(directory, phase="L65", slot=1, facade=None):
    service = make_service()
    session = validation.ValidationSession.__new__(validation.ValidationSession)
    session.torch = facade or TorchFacade()
    session.service, session.engine = service, service.learning
    session.phase, session.slot = phase, slot
    session.initial_observation_recorded = False
    session.writer = RecordingWriter(session.torch, directory, 1 << 20)
    session.counter = SimpleNamespace(charge=lambda *args: None)
    session.observer = SimpleNamespace(install_adam=lambda: EVENTS.append("install"))

    def opened(request):
        EVENTS.append("loaded")
        with torch.no_grad():
            service.model.p.fill_(2.0)
        service.learning.optimizer = FakeOptimizer(service.model)
        service.learning.optimizer.load_artificial_state()
        return {"already_committed": False}

    session.original_dispatch = opened
    return session


def require_complete(state):
    required = {"model", "optimizer", "torch_rng", "cuda_rng", "account_sampling", "gradients",
                "optimizer_applicability", "optimizer_id_mapping", "source_boundary"}
    assert required <= state.keys(), "missing full-state component"
    assert {"p", "q", "p_alias", "buffer"} <= state["model"].keys(), "missing persistent buffer or parameter"


def require_loaded(state):
    assert torch.equal(state["model"]["p"], torch.tensor([2.0, 2.0])), "wrong observation point"


def require_mapping(mapping):
    entries = mapping["groups"][0]["entries"]
    assert [(entry["state_id"], entry["parameter_name"]) for entry in entries] == [(73, "q"), (11, "p")], "wrong ID/name pairing"
    assert entries[0]["has_state"] is True and entries[1]["has_state"] is False, "missing state must not be zero state"


class RuntimeObservationTests(unittest.TestCase):
    def setUp(self):
        self.root = ROOT / self._testMethodName
        self.root.mkdir()
        EVENTS.clear()

    def test_loaded_learner_once_full_state_mapping_and_no_index_gate(self):
        session = make_session(self.root / "captures")
        rng = torch.get_rng_state().clone()
        session.dispatch({"phase": "learn_open"})
        session.dispatch({"phase": "learn_open"})
        session.writer.close()
        self.assertEqual(EVENTS[:3], ["loaded", "record", "install"])
        self.assertEqual(EVENTS.count("record"), 1)
        state = torch.load(session.writer.directory / "initial-state.pt", map_location="cpu", weights_only=True)
        require_complete(state)
        require_loaded(state)
        mapping = state["optimizer_id_mapping"]
        require_mapping(mapping)
        entries = mapping["groups"][0]["entries"]
        self.assertEqual(entries[1]["aliases"], ["p", "p_alias"])
        self.assertEqual(entries[0]["gradient"], {"present": False, "zero": None, "zero_status": "not_applicable"})
        self.assertEqual(entries[1]["gradient"]["zero"], True)
        self.assertIsNone(mapping["defaults"]["foreach"])
        self.assertIsNone(mapping["defaults"]["fused"])
        self.assertEqual(mapping["runtime_dispatch"], "unknown")
        self.assertEqual(state["optimizer"]["state"][73]["step"].item(), 4.0)
        self.assertEqual(state["account_sampling"], {})
        self.assertEqual(state["point"], "post_learn_open_pre_collection_and_update")
        self.assertTrue(torch.equal(rng, torch.get_rng_state()))
        self.assertEqual((session.writer.directory / "index.jsonl").read_bytes(), b"")
        self.assertEqual(session.writer.keys, set())
        self.assertEqual(session.writer.count, 0)
        rows = [json.loads(line) for line in (session.writer.directory / "runtime-observations.jsonl").read_text().splitlines()]
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["slot"], 1)
        payload = session.writer.directory / rows[0]["file"]
        self.assertEqual(rows[0]["sha256"], hashlib.sha256(payload.read_bytes()).hexdigest())
        self.assertEqual(rows[0]["bytes"], payload.stat().st_size)
        self.assertLessEqual(payload.stat().st_size, session.writer.bytes)

    def test_consumer_and_cold_points_and_existing_stop_baseline_scope(self):
        consumer = make_session(self.root / "consumer", phase="C65")
        consumer.observe_initial_state()
        consumer.writer.close()
        image = torch.load(consumer.writer.directory / "initial-state.pt", map_location="cpu", weights_only=True)
        self.assertIsNone(image["optimizer"])
        self.assertEqual(image["optimizer_applicability"], "not_applicable")
        self.assertEqual(image["point"], "post_service_load_pre_first_consume")
        cold = make_session(self.root / "cold", phase="L65-cold", slot=12)
        cold.dispatch({"phase": "learn_open"})
        cold.writer.close()
        image = torch.load(cold.writer.directory / "initial-state.pt", map_location="cpu", weights_only=True)
        self.assertEqual(image["point"], "post_learn_open_pre_collection_sampling_restore")
        self.assertEqual(image["account_sampling"], {})
        for slot in (9, 10):
            stopped = make_session(self.root / ("stop-%d" % slot), slot=slot)
            stopped.observe_initial_state()
            stopped.writer.close()
            self.assertFalse((stopped.writer.directory / "initial-state.pt").exists())

    def test_snapshot_is_independent_and_negative_checks_detect_gaps(self):
        service = make_service()
        service.learning.optimizer = FakeOptimizer(service.model)
        service.learning.optimizer.load_artificial_state()
        service.accounts["artificial"] = {"calls": 65, "rng": FakeGenerator()}
        facade = TorchFacade()
        before, _ = capture.initial_runtime_snapshot(facade, service, service.learning.optimizer,
                                                      "learner", "L65", "wrong_preload")
        with self.assertRaises(AssertionError):
            require_loaded(before)
        with torch.no_grad():
            service.model.p.fill_(2.0)
            service.model.p.grad.fill_(1.0)
        snapshot, metadata = capture.initial_runtime_snapshot(facade, service, service.learning.optimizer,
                                                               "learner", "L65", "loaded")
        require_complete(snapshot)
        require_loaded(snapshot)
        require_mapping(snapshot["optimizer_id_mapping"])
        self.assertEqual(metadata["gradient_status"]["p"]["zero"], False)
        for missing in ("optimizer", "torch_rng", "cuda_rng"):
            bad = dict(snapshot)
            del bad[missing]
            with self.assertRaises(AssertionError):
                require_complete(bad)
        bad = dict(snapshot, model={key: value for key, value in snapshot["model"].items() if key != "buffer"})
        with self.assertRaises(AssertionError):
            require_complete(bad)
        wrong = copy.deepcopy(snapshot["optimizer_id_mapping"])
        wrong["groups"][0]["entries"][0]["state_id"] = 0
        with self.assertRaises(AssertionError):
            require_mapping(wrong)
        # Demonstrate the shallow state_dict hazard, then verify actual snapshot stability.
        shallow = service.model.state_dict()
        with torch.no_grad():
            service.model.p.add_(10)
            service.model.buffer.add_(10)
            service.learning.optimizer.state[73]["exp_avg"].add_(10)
            service.accounts["artificial"]["rng"].state.add_(10)
        self.assertFalse(torch.equal(shallow["p"], snapshot["model"]["p"]))
        require_loaded(snapshot)
        self.assertEqual(snapshot["model"]["buffer"].tolist(), [7])
        self.assertEqual(snapshot["optimizer"]["state"][73]["exp_avg"].tolist(), [3.0])
        self.assertEqual(snapshot["account_sampling"]["artificial"]["rng"].tolist(), [1, 2, 3])

    def test_cuda_scope_uses_only_stubs_and_does_not_initialize_unused_devices(self):
        cpu_active = TorchFacade(True)
        cpu_state, _ = capture.initial_runtime_snapshot(cpu_active, make_service(), None,
                                                       "consumer", "C65", "loaded")
        self.assertEqual(cpu_active.cuda.calls, ["is_initialized"])
        self.assertEqual(cpu_state["cuda_rng"], {"initialized": True, "scope": "not_applicable", "states": []})
        device = SimpleNamespace(type="cuda", index=0)
        service = make_service(device)
        unused = TorchFacade(False)
        state, _ = capture.initial_runtime_snapshot(unused, service, None, "consumer", "C65", "loaded")
        self.assertEqual(unused.cuda.calls, ["is_initialized"])
        self.assertEqual(state["cuda_rng"], {"initialized": False, "scope": "not_initialized", "states": []})
        active = TorchFacade(True)
        state, _ = capture.initial_runtime_snapshot(active, service, None, "consumer", "C65", "loaded")
        self.assertEqual(active.cuda.calls, ["is_initialized", ["get_rng_state", 0]])
        active.cuda.state.add_(10)
        self.assertEqual(state["cuda_rng"]["states"][0]["state"].tolist(), [4, 5, 6])
        self.assertEqual(state["cuda_rng"]["scope"], "actual_initialized_service_device")

    def test_update_metadata_reuses_state_and_records_mapping_gaps_without_a_new_gate(self):
        service = make_service()
        optimizer = FakeOptimizer(service.model)
        optimizer.load_artificial_state()
        service.learning.optimizer = optimizer
        writer = capture.CaptureWriter(TorchFacade(), self.root / "captures", 1 << 20)
        observer = capture.CaptureObserver.__new__(capture.CaptureObserver)
        observer.torch, observer.service, observer.engine = TorchFacade(), service, service.learning
        observer.writer, observer.stage = writer, "L65"
        state = optimizer.state_dict()
        observer.observe_optimizer_mapping(state, {"p": {"present": True, "zero": True},
                                                   "q": {"present": False, "zero": None}}, 1)
        writer.close()
        record = json.loads((writer.directory / "runtime-observations.jsonl").read_text())
        require_mapping(record["mapping"])
        self.assertEqual(record["gradient_observation_point"], "pre_update")
        self.assertEqual((writer.directory / "index.jsonl").read_bytes(), b"")
        incomplete = optimizer.state_dict()
        incomplete["param_groups"][0]["params"] = [73]
        incomplete["state"][999] = {"step": torch.tensor(1)}
        mapping = capture.optimizer_id_mapping(TorchFacade(), service.model, optimizer, incomplete)
        self.assertTrue(mapping["group_length_mismatches"])
        self.assertEqual(mapping["unmapped_state_ids"], [999])
        self.assertIsNone(mapping["groups"][0]["entries"][1]["state_id"])

    def test_original_comparison_gates_and_consumer_hook_order_are_unchanged(self):
        before = ROOT.parent / "before"
        pairs = [("performance_capture.py", ["compare_value"]),
                 ("performance_validation.py", ["compare_capture", "compare_slots", "capture_index", "buffer_gates"])]
        for filename, names in pairs:
            old = ast.parse((before / filename).read_text())
            new = ast.parse((PYTHON / filename).read_text())
            for name in names:
                old_node = next(item for item in old.body if isinstance(item, ast.FunctionDef) and item.name == name)
                new_node = next(item for item in new.body if isinstance(item, ast.FunctionDef) and item.name == name)
                self.assertEqual(ast.dump(old_node, include_attributes=False), ast.dump(new_node, include_attributes=False))
        tree = ast.parse((PYTHON / "performance_validation.py").read_text())
        serve = next(item for item in tree.body if isinstance(item, ast.FunctionDef) and item.name == "serve")
        calls = [(node.lineno, ast.unparse(node.func)) for node in ast.walk(serve) if isinstance(node, ast.Call)]
        observed = next(line for line, name in calls if name == "session.observe_initial_state")
        consumed = next(line for line, name in calls if name == "service.run")
        self.assertLess(observed, consumed)
        self.assertEqual(capture.TOLERANCES, {"activation": (1e-6, 1e-5), "probability": (1e-7, 1e-5),
                                             "loss": (1e-7, 1e-4), "gradient": (1e-7, 1e-3),
                                             "parameter": (1e-6, 1e-5), "exact": (0.0, 0.0)})


if __name__ == "__main__":
    started = time.monotonic()
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(RuntimeObservationTests)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    report = {"tests": result.testsRun, "failures": len(result.failures), "errors": len(result.errors),
              "elapsed_seconds": time.monotonic() - started, "torch_version": str(torch.__version__),
              "cuda_visible_devices": os.environ.get("CUDA_VISIBLE_DEVICES"),
              "optimizer_step_calls": FakeOptimizer.step_calls,
              "production_policy_constructions": 0, "forward_calls": 0, "cuda_api": "stub-only",
              "actual_checkpoint_or_capture_reads": 0,
              "source_sha256": {name: hashlib.sha256((PYTHON / name).read_bytes()).hexdigest()
                                for name in ("performance_capture.py", "performance_validation.py")}}
    (ROOT / "result.json").write_text(json.dumps(report, indent=2) + "\n")
    sys.exit(0 if result.wasSuccessful() and FakeOptimizer.step_calls == 0 else 1)
