"""#377 原生产worker的有界观测入口；不实现JSON/二进制传输协议。

serve <model> <device> <threads> 与生产脚本同形。Rust ModelWorker显式传入：
B_PERFORMANCE_MANIFEST、B_PERFORMANCE_SLOT、B_PERFORMANCE_PHASE、B_PERFORMANCE_OUTPUT。
停止槽另传B_PERFORMANCE_READY_FD；取消字节仍只由原Service.check_cancel消费。

manifest绑定source.root/source.files、observer、method、timing、limits、f4和reference_loss。
本文件的导入及compare参数解析不导入torch；只有实际serve/compare操作导入。
"""
import ast
import copy
import hashlib
import importlib
import json
import math
import os
from pathlib import Path
import select
import sys
import time

from performance_capture import (CAPTURE_VERSION, BufferObserver, CaptureObserver, CaptureWriter,
                                 WorkCounter, compare_value, cpu_tree, initial_runtime_snapshot)


VERSION = "b-performance-validation-v1"
# R0/A/B使用同一观测实现；实际运行由根完成源码/槽位绑定及必要独审后统一协调。
SUPPORTED_VARIANTS = ("R0", "A", "B")
SLOTS = {
    1: ("R0", 272, 2), 2: ("R0", 272, 2), 3: ("R0", 272, 2),
    4: ("A", 272, 2), 5: ("A", 272, 2), 6: ("B", 272, 2),
    7: ("B", 272, 2), 8: ("B", 272, 2), 9: ("A", 166, 0),
    10: ("B", 166, 0), 11: ("B", 272, 2), 12: ("B", 272, 2),
}
F4_CASES = ("F-N", "F-P", "F-R", "F-S")
THREAD_KEYS = ("torch_intra_threads", "torch_interop_threads", "omp_threads", "mkl_threads")
MAIN_LABEL_RULE = {
    "version": "f4-python-binary64-return-v1",
    "advantage": {"F-N": 1.0, "F-P": 1.0, "F-R": -1.0, "F-S": -1.0},
    "old_value_source_dtype": "torch.float32",
    "old_joint_logp_source_dtype": "torch.float64",
    "critic_target": "float(old_value) + advantage in Python binary64",
    "advantage_unit": "original_label_unit_without_money_scale_multiplication",
}
BOUNDARY_RULE = {
    "version": "f4-fp64-clip-sides-v1", "delta": 2.0 ** -20,
    "targets": {"F-N": "1-epsilon-delta", "F-P": "1-epsilon+delta",
                "F-R": "1+epsilon-delta", "F-S": "1+epsilon+delta"},
    "advantages": [1.0, -1.0],
    "old_joint_logp": "float(new_joint_logp) - math.log(target_ratio) in Python binary64",
}


def bounded_bytes(path, maximum):
    path = Path(path)
    if maximum <= 0 or path.stat().st_size > maximum:
        raise ValueError("输入超过冻结读取字节界限: " + path.name)
    with path.open("rb") as stream:
        value = stream.read(maximum + 1)
    if len(value) > maximum:
        raise ValueError("输入在读取期间超过冻结界限")
    return value


def bound_read(binding):
    value = bounded_bytes(binding["path"], int(binding["max_bytes"]))
    if hashlib.sha256(value).hexdigest() != binding["sha256"]:
        raise ValueError("只读材料SHA与冻结绑定不同: " + Path(binding["path"]).name)
    return value


def write_json_new(path, value):
    data = json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False, indent=2).encode()
    with Path(path).open("xb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
    return {"path": str(path), "sha256": hashlib.sha256(data).hexdigest(),
            "bytes": len(data), "max_bytes": len(data)}


def load_bound_source(manifest):
    """一个worker只装一个源码根；拒绝sys.modules或sys.path串到另一候选。"""
    root = Path(manifest["source"]["root"]).resolve()
    names = ("model_worker", "learning_engine", "model_protocol", "input_cache")
    if any(name in sys.modules for name in names):
        raise RuntimeError("worker导入前已有模型模块，不能混用候选")
    for name, digest in manifest["source"]["files"].items():
        path = (root / name).resolve()
        if path.parent != root or path.suffix != ".py":
            raise ValueError("源码绑定必须是同根Python文件")
        raw = bounded_bytes(path, 1 << 20)
        if hashlib.sha256(raw).hexdigest() != digest:
            raise ValueError("源码SHA不符: " + name)
    if not {"model_worker.py", "learning_engine.py"} <= set(manifest["source"]["files"]):
        raise ValueError("参考/候选源码绑定不完整")
    sys.path.insert(0, str(root))
    module = importlib.import_module("model_worker")
    if Path(module.__file__).resolve() != root / "model_worker.py":
        raise RuntimeError("实际模型模块与绑定根不同")
    return module


def original_loss(torch, source_binding):
    """直接复用固定R0 step的六个赋值表达式；不另写正式PPO算法。"""
    source = bound_read(source_binding).decode()
    tree = ast.parse(source)
    engine = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == "LearningEngine")
    step = next(node for node in engine.body if isinstance(node, ast.FunctionDef) and node.name == "step")
    names = ("scale", "advantage", "ratio", "actor", "critic", "loss")
    statements = []
    for node in step.body:
        if (isinstance(node, ast.Assign) and len(node.targets) == 1
                and isinstance(node.targets[0], ast.Name) and node.targets[0].id in names):
            statements.append(copy.deepcopy(node))
    if [node.targets[0].id for node in statements] != list(names):
        raise ValueError("固定R0的原loss表达式不可按声明范围提取")
    template = ast.parse("def observed_loss(self, logp, value, label, key):\n    pass\n").body[0]
    template.body = statements + [ast.Return(value=ast.Dict(
        keys=[ast.Constant(name) for name in names], values=[ast.Name(id=name, ctx=ast.Load()) for name in names]))]
    program = ast.fix_missing_locations(ast.Module(body=[template], type_ignores=[]))
    identity = hashlib.sha256(ast.dump(program, include_attributes=False).encode()).hexdigest()
    namespace = {"torch": torch}
    exec(compile(program, "<frozen-r0-loss-observation>", "exec"), namespace)
    return namespace["observed_loss"], identity


def buffer_gates(slot, metrics, evidence):
    """从真实finish载荷派生本方法的结构覆盖，不从配置名称推断。"""
    counts = evidence["counts"]
    if slot == 7:
        return {"resident_gpu_inputs": metrics.get("cache_gpu_payload_bytes", 0) > 0,
                "no_host_inputs": metrics.get("cache_host_payload_bytes", 0) == 0,
                "no_unnecessary_stream": not counts.get("allocate") and not counts.get("h2d_submitted")}
    if slot == 8:
        return {"fixed_resident_and_streamed_inputs": metrics.get("cache_gpu_payload_bytes", 0) > 0
                    and metrics.get("cache_host_payload_bytes", 0) > 0,
                "two_independent_slots": evidence["allocated_slots"] == 2,
                "both_slots_used": len(evidence["slots_used"]) == 2,
                "completed_host_reuse": counts.get("host_reuse_ready", 0) > 0,
                "raw_reuse_owned_dependency": counts.get("reuse_wait_submitted", 0) > 0,
                "actual_h2d": counts.get("h2d_submitted", 0) > 0,
                "actual_owned_copy": counts.get("owned_copy_submitted", 0) > 0,
                "next_segment_prefetch_consumed": counts.get("prefetch_h2d_submitted", 0) > 0
                    and counts.get("prefetch_owned_copy_submitted", 0) > 0,
                "no_unconsumed_normal_upload": evidence["uploaded_generations_not_yet_consumed"] == 0,
                "five_live_tail_graphs": evidence["peak_live_accounts"] == 5,
                "explicit_graph_inputs_released": evidence["remaining_explicit_graph_input_bytes"] == 0}
    return {}


def full_work_counts(slot):
    """The approved finite program, independent of observed or allocated GPU capacity."""
    _, frames, adam = SLOTS[slot]
    workers = 3 if slot == 12 else 2
    learners = 2 if slot == 12 else 1
    backward = 1 if slot in (9, 10) else 14
    counts = {"worker_opens": workers, "model_loads": workers,
              "policy_constructions": workers, "service_model_deserializations": workers,
              "consumer_seq3_deserializations": 1, "learner_bootstrap_deserializations": learners,
              "checkpoint_state_deserializations": learners, "production_learn_open": learners,
              "main_gru_frames": frames, "main_gru_calls": frames,
              "production_backward": backward, "actual_adam": adam, "reference_adam": adam}
    for key in ("model_loads", "policy_constructions", "service_model_deserializations",
                "consumer_seq3_deserializations", "learner_bootstrap_deserializations",
                "checkpoint_state_deserializations", "production_learn_open", "main_gru_calls",
                "production_backward"):
        counts[key + "_completed"] = counts[key]
    counts["main_gru_completed"] = frames
    return counts


class LoadObservations:
    """One worker's actual constructor/load calls, not inferred from its hello response."""
    def __init__(self, torch, module, model_path, phase, counter):
        self.torch, self.module, self.counter = torch, module, counter
        self.model_path = Path(model_path).resolve()
        self.checkpoint_path = None
        self.phase = phase
        self.original_load, self.original_policy = torch.load, module.Policy

    def install(self):
        def load(path, *args, **kwargs):
            actual = Path(path).resolve()
            if actual == self.model_path:
                names = ("service_model_deserializations",
                         "consumer_seq3_deserializations" if self.phase == "C65"
                         else "learner_bootstrap_deserializations")
            elif self.checkpoint_path is not None and actual == self.checkpoint_path:
                names = ("checkpoint_state_deserializations",)
            else:
                # This observed closed program has no other production load.
                # A new path is an unbudgeted attempt, not silently a checkpoint.
                self.counter.charge("unexpected_model_deserializations")
                raise RuntimeError("unbound model/checkpoint deserialization")
            for name in names:
                self.counter.charge(name)
            result = self.original_load(path, *args, **kwargs)
            for name in names:
                self.counter.charge(name + "_completed")
            return result
        def policy(*args, **kwargs):
            self.counter.charge("policy_constructions")
            result = self.original_policy(*args, **kwargs)
            self.counter.charge("policy_constructions_completed")
            return result
        self.torch.load, self.module.Policy = load, policy

    def close(self):
        self.torch.load, self.module.Policy = self.original_load, self.original_policy


class ValidationSession:
    """仅包装真实Service对象；所有请求仍由该源码的Service.run分派。"""

    def __init__(self, torch, service, manifest, manifest_sha, slot, phase, output, counter):
        self.torch, self.service, self.engine = torch, service, service.learning
        self.manifest, self.slot, self.phase = manifest, slot, phase
        self.output, self.counter = output, counter
        self.worker_directory = output / (phase + "-%d" % os.getpid())
        self.worker_directory.mkdir()
        self.writer = CaptureWriter(torch, self.worker_directory / "captures", int(manifest["limits"]["capture_bytes"]))
        self.initial_observation_recorded = False
        self.observer = CaptureObserver(torch, service, self.writer, counter, phase)
        self.observer.observe_projection_features = True  # the hook itself is learning-only
        self.buffers = BufferObserver(torch, self.writer)
        cache = getattr(self.engine, "cache", None)
        self.original_buffer_observer = getattr(cache, "buffer_observer", None)
        if cache is not None and hasattr(cache, "buffer_observer"):
            cache.buffer_observer = self.buffers
        self.original_sequence_observer = getattr(self.engine, "sequence_observer", None)
        if hasattr(self.engine, "sequence_observer"):
            self.engine.sequence_observer = self.observer
        self.registered = self.initial_done = self.prepared = False
        self.fixtures, self.samples, self.labels = {}, [], None
        self.baseline = None
        self.sampling_before_learning = None
        self.cold_restore_proof = None
        self.original_dispatch = self.engine.dispatch
        self.engine.dispatch = self.dispatch
        self.loss_function, loss_sha = original_loss(torch, manifest["reference_loss"])
        self.metadata = {"version": VERSION, "capture_version": CAPTURE_VERSION, "slot": slot,
                         "variant": manifest["variant"], "phase": phase, "pid": os.getpid(),
                         "manifest_sha256": manifest_sha, "source": manifest["source"],
                         "observer": manifest["observer"], "method": manifest["method"],
                         "execution_method": manifest["execution_method"],
                         "capture_method": manifest["capture_method"],
                         "auxiliary_loss_ast_sha256": loss_sha,
                         "service_identity": dict(service.identity)}
        runtime = service.identity.get("numerical_runtime", {})
        observed_threads = {"torch_intra_threads": torch.get_num_threads(),
                            "torch_interop_threads": torch.get_num_interop_threads(),
                            "omp_threads": os.environ.get("OMP_NUM_THREADS"),
                            "mkl_threads": os.environ.get("MKL_NUM_THREADS")}
        expected_runtime = manifest["expected_runtime"]
        if set(expected_runtime) - set(THREAD_KEYS):
            raise ValueError("线程预期只能列批准的四个精确路径")
        if manifest["variant"] != "R0" and set(expected_runtime) != set(THREAD_KEYS):
            raise ValueError("候选必须冻结四项线程执行设置")
        if any(observed_threads[key] != value for key, value in expected_runtime.items()):
            raise ValueError("实际线程设置与冻结manifest不同")
        if any(key in runtime and runtime[key] != value for key, value in observed_threads.items()):
            raise ValueError("生产identity的线程值与槽内实际观测不同")
        self.metadata["thread_binding"] = {"expected": expected_runtime,
                                            "actual": observed_threads,
                                            "original_identity_fields": [key for key in THREAD_KEYS if key in runtime],
                                            "historical_unbound": [key for key in THREAD_KEYS if key not in expected_runtime]}
        self.boundary_labels = {}
        self.boundary_binding = None
        if manifest["f4"]["label_rule"] != MAIN_LABEL_RULE or manifest["f4"]["boundary_rule"] != BOUNDARY_RULE:
            raise ValueError("F4主标签/PPO边界与已冻结v2常量不同")
        if manifest["f4"]["order"] != list(F4_CASES):
            raise ValueError("F4必须按F-N/P/R/S固定顺序")
        self.ready_fd = None
        self.stop_notified = False
        if slot in (9, 10) and phase != "C65":
            self._install_stop_barrier()
        if hasattr(service, "frame"):
            original_frame = service.frame
            def frame(requests):
                self.counter.charge("production_frame_calls", len(requests))
                return original_frame(requests)
            self.observer._replace(service, "frame", frame)
        if hasattr(service, "restore_collection_sampling_state"):
            original_restore = service.restore_collection_sampling_state
            def restore(request):
                self.counter.charge("production_sampling_restore")
                result = original_restore(request)
                restored = service.collection_sampling_state()
                rows = compare_value(torch, request["state"], restored, "exact", "sampling_restore")
                self.cold_restore_proof = {"pass": all(row["pass"] for row in rows), "comparisons": rows,
                                           "expected_calls": request["expected_calls"], "result": result}
                if not self.cold_restore_proof["pass"]:
                    raise RuntimeError("生产冷恢复没有精确保留封存CPU采样状态")
                self.counter.charge("production_sampling_restore_completed")
                return result
            self.observer._replace(service, "restore_collection_sampling_state", restore)
        write_json_new(self.worker_directory / "identity.json", self.metadata)

    def _install_stop_barrier(self):
        self.ready_fd = int(os.environ["B_PERFORMANCE_READY_FD"])
        cancel_fd = self.service.cancel_fd
        if cancel_fd is None:
            raise ValueError("停止槽没有生产取消FD")
        original = self.service.check_cancel
        def check_cancel():
            if (not self.stop_notified and self.engine.epoch == 1
                    and sum(self.engine.counts.values()) == 32
                    and all(not values for values in self.engine.pending_loss.values())):
                self.stop_notified = True
                os.write(self.ready_fd, b"R")
                self.counter.charge("stop_safe_point_notifications")
                remaining = max(0.0, self.counter.deadline - time.monotonic())
                readable, _, _ = select.select([cancel_fd], [], [], remaining)
                if not readable:
                    raise TimeoutError("已通知32步安全点，但截止前未收到生产取消信号")
            # 只观察可读性；字节由生产实现消费并产生真实InterruptedError。
            return original()
        self.observer._replace(self.service, "check_cancel", check_cancel)

    def _sampling(self):
        return {key: {"calls": value["calls"], "rng": value["rng"].get_state()}
                for key, value in self.service.accounts.items()}

    def _model_state(self):
        return {"parameters": dict(self.service.model.named_parameters()),
                "optimizer": self.engine.optimizer.state_dict(),
                "consumed": self.engine.consumed, "sequence": self.engine.sequence}

    def observe_initial_state(self):
        # Existing stop baselines are unchanged, and successful repeated opens
        # do not create another full state copy or a new business guard.
        if self.initial_observation_recorded or self.slot in (9, 10):
            return
        role = "consumer" if self.phase == "C65" else "learner"
        if role == "consumer":
            optimizer = None
            point = "post_service_load_pre_first_consume"
        else:
            optimizer = self.engine.optimizer
            point = ("post_learn_open_pre_collection_sampling_restore" if self.phase == "L65-cold"
                     else "post_learn_open_pre_collection_and_update")
        snapshot, metadata = initial_runtime_snapshot(self.torch, self.service, optimizer,
                                                      role, self.phase, point)
        metadata["slot"] = self.slot
        self.writer.write_initial_observation(snapshot, metadata)
        self.initial_observation_recorded = True

    def register(self, request):
        if self.registered:
            raise ValueError("同一worker不能重新注册短样本")
        samples = request["samples"]
        if len(samples) != 65 or [item["ordinal"] for item in samples] != list(range(65)):
            raise ValueError("C65/L65必须是冻结的65个ordinal")
        if [item["call"] for item in samples] != list(range(1, 66)):
            raise ValueError("短样本不是原合法call1至65")
        prefix = "C65" if self.phase == "C65" else "L65"
        if [item["name"] for item in samples] != ["%s/%03d" % (prefix, index) for index in range(65)]:
            raise ValueError("观测样本名称不对应冻结ordinal")
        mapping = {(item["account"], item["call"]): item["name"] for item in samples}
        if len(mapping) != 65 or len(set(mapping.values())) != 65:
            raise ValueError("短样本身份重复")
        fixtures = request.get("fixtures", [])
        if self.phase != "C65":
            if len(fixtures) != 4 or {item["case"] for item in fixtures} != set(F4_CASES):
                raise ValueError("F4合法构造材料不完整")
            self.fixtures = {item["case"]: item for item in fixtures}
            for case in F4_CASES:
                frame = self.fixtures[case]["frame"]
                key = (frame["account"], frame["call"])
                if key in mapping or frame["call"] != 1:
                    raise ValueError("F4必须有独立冻结序列身份和call1")
                mapping[key] = case
                self.observer.f4_keys[key] = case
            if len({self.fixtures[case]["frame"]["account"] for case in F4_CASES}) != 4:
                raise ValueError("F4不能互相续接hidden")
            accounts = {item["account"] for item in samples}
            accounts.update(self.fixtures[case]["frame"]["account"] for case in F4_CASES)
            if len(accounts) != 5 or self.manifest["f4"]["account_weights"] != {key: 1.0 / 5 for key in accounts}:
                raise ValueError("原L65与四条工程sequence必须各权重1/5")
        self.samples, self.observer.step_keys = samples, mapping
        self.registered = True
        self.writer.write("binding/" + self.phase + "/samples", samples, "exact")
        if fixtures:
            self.writer.write("binding/" + self.phase + "/fixtures", fixtures, "exact")
        return {"registered": True, "phase": self.phase, "samples": 65,
                "fixture_cases": list(self.fixtures),
                "account_weights": self.manifest["f4"]["account_weights"],
                "identity": self.metadata}

    def initial_f4(self, request):
        if not self.registered or self.phase != "L65" or self.initial_done:
            raise ValueError("F4初始捕获只能在已注册的L65采集worker执行一次")
        if self.engine.optimizer is None:
            raise ValueError("F4需要原learn_open已恢复完整起点")
        if "fixtures" in request and request["fixtures"] != list(self.fixtures.values()):
            raise ValueError("F4不能在已注册后换构造")
        observations = {}
        before_rng = cpu_tree(self.torch, self._sampling())
        for case in F4_CASES:
            self.observer.begin("initial/" + case)
            with self.torch.no_grad():
                result = self.engine.teacher(self.fixtures[case]["frame"], self.engine.zeros(), trace=True)
            observations[case] = {"old_joint_logp": float(result[0].detach()),
                                  "old_value": float(result[1].detach()),
                                  "logp_dtype": str(result[0].dtype), "value_dtype": str(result[1].dtype)}
            self.observer.finish()
        unchanged = compare_value(self.torch, before_rng, cpu_tree(self.torch, self._sampling()), "exact")
        if not all(row["pass"] for row in unchanged):
            raise RuntimeError("F4 teacher改变了业务CPU采样RNG")
        for value in observations.values():
            if (value["value_dtype"] != "torch.float32" or value["logp_dtype"] != "torch.float64"
                    or not math.isfinite(value["old_value"]) or not math.isfinite(value["old_joint_logp"])):
                raise ValueError("候选F4没有保留有限的FP32 value/FP64 logp")
        if self.slot == 1:
            self.labels = self.make_main_labels(observations)
            binding = write_json_new(self.output / "f4-labels.json",
                                     {"version": VERSION, "rule": self.manifest["f4"]["label_rule"],
                                      "observations": observations, "labels": self.labels})
        else:
            reference = json.loads(bound_read(self.manifest["f4"]["reference_labels"]))
            if reference["rule"] != self.manifest["f4"]["label_rule"]:
                raise ValueError("候选不能替换槽1的合成标签规则")
            self.labels, binding = reference["labels"], self.manifest["f4"]["reference_labels"]
        self.initial_done = True
        return {"observations": observations, "labels": self.labels,
                "labels_binding": binding, "capture_directory": str(self.writer.directory),
                "counts": dict(self.counter.counts)}

    def make_main_labels(self, observations):
        result = {}
        for case in F4_CASES:
            value = observations[case]
            if value["value_dtype"] != "torch.float32" or value["logp_dtype"] != "torch.float64":
                raise ValueError("F4原value/logp精度不符")
            advantage = MAIN_LABEL_RULE["advantage"][case]
            old_value, old_logp = value["old_value"], value["old_joint_logp"]
            target = old_value + advantage
            if not all(math.isfinite(number) for number in (old_value, old_logp, target)):
                raise ValueError("F4原值/合成return不是有限值")
            result[case] = {"old_joint_logp": old_logp, "advantage": advantage, "critic_target": target}
        return result

    def prepare(self, request):
        if not self.registered or self.prepared:
            raise ValueError("短槽只能准备一次已注册的学习材料")
        if self.labels is None:
            # 冷worker只读同槽热worker的已冻结标签，不再次teacher或重新贴标签。
            reference = json.loads(bound_read(request["labels_binding"]))
            self.labels = reference["labels"]
        labels, schedule = request["labels"], request["schedule"]
        expected = []
        for item in self.samples:
            expected.append({"account": item["account"], "index": item["ordinal"]})
        for case in self.manifest["f4"]["order"]:
            frame = self.fixtures[case]["frame"]
            if labels.get(frame["account"]) != [self.labels[case]]:
                raise ValueError("实际F4标签与槽1冻结值不同")
            expected.append({"account": frame["account"], "index": 0})
        if schedule != expected or sum(map(len, labels.values())) != 69:
            raise ValueError("学习顺序/完整样本数与冻结方法不同")
        if self.engine.weights != self.manifest["f4"]["account_weights"]:
            raise ValueError("实际原loss账户权重与冻结方法不同")
        self.sampling_before_learning = cpu_tree(self.torch, self._sampling())
        self.observer.auxiliary = self.auxiliary
        self.writer.write("binding/learning_labels", labels, "exact")
        self.writer.write("binding/learning_schedule", schedule, "exact")
        self.prepared = True
        return {"prepared": True, "samples": 69,
                "production_prepare_required": self.manifest["variant"] != "R0"}

    def auxiliary(self, observer, case, frame, label, tensors):
        torch = self.torch
        epoch = str(self.engine.epoch)
        epsilon = self.engine.config["epsilon"]
        delta = BOUNDARY_RULE["delta"]
        lower, upper = 1.0 - epsilon, 1.0 + epsilon
        targets = {"F-N": lower - delta, "F-P": lower + delta,
                   "F-R": upper - delta, "F-S": upper + delta}
        if tensors["logp"].dtype != torch.float64 or tensors["value"].dtype != torch.float32:
            raise ValueError("辅助边界不是原FP64 logp/FP32 value")
        if self.slot == 1:
            target = targets[case]
            if not math.isfinite(target) or target <= 0:
                raise ValueError("冻结边界目标ratio不合法")
            old = float(tensors["logp"].detach()) - math.log(target)
            entry = {"old_joint_logp": old, "target_ratio": target, "epsilon": epsilon}
            self.boundary_labels.setdefault(epoch, {})[case] = entry
        else:
            if not self.boundary_labels:
                frozen = json.loads(bound_read(self.manifest["f4"]["reference_boundary_labels"]))
                if frozen["rule"] != BOUNDARY_RULE:
                    raise ValueError("候选不能替换槽1的辅助标签规则")
                self.boundary_labels = frozen["labels"]
            entry = self.boundary_labels[epoch][case]
            if entry["target_ratio"] != targets[case] or entry["epsilon"] != epsilon:
                raise ValueError("候选clip参数/辅助目标与槽1不同")
        for advantage in BOUNDARY_RULE["advantages"]:
            auxiliary_label = dict(label, old_joint_logp=entry["old_joint_logp"], advantage=advantage)
            parts = self.loss_function(self.engine, tensors["logp"], tensors["value"], auxiliary_label, frame["account"])
            ratio = float(parts["ratio"].detach())
            side = "below" if ratio < lower else ("above" if ratio > upper else "inside")
            expected_side = "below" if case == "F-N" else ("above" if case == "F-S" else "inside")
            left = parts["ratio"] * parts["advantage"]
            right = torch.clamp(parts["ratio"], lower, upper) * parts["advantage"]
            lvalue, rvalue = float(left.detach()), float(right.detach())
            branch = "unclipped" if lvalue < rvalue else ("clipped" if rvalue < lvalue else "tie")
            expected_branch = ("tie" if expected_side == "inside" else
                               ("clipped" if (expected_side == "below" and advantage < 0)
                                or (expected_side == "above" and advantage > 0) else "unclipped"))
            if (not all(math.isfinite(value) for value in (ratio, lvalue, rvalue)) or ratio <= 0
                    or side != expected_side or branch != expected_branch
                    or ratio == lower or ratio == upper):
                raise RuntimeError("PPO边界实际ratio/clip-min分支未达到冻结覆盖")
            name = "positive" if advantage > 0 else "negative"
            key = "auxiliary/%s/%s/%s" % (epoch, case, name)
            self.writer.write(key + "/label", auxiliary_label, "exact")
            self.writer.write(key + "/ratio", parts["ratio"], "probability")
            self.writer.write(key + "/loss", {key: parts[key] for key in ("actor", "critic", "loss")}, "loss")
            self.writer.write(key + "/branch", {"side": side, "min_branch": branch}, "exact")
            self.counter.charge("auxiliary_gradient_calls")
            self.writer.write(key + "/gradients", observer.named_gradients(parts["loss"]), "gradient")
        if self.slot == 1 and epoch == "2" and case == "F-S":
            self.boundary_binding = write_json_new(self.output / "ppo-boundary-labels.json",
                                                   {"version": VERSION, "rule": BOUNDARY_RULE,
                                                    "labels": self.boundary_labels})

    def finish(self, request):
        result = {"version": VERSION, "slot": self.slot, "phase": self.phase,
                  "stop_notified": self.stop_notified,
                  "boundary_labels_binding": self.boundary_binding,
                  "cold_restore_proof": self.cold_restore_proof,
                  "capture_directory": str(self.writer.directory),
                  "buffer_evidence": self.buffers.summary(),
                  "hidden_scope": "collection_each_call_all_layers; learning_each_last_layer_and_full_32_64_65; F4_all_layers"}
        final_learning = self.prepared and (self.engine.epoch > 0 or self.stop_notified)
        if self.sampling_before_learning is not None and final_learning:
            rows = compare_value(self.torch, self.sampling_before_learning,
                                 cpu_tree(self.torch, self._sampling()), "exact", "sampling")
            result["learning_sampling_unchanged"] = rows
            self.writer.write("learning_sampling_state", self._sampling(), "exact")
            if not all(row["pass"] for row in rows):
                raise RuntimeError("学习或辅助观测改变了业务CPU采样RNG")
        if self.stop_notified:
            rows = compare_value(self.torch, self.baseline, cpu_tree(self.torch, self._model_state()), "exact", "stop_state")
            result["stop_state_unchanged"] = rows
            if not all(row["pass"] for row in rows):
                raise RuntimeError("受控停止改变了参数/Adam/consumed/sequence")
            if self.counter.counts.get("actual_adam", 0) or self.counter.counts.get("reference_adam", 0):
                raise RuntimeError("停止槽执行了Adam")
        if final_learning:
            state = self.engine.checkpoint_state()
            # 参数/Adam已在两次更新各保存一次；这里只保留同一生产函数的其余字段。
            metadata = {key: value for key, value in state.items() if key not in ("model", "optimizer")}
            metadata["config"] = {key: value for key, value in metadata["config"].items()
                                  if key != "checkpoint_directory"}
            metadata["identity"] = {key: value for key, value in metadata["identity"].items()
                                    if key != "execution"}
            runtime = metadata["identity"].get("numerical_runtime")
            if runtime is not None:
                metadata["identity"]["numerical_runtime"] = {key: value for key, value in runtime.items()
                                                             if key not in THREAD_KEYS}
            self.writer.write("final_checkpoint_fields", metadata, "exact")
        cache = getattr(self.engine, "cache", None)
        if cache is not None and self.phase != "C65":
            status = cache.status()
            flags, retained = [], 0
            for frames in cache.frames.values():
                for frame in frames:
                    for tokens in [frame["global"]] + frame["markets"]:
                        storage = tokens._storage
                        flags.append(not self.torch.is_inference(storage) and not storage.requires_grad and storage.grad_fn is None)
                        retained += tokens.retained_bytes
            result["cache_evidence"] = {"status": status, "input_sequences": len(flags),
                                         "raw_input_bytes": retained, "ordinary_inputs": all(flags),
                                         "all_69_frames": sum(status["accounts"].values()) == 69,
                                         "production_mode": self.service.performance["mode"]}
            if not all(flags) or sum(status["accounts"].values()) != 69:
                raise RuntimeError("生产缓存未保留69帧普通权重无关输入")
        result.update({"counts": dict(self.counter.counts), "capture_bytes": self.writer.bytes,
                       "capture_events": self.writer.count, "capture_hook_copy_seconds": self.observer.capture_seconds,
                       "service_metrics": self.service.snapshot()})
        if final_learning and self.manifest["variant"] == "B" and self.slot in (7, 8):
            gates = buffer_gates(self.slot, result["service_metrics"], result["buffer_evidence"])
            result["buffer_gates"] = gates
            if not all(gates.values()):
                raise RuntimeError("B真实buffer结构覆盖未通过: " + json.dumps(gates))
        write_json_new(self.worker_directory / "finish.json", result)
        return result

    def dispatch(self, request):
        phase = request["phase"]
        functions = {"learn_validation_register": self.register,
                     "learn_validation_initial_f4": self.initial_f4,
                     "learn_validation_prepare": self.prepare,
                     "learn_validation_finish": self.finish}
        if phase in functions:
            return functions[phase](request)
        if phase == "learn_commit":
            raise ValueError("短槽不得提交正式或临时学习checkpoint")
        if phase == "learn_open":
            loads = getattr(self.service, "validation_load_observations", None)
            if loads is not None:
                loads.checkpoint_path = (Path(request["config"]["checkpoint_directory"]) / "checkpoint.pt").resolve()
        self.counter.charge("production_" + phase)
        result = self.original_dispatch(request)
        if phase == "learn_open":
            self.counter.charge("production_learn_open_completed")
        if phase == "learn_open" and not result.get("already_committed", False):
            self.observe_initial_state()
            self.observer.install_adam()
            if self.slot in (9, 10):
                self.baseline = cpu_tree(self.torch, self._model_state())
        return result

    def close(self):
        cache = getattr(self.engine, "cache", None)
        if cache is not None and hasattr(cache, "buffer_observer"):
            cache.buffer_observer = self.original_buffer_observer
        if hasattr(self.engine, "sequence_observer"):
            self.engine.sequence_observer = self.original_sequence_observer
        self.engine.dispatch = self.original_dispatch
        self.observer.close()
        self.writer.close()
        if self.ready_fd is not None:
            os.close(self.ready_fd)


def serve(model_path, device, threads):
    manifest_raw = bounded_bytes(os.environ["B_PERFORMANCE_MANIFEST"], 1 << 20)
    manifest = json.loads(manifest_raw)
    manifest_sha = hashlib.sha256(manifest_raw).hexdigest()
    slot = int(os.environ["B_PERFORMANCE_SLOT"])
    phase = os.environ["B_PERFORMANCE_PHASE"]
    variant, forwards, adam = SLOTS[slot]
    if manifest["version"] != VERSION or manifest["variant"] != variant or manifest["slot"] != slot:
        raise ValueError("短槽身份/版本/固定12槽计划不符")
    if variant not in SUPPORTED_VARIANTS:
        raise ValueError("当前冻结入口仅R0/A；B生产序列与双buffer证据尚待绑定，不能冒称覆盖")
    if phase not in ("C65", "L65", "L65-cold") or (phase == "L65-cold" and slot != 12):
        raise ValueError("未知或未批准的短槽进程阶段")
    bound_read(manifest["method"])
    bound_read(manifest["execution_method"])
    bound_read(manifest["capture_method"])
    for filename in ("performance_validation.py", "performance_capture.py"):
        raw = bounded_bytes(Path(__file__).parent / filename, 1 << 20)
        if hashlib.sha256(raw).hexdigest() != manifest["observer"][filename]:
            raise ValueError("观测包装SHA与首槽冻结版本不同")
    timing = manifest["timing"]
    deadline = min(timing["started_monotonic"] + 270, timing["deadline_monotonic"],
                   timing["stage_deadline_monotonic"], timing["short_deadline_monotonic"])
    output = Path(os.environ["B_PERFORMANCE_OUTPUT"]).resolve()
    if not output.is_dir():
        raise ValueError("Rust短槽driver必须先建立独立输出目录")
    counter = WorkCounter(output, deadline, forwards, adam, manifest_sha, timing["started_monotonic"],
                          additional_limits=full_work_counts(slot))
    session = loads = None
    try:
        module = load_bound_source(manifest)
        # 仅实际worker执行到这里才导入torch；本轮开发不执行本入口。
        import torch
        loads = LoadObservations(torch, module, model_path, phase, counter)
        loads.install()
        counter.charge("model_loads")
        service = module.Service(model_path, device, int(threads))
        counter.charge("model_loads_completed")
        service.validation_load_observations = loads
        root = Path(manifest["source"]["root"]).resolve()
        for name in ("learning_engine", "input_cache", "model_protocol"):
            loaded = sys.modules.get(name)
            if loaded is not None and Path(loaded.__file__).resolve().parent != root:
                raise RuntimeError("实际依赖从另一源码根装载: " + name)
        session = ValidationSession(torch, service, manifest, manifest_sha, slot, phase, output, counter)
        if phase == "C65":
            session.observe_initial_state()
        service.run()
    except BaseException as error:
        write_json_new(output / ("worker-failure-%d.json" % os.getpid()),
                       {"slot": slot, "phase": phase, "type": type(error).__name__,
                        "error": str(error), "counts": counter.counts,
                        "elapsed": time.monotonic() - timing["started_monotonic"]})
        raise
    finally:
        if session is not None:
            session.close()
        if loads is not None:
            loads.close()
        counter.close()


def capture_index(root):
    """仅枚举调用者给定短槽目录的一层worker捕获，不扫描业务材料。"""
    root = Path(root).resolve()
    identities, records, finished = [], {}, []
    for worker in sorted(root.iterdir()):
        if not worker.is_dir() or not (worker / "identity.json").is_file():
            continue
        identity = json.loads(bounded_bytes(worker / "identity.json", 1 << 20))
        identities.append(identity)
        finished.append(json.loads(bounded_bytes(worker / "finish.json", 16 << 20)))
        directory = (worker / "captures").resolve()
        for line in bounded_bytes(directory / "index.jsonl", 4 << 20).splitlines():
            item = json.loads(line)
            path = (directory / item["file"]).resolve()
            if path.parent != directory or path.suffix != ".pt":
                raise ValueError("捕获index越出当前worker目录")
            item["path"] = path
            records.setdefault(item["key"], []).append(item)
    if not identities or len(identities) > 3:
        raise ValueError("短槽必须有明确的consumer/learner及可选cold worker")
    slot = identities[0]["slot"]
    if any(item["slot"] != slot for item in identities):
        raise ValueError("同一捕获目录混入不同短槽")
    expected_phases = ["C65", "L65", "L65-cold"] if slot == 12 else ["C65", "L65"]
    if sorted(item["phase"] for item in identities) != sorted(expected_phases):
        raise ValueError("真实worker进程阶段不完整或重复")
    with (root / "work.jsonl").open("rb") as stream:
        stream.seek(0, os.SEEK_END)
        stream.seek(max(0, stream.tell() - 65536))
        count = json.loads(stream.read().splitlines()[-1])
    variant, forwards, adam = SLOTS[slot]
    actual = count["counts"]
    gates = {"main_gru_frames": actual.get("main_gru_frames", 0) == forwards,
             "actual_adam": actual.get("actual_adam", 0) == adam,
             "reference_adam": actual.get("reference_adam", 0) == adam,
             "actual_adam_completed": actual.get("actual_adam_completed", 0) == adam,
             "reference_adam_completed": actual.get("reference_adam_completed", 0) == adam}
    gates.update({name: actual.get(name, 0) == expected for name, expected in full_work_counts(slot).items()})
    gates["auxiliary_autograd_grad_complete"] = (actual.get("auxiliary_autograd_grad", 0)
                                                == actual.get("auxiliary_autograd_grad_completed", 0))
    gates["no_unbound_load"] = actual.get("unexpected_model_deserializations", 0) == 0
    if any(item["variant"] != variant for item in identities):
        raise ValueError("捕获候选类型与固定槽表不符")
    if slot in (9, 10):
        gates["production_stop"] = any(item["stop_notified"] for item in finished)
    if slot == 12:
        gates["production_cold_restore"] = any(item.get("cold_restore_proof", {}).get("pass", False)
                                                  for item in finished if item.get("cold_restore_proof") is not None)
        gates["one_production_restore"] = actual.get("production_sampling_restore", 0) == 1
        gates["one_production_restore_completed"] = actual.get("production_sampling_restore_completed", 0) == 1
    if variant != "R0":
        gates["production_collection_frames"] = actual.get("production_frame_calls", 0) == 130
    if slot in (7, 8):
        learner_finish = next(item for item in finished if item["phase"] == "L65")
        for name, passed in buffer_gates(slot, learner_finish["service_metrics"], learner_finish["buffer_evidence"]).items():
            gates["buffer_" + name] = passed
    return {"slot": slot, "records": records, "identities": identities,
            "counts": actual, "count_gates": gates}


def load_capture(torch, item):
    path = item["path"]
    if path.stat().st_size != item["bytes"]:
        raise ValueError("捕获文件长度与index不同")
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1 << 20), b""):
            digest.update(block)
    if digest.hexdigest() != item["sha256"]:
        raise ValueError("捕获文件SHA与index不同")
    return torch.load(path, map_location="cpu", weights_only=True)


def compare_capture(torch, reference, candidate, kind, path):
    if kind == "observed":
        rows = compare_value(torch, reference["kinds"], candidate["kinds"], "exact", path + "/kinds")
        if reference["values"].keys() != candidate["values"].keys():
            rows.append({"path": path, "pass": False, "reason": "observed-fields"})
        for name in reference["values"].keys() & candidate["values"].keys():
            rows.extend(compare_value(torch, reference["values"][name], candidate["values"][name],
                                      reference["kinds"][name], path + "/" + name))
        return rows
    if kind == "coverage":
        changed = [name for name in reference["parameters"].keys() & candidate["parameters"].keys()
                   if (reference["parameters"][name]["present"], reference["parameters"][name]["zero"])
                   != (candidate["parameters"][name]["present"], candidate["parameters"][name]["zero"])]
        return [{"path": path, "pass": bool(reference["pass"] and candidate["pass"]),
                 "gradient_presence_or_zero_changes": changed,
                 "reference_required": reference["required"], "candidate_required": candidate["required"]}]
    if kind == "diagnostic":
        return [{"path": path, "pass": all(row["pass"] for row in reference + candidate),
                 "check": "both_same_gradient_adam_comparisons_pass"}]
    return compare_value(torch, reference, candidate, kind, path)


def compare_slots(reference_root, candidate_root, destination):
    """读取既有小捕获；不装载Policy、不执行forward或Adam，不扩大任何数值门。"""
    import torch
    reference, candidate = capture_index(reference_root), capture_index(candidate_root)
    rmeta, cmeta = reference["identities"][0], candidate["identities"][0]
    if any(rmeta[key] != cmeta[key] for key in ("observer", "method", "execution_method", "capture_method",
                                               "auxiliary_loss_ast_sha256")):
        raise ValueError("比较的观测包装或研究方法绑定不同")
    stop = candidate["slot"] in (9, 10)
    rows, compared, scope_exclusions = [], [], []
    for key, alternatives in candidate["records"].items():
        if stop and reference["slot"] not in (9, 10) and key == "final_checkpoint_fields":
            scope_exclusions.append({"key": key, "reason": "stop state is checked against its own preupdate state; normal reference has two updates"})
            continue
        canonical = key.replace("binding/L65-cold/", "binding/L65/")
        if canonical not in reference["records"]:
            rows.append({"path": key, "pass": False, "reason": "reference-key-missing"})
            continue
        original = reference["records"][canonical][0]
        expected = load_capture(torch, original)
        for actual in alternatives:
            if actual["kind"] != original["kind"]:
                rows.append({"path": key, "pass": False, "reason": "capture-kind"})
                continue
            value = load_capture(torch, actual)
            rows.extend(compare_capture(torch, expected, value, original["kind"], key))
        compared.append(canonical)
    if not stop:
        missing = reference["records"].keys() - set(compared)
        rows.extend({"path": key, "pass": False, "reason": "candidate-key-missing"} for key in missing)
    elif reference["slot"] not in (9, 10):
        # 此唯一停止点只覆盖首32步，且没有实际/参考Adam；不声称完成正常槽的数值覆盖。
        required = {key for key in reference["records"] if key.startswith(("collect/", "initial/", "binding/"))}
        required.update("learn/1/" + name for name in ["L65/%03d" % i for i in range(32)])
        rows.extend({"path": key, "pass": False, "reason": "stop-prefix-missing"}
                    for key in required - set(compared))
    count_pass = all(reference["count_gates"].values()) and all(candidate["count_gates"].values())
    report = {"version": VERSION, "reference_slot": reference["slot"], "candidate_slot": candidate["slot"],
              "coverage": "declared_stop_after_first_32_only" if stop else "frozen_normal_slot",
              "pass": count_pass and all(row["pass"] for row in rows),
              "reference_counts": reference["counts"], "candidate_counts": candidate["counts"],
              "reference_count_gates": reference["count_gates"], "candidate_count_gates": candidate["count_gates"],
              "scope_exclusions": scope_exclusions,
              "comparisons": rows,
              "optimizer_state_note": "No new tolerance: any state difference requires source-specific review."}
    write_json_new(destination, report)
    print(json.dumps({"pass": report["pass"], "reference_slot": reference["slot"],
                      "candidate_slot": candidate["slot"], "report": str(destination)}, ensure_ascii=False))


if __name__ == "__main__":
    if len(sys.argv) == 5 and sys.argv[1] == "serve":
        serve(*sys.argv[2:])
    elif len(sys.argv) == 5 and sys.argv[1] == "compare":
        compare_slots(*sys.argv[2:])
    else:
        raise SystemExit("用法: performance_validation.py serve <模型> <设备> <线程数> | compare <参考短槽> <候选短槽> <报告>")
