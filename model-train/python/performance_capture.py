"""#377 有界短验证的只读观测包装；导入本模块不导入 torch 或装载模型。

主路径仍调用被绑定源码的 Service/LearningEngine。hook 返回 None，不替换算子结果。
辅助梯度遵循 PyTorch 2.8 autograd.grad 的 retain_graph/allow_unused 语义。
"""
import copy
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import struct
import sys
import time


CAPTURE_VERSION = "b-performance-capture-v1"
TOLERANCES = {
    "activation": (1e-6, 1e-5),
    "probability": (1e-7, 1e-5),
    "loss": (1e-7, 1e-4),
    "gradient": (1e-7, 1e-3),
    "parameter": (1e-6, 1e-5),
    "exact": (0.0, 0.0),
}


def cpu_tree(torch, value):
    """只复制实际观测值；不保留图、CUDA storage或可变参数别名。"""
    if isinstance(value, torch.Tensor):
        return value.detach().to(device="cpu", copy=True).contiguous()
    if isinstance(value, dict):
        return {key: cpu_tree(torch, item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return type(value)(cpu_tree(torch, item) for item in value)
    if isinstance(value, str):
        return str(value)
    if value is None or isinstance(value, (int, float, bool)):
        return value
    raise TypeError("未声明的捕获类型: " + type(value).__name__)


RUNTIME_OBSERVATION_VERSION = "b-runtime-state-observation-v1"


def _observation_metadata(torch, value):
    """JSON metadata without reading a CUDA tensor scalar or choosing a backend."""
    if isinstance(value, torch.Tensor):
        return {"type": "tensor", "shape": list(value.shape), "dtype": str(value.dtype),
                "device": str(value.device), "value": "in_tensor_state_artifact"}
    if isinstance(value, dict):
        return {str(key): _observation_metadata(torch, item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_observation_metadata(torch, item) for item in value]
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return {"type": type(value).__module__ + "." + type(value).__qualname__, "value": "unknown"}


def optimizer_id_mapping(torch, model, optimizer, serialized_state=None, gradients=None):
    """Observe actual group IDs and Parameter identities; gaps are data, not new gates."""
    if optimizer is None:
        return {"applicability": "not_applicable", "optimizer_type": None, "groups": [],
                "defaults": None, "runtime_dispatch": "unknown", "unmapped_state_ids": [],
                "group_length_mismatches": [], "unnamed_runtime_parameters": [],
                "named_parameters_outside_optimizer": []}
    state = optimizer.state_dict() if serialized_state is None else serialized_state
    aliases = {}
    for name, parameter in model.named_parameters(remove_duplicate=False):
        aliases.setdefault(id(parameter), []).append(name)
    serialized_groups = state.get("param_groups", [])
    runtime_groups = optimizer.param_groups
    groups, mismatches, unnamed, used_ids, used_parameters = [], [], [], set(), set()
    for group_index in range(max(len(serialized_groups), len(runtime_groups))):
        saved = serialized_groups[group_index] if group_index < len(serialized_groups) else {}
        runtime = runtime_groups[group_index] if group_index < len(runtime_groups) else {}
        identifiers, parameters = saved.get("params", []), runtime.get("params", [])
        if len(identifiers) != len(parameters):
            mismatches.append({"group_index": group_index, "serialized": len(identifiers),
                               "runtime": len(parameters)})
        entries = []
        for position in range(max(len(identifiers), len(parameters))):
            state_id = identifiers[position] if position < len(identifiers) else None
            parameter = parameters[position] if position < len(parameters) else None
            names = aliases.get(id(parameter), []) if parameter is not None else []
            if position < len(identifiers):
                used_ids.add(state_id)
            if parameter is not None:
                used_parameters.add(id(parameter))
                if not names:
                    unnamed.append({"group_index": group_index, "position": position})
            present = state_id in state.get("state", {}) if position < len(identifiers) else False
            gradient = None if parameter is None else parameter.grad
            evidence = next((gradients[name] for name in names if gradients and name in gradients), None)
            if evidence is None:
                evidence = {"present": gradient is not None, "zero": None,
                            "zero_status": "not_evaluated" if gradient is not None else "not_applicable"}
            entries.append({"position": position, "state_id": state_id,
                            "parameter_name": names[0] if names else None, "aliases": names,
                            "shape": list(parameter.shape) if parameter is not None else None,
                            "dtype": str(parameter.dtype) if parameter is not None else None,
                            "requires_grad": parameter.requires_grad if parameter is not None else None,
                            "has_state": present,
                            "state_fields": sorted(str(key) for key in state["state"][state_id]) if present else [],
                            "gradient": evidence})
        groups.append({"group_index": group_index, "serialized_ids": list(identifiers),
                       "runtime_parameter_count": len(parameters), "entries": entries,
                       "configuration": _observation_metadata(torch, {key: value for key, value in saved.items()
                                                                         if key != "params"})})
    return {"applicability": "present", "optimizer_type": type(optimizer).__module__ + "." + type(optimizer).__qualname__,
            "groups": groups, "defaults": _observation_metadata(torch, optimizer.defaults),
            "runtime_dispatch": "unknown",
            "unmapped_state_ids": [key for key in state.get("state", {}) if key not in used_ids],
            "group_length_mismatches": mismatches, "unnamed_runtime_parameters": unnamed,
            "named_parameters_outside_optimizer": [names for key, names in aliases.items() if key not in used_parameters]}


def _tensor_payload(torch, value):
    if isinstance(value, torch.Tensor):
        size = value.numel() * value.element_size()
        return size, size if value.device.type == "cuda" else 0, 1
    if isinstance(value, dict):
        children = value.values()
    elif isinstance(value, (tuple, list)):
        children = value
    else:
        return 0, 0, 0
    parts = [_tensor_payload(torch, item) for item in children]
    return tuple(sum(part[index] for part in parts) for index in range(3))


def initial_runtime_snapshot(torch, service, optimizer, role, phase, point):
    """One independent CPU snapshot, without initializing or advancing generators."""
    started = time.monotonic()
    optimizer_state = None if optimizer is None else optimizer.state_dict()
    # is_initialized only inspects existing host state; no generator getter is
    # called for a CPU service or an uninitialized CUDA service.
    cuda = {"initialized": bool(torch.cuda.is_initialized()), "scope": "not_applicable", "states": []}
    if service.device.type == "cuda":
        cuda["scope"] = "actual_initialized_service_device" if cuda["initialized"] else "not_initialized"
        if cuda["initialized"]:
            device = service.device.index
            if device is None:
                device = torch.cuda.current_device()
            cuda["states"].append({"device": device, "state": torch.cuda.get_rng_state(device)})
    sampling = {name: {"calls": account["calls"], "rng": account["rng"].get_state()}
                for name, account in service.accounts.items()}
    raw = {"version": RUNTIME_OBSERVATION_VERSION, "role": role, "phase": phase, "point": point,
           "model": service.model.state_dict(), "optimizer": optimizer_state,
           "optimizer_applicability": "not_applicable" if optimizer is None else "present",
           "optimizer_defaults": None if optimizer is None else optimizer.defaults,
           "torch_rng": torch.get_rng_state(), "cuda_rng": cuda, "account_sampling": sampling,
           "gradients": {name: parameter.grad for name, parameter in service.model.named_parameters()}}
    learning = service.learning
    group = getattr(learning, "group", None)
    raw["source_boundary"] = {"group_id": group.get("id") if isinstance(group, dict) else None,
                              "sequence": getattr(learning, "sequence", None),
                              "checkpoint_sha256": getattr(service, "identity", {}).get("checkpoint_sha256")}
    payload, d2h, _ = _tensor_payload(torch, raw)
    # state_dict is a shallow view; cpu_tree supplies all owned storage in one pass.
    snapshot = cpu_tree(torch, raw)
    gradients = {name: {"present": value is not None,
                        "zero": None if value is None else not bool(torch.count_nonzero(value)),
                        "zero_status": "not_applicable" if value is None else "observed_on_cpu"}
                 for name, value in snapshot["gradients"].items()}
    mapping = optimizer_id_mapping(torch, service.model, optimizer, optimizer_state, gradients)
    snapshot["optimizer_id_mapping"] = mapping
    metadata = {"version": RUNTIME_OBSERVATION_VERSION, "record_kind": "initial_state", "role": role,
                "phase": phase, "point": point, "event_monotonic": started,
                "tensor_payload_bytes": payload, "cuda_tensor_payload_bytes": d2h,
                "parameter_count": len(list(service.model.named_parameters())),
                "buffer_count": len(list(service.model.named_buffers())),
                "optimizer_state_tensor_count": _tensor_payload(torch, optimizer_state)[2],
                "service_device": str(service.device),
                "model_schema": {name: _observation_metadata(torch, value)
                                 for name, value in snapshot["model"].items()},
                "gradient_status": gradients, "cuda_rng_scope": cuda["scope"],
                "account_sampling_status": "not_constructed" if not sampling else "observed",
                "optimizer_id_mapping": mapping}
    return snapshot, metadata


def gradient_summary(torch, gradients):
    result = {}
    for name, gradient in gradients.items():
        if gradient is None:
            result[name] = {"present": False, "zero": None, "norm": None}
        else:
            value = gradient.detach().cpu()
            result[name] = {"present": True, "zero": not bool(torch.count_nonzero(value)),
                            "norm": float(value.double().norm()), "shape": list(value.shape),
                            "dtype": str(value.dtype), "finite": bool(torch.isfinite(value).all())}
    return result


def compare_value(torch, reference, candidate, kind="exact", path=""):
    """逐叶比较，缺键、None/零、shape和dtype差异均保留；不扩大容差。"""
    rows = []
    if isinstance(reference, torch.Tensor) or isinstance(candidate, torch.Tensor):
        if not isinstance(reference, torch.Tensor) or not isinstance(candidate, torch.Tensor):
            return [{"path": path, "pass": False, "reason": "tensor/type"}]
        same = reference.shape == candidate.shape and reference.dtype == candidate.dtype
        if not same:
            return [{"path": path, "pass": False, "reason": "shape/dtype",
                     "reference": [list(reference.shape), str(reference.dtype)],
                     "candidate": [list(candidate.shape), str(candidate.dtype)]}]
        if not reference.is_floating_point():
            return [{"path": path, "pass": bool(torch.equal(reference, candidate)),
                     "kind": "exact", "elements": reference.numel()}]
        if not bool(torch.isfinite(reference).all() and torch.isfinite(candidate).all()):
            return [{"path": path, "pass": False, "reason": "nonfinite"}]
        r, c = reference.double(), candidate.double()
        absolute = (c - r).abs()
        atol, rtol = TOLERANCES[kind]
        allowed = atol + rtol * r.abs()
        relative_domain = r.abs() > (atol / rtol if rtol else 0.0)
        relative = absolute[relative_domain] / r.abs()[relative_domain]
        rows.append({"path": path, "kind": kind,
                     "pass": bool(torch.equal(reference, candidate)) if kind == "exact"
                     else bool((absolute <= allowed).all()),
                     "max_abs": float(absolute.max()) if absolute.numel() else 0.0,
                     "max_relative": float(relative.max()) if relative.numel() else None,
                     "relative_elements": relative.numel(),
                     "near_zero_elements": reference.numel() - relative.numel(),
                     "norm_difference": float(c.norm() - r.norm()),
                     "atol": atol, "rtol": rtol})
        return rows
    if isinstance(reference, dict) or isinstance(candidate, dict):
        if not isinstance(reference, dict) or not isinstance(candidate, dict):
            return [{"path": path, "pass": False, "reason": "mapping/type"}]
        if reference.keys() != candidate.keys():
            rows.append({"path": path, "pass": False, "reason": "keys",
                         "missing": sorted(str(k) for k in reference.keys() - candidate.keys()),
                         "extra": sorted(str(k) for k in candidate.keys() - reference.keys())})
        for key in reference.keys() & candidate.keys():
            rows.extend(compare_value(torch, reference[key], candidate[key], kind, path + "/" + str(key)))
        return rows
    if isinstance(reference, (list, tuple)) or isinstance(candidate, (list, tuple)):
        if type(reference) is not type(candidate) or len(reference) != len(candidate):
            return [{"path": path, "pass": False, "reason": "sequence/type/length"}]
        for index, (r, c) in enumerate(zip(reference, candidate)):
            rows.extend(compare_value(torch, r, c, kind, path + "/" + str(index)))
        return rows
    if type(reference) is not type(candidate):
        return [{"path": path, "pass": False, "reason": "scalar/type"}]
    if isinstance(reference, float):
        if not math.isfinite(reference) or not math.isfinite(candidate):
            return [{"path": path, "pass": False, "reason": "nonfinite"}]
        atol, rtol = TOLERANCES[kind]
        difference = abs(candidate - reference)
        passed = (struct.pack("!d", reference) == struct.pack("!d", candidate)
                  if kind == "exact" else difference <= atol + rtol * abs(reference))
        return [{"path": path, "pass": passed, "kind": kind, "max_abs": difference,
                 "max_relative": difference / abs(reference)
                 if abs(reference) > (atol / rtol if rtol else 0.0) else None,
                 "atol": atol, "rtol": rtol}]
    return [{"path": path, "pass": reference == candidate, "kind": "exact"}]


class WorkCounter:
    """先计发起再进实际算子；外层监督仍由开发主协调负责。"""

    JOURNAL_VERSION = "b-work-journal-v2"

    def __init__(self, directory, deadline, planned_forwards, planned_adam, identity, started,
                 additional_limits=None):
        self.deadline = deadline
        self.started = started
        self.identity = identity
        self.counts = {}
        self.limits = {"main_gru_frames": min(512, planned_forwards),
                       "actual_adam": min(2, planned_adam),
                       "reference_adam": min(2, planned_adam)}
        if additional_limits:
            self.limits.update(additional_limits)
        self.log = (Path(directory) / "work.jsonl").open("a+b", buffering=0)
        self._journal_format = None
        self.charge("worker_opens")

    def check(self):
        if time.monotonic() >= self.deadline:
            raise TimeoutError("短槽已到单调截止；不得增加计算")

    def _append(self, record):
        self.log.seek(0, os.SEEK_END)
        payload = (json.dumps(record, separators=(",", ":")) + "\n").encode()
        while payload:
            written = self.log.write(payload)
            if not written:
                raise OSError("短槽计数写入未推进")
            payload = payload[written:]

    def _bind_journal(self, length):
        # 同一追加文件的首条不再变化；每个独立打开的 worker 在原排他锁内核对一次。
        # 后续仍逐 charge 读取尾累计，不缓存文件 offset 或另一份持久计数。
        binding = {"version": self.JOURNAL_VERSION, "identity": self.identity,
                   "deadline": self.deadline, "limits": self.limits}
        if not length:
            self._append(binding)
            self._journal_format = 2
            return
        self.log.seek(0)
        prefix = self.log.read(min(length, 65536))
        if b"\n" not in prefix:
            raise ValueError("短槽计数首条未完整落盘")
        first = json.loads(prefix.split(b"\n", 1)[0])
        if (first.get("identity") != self.identity or first.get("deadline") != self.deadline
                or first.get("limits") != self.limits):
            raise ValueError("跨worker短槽计数绑定不同")
        if first.get("version") == self.JOURNAL_VERSION:
            self._journal_format = 2
        elif "counts" in first:
            # 原完整事件格式仍可读写；旧失败的恢复权限不由此处授予。
            self._journal_format = 1
        else:
            raise ValueError("短槽计数首条格式未知")

    def charge(self, name, amount=1):
        self.check()
        fcntl.flock(self.log.fileno(), fcntl.LOCK_EX)
        try:
            self.log.seek(0, os.SEEK_END)
            length = self.log.tell()
            if self._journal_format is None:
                self._bind_journal(length)
            if length:
                self.log.seek(max(0, length - 65536))
                tail = self.log.read(65536)
                if not tail.endswith(b"\n"):
                    raise ValueError("短槽计数末条未完整落盘")
                previous = json.loads(tail.splitlines()[-1])
                if self._journal_format == 1 and (
                        previous["identity"] != self.identity or previous["deadline"] != self.deadline
                        or previous["limits"] != self.limits):
                    raise ValueError("跨worker短槽计数绑定不同")
                self.counts = {} if previous.get("version") == self.JOURNAL_VERSION else previous["counts"]
            following = self.counts.get(name, 0) + int(amount)
            if name in self.limits and following > self.limits[name]:
                raise RuntimeError("短槽工作量超出冻结计划: " + name)
            self.counts[name] = following
            event = {"kind": name, "amount": int(amount), "counts": self.counts}
            if self._journal_format == 1:
                event.update(identity=self.identity, deadline=self.deadline, limits=self.limits)
            event.update(pid=os.getpid(), elapsed=time.monotonic() - self.started)
            self._append(event)
        finally:
            fcntl.flock(self.log.fileno(), fcntl.LOCK_UN)

    def close(self):
        self.log.close()


class _BoundedWriter:
    def __init__(self, stream, owner):
        self.stream, self.owner = stream, owner

    def write(self, data):
        if self.owner.bytes + memoryview(data).nbytes > self.owner.maximum:
            raise RuntimeError("捕获产物超出冻结字节上限")
        written = self.stream.write(data)
        self.owner.bytes += written
        return written

    def flush(self):
        self.stream.flush()

    def tell(self):
        return self.stream.tell()


class CaptureWriter:
    """一次事件一个小捕获，参数/梯度只存方法要求的版本；不依赖NumPy。"""

    def __init__(self, torch, directory, maximum_bytes):
        self.torch = torch
        self.directory = Path(directory)
        self.directory.mkdir()
        self.maximum, self.bytes, self.count = maximum_bytes, 0, 0
        self.keys = set()
        self.index = (self.directory / "index.jsonl").open("x", buffering=1)

    def write(self, key, value, kind):
        if key in self.keys:
            raise ValueError("捕获身份重复: " + key)
        name = "%06d.pt" % self.count
        path = self.directory / name
        with path.open("xb") as stream:
            self.torch.save(cpu_tree(self.torch, value), _BoundedWriter(stream, self))
        digest = hashlib.sha256()
        with path.open("rb") as stream:
            for block in iter(lambda: stream.read(1 << 20), b""):
                digest.update(block)
        self.index.write(json.dumps({"key": key, "file": name, "kind": kind,
                                     "bytes": path.stat().st_size,
                                     "sha256": digest.hexdigest()}, ensure_ascii=False) + "\n")
        self.keys.add(key)
        self.count += 1

    def write_metadata(self, name, value):
        if Path(name).name != name:
            raise ValueError("结构观察文件名越界")
        encoded = (json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False) + "\n").encode()
        with (self.directory / name).open("ab") as stream:
            _BoundedWriter(stream, self).write(encoded)

    def write_initial_observation(self, snapshot, metadata):
        """Owned CPU data only; deliberately absent from the comparison index."""
        path = self.directory / "initial-state.pt"
        with path.open("xb") as stream:
            self.torch.save(snapshot, _BoundedWriter(stream, self))
        digest = hashlib.sha256()
        with path.open("rb") as stream:
            for block in iter(lambda: stream.read(1 << 20), b""):
                digest.update(block)
        record = dict(metadata, file=path.name, bytes=path.stat().st_size, sha256=digest.hexdigest(),
                      recording_elapsed_seconds=time.monotonic() - metadata["event_monotonic"])
        self.write_metadata("runtime-observations.jsonl", record)
        return record

    def close(self):
        self.index.close()


class BufferObserver:
    """仅验证真实缓存回调；不持有Tensor/Event，不替生产者安排同步或复制。"""

    def __init__(self, torch, writer):
        self.torch, self.writer = torch, writer
        self.path = writer.directory / "buffer-events.jsonl"
        self.events = 0
        self.slots = {}
        self.generations = {}
        self.host_ready = set()
        self.reuse_dependencies = set()
        self.live = {}
        self.peak_live_bytes = 0
        self.peak_live_accounts = 0
        self.counts = {}
        self.copy_stream = None

    @staticmethod
    def stream(value):
        return {"device": str(value.device), "cuda_stream": int(value.cuda_stream)}

    def tensor(self, value):
        if not isinstance(value, self.torch.Tensor):
            raise TypeError("buffer事实未提供实际Tensor")
        storage = value.untyped_storage()
        result = {"device": str(value.device), "shape": list(value.shape),
                  "dtype": str(value.dtype), "bytes": value.numel() * value.element_size(),
                  "base_pointer": storage.data_ptr(), "storage_bytes": storage.nbytes(),
                  "storage_offset": value.storage_offset(), "view": value._base is not None,
                  "inference": value.is_inference(), "requires_grad": value.requires_grad,
                  "pinned": value.is_pinned() if value.device.type == "cpu" else False}
        if result["inference"]:
            raise RuntimeError("流式暂存或图输入是inference Tensor")
        return result

    def columns(self, values):
        result = []
        def visit(value):
            if isinstance(value, self.torch.Tensor):
                result.append(self.tensor(value))
            elif isinstance(value, (list, tuple)):
                for item in value:
                    visit(item)
            else:
                raise TypeError("图输入列的结构未声明")
        visit(values)
        return result

    @staticmethod
    def key(column):
        return (column["device"], column["base_pointer"], column["storage_offset"],
                tuple(column["shape"]), column["dtype"])

    def __call__(self, record):
        event = record["event"]
        row = {"event": event, "ordinal": self.events}
        self.counts[event] = self.counts.get(event, 0) + 1
        if event == "allocate":
            if self.slots or len(record["slots"]) != 2:
                raise RuntimeError("流式槽必须恰好一次分配两个独立host/raw base")
            for slot in record["slots"]:
                key = slot["slot"]
                host, raw = self.tensor(slot["host"]), self.tensor(slot["raw"])
                if (key in self.slots or host["view"] or raw["view"] or not host["pinned"]
                        or host["device"] != "cpu" or not raw["device"].startswith("cuda")):
                    raise RuntimeError("流式槽不是独立普通pinned CPU/GPU base")
                self.slots[key] = {"host": host, "raw": raw}
            if (len({slot["host"]["base_pointer"] for slot in self.slots.values()}) != 2
                    or len({slot["raw"]["base_pointer"] for slot in self.slots.values()}) != 2):
                raise RuntimeError("两个流式槽共用base storage")
            self.copy_stream = self.stream(record["copy_stream"])
            row.update({"slots": self.slots, "copy_stream": self.copy_stream})
        elif event == "host_reuse_ready":
            slot, generation = record["slot"], record["generation"]
            complete = record["ready_event"].query()
            if record["completed"] is not True or not complete:
                raise RuntimeError("CPU复写pinned前原H2D尚未实际完成")
            self.host_ready.add((slot, generation))
            row.update({"slot": slot, "generation": generation, "ready_completed": complete,
                        "ready_event_object": id(record["ready_event"])})
        elif event == "reuse_wait_submitted":
            slot, generation = record["slot"], record["previous_generation"]
            previous = self.generations.get((slot, generation))
            if previous is None or not previous.get("owned_copy_submitted"):
                raise RuntimeError("raw复用没有上一代独占列复制事件")
            if id(record["owned_event"]) != previous["owned_event_object"]:
                raise RuntimeError("raw复用依赖了错误代次的owned事件")
            self.reuse_dependencies.add((slot, generation))
            row.update({"slot": slot, "previous_generation": generation,
                        "owned_event_object": id(record["owned_event"]),
                        "owned_completed_at_wait_submission": record["owned_event"].query(),
                        "wait_submitted": True, "copy_stream": self.stream(record["copy_stream"])})
        elif event == "h2d_submitted":
            slot, generation = record["slot"], record["generation"]
            if slot not in self.slots or (slot, generation) in self.generations:
                raise RuntimeError("流式H2D的slot/代次没有唯一分配")
            prior = [number for (key, number) in self.generations if key == slot]
            if prior:
                previous = max(prior)
                if (slot, previous) not in self.host_ready or (slot, previous) not in self.reuse_dependencies:
                    raise RuntimeError("流式复用缺实际host完成或已提交的raw依赖")
                if generation <= previous:
                    raise RuntimeError("流式代次未严格推进")
            host, raw = self.tensor(record["host"]), self.tensor(record["raw"])
            if (host["base_pointer"] != self.slots[slot]["host"]["base_pointer"]
                    or raw["base_pointer"] != self.slots[slot]["raw"]["base_pointer"]):
                raise RuntimeError("运行中替换了冻结流式base")
            count = record["bytes"]
            if count <= 0 or count > min(host["storage_bytes"], raw["storage_bytes"]):
                raise RuntimeError("流式H2D字节没有落在实际独立槽内")
            prefetched = record.get("prefetch", False)
            if prefetched:
                self.counts["prefetch_h2d_submitted"] = self.counts.get("prefetch_h2d_submitted", 0) + 1
            state = {"bytes": count, "ready_event_object": id(record["ready_event"]), "prefetch": prefetched}
            self.generations[(slot, generation)] = state
            row.update(dict(record, host=host, raw=raw,
                            ready_event=id(record["ready_event"]), copy_stream=self.stream(record["copy_stream"])))
            row["ready_completed_at_submission"] = record["ready_event"].query()
        elif event == "owned_copy_submitted":
            slot, generation = record["slot"], record["generation"]
            state = self.generations.get((slot, generation))
            if state is None or state.get("owned_copy_submitted") or record["bytes"] != state["bytes"]:
                raise RuntimeError("独占列复制没有对应本代H2D")
            if record.get("prefetch", False) != state["prefetch"]:
                raise RuntimeError("预取raw在消费时改成了不同代次/用途")
            if state["prefetch"]:
                self.counts["prefetch_owned_copy_submitted"] = self.counts.get("prefetch_owned_copy_submitted", 0) + 1
            columns = self.columns(record["owned_columns"])
            raw = self.tensor(record["raw"])
            if raw["base_pointer"] != self.slots[slot]["raw"]["base_pointer"]:
                raise RuntimeError("独占列复制使用了错误raw槽")
            bases = {value["raw"]["base_pointer"] for value in self.slots.values()}
            if any(column["device"] != raw["device"] for column in columns):
                raise RuntimeError("流式图输入列不在同一实际CUDA设备")
            if any(column["base_pointer"] in bases for column in columns):
                raise RuntimeError("图输入列仍别名GPU raw暂存槽")
            compute_stream = self.stream(record["compute_stream"])
            if compute_stream == self.copy_stream:
                raise RuntimeError("真实传输与计算未采用两个CUDA stream")
            state.update({"owned_copy_submitted": True, "owned_event_object": id(record["owned_event"])})
            row.update(dict(record, raw=raw, owned_columns=columns,
                            owned_event=id(record["owned_event"]), compute_stream=compute_stream))
            row["owned_completed_at_submission"] = record["owned_event"].query()
        elif event in ("graph_inputs_retained", "graph_inputs_released"):
            account = record["account"]
            columns = self.columns(record["owned_columns"])
            if record["bytes"] != sum(column["bytes"] for column in columns):
                raise RuntimeError("图输入事件字节与实际Tensor列不同")
            if event == "graph_inputs_retained":
                self.live.setdefault(account, []).extend(columns)
            else:
                known = self.live.get(account, [])
                for column in columns:
                    match = next((index for index, value in enumerate(known)
                                  if self.key(value) == self.key(column)), None)
                    if match is None:
                        raise RuntimeError("原backward后的显式释放列不属于实际保留集合")
                    known.pop(match)
                if not known:
                    self.live.pop(account, None)
            live = sum(column["bytes"] for values in self.live.values() for column in values)
            if live != record["live_bytes"]:
                raise RuntimeError("真实图输入留存量与逐列观测不同")
            self.peak_live_bytes = max(self.peak_live_bytes, live)
            self.peak_live_accounts = max(self.peak_live_accounts, len(self.live))
            row.update({"account": account, "owned_columns": columns, "bytes": record["bytes"],
                        "live_bytes": live, "backward_completion": "not_asserted"})
        else:
            raise ValueError("未冻结的buffer观察事件: " + str(event))
        self.writer.write_metadata("buffer-events.jsonl", row)
        self.events += 1

    def summary(self):
        return {"path": str(self.path) if self.events else None, "events": self.events, "counts": dict(self.counts),
                "allocated_slots": len(self.slots),
                "generations": len(self.generations),
                "uploaded_generations_not_yet_consumed": sum(not value.get("owned_copy_submitted", False) for value in self.generations.values()),
                "slots_used": sorted({key for key, _ in self.generations}),
                "peak_explicit_graph_input_bytes": self.peak_live_bytes,
                "peak_live_accounts": self.peak_live_accounts,
                "remaining_explicit_graph_input_bytes": sum(column["bytes"] for values in self.live.values() for column in values),
                "limits": "observed submission/completion at callback; event objects not queried after generation reuse; graph release means backward submitted, not device complete"}


def projection_feature_evidence(projected, features):
    """Only inspect the two actual tensors; no clone, forward or equality reduction."""
    def layout(value):
        return {"shape": list(value.shape), "stride": list(value.stride()),
                "dtype": str(value.dtype), "device": str(value.device),
                "storage_pointer": value.untyped_storage().data_ptr(),
                "storage_offset": value.storage_offset(),
                "numel": value.numel(), "contiguous": value.is_contiguous(),
                "ordinary_tensor": not value.is_inference()}
    left, right = layout(projected), layout(features)
    alias = (left["storage_pointer"] == right["storage_pointer"]
             and left["storage_offset"] == right["storage_offset"]
             and left["numel"] == right["numel"] and left["dtype"] == right["dtype"]
             and left["device"] == right["device"] and left["contiguous"] and right["contiguous"])
    return {"projection": left, "gru_features": right, "same_contiguous_view": alias,
            "stored_value_objects": 1 if alias else 2}


class CaptureObserver:
    """真实算子的观测包装。B可调用learning_result，不能用回调替代实际算子。"""

    def __init__(self, torch, service, writer, counter, stage):
        self.torch, self.service = torch, service
        self.writer, self.counter, self.stage = writer, counter, stage
        self.engine = service.learning
        self.current = None
        self.values = {}
        self.handles, self.restore = [], []
        self.f4_keys = {}
        self.auxiliary = None
        self.graph = None
        self.step_keys = {}
        self.active_frame = None
        self.sequence = None
        self.capture_seconds = 0.0
        self.observe_projection_features = False
        self.projected_input = None
        self._install()

    def _replace(self, owner, name, function):
        old = getattr(owner, name)
        self.restore.append((owner, name, old))
        setattr(owner, name, function)
        return old

    def begin(self, key):
        if self.current is not None:
            raise RuntimeError("未完成上一捕获事件")
        self.current, self.values, self.graph = key, {}, None

    def observe(self, name, value, kind="activation"):
        if self.current is None:
            raise RuntimeError("实际模型调用没有绑定捕获身份")
        started = time.perf_counter()
        self.values.setdefault((name, kind), []).append(cpu_tree(self.torch, value))
        self.capture_seconds += time.perf_counter() - started

    def _sequence_observe(self, index, name, value, kind="activation"):
        started = time.perf_counter()
        self.sequence["values"][index].setdefault((name, kind), []).append(cpu_tree(self.torch, value))
        self.capture_seconds += time.perf_counter() - started

    def _sequence_rows(self, name, value):
        count = len(self.sequence["frames"])
        if value.ndim == 3:
            for index, coordinate in enumerate(self.sequence["coordinates"]):
                self._sequence_observe(index, name, value[coordinate["batch"], coordinate["time"]])
            return
        rows = math.prod(value.shape[:-1]) if value.ndim else 1
        if rows == count:
            values = value.reshape(count, -1)
            for index in range(count):
                self._sequence_observe(index, name, values[index])
        elif rows == 1:
            index = self.sequence["cursors"].get(name, 0)
            if index >= count:
                raise RuntimeError("序列观察的逐帧算子数超过声明样本")
            self._sequence_observe(index, name, value.reshape(-1))
            self.sequence["cursors"][name] = index + 1
        else:
            raise RuntimeError("实际序列算子shape没有对应冻结样本坐标")

    def _full_hidden(self, frame, before=False):
        return ((frame["account"], frame["call"]) in self.f4_keys
                or frame["call"] in ((1, 33, 65) if before else (32, 64, 65)))

    def begin_sequence(self, frames, coordinates):
        """B在实际编码/GRU前调用；coordinates为每帧的batch/time及可选data_index。"""
        if self.current is not None or self.sequence is not None or len(frames) != len(coordinates):
            raise RuntimeError("序列捕获生命周期或坐标长度不符")
        keys = [(frame["account"], frame["call"]) for frame in frames]
        if len(set(keys)) != len(keys) or any(key not in self.step_keys for key in keys):
            raise ValueError("序列样本身份不属于冻结集合")
        if len({(item["batch"], item["time"]) for item in coordinates}) != len(coordinates):
            raise ValueError("序列GRU坐标重复")
        self.sequence = {"frames": frames, "coordinates": coordinates,
                         "positions": {key: index for index, key in enumerate(keys)},
                         "values": [{} for _ in frames], "finished": set(), "cursors": {},
                         "heads_before": self.service.metrics.get("recompute_heads", 0)}

    def sequence_sample(self, frame, label, tensors, terms):
        """B在真实该帧loss形成后、正式backward前回传同一图；不额外计算模型。"""
        if self.sequence is None or self.current is not None:
            raise RuntimeError("序列样本未绑定其实际GRU跨度")
        key = (frame["account"], frame["call"])
        expected_dtype = {"logp": self.torch.float64, "value": self.torch.float32,
                          "ratio": self.torch.float64, "actor": self.torch.float64,
                          "critic": self.torch.float32, "loss": self.torch.float64}
        devices = {}
        for name, dtype in expected_dtype.items():
            value = tensors[name]
            if value.dtype != dtype or value.device.type != "cuda":
                raise RuntimeError("B真实学习Tensor未保留设备/dtype: " + name)
            devices[name] = {"device": str(value.device), "dtype": str(value.dtype)}
        for term in terms:
            if any(not isinstance(term[name], self.torch.Tensor)
                   or term[name].device.type != "cuda" or term[name].dtype != self.torch.float64
                   for name in ("logits", "logp")):
                raise RuntimeError("B条件概率不在真实CUDA FP64支路")
        self.writer.write_metadata("device-learning.jsonl", {"epoch": self.engine.epoch,
                                   "account": key[0], "call": key[1], "tensors": devices,
                                   "probability_heads": len(terms)})
        index = self.sequence["positions"][key]
        if index in self.sequence["finished"]:
            raise ValueError("序列样本重复捕获")
        self.current = "learn/%d/%s" % (self.engine.epoch, self.step_keys[key])
        self.values = self.sequence["values"][index]
        self.observe("joint_logp", tensors["logp"], "probability")
        self.observe("teacher_value", tensors["value"])
        def plain(value):
            return value.detach().cpu().tolist() if isinstance(value, self.torch.Tensor) else value
        self.observe("head_logits", [plain(term["logits"]) for term in terms])
        self.observe("head_logp", [plain(term["logp"]) for term in terms], "probability")
        self.observe("head_discrete", [{key: term[key] for key in ("allowed", "selected")}
                                       for term in terms], "exact")
        self.learning_result(frame, label, tensors)
        self.finish()
        self.sequence["finished"].add(index)

    def end_sequence(self):
        if self.sequence is None or len(self.sequence["finished"]) != len(self.sequence["frames"]):
            raise RuntimeError("实际序列有未捕获样本")
        self.counter.charge("teacher_probability_calls",
                            self.service.metrics.get("recompute_heads", 0) - self.sequence["heads_before"])
        self.sequence = None

    def finish(self):
        key = self.current
        if key is None:
            raise RuntimeError("捕获事件未开始")
        self.writer.write(key, {"values": {name: values for (name, kind), values in self.values.items()},
                                "kinds": {name: kind for name, kind in self.values}}, "observed")
        self.current, self.values, self.graph, self.active_frame = None, {}, None, None

    def _token_bits(self, sequences):
        result = []
        for sequence in sequences:
            if hasattr(sequence, "_storage"):
                result.append({"count": len(sequence), "raw": cpu_tree(self.torch, sequence._storage)})
                continue
            n = len(sequence)
            raw = bytearray(n * 13)
            for i, (field, category, numeric, kind) in enumerate(sequence):
                struct.pack_into("<I", raw, i * 4, field)
                struct.pack_into("<I", raw, n * 4 + i * 4, category)
                struct.pack_into("<f", raw, n * 8 + i * 4, numeric)
                raw[n * 12 + i] = kind
            tensor = (self.torch.frombuffer(raw, dtype=self.torch.uint8).clone() if n
                      else self.torch.empty(0, dtype=self.torch.uint8))
            result.append({"count": n, "raw": tensor})
        return result

    def _learning_projection(self):
        return self.observe_projection_features and (self.sequence is not None
            or (self.current is not None and self.current.startswith("learn/")))

    def _install(self):
        torch, model = self.torch, self.service.model

        def gru_pre(module, args):
            value = args[0]
            if isinstance(value, torch.Tensor):
                frames = value.shape[0] * value.shape[1] if value.ndim == 3 else value.shape[0]
            else:
                frames = value.data.shape[0]
            self.counter.charge("main_gru_frames", frames)
            self.counter.charge("main_gru_calls")
            if self._learning_projection():
                projected = self.projected_input
                if projected is None:
                    raise RuntimeError("实际GRU之前没有同帧投影输出")
                evidence = projection_feature_evidence(projected, value)
                if self.sequence is None:
                    frame = self.active_frame
                    self.observe("projection_output", projected)
                    if not evidence["same_contiguous_view"]:
                        self.observe("gru_features", value.reshape(-1))
                else:
                    if len(self.sequence["frames"]) != 1:
                        raise RuntimeError("串行主干观察只适用于单帧真实GRU")
                    frame = self.sequence["frames"][0]
                    self._sequence_observe(0, "projection_output", projected)
                    if not evidence["same_contiguous_view"]:
                        self._sequence_observe(0, "gru_features", value.reshape(-1))
                self.writer.write_metadata("projection-features.jsonl", {
                    "account": frame["account"], "call": frame["call"], "batch": 0, "time": 0,
                    "epoch": self.engine.epoch, "phase": self.stage,
                    "sample": self.step_keys.get((frame["account"], frame["call"])),
                    "train": bool(model.training), "grad_enabled": bool(torch.is_grad_enabled()),
                    "precision": self.service.identity.get("numerical_runtime"), **evidence})
                self.projected_input = None
            if self.sequence is not None:
                for index, (frame, coordinate) in enumerate(zip(self.sequence["frames"], self.sequence["coordinates"])):
                    if coordinate["time"] == 0 and self._full_hidden(frame, before=True):
                        self._sequence_observe(index, "hidden_before", args[1][:, coordinate["batch"]])
            elif not self.current.startswith("learn/") or self._full_hidden(self.active_frame, before=True):
                self.observe("hidden_before", args[1][:, 0])

        def gru_after(module, args, result):
            value = args[0]
            frames = ((value.shape[0] * value.shape[1] if value.ndim == 3 else value.shape[0])
                      if isinstance(value, torch.Tensor) else value.data.shape[0])
            self.counter.charge("main_gru_completed", frames)
            self.counter.charge("main_gru_calls_completed")
            if self.sequence is not None:
                terminal = {}
                for coordinate in self.sequence["coordinates"]:
                    terminal[coordinate["batch"]] = max(terminal.get(coordinate["batch"], -1), coordinate["time"])
                for index, (frame, coordinate) in enumerate(zip(self.sequence["frames"], self.sequence["coordinates"])):
                    output = (result[0][coordinate["batch"], coordinate["time"]]
                              if isinstance(result[0], torch.Tensor) else result[0].data[coordinate["data_index"]])
                    self._sequence_observe(index, "gru_output", output)
                    if coordinate["time"] == terminal[coordinate["batch"]] and self._full_hidden(frame):
                        self._sequence_observe(index, "hidden_after", result[1][:, coordinate["batch"]])
            else:
                self.observe("gru_output", result[0][0, 0])
                if not self.current.startswith("learn/") or self._full_hidden(self.active_frame):
                    self.observe("hidden_after", result[1][:, 0])

        self.handles.extend([model.gru.register_forward_pre_hook(gru_pre),
                             model.gru.register_forward_hook(gru_after)])
        def pooled_input(module, args):
            if self.sequence is None:
                self.observe("pooled_input", args[0])
            else:
                self._sequence_rows("pooled_input", args[0])
        def value_output(module, args, output):
            if self.sequence is None:
                self.observe("value", output)
            else:
                self._sequence_rows("value", output)
        def projected_output(module, args, output):
            if self._learning_projection():
                self.projected_input = output
        self.handles.append(model.input.register_forward_hook(projected_output))
        self.handles.append(model.input.register_forward_pre_hook(pooled_input))
        self.handles.append(model.value.register_forward_hook(value_output))
        for name in ("field", "category", "kind", "numeric"):
            def input_bits(module, args, name=name):
                value = args[0]
                if name == "numeric":
                    value = value.detach().contiguous().view(torch.uint8)
                if self.sequence is None:
                    self.observe("input_" + name + "_actual", value, "exact")
                else:
                    offset = 0
                    for index, frame in enumerate(self.sequence["frames"]):
                        length = len(frame["global"]) + sum(len(sequence) for sequence in frame["markets"])
                        self._sequence_observe(index, "input_" + name + "_actual", value[offset:offset + length], "exact")
                        offset += length
                    if offset != value.shape[0]:
                        raise RuntimeError("序列token输入未按冻结帧完整覆盖")
            self.handles.append(getattr(model, name).register_forward_pre_hook(input_bits))
        for name in ("option", "bit", "decoder"):
            def pre(module, args, name=name):
                values = args[0]
                units = math.prod(values.shape[:-1]) if values.ndim > 1 else 1
                self.counter.charge(name + "_operator_calls")
                self.counter.charge(name + "_operator_units", units)
            self.handles.append(getattr(model, name).register_forward_pre_hook(pre))

        def encoding_inputs(sequences):
            bits = self._token_bits(sequences)
            if self.sequence is None:
                self.observe("input_tokens", bits, "exact")
            else:
                offset = 0
                for index, frame in enumerate(self.sequence["frames"]):
                    length = 1 + len(frame["markets"])
                    self._sequence_observe(index, "input_tokens", bits[offset:offset + length], "exact")
                    offset += length
                if offset != len(bits):
                    raise RuntimeError("序列市场/token次序未覆盖声明样本")

        def encoding_output(encoded):
            if self.sequence is None:
                self.observe("encoded", encoded)
            else:
                offset = 0
                for index, frame in enumerate(self.sequence["frames"]):
                    length = 1 + len(frame["markets"])
                    self._sequence_observe(index, "encoded", encoded[offset:offset + length])
                    offset += length
                if offset != encoded.shape[0]:
                    raise RuntimeError("序列编码输出未按市场层次覆盖声明样本")

        original_encode = model.encode
        self.encoding_wrapper_depth = 0
        def encode(sequences, device):
            encoding_inputs(sequences)
            self.encoding_wrapper_depth += 1
            try:
                result = original_encode(sequences, device)
            finally:
                self.encoding_wrapper_depth -= 1
            encoding_output(result[0])
            return result
        self._replace(model, "encode", encode)
        if hasattr(model, "encode_columns"):
            original_encode_columns = model.encode_columns
            def encode_columns(columns, lengths):
                capture = self.sequence is not None and self.encoding_wrapper_depth == 0
                if capture:
                    sequences = [value for frame in self.sequence["frames"]
                                 for value in [frame["global"]] + list(frame["markets"])]
                    if list(lengths) != [len(value) for value in sequences]:
                        raise RuntimeError("真实合并列与冻结sequence长度不同")
                    encoding_inputs(sequences)
                result = original_encode_columns(columns, lengths)
                if capture:
                    encoding_output(result)
                return result
            self._replace(model, "encode_columns", encode_columns)

        original_choose = self.service.choose
        def choose(logits, allowed, account):
            self.counter.charge("consumer_probability_calls")
            before = account["rng"].get_state().clone()
            result = original_choose(logits, allowed, account)
            self.counter.charge("cpu_sampling_calls", result[2])
            self.observe("head_logits", logits)
            self.observe("head_logp", result[1], "probability")
            self.observe("head_discrete", {"allowed": allowed, "selected": result[0],
                                           "rng_before": before, "rng_after": account["rng"].get_state()}, "exact")
            return result
        self._replace(self.service, "choose", choose)

        original_begin, original_decode = self.service.begin, self.service.decode
        collection = {}
        def finish_collection():
            self.observe("joint_logp", collection["joint"], "probability")
            self.observe("sampling_state", {"calls": collection["account"]["calls"],
                                            "rng": collection["account"]["rng"].get_state()}, "exact")
            self.finish()
            collection.clear()
        def begin(requests):
            if len(requests) != 1:
                raise RuntimeError("冻结C65/L65采集接口要求每次一个真实账户帧")
            frame = requests[0]
            sample = self.step_keys[(frame["account"], frame["call"])]
            self.begin("collect/" + sample)
            result = original_begin(requests)
            selected = result[0]["option"]
            self.observe("collection_discrete", {"account": frame["account"], "call": frame["call"],
                                                   "options": frame["options"], "selected_option": selected}, "exact")
            collection.update({"joint": result[0]["log_probability"],
                               "account": self.service.accounts[frame["account"]]})
            if frame["options"][selected]["kind"] == "NoTrade":
                finish_collection()
            return result
        def decode(request):
            self.observe("rpc_request", request, "exact")
            result = original_decode(request)
            self.observe("rpc_discrete", {key: result[key] for key in ("post_only", "index", "rank")
                                           if key in result}, "exact")
            collection["joint"] += result["log_probability"]
            if request["phase"] == "quantity":
                finish_collection()
            return result
        self._replace(self.service, "begin", begin)
        self._replace(self.service, "decode", decode)

        original_teacher = self.engine.teacher
        def teacher(frame, hidden, trace=False):
            before = self.service.metrics.get("recompute_heads", 0)
            try:
                result = original_teacher(frame, hidden, trace=True)
            finally:
                used = self.service.metrics.get("recompute_heads", 0) - before
                self.counter.charge("teacher_probability_calls", used)
            self.graph = result
            self.observe("joint_logp", result[0], "probability")
            self.observe("teacher_value", result[1])
            self.observe("head_logits", [term["logits"] for term in result[3]])
            self.observe("head_logp", [term["logp"] for term in result[3]], "probability")
            self.observe("head_discrete", [{key: term[key] for key in ("allowed", "selected")}
                                           for term in result[3]], "exact")
            return result if trace else (result[0], result[1], result[2], [])
        self._replace(self.engine, "teacher", teacher)

        original_step = self.engine.step
        code = original_step.__func__.__code__
        def step(request, audit=False):
            frame = request["frame"]
            if self.sequence is not None:
                raise RuntimeError("B序列路径不能再用逐帧step补做前向")
            sample = self.step_keys[(frame["account"], frame["call"])]
            self.begin("learn/%d/%s" % (self.engine.epoch, sample))
            self.active_frame = frame
            found = {}
            previous = sys.getprofile()
            def profile(current, event, value):
                if event == "return" and current.f_code is code:
                    found.update({key: current.f_locals[key] for key in
                                  ("logp", "value", "ratio", "actor", "critic", "loss")
                                  if key in current.f_locals})
                if previous is not None:
                    previous(current, event, value)
            sys.setprofile(profile)
            try:
                result = original_step(request, audit=audit)
            except BaseException:
                # 停止槽保留已执行第32步的实际值，不能把它从证据或计数中抹掉。
                if not audit and set(found) == {"logp", "value", "ratio", "actor", "critic", "loss"}:
                    self.learning_result(frame, request["label"], found)
                self.finish()
                raise
            finally:
                sys.setprofile(previous)
            if not audit:
                self.learning_result(frame, request["label"], found)
            self.finish()
            return result
        self._replace(self.engine, "step", step)
        original_backward = torch.Tensor.backward
        def backward(tensor, *args, **kwargs):
            self.counter.charge("production_backward")
            result = original_backward(tensor, *args, **kwargs)
            self.counter.charge("production_backward_completed")
            return result
        self._replace(torch.Tensor, "backward", backward)
        self.engine.validation_observer = self

    def named_gradients(self, loss):
        named = dict(self.service.model.named_parameters())
        active = {name: parameter for name, parameter in named.items() if parameter.requires_grad}
        before = [(id(p.grad), p.grad._version if p.grad is not None else None) for p in named.values()]
        self.counter.charge("auxiliary_autograd_grad")
        gradients = self.torch.autograd.grad(loss, tuple(active.values()), retain_graph=True,
                                             allow_unused=True, materialize_grads=False)
        self.counter.charge("auxiliary_autograd_grad_completed")
        after = [(id(p.grad), p.grad._version if p.grad is not None else None) for p in named.values()]
        if before != after:
            raise RuntimeError("辅助梯度改动了正式.grad")
        result = dict(zip(active, gradients))
        return {name: result.get(name) for name in named}

    def learning_result(self, frame, label, tensors):
        """B只读回调协议：传实际图中的logp/value/ratio/actor/critic/loss。"""
        required = {"logp", "value", "ratio", "actor", "critic", "loss"}
        if set(tensors) != required:
            raise RuntimeError("原学习计算没有提供完整可观测局部值")
        self.observe("label", label, "exact")
        self.observe("ratio", tensors["ratio"], "probability")
        for name in ("actor", "critic", "loss"):
            self.observe(name, tensors[name], "loss")
        fixture = self.f4_keys.get((frame["account"], frame["call"]))
        if fixture is not None:
            for name in ("actor", "critic", "loss"):
                gradients = cpu_tree(self.torch, self.named_gradients(tensors[name]))
                self.writer.write(self.current + "/component_gradient/" + name, gradients, "gradient")
                summary = gradient_summary(self.torch, gradients)
                required = []
                if name == "actor" and fixture in ("F-N", "F-P", "F-R"):
                    required.append(("actor", ("option.", "option_bias")))
                    if fixture in ("F-P", "F-R"):
                        required.extend((("decoder", ("decoder.",)), ("bit", ("bit.", "bit_bias"))))
                coverage = {label: any(item["present"] and not item["zero"] for parameter, item in summary.items()
                                       if parameter.startswith(prefixes)) for label, prefixes in required}
                coverage["finite"] = all(not item["present"] or item["finite"] for item in summary.values())
                self.writer.write(self.current + "/gradient_summary/" + name,
                                  {"parameters": summary, "required": coverage,
                                   "pass": all(coverage.values())}, "coverage")
                if not all(coverage.values()):
                    raise RuntimeError("F4必要Actor/条件头没有非零梯度覆盖: " + fixture)
            if self.auxiliary is None:
                raise RuntimeError("F4辅助标签方法尚未冻结")
            self.auxiliary(self, fixture, frame, label, tensors)

    def observe_optimizer_mapping(self, optimizer_state, gradients, epoch):
        self.writer.write_metadata("runtime-observations.jsonl",
                                   {"version": RUNTIME_OBSERVATION_VERSION,
                                    "record_kind": "optimizer_id_mapping", "point": "post_actual_update",
                                    "phase": self.stage, "epoch": epoch,
                                    "event_monotonic": time.monotonic(),
                                    "gradient_observation_point": "pre_update",
                                    "mapping": optimizer_id_mapping(self.torch, self.service.model,
                                                                    self.engine.optimizer, optimizer_state, gradients)})

    def install_adam(self):
        """每次更新前复制同参数、配置、完整状态和梯度；参考Adam不执行forward。"""
        torch, optimizer = self.torch, self.engine.optimizer
        if type(optimizer) is not torch.optim.Adam:
            raise TypeError("短验证只支持原torch.optim.Adam")
        original = optimizer.step

        def step(*args, **kwargs):
            if args or kwargs:
                raise RuntimeError("短验证不允许新增Adam closure")
            names = {id(parameter): name for name, parameter in self.service.model.named_parameters()}
            clones, groups = {}, []
            for group in optimizer.param_groups:
                copied = {key: copy.deepcopy(value) for key, value in group.items() if key != "params"}
                copied["params"] = []
                for parameter in group["params"]:
                    if id(parameter) not in names or id(parameter) in clones:
                        raise RuntimeError("Adam参数映射重复或不属于命名参数集合")
                    duplicate = torch.nn.Parameter(parameter.detach().clone(), requires_grad=parameter.requires_grad)
                    duplicate.grad = None if parameter.grad is None else parameter.grad.detach().clone()
                    clones[id(parameter)] = duplicate
                    copied["params"].append(duplicate)
                groups.append(copied)
            if set(clones) != set(names):
                raise RuntimeError("Adam参数集合不完整")
            reference = torch.optim.Adam(groups, **copy.deepcopy(optimizer.defaults))
            reference.load_state_dict(copy.deepcopy(optimizer.state_dict()))
            epoch = self.engine.epoch
            gradients = {name: parameter.grad for name, parameter in self.service.model.named_parameters()}
            self.writer.write("update/%d/gradients" % epoch, gradients, "gradient")
            summary = gradient_summary(torch, gradients)
            finite = all(not item["present"] or item["finite"] for item in summary.values())
            self.writer.write("update/%d/gradient_summary" % epoch,
                              {"parameters": summary, "required": {"finite": finite}, "pass": finite}, "coverage")
            if not finite:
                raise RuntimeError("实际Adam前存在非有限梯度")
            self.counter.charge("actual_adam")
            result = original()
            self.counter.charge("actual_adam_completed")
            self.counter.charge("reference_adam")
            reference.step()
            self.counter.charge("reference_adam_completed")
            actual_parameters = cpu_tree(torch, dict(self.service.model.named_parameters()))
            reference_parameters = cpu_tree(torch, {names[key]: value for key, value in clones.items()})
            optimizer_state = optimizer.state_dict()
            states = cpu_tree(torch, optimizer_state)
            rows = compare_value(torch, reference_parameters, actual_parameters, "parameter", "parameters")
            rows += compare_value(torch, cpu_tree(torch, reference.state_dict()), states, "exact", "optimizer")
            self.writer.write("update/%d/parameters" % epoch, actual_parameters, "parameter")
            self.writer.write("update/%d/optimizer" % epoch, states, "exact")
            self.writer.write("update/%d/parameter_mapping" % epoch,
                              [[names[id(p)] for p in group["params"]] for group in optimizer.param_groups], "exact")
            self.observe_optimizer_mapping(optimizer_state, summary, epoch)
            self.writer.write("update/%d/same_gradient_adam" % epoch, rows, "diagnostic")
            if not all(row["pass"] for row in rows):
                raise RuntimeError("同梯度原Adam比较存在不一致，未放宽状态门")
            return result
        self._replace(optimizer, "step", step)

    def close(self):
        for handle in self.handles:
            handle.remove()
        for owner, name, old in reversed(self.restore):
            setattr(owner, name, old)
        if getattr(self.engine, "validation_observer", None) is self:
            del self.engine.validation_observer
