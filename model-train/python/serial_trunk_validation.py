"""#377 approved A32/B-serial32 entry. Importing this module does not import torch.

run <config.json> <new-output>; serve <config.json> <output>; compare <A> <B> <report> <contract>.
Only serve constructs the bound production Service. No production model/math is copied here.
"""
import hashlib
import io
import json
import os
from pathlib import Path
import selectors
import signal
import struct
import subprocess
import sys
import time

from performance_validation import (bound_read, bounded_bytes, load_bound_source,
                                    original_loss, write_json_new)

VERSION = "b-serial-trunk-forward-v1"
COMPARISON_VERSION = "b-serial-trunk64-comparison-v1"
VARIANTS = ("A", "B-serial-trunk")
ZERO_COUNTS = ("backward", "actual_adam", "reference_adam", "commit", "export", "new_sampling")
SINGLE_COUNTS = ("bounded_drivers", "policy_constructions", "model_loads", "formal_learn_open",
                 "checkpoint_state_deserializations", "service_model_deserializations")


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def read_config(path):
    raw = bounded_bytes(path, 16 << 20)
    value = json.loads(raw)
    if value["version"] != VERSION or value["variant"] not in VARIANTS:
        raise ValueError("wrong bounded forward config")
    timing = value["timing"]
    deadline = min(timing[name] for name in ("deadline_monotonic", "stage_deadline_monotonic",
                                            "short_deadline_monotonic"))
    if not timing["started_monotonic"] <= time.monotonic() < deadline:
        raise TimeoutError("bounded forward timing is not active")
    return value, digest(raw), deadline


def count_gates(counts):
    return {**{key: counts.get(key, 0) == 1 for key in SINGLE_COUNTS},
            **{key: counts.get(key, 0) == 0 for key in ZERO_COUNTS},
            "main_gru_frames": counts.get("main_gru_frames", 0) == 32,
            "main_gru_calls": counts.get("main_gru_calls", 0) == 32,
            "main_gru_completed": counts.get("main_gru_completed", 0) == 32}


def first_block(binding):
    """Hash the bound existing file; parse only its first 32 records, never recompute labels."""
    stream = io.BytesIO(bound_read(binding))
    samples = []
    for ordinal in range(32):
        line = stream.readline()
        if not line:
            raise ValueError("L65 prefix ended before 32 frames")
        item = json.loads(line)
        frame = item["frame"]
        if (item["ordinal"] != ordinal or item["expected_call"] != ordinal + 1
                or frame["call"] != ordinal + 1 or "label" not in item):
            raise ValueError("L65 ordinal/call/label mismatch")
        if samples and frame["account"] != samples[0]["frame"]["account"]:
            raise ValueError("first block changes account")
        samples.append(item)
    return samples


def packet(request):
    """Inverse of production model_protocol.read_packet, preserving u32/u32/f32/u8 bits."""
    item = dict(request)
    chunks, layout = [], None
    if "global" in item:
        sequences = [item.pop("global")] + list(item.pop("markets"))
        sizes = []
        for sequence in sequences:
            n = len(sequence)
            raw = bytearray(13 * n)
            for i, token in enumerate(sequence):
                # Rust Token is a serde tuple struct, serialized as four array elements.
                f, c, v, k = token
                struct.pack_into("<I", raw, i * 4, f)
                struct.pack_into("<I", raw, n * 4 + i * 4, c)
                struct.pack_into("<f", raw, n * 8 + i * 4, v)
                raw[n * 12 + i] = k
            chunks.append(raw)
            sizes.append(n)
        layout = {"global": sizes[0], "markets": sizes[1:]}
    metadata = json.dumps({"batch": [item], "token_layouts": [layout]},
                          ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode()
    payload = b"".join(chunks)
    return struct.pack("<4sIQ", b"BMF1", len(metadata), len(payload)) + metadata + payload


class PipeClient:
    def __init__(self, process, deadline, maximum):
        self.process, self.deadline, self.maximum = process, deadline, maximum
        self.read_buffer = bytearray()
        os.set_blocking(process.stdin.fileno(), False)
        os.set_blocking(process.stdout.fileno(), False)

    def _ready(self, fd, event):
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("forward driver deadline")
        with selectors.DefaultSelector() as selector:
            selector.register(fd, event)
            if not selector.select(remaining):
                raise TimeoutError("forward pipe deadline")

    def exchange(self, raw):
        view = memoryview(raw)
        while view:
            self._ready(self.process.stdin, selectors.EVENT_WRITE)
            n = os.write(self.process.stdin.fileno(), view)
            view = view[n:]
        while b"\n" not in self.read_buffer:
            self._ready(self.process.stdout, selectors.EVENT_READ)
            part = os.read(self.process.stdout.fileno(), min(65536, self.maximum + 1 - len(self.read_buffer)))
            if not part:
                raise EOFError("production worker ended before response")
            self.read_buffer.extend(part)
            if len(self.read_buffer) > self.maximum:
                raise ValueError("production response exceeds existing stdout boundary")
        line, _, tail = self.read_buffer.partition(b"\n")
        self.read_buffer = bytearray(tail)
        return json.loads(line)

    def request(self, request):
        result = self.exchange(packet(request))["batch"]
        if len(result) != 1 or not isinstance(result[0], dict) or "error" in result[0]:
            raise RuntimeError("production request failed: " + str(result))
        return result[0]


def serve(config_path, output):
    """One actual production Service.run, one load and one formal open; no alternate runner."""
    from performance_capture import (CaptureObserver, CaptureWriter, WorkCounter,
                                     initial_runtime_snapshot)
    config, config_sha, deadline = read_config(config_path)
    output = Path(output)
    counter = WorkCounter(output, deadline, 32, 0, config_sha, config["timing"]["started_monotonic"])
    counter.charge("bounded_drivers")
    module = load_bound_source(config)
    import torch
    writer = observer = service = None
    replacements = []

    def replace(owner, name, function):
        old = getattr(owner, name)
        replacements.append((owner, name, old))
        setattr(owner, name, function)
        return old

    def forbidden(name):
        def stop(*args, **kwargs):
            counter.charge(name)
            raise RuntimeError("forward-only entry forbids " + name)
        return stop

    model_path = str(Path(config["bootstrap_model"]["path"]).resolve())
    checkpoint_path = str(Path(config["checkpoint"]["path"]).resolve())
    real_load = torch.load
    def observed_load(path, *args, **kwargs):
        actual = str(Path(path).resolve())
        kind = ("service_model_deserializations" if actual == model_path else
                "checkpoint_state_deserializations" if actual == checkpoint_path else None)
        if kind is None:
            raise RuntimeError("unexpected production deserialization")
        counter.charge(kind)
        if counter.counts[kind] != 1:
            raise RuntimeError("duplicate production deserialization")
        return real_load(path, *args, **kwargs)
    replace(torch, "load", observed_load)
    real_policy = module.Policy
    def one_policy(*args, **kwargs):
        counter.charge("policy_constructions")
        if counter.counts["policy_constructions"] != 1:
            raise RuntimeError("additional Policy construction")
        return real_policy(*args, **kwargs)
    replace(module, "Policy", one_policy)
    replace(torch.Tensor, "backward", forbidden("backward"))
    replace(torch.autograd, "backward", forbidden("backward"))
    replace(torch.autograd, "grad", forbidden("backward"))
    replace(torch.optim.Adam, "step", forbidden("actual_adam"))
    try:
        counter.charge("model_loads")
        service = module.Service(model_path, config["learner_worker"]["device"],
                                 int(config["learner_worker"]["torch_threads"]))
        engine = service.learning
        writer = CaptureWriter(torch, output / "captures", config["limits"]["capture_bytes"])
        observer = CaptureObserver(torch, service, writer, counter, "L65")
        observer.observe_projection_features = True
        if hasattr(engine, "sequence_observer"):
            engine.sequence_observer = observer
        replace(service, "choose", forbidden("new_sampling"))
        replace(engine, "commit", forbidden("commit"))
        loss_function, loss_sha = original_loss(torch, config["reference_loss"])
        original_dispatch = engine.dispatch
        opened = registered = forwarded = False
        sample_keys = []
        initial = None

        def dispatch(request):
            nonlocal opened, registered, forwarded, sample_keys, initial
            phase = request["phase"]
            counter.check()
            service.check_cancel()
            if phase == "learn_open":
                if opened:
                    raise RuntimeError("duplicate formal learn_open")
                counter.charge("formal_learn_open")
                result = original_dispatch(request)
                if result.get("already_committed"):
                    raise RuntimeError("seq2 start already consumed this group")
                snapshot, metadata = initial_runtime_snapshot(torch, service, engine.optimizer, "learner", "L65",
                                                              "post_learn_open_pre_first_learning")
                writer.write_initial_observation(snapshot, metadata)
                initial = {"sequence": engine.sequence, "consumed": dict(engine.consumed),
                           "torch_rng": snapshot["torch_rng"], "cuda_rng": snapshot["cuda_rng"]}
                opened = True
                return result
            if not opened:
                raise RuntimeError("formal learn_open must precede cache and learning")
            if phase == "learn_serial_register":
                sample_keys = request["samples"]
                if registered or len(sample_keys) != 32:
                    raise ValueError("expected exactly one 32-frame registration")
                account = sample_keys[0]["account"]
                if any(row != {"account": account, "call": i + 1, "ordinal": i,
                               "name": "L65/%03d" % i} for i, row in enumerate(sample_keys)):
                    raise ValueError("non-continuous registered first block")
                observer.step_keys = {(row["account"], row["call"]): row["name"] for row in sample_keys}
                registered = True
                return {"registered": True, "samples": 32}
            if phase in ("learn_cache_frame", "learn_prepare"):
                if not registered or forwarded:
                    raise RuntimeError("cache/prepare outside bounded prefix")
                return original_dispatch(request)
            if phase != "learn_serial_forward" or not registered or forwarded:
                raise ValueError("request not part of the approved forward-only sequence")
            forwarded = True
            schedule = engine.cache.schedule
            if (schedule != tuple((sample_keys[0]["account"], i) for i in range(32))
                    or any(engine.cache.frame(key, i)["call"] != i + 1 for key, i in schedule)):
                raise ValueError("actual cache/schedule differs from registered first block")
            if config["variant"] == "B-serial-trunk" and not engine.device_learning():
                raise ValueError("candidate must run actual device label/heads/loss path")
            if config["variant"] == "A" and service.performance["mode"] != "storage_v1":
                raise ValueError("reference must be frozen A storage_v1")
            engine.start_epoch()
            if config["variant"] == "B-serial-trunk":
                engine._prepare_device_labels()
                engine._device_forward(0, 32)  # no _device_segment/end_epoch: no implicit backward
            else:
                for account, index in schedule:
                    frame, label = engine.cache.frame(account, index), engine.cache.labels[account][index]
                    observer.begin("learn/1/" + observer.step_keys[(account, frame["call"])])
                    observer.active_frame = frame
                    before = engine.hidden.get(account)
                    if before is None:
                        before = engine.zeros()
                    logp, value, following, _ = engine.teacher(frame, before)
                    engine.hidden[account] = following
                    tensors = loss_function(engine, logp, value, label, account)
                    engine.pending_loss.setdefault(account, []).append(tensors["loss"])
                    engine.counts[account] = engine.counts.get(account, 0) + 1
                    observer.learning_result(frame, label, {key: tensors[key] for key in
                                                           ("ratio", "actor", "critic", "loss")}
                                             | {"logp": logp, "value": value})
                    observer.finish()
                logp = value = following = tensors = before = None
            counter.check()
            service.check_cancel()
            # Explicitly release the last retained graph, all hidden roots and pending losses.
            engine.hidden.clear()
            engine.pending_loss.clear()
            observer.graph = None
            observer.projected_input = None
            if hasattr(engine.cache, "owned_columns"):
                for account in list(engine.cache.owned_columns):
                    engine.cache.release_account_columns(account)
            service.sync()
            counts = {**{name: 0 for name in ZERO_COUNTS}, **counter.counts}
            gates = count_gates(counts)
            end = {"gradients_absent": all(p.grad is None for p in service.model.parameters()),
                   "sequence_unchanged": engine.sequence == initial["sequence"],
                   "consumed_unchanged": engine.consumed == initial["consumed"],
                   "no_sampling_accounts": service.accounts == {},
                   "no_pending_graphs": not engine.hidden and not engine.pending_loss,
                   "torch_rng_unchanged": torch.equal(initial["torch_rng"], torch.get_rng_state())}
            if initial["cuda_rng"]["states"]:
                end["cuda_rng_unchanged"] = all(torch.equal(row["state"], torch.cuda.get_rng_state(row["device"]))
                                                   for row in initial["cuda_rng"]["states"])
            result = {"version": VERSION, "variant": config["variant"], "config_sha256": config_sha,
                      "counts": counts, "count_gates": gates, "end_state_gates": end,
                      "loss_expression_sha256": loss_sha, "identity": service.identity,
                      "service_metrics": service.snapshot(), "capture_bytes": writer.bytes,
                      "pass": all(gates.values()) and all(end.values()),
                      "elapsed": time.monotonic() - config["timing"]["started_monotonic"]}
            write_json_new(output / "worker-finish.json", result)
            return {"finished": True, "pass": result["pass"], "counts": counts}

        replace(engine, "dispatch", dispatch)
        service.run()
        if not forwarded:
            raise RuntimeError("production worker ended without the bounded forward result")
    finally:
        if observer is not None:
            observer.close()
        if writer is not None:
            writer.close()
        for owner, name, old in reversed(replacements):
            setattr(owner, name, old)
        counter.close()


def run(config_path, output):
    config, config_sha, deadline = read_config(config_path)
    output = Path(output)
    output.mkdir()  # exclusive: old failures/output cannot be reused
    process = None
    cancel_read = cancel_write = None
    result = {"version": VERSION, "variant": config["variant"], "config_sha256": config_sha,
              "pass": False, "all_workers_waited": False}
    started = time.monotonic()
    try:
        worker = config["learner_worker"]
        if str(Path(worker["model"]).resolve()) != str(Path(config["bootstrap_model"]["path"]).resolve()):
            raise ValueError("Service model differs from bound bootstrap")
        expected_checkpoint = Path(config["learning_open"]["config"]["checkpoint_directory"]) / "checkpoint.pt"
        if expected_checkpoint.resolve() != Path(config["checkpoint"]["path"]).resolve():
            raise ValueError("formal open does not point at isolated seq2 checkpoint")
        for name in ("bootstrap_model", "checkpoint", "reference_loss"):
            bound_read(config[name])
        samples = first_block(config["learner_samples"])
        sample_binding = [{"account": row["frame"]["account"], "call": row["expected_call"],
                           "ordinal": row["ordinal"], "name": "L65/%03d" % row["ordinal"]} for row in samples]
        labels = {samples[0]["frame"]["account"]: [row["label"] for row in samples]}
        schedule = [{"account": samples[0]["frame"]["account"], "index": i} for i in range(32)]
        env = os.environ.copy()
        env["B_MODEL_INTEROP_THREADS"] = str(worker["performance"]["torch_interop_threads"])
        env["OMP_NUM_THREADS"] = str(worker["torch_threads"])
        env["MKL_NUM_THREADS"] = str(worker["torch_threads"])
        env["PYTHONDONTWRITEBYTECODE"] = "1"
        cancel_read, cancel_write = os.pipe()
        env["B_MODEL_CANCEL_FD"] = str(cancel_read)
        # Inherit the outer driver's dedicated PG. The outer supervisor owns whole-PG escalation.
        with (output / "stderr.log").open("xb") as errors:
            process = subprocess.Popen([worker["python"], str(Path(__file__).resolve()), "serve",
                                        str(Path(config_path).resolve()), str(output.resolve())],
                                       stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=errors,
                                       env=env, pass_fds=(cancel_read,))
            os.close(cancel_read)
            cancel_read = None
            client = PipeClient(process, deadline, config["limits"]["stdout_bytes"])
            hello = {"hello": {"encoding": worker["encoding"], "encoding_version": "b-public-token-log1p-v1",
                                "transport": "b-frame-v1", "performance": worker["performance"]}}
            ready = client.exchange((json.dumps(hello, allow_nan=False) + "\n").encode())
            if ready.get("ready") is not True or ready.get("transport") != "b-frame-v1":
                raise ValueError("production compact hello failed")
            opened = client.request(dict(config["learning_open"], phase="learn_open"))
            if opened.get("already_committed") is not False:
                raise ValueError("formal seq2 learning open is not fresh")
            client.request({"phase": "learn_serial_register", "samples": sample_binding})
            for row in samples:
                cached = client.request(dict(row["frame"], phase="learn_cache_frame"))
                if cached.get("cached") is not True or cached["frame"]["index"] != row["ordinal"]:
                    raise ValueError("production cache did not retain the prefix in order")
            prepared = client.request({"phase": "learn_prepare", "labels": labels, "schedule": schedule})
            if prepared.get("prepared") is not True or prepared.get("samples") != 32:
                raise ValueError("production prepare did not seal exactly 32 full labels")
            finished = client.request({"phase": "learn_serial_forward"})
            if finished.get("finished") is not True or finished.get("pass") is not True:
                raise ValueError("bounded forward result failed")
            process.stdin.close()  # production Service.run finally closes native/cancel resources
            process.wait(timeout=max(0.001, deadline - time.monotonic()))
            if process.returncode != 0:
                raise RuntimeError("production worker nonzero exit")
            finish = json.loads(bounded_bytes(output / "worker-finish.json", 16 << 20))
            result.update({"counts": finish["counts"], "count_gates": finish["count_gates"],
                           "end_state_gates": finish["end_state_gates"], "capture_bytes": finish["capture_bytes"],
                           "pass": finish["pass"], "worker_exit": {"code": process.returncode, "waited": True},
                           "all_workers_waited": True})
    except BaseException as error:
        result.update(error_type=type(error).__name__, error=str(error))
    finally:
        if cancel_read is not None:
            os.close(cancel_read)
        if process is not None and process.poll() is None:
            if cancel_write is not None:
                try:
                    os.write(cancel_write, b"!")
                except OSError:
                    pass
            if process.stdin is not None and not process.stdin.closed:
                process.stdin.close()
            try:
                process.wait(timeout=min(2.0, max(0.001, deadline - time.monotonic())))
            except subprocess.TimeoutExpired:
                process.terminate()
                try:
                    process.wait(timeout=min(2.0, max(0.001, deadline - time.monotonic())))
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()  # no un-reaped child; external supervisor enforces absolute PG deadline
        if process is not None:
            result["worker_exit"] = {"code": process.returncode, "waited": process.returncode is not None}
            result["all_workers_waited"] = process.returncode is not None
        if cancel_write is not None:
            os.close(cancel_write)
        result["elapsed"] = time.monotonic() - started
        result["completed_monotonic"] = time.monotonic()
        result["pass"] = (result["pass"] and result["all_workers_waited"]
                          and result["worker_exit"]["code"] == 0 and result["completed_monotonic"] < deadline)
        write_json_new(output / "driver-result.json", result)
    return 0 if result["pass"] else 1


def read_artifacts(torch, root):
    root = Path(root)
    result = json.loads(bounded_bytes(root / "driver-result.json", 16 << 20))
    finish = json.loads(bounded_bytes(root / "worker-finish.json", 16 << 20))
    records = {}
    for line in bounded_bytes(root / "captures" / "index.jsonl", 4 << 20).splitlines():
        item = json.loads(line)
        if item["key"] in records or Path(item["file"]).name != item["file"]:
            raise ValueError("duplicate or escaping capture key")
        raw = bound_read({"path": str(root / "captures" / item["file"]),
                          "sha256": item["sha256"], "max_bytes": item["bytes"]})
        records[item["key"]] = torch.load(io.BytesIO(raw), map_location="cpu", weights_only=True)
    metadata = [json.loads(line) for line in bounded_bytes(root / "captures" / "runtime-observations.jsonl",
                                                         16 << 20).splitlines()]
    initial_rows = [row for row in metadata if row.get("record_kind") == "initial_state"]
    if len(initial_rows) != 1:
        raise ValueError("missing/duplicate initial full state")
    item = initial_rows[0]
    if Path(item["file"]).name != item["file"]:
        raise ValueError("escaping initial state")
    initial = torch.load(io.BytesIO(bound_read({"path": str(root / "captures" / item["file"]),
                                               "sha256": item["sha256"], "max_bytes": item["bytes"]})),
                         map_location="cpu", weights_only=True)
    projection = [json.loads(line) for line in bounded_bytes(root / "captures" / "projection-features.jsonl",
                                                            16 << 20).splitlines()]
    return {"driver": result, "finish": finish, "records": records, "initial": initial,
            "projection": projection}


def compare_artifacts(torch, reference, candidate):
    """Only reuse original double/left-reference gates. No threshold argument exists."""
    from performance_capture import compare_value
    if reference["driver"]["variant"] != "A" or candidate["driver"]["variant"] != "B-serial-trunk":
        raise ValueError("comparison direction must be frozen A -> B-serial-trunk")
    count = {name: count_gates(value["finish"]["counts"])
             for name, value in (("reference", reference), ("candidate", candidate))}
    initial_rows = compare_value(torch, reference["initial"], candidate["initial"], "exact", "initial")
    initial_gates = {"complete_initial_exact": bool(initial_rows) and all(row["pass"] for row in initial_rows)}
    rows, forward = [], {}
    expected = {"learn/1/L65/%03d" % i for i in range(32)}
    if set(reference["records"]) != expected or set(candidate["records"]) != expected:
        raise ValueError("missing/extra first-block capture")
    for key in sorted(expected):
        left, right = reference["records"][key], candidate["records"][key]
        if left["kinds"] != right["kinds"] or left["values"].keys() != right["values"].keys():
            raise ValueError("observed tensor schema differs at " + key)
        for name, kind in left["kinds"].items():
            if kind not in ("activation", "probability", "loss", "exact"):
                raise ValueError("unexpected forward kind")
            batch = compare_value(torch, left["values"][name], right["values"][name], kind, key + "/" + name)
            rows.extend(batch)
            forward[key + "/" + name] = bool(batch) and all(row["pass"] for row in batch)
    for side, artifact in (("reference", reference), ("candidate", candidate)):
        projection = artifact["projection"]
        if len(projection) != 32:
            raise ValueError("missing projection/features observation")
        account = projection[0]["account"]
        for ordinal, row in enumerate(projection):
            if row["call"] != ordinal + 1 or row["account"] != account or row["batch"] != 0 or row["time"] != 0:
                raise ValueError("projection observation identity mismatch")
            forward[side + "/projection-mode/%d" % ordinal] = (row["train"] is True and row["grad_enabled"] is True
                and row["projection"]["ordinary_tensor"] is True and row["gru_features"]["ordinary_tensor"] is True)
        for name, passed in artifact["finish"]["end_state_gates"].items():
            forward[side + "/end/" + name] = passed
    # Addresses differ between processes; compare the exact layouts/alias relationship, never pointer values.
    def semantic_projection(row):
        return {key: ({name: value for name, value in item.items() if name != "storage_pointer"}
                      if key in ("projection", "gru_features") else item)
                for key, item in row.items()}
    layout_rows = compare_value(torch, [semantic_projection(row) for row in reference["projection"]],
                                [semantic_projection(row) for row in candidate["projection"]], "exact", "projection-layout")
    forward["projection_layout_exact"] = bool(layout_rows) and all(row["pass"] for row in layout_rows)
    rows += layout_rows
    return count, initial_gates, forward, initial_rows + rows


def compare(reference_root, candidate_root, report_path, contract_path):
    started = time.monotonic()
    report = {"version": COMPARISON_VERSION, "status": "UNKNOWN", "pass": False}
    try:
        raw = bounded_bytes(contract_path, 16 << 20)
        contract = json.loads(raw)
        if contract["version"] != "b-serial-trunk64-contract-v1":
            raise ValueError("unknown comparison contract")
        for name, binding in contract["programs"].items():
            bound_read(binding)
        if Path(contract["programs"]["entry"]["path"]).resolve() != Path(__file__).resolve():
            raise ValueError("comparison entry differs from frozen contract")
        import torch
        reference, candidate = read_artifacts(torch, reference_root), read_artifacts(torch, candidate_root)
        configs = {}
        for name, artifact in (("A", reference), ("B-serial-trunk", candidate)):
            if (artifact["driver"]["version"] != VERSION or artifact["driver"]["pass"] is not True
                    or artifact["driver"]["all_workers_waited"] is not True):
                raise ValueError("driver did not complete normally")
            # The frozen input config is persisted by the controller; bind exact source/initialization.
            config_raw = bounded_bytes(contract["driver_configs"][name], 16 << 20)
            if digest(config_raw) != artifact["driver"]["config_sha256"]:
                raise ValueError("driver output not bound to the compared config")
            config = json.loads(config_raw)
            if config["source"] != contract["sources"][name]:
                raise ValueError("implementation source binding differs")
            identity = artifact["finish"]["identity"]
            if (identity["model_sha256"] != config["bootstrap_model"]["sha256"]
                    or identity["checkpoint_sha256"] != config["checkpoint"]["sha256"]):
                raise ValueError("actual load/open differs from bound legal start")
            configs[name] = config
        for field in ("bootstrap_model", "checkpoint", "learner_samples", "reference_loss"):
            if configs["A"][field]["sha256"] != configs["B-serial-trunk"][field]["sha256"]:
                raise ValueError("comparison did not use the same frozen " + field)
        def open_semantics(value):
            value = dict(value)
            value["config"] = {key: item for key, item in value["config"].items()
                               if key != "checkpoint_directory"}
            return value
        if open_semantics(configs["A"]["learning_open"]) != open_semantics(configs["B-serial-trunk"]["learning_open"]):
            raise ValueError("formal learning group/config/weights differ")
        count, initial, forward, rows = compare_artifacts(torch, reference, candidate)
        for name, artifact in (("reference", reference), ("candidate", candidate)):
            snapshot = artifact["initial"]
            initial[name + "/legal_seq2"] = snapshot["source_boundary"]["sequence"] == 2
            mapping = snapshot["optimizer_id_mapping"]
            initial[name + "/actual_id_name_complete"] = (mapping["applicability"] == "present"
                and all(not mapping[key] for key in ("unmapped_state_ids", "group_length_mismatches",
                                                    "unnamed_runtime_parameters", "named_parameters_outside_optimizer")))
        passed = all(all(gates.values()) for gates in count.values()) and all(initial.values()) and all(forward.values())
        report.update({"status": "PASS" if passed else "FAIL", "pass": passed,
                       "contract_sha256": digest(raw), "count_gates": count,
                       "initial_state_gates": initial, "forward_gates": forward, "comparisons": rows,
                       "scope_exclusions": ["backward/gradients", "Adam/update", "epoch2", "stop/cold/stream",
                                            "12slot/16pair", "full performance", "profit/adoption"]})
    except Exception as error:
        report.update(error_type=type(error).__name__, error=str(error))
    report["cost"] = {"wall_seconds": time.monotonic() - started, "physical_io_bytes": "Unknown"}
    write_json_new(report_path, report)
    return {"PASS": 0, "FAIL": 1, "UNKNOWN": 2}[report["status"]]


if __name__ == "__main__":
    if len(sys.argv) == 4 and sys.argv[1] == "run":
        raise SystemExit(run(sys.argv[2], sys.argv[3]))
    if len(sys.argv) == 4 and sys.argv[1] == "serve":
        serve(sys.argv[2], sys.argv[3])
    elif len(sys.argv) == 6 and sys.argv[1] == "compare":
        raise SystemExit(compare(*sys.argv[2:]))
    elif sys.argv[1:2] != ["run"]:
        raise SystemExit("run <config> <new-output> | serve <config> <output> | compare <A> <B> <report> <contract>")
