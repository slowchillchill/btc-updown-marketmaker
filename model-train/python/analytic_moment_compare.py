"""Explicit #377 v4/v3 comparison entry; the original compare stays unchanged.

compare <left-reference-slot> <candidate-slot> <new-report> <frozen-contract>
No Policy construction, forward, backward, optimizer step or CUDA access.
Only the coordinator may invoke this on real captures inside the approved short
window. Importing this file does not import torch or read any input.
"""
import ast
import hashlib
import json
import math
from pathlib import Path
import sys
import time

from analytic_moment_bound import NotApplicable, advance, difference_within
from performance_capture import (RUNTIME_OBSERVATION_VERSION, TOLERANCES,
                                 _observation_metadata, compare_value)
from performance_validation import (SLOTS, bound_read, bounded_bytes, capture_index,
                                    compare_capture, load_capture, write_json_new)


VERSION = "b-analytic-short-comparison-v1"
CONTRACT_VERSION = "b-analytic-comparison-contract-v1"
PAIRS = ((1, 2), (1, 3), (1, 4), (3, 4), (1, 5), (1, 6), (5, 6),
         (1, 7), (1, 8), (7, 8), (1, 9), (1, 10), (9, 10),
         (1, 11), (1, 12), (11, 12))
METHODS = {
    "validation": "2d3af4f7f943016ba90d4608e0042bd5e8f08d32804ce0cccd067a6d4bab87dc",
    "analytic": "e3d686b5b0402d022987056e627d167ee5effc2ecf2e5d791dbfd0214efe0bee",
    "review": "a525ba77c269614af6c60e1dd0f5b9022e260ebb2c1805a4720d0aa6584e1d6f",
}
C65_APPLICABILITY = {
    "method": "77e5e8fe0fdb49b561dd2e9b9086513de0919774d1efa68a898dd28e2382bc84",
    "source_manifest": "beb2b6c094b54aff928b7b099446597b87ee5599a1b9850ca7fc2d41646200df",
    "source_evidence": "e58de84c4f2427e53842b0ceb5866278ff52f6949e713300756b895d99e4ce82",
    "seed_addendum": "a14cce2a50db5907f7a1371f356fd40dde366c9012329bfb363b6cf1678dd971",
    "sampling_source": "ce7104bbca0958a90e6ed88478d5e692298eaee88e3c1f448b1bc2508882bdfd",
}
CLAIMS = {
    "pre1": "post_learn_open_initial_state_before_first_actual_step",
    "pre2": "previous_actual_post_state_without_intervening_state_write",
    "reference": "independent_parameter_and_full_state_clones_same_full_gradient",
    "dispatch": "eager_exact_Adam_nonfused_single_or_foreach_union",
    "gradient_gate": "compare_value_double_candidate_minus_left_reference_1e-7_1e-3",
}
# Each function belongs to the reviewed source closure, not to a run-time probe.
FUNCTIONS = {
    "observer.performance_capture.py": ("compare_value", "CaptureObserver.install_adam",
                                        "initial_runtime_snapshot", "optimizer_id_mapping"),
    "observer.performance_validation.py": ("ValidationSession.observe_initial_state", "serve"),
    "optimizer.adam.py": ("Adam.__init__", "Adam.step", "adam"),
    "optimizer.optimizer.py": ("_default_to_fused_or_foreach",),
}
for _variant in ("R0", "A", "B"):
    FUNCTIONS["implementations.%s.learning_engine.py" % _variant] = (
        "LearningEngine.open", "LearningEngine.end_epoch", "LearningEngine.checkpoint_state")
    FUNCTIONS["implementations.%s.model_worker.py" % _variant] = ("Service.__init__",)


def ast_function(raw, qualified):
    body = ast.parse(raw).body
    node = None
    for name in qualified.split("."):
        node = next((item for item in body if isinstance(item, (ast.ClassDef, ast.FunctionDef,
                                                               ast.AsyncFunctionDef)) and item.name == name), None)
        if node is None:
            raise NotApplicable("source function missing: " + qualified)
        body = node.body
    return hashlib.sha256(ast.dump(node, include_attributes=False).encode()).hexdigest()


def source_leaves(sources):
    result = {}
    def visit(prefix, value):
        if isinstance(value, dict) and {"path", "sha256", "max_bytes"} <= value.keys():
            result[prefix] = value
        elif isinstance(value, dict):
            for key, child in value.items():
                visit(prefix + "." + key if prefix else key, child)
        else:
            raise NotApplicable("source binding must be a BoundFile")
    visit("", sources)
    return result


def read_contract(path):
    # Reuse the existing binding JSON limit (16 MiB); no new report-size policy.
    raw = bounded_bytes(path, 16 << 20)
    contract = json.loads(raw)
    if (set(contract) != {"version", "method", "sources", "source_constraints", "pairs"}
            or contract["version"] != CONTRACT_VERSION or contract["pairs"] != [list(p) for p in PAIRS]):
        raise NotApplicable("not the fixed approved comparison contract")
    if set(contract["method"]) != set(METHODS):
        raise NotApplicable("method binding incomplete")
    for name, digest in METHODS.items():
        binding = contract["method"][name]
        if binding["sha256"] != digest:
            raise NotApplicable("adopted method SHA mismatch: " + name)
        bound_read(binding)
    leaves = source_leaves(contract["sources"])
    if not set(FUNCTIONS) <= leaves.keys():
        raise NotApplicable("source closure incomplete")
    source_raw = {name: bound_read(binding) for name, binding in leaves.items()}
    for filename in ("performance_capture.py", "performance_validation.py"):
        if source_raw["observer." + filename] != (Path(__file__).parent / filename).read_bytes():
            raise NotApplicable("loaded original gate/observer source differs from binding")
    constraints = json.loads(bound_read(contract["source_constraints"]))
    if (constraints.get("version") != "b-analytic-source-constraints-v1"
            or constraints.get("claims") != CLAIMS or set(constraints.get("functions", {})) != set(FUNCTIONS)):
        raise NotApplicable("reviewed pre-state/dispatch source constraints missing")
    for source, functions in FUNCTIONS.items():
        expected = constraints["functions"][source]
        if set(expected) != set(functions):
            raise NotApplicable("source constraint function set mismatch")
        for qualified in functions:
            if ast_function(source_raw[source], qualified) != expected[qualified]:
                raise NotApplicable("source constraint AST mismatch: " + source + "/" + qualified)
    applicability = constraints.get("c65_rng_applicability", {})
    if set(applicability) != set(C65_APPLICABILITY):
        raise NotApplicable("fixed C65 role/point RNG applicability evidence missing")
    for name, digest in C65_APPLICABILITY.items():
        if applicability[name]["sha256"] != digest:
            raise NotApplicable("C65 applicability source/method version mismatch: " + name)
        bound_read(applicability[name])
    source_manifest = json.loads(bound_read(applicability["source_manifest"]))
    for source in source_manifest["files"].values():
        bound_read({"path": source["path"], "sha256": source["sha256"], "max_bytes": source["bytes"]})
    for variant, bindings in contract["sources"]["implementations"].items():
        for filename, binding in bindings.items():
            if binding["sha256"] != source_manifest["files"][variant + "/" + filename]["sha256"]:
                raise NotApplicable("C65 actual implementation is outside the fixed reviewed source set")
    for filename, binding in contract["sources"]["observer"].items():
        if binding["sha256"] != source_manifest["files"]["observer/" + filename]["sha256"]:
            raise NotApplicable("C65 actual observer differs from fixed source evidence")
    if TOLERANCES["gradient"] != (1e-7, 1e-3) or TOLERANCES["parameter"] != (1e-6, 1e-5):
        raise NotApplicable("original gradient/parameter gate changed")
    return contract, {"comparison_contract_sha256": hashlib.sha256(raw).hexdigest(),
                      "method": {name: binding["sha256"] for name, binding in contract["method"].items()},
                      "source_constraints_sha256": contract["source_constraints"]["sha256"]}


def exact_rows(torch, reference, candidate, path):
    return compare_value(torch, reference, candidate, "exact", path)


def all_pass(rows):
    return all(row.get("pass") is True for row in rows)


def canonical_optimizer(torch, state, mapping):
    """Join actual state IDs through the observed objects; never enumerate names."""
    if mapping.get("applicability") != "present" or state is None:
        raise NotApplicable("learner optimizer/mapping absent")
    for key in ("unmapped_state_ids", "group_length_mismatches", "unnamed_runtime_parameters",
                "named_parameters_outside_optimizer"):
        if mapping.get(key) != []:
            raise NotApplicable("incomplete real optimizer mapping: " + key)
    if len(mapping["groups"]) != len(state["param_groups"]):
        raise NotApplicable("optimizer group count mismatch")
    result, groups, identifiers = {}, [], set()
    for index, (observed, saved) in enumerate(zip(mapping["groups"], state["param_groups"])):
        entries, ids = observed["entries"], saved["params"]
        if (observed["group_index"] != index or observed["serialized_ids"] != ids
                or observed["runtime_parameter_count"] != len(ids) or len(entries) != len(ids)):
            raise NotApplicable("actual object/group/serialized-ID position mismatch")
        config = {key: value for key, value in saved.items() if key != "params"}
        if not all_pass(exact_rows(torch, observed["configuration"],
                                   _observation_metadata(torch, config), "mapping/config")):
            raise NotApplicable("observed loaded configuration does not match actual state")
        names = []
        for position, (entry, identifier) in enumerate(zip(entries, ids)):
            name = entry["parameter_name"]
            present = identifier in state["state"]
            if (entry["position"] != position or entry["state_id"] != identifier
                    or not isinstance(name, str) or name in result or identifier in identifiers
                    or not entry["aliases"] or entry["aliases"][0] != name
                    or entry["has_state"] is not present
                    or entry["state_fields"] != sorted(str(key) for key in state["state"].get(identifier, {}))):
                raise NotApplicable("real parameter/state-ID mapping is ambiguous or stale")
            identifiers.add(identifier)
            names.append(name)
            result[name] = {"state": state["state"].get(identifier), "group": index,
                            "position": position, "aliases": entry["aliases"],
                            "shape": entry["shape"], "dtype": entry["dtype"],
                            "configuration": config}
        groups.append({"names": names, "configuration": config})
    if set(state["state"]) - identifiers:
        raise NotApplicable("optimizer state contains unmapped IDs")
    return {"parameters": result, "groups": groups, "type": mapping["optimizer_type"],
            "defaults": mapping["defaults"]}


def configuration(config):
    if (config.get("weight_decay") != 0 or config.get("amsgrad") is not False
            or config.get("capturable") is not False or config.get("differentiable") is not False
            or config.get("fused") not in (None, False)
            or config.get("foreach") not in (None, False, True)
            or config.get("decoupled_weight_decay", False) is not False):
        raise NotApplicable("loaded Adam configuration outside single/foreach analytic coverage")
    for key in ("lr", "eps"):
        if type(config.get(key)) not in (int, float) or not math.isfinite(config[key]) or config[key] < 0:
            raise NotApplicable("non-scalar or nonfinite optimizer configuration")
    betas = config.get("betas")
    if (type(betas) not in (tuple, list) or len(betas) != 2
            or any(type(beta) is not float or not 0 <= beta < 1 for beta in betas)
            or type(config.get("maximize")) is not bool):
        raise NotApplicable("Python-float betas/maximize configuration required")
    return betas, config["maximize"]


class Slot:
    def __init__(self, torch, root, contract, cost):
        self.torch, self.root, self.contract, self.cost = torch, Path(root), contract, cost
        self.index = capture_index(self.root)
        self.slot = self.index["slot"]
        self.workers = {}
        for directory in sorted(self.root.iterdir()):
            if directory.is_dir() and (directory / "identity.json").is_file():
                identity = json.loads(bounded_bytes(directory / "identity.json", 1 << 20))
                finished = bounded_bytes(directory / "finish.json", 16 << 20)
                self.cost["additional_metadata_bytes_read"] += len(finished)
                self.workers[identity["phase"]] = {"identity": identity, "directory": directory,
                                                   "finish": json.loads(finished)}
        self.initial = {}
        self.metadata = {}
        self.learning_observed_schema = {}
        self.learning_coordinates = {}
        self.validate_identity()

    def validate_identity(self):
        sources = self.contract["sources"]
        for worker in self.workers.values():
            identity = worker["identity"]
            variant = identity["variant"]
            if identity["service_identity"]["torch"] != "2.8.0+cu128":
                raise NotApplicable("actual worker torch version is outside reviewed source")
            for filename, binding in sources["observer"].items():
                if identity["observer"].get(filename) != binding["sha256"]:
                    raise NotApplicable("worker observer differs from reviewed binding")
            for filename, binding in sources["implementations"][variant].items():
                if identity["source"]["files"].get(filename) != binding["sha256"]:
                    raise NotApplicable("worker implementation differs from reviewed binding")

    def load_artifact(self, item):
        """Load an indexed event or an initial-state sidecar through one bound reader."""
        self.cost["artifact_loads"] += 1
        self.cost["artifact_bytes_loaded"] += int(item["bytes"])
        return load_capture(self.torch, item)

    def load_item(self, item):
        value = self.load_artifact(item)
        if item["kind"] == "observed" and item["key"].startswith("learn/"):
            self.learning_observed_schema[item["key"]] = {"fields": set(value["values"]),
                                                          "kinds": dict(value["kinds"])}
        if item["key"].startswith("binding/L65"):
            if item["key"].endswith("/samples"):
                self.learning_coordinates.update({row["name"]: (row["account"], row["call"]) for row in value})
            elif item["key"].endswith("/fixtures"):
                self.learning_coordinates.update({row["case"]: (row["frame"]["account"], row["frame"]["call"])
                                                  for row in value})
        return value

    def one(self, key):
        items = self.index["records"].get(key, [])
        if len(items) != 1:
            raise NotApplicable("required unique capture missing/duplicated: " + key)
        return self.load_item(items[0])

    def observations(self, phase):
        if phase not in self.metadata:
            directory = self.workers[phase]["directory"] / "captures"
            # Same established metadata binding limit, not an arbitrary new cap.
            raw = bounded_bytes(directory / "runtime-observations.jsonl", 16 << 20)
            self.cost["additional_metadata_bytes_read"] += len(raw)
            records = [json.loads(line) for line in raw.splitlines()]
            if any(item.get("version") != RUNTIME_OBSERVATION_VERSION or item.get("phase") != phase
                   for item in records):
                raise NotApplicable("runtime observation version/phase mismatch")
            self.metadata[phase] = records
        return self.metadata[phase]

    def start(self, phase):
        if phase not in self.initial:
            records = [item for item in self.observations(phase) if item["record_kind"] == "initial_state"]
            if len(records) != 1:
                raise NotApplicable("initial full-state evidence missing/duplicated")
            record = records[0]
            point = {"C65": "post_service_load_pre_first_consume",
                     "L65": "post_learn_open_pre_collection_and_update",
                     "L65-cold": "post_learn_open_pre_collection_sampling_restore"}[phase]
            role = "consumer" if phase == "C65" else "learner"
            directory = (self.workers[phase]["directory"] / "captures").resolve()
            path = (directory / record["file"]).resolve()
            if path.parent != directory or path.name != "initial-state.pt":
                raise NotApplicable("initial-state path mismatch")
            item = dict(record, path=path)
            snapshot = self.load_artifact(item)
            for target in (record, snapshot):
                if (target.get("version") != RUNTIME_OBSERVATION_VERSION or target.get("phase") != phase
                        or target.get("role") != role or target.get("point") != point):
                    raise NotApplicable("initial full-state point/role mismatch")
            if record.get("slot") != self.slot:
                raise NotApplicable("initial-state slot mismatch")
            if not all_pass(exact_rows(self.torch, record["optimizer_id_mapping"],
                                       snapshot["optimizer_id_mapping"], "initial/mapping")):
                raise NotApplicable("initial mapping metadata/artifact mismatch")
            self.initial[phase] = snapshot
        return self.initial[phase]

    def update_mapping(self, epoch):
        phase = "L65-cold" if self.slot == 12 else "L65"
        records = [item for item in self.observations(phase)
                   if item["record_kind"] == "optimizer_id_mapping" and item.get("epoch") == epoch]
        if (len(records) != 1 or records[0].get("point") != "post_actual_update"
                or records[0].get("gradient_observation_point") != "pre_update"):
            raise NotApplicable("unique post-state/pre-gradient mapping absent")
        return records[0]["mapping"]

    def semantics(self, report, label):
        """Consume existing stop/cold proof, never synthesize a missing update."""
        if self.slot in (9, 10):
            finished = self.workers["L65"]["finish"]
            for key in ("stop_state_unchanged", "learning_sampling_unchanged"):
                rows = finished.get(key)
                if not isinstance(rows, list) or not rows or any(type(row.get("pass")) is not bool for row in rows):
                    raise NotApplicable("stop missing existing own-baseline exact proof: " + key)
                report["comparisons"].extend(dict(row, path="stop/" + label + "/" + key + "/" + row["path"])
                                              for row in rows)
                report["optimizer_structure_gates"]["stop/" + label + "/" + key] = all_pass(rows)
            required = {"binding/C65/samples", "binding/L65/samples", "learning_sampling_state"}
            required.update("learn/1/L65/%03d" % index for index in range(32))
            if required - self.index["records"].keys():
                raise NotApplicable("stop missing the approved common 32-step prefix/binding")
        if self.slot == 12:
            proof = self.workers["L65-cold"]["finish"].get("cold_restore_proof")
            rows = None if not isinstance(proof, dict) else proof.get("comparisons")
            if not isinstance(rows, list) or not rows or any(type(row.get("pass")) is not bool for row in rows):
                raise NotApplicable("cold missing existing sampling-restore exact proof")
            expected = proof.get("expected_calls")
            if (not isinstance(expected, dict) or len(expected) != 1
                    or any(type(value) is not int or value != 65 for value in expected.values())):
                raise NotApplicable("cold restore missing the original one-account/65-call binding")
            result = proof.get("result", {})
            report["comparisons"].extend(dict(row, path="cold/" + label + "/" + row["path"]) for row in rows)
            restored = (proof.get("pass") is True and all_pass(rows)
                        and result.get("restored") is True and result.get("calls") == 65
                        and result.get("accounts") == sorted(expected))
            report["optimizer_structure_gates"]["cold/" + label + "/original_restore_evidence"] = restored
            report["comparisons"].append({"path": "cold/" + label + "/original_restore_evidence", "pass": restored})


def global_rng_schema(torch, snapshot):
    """Require complete records even where cross-worker equality is inapplicable."""
    def state_schema(value):
        if (not isinstance(value, torch.Tensor) or value.dtype != torch.uint8
                or value.device.type != "cpu" or value.layout != torch.strided
                or value.ndim != 1 or value.numel() == 0):
            raise NotApplicable("global RNG record must be a nonempty CPU ByteTensor")
        return {"shape": list(value.shape), "dtype": str(value.dtype)}
    cpu = state_schema(snapshot["torch_rng"])
    cuda = snapshot["cuda_rng"]
    if (not isinstance(cuda, dict) or set(cuda) != {"initialized", "scope", "states"}
            or type(cuda["initialized"]) is not bool or not isinstance(cuda["states"], list)):
        raise NotApplicable("complete CUDA global RNG schema missing")
    scope = cuda["scope"]
    if scope == "actual_initialized_service_device":
        if not cuda["initialized"] or len(cuda["states"]) != 1:
            raise NotApplicable("initialized actual-service CUDA RNG state missing")
    elif scope in ("not_applicable", "not_initialized"):
        if cuda["states"] or (scope == "not_initialized" and cuda["initialized"]):
            raise NotApplicable("CUDA RNG applicability/schema inconsistent")
    else:
        raise NotApplicable("unbound CUDA global RNG recording scope")
    states = []
    for item in cuda["states"]:
        if set(item) != {"device", "state"} or type(item["device"]) is not int or item["device"] < 0:
            raise NotApplicable("CUDA RNG device/state schema missing")
        states.append(dict(device=item["device"], state=state_schema(item["state"])))
    return {"torch_rng": cpu, "cuda_rng": {"initialized": cuda["initialized"], "scope": scope, "states": states}}


def initial_projection(torch, snapshot, applicability=None):
    result = {key: snapshot[key] for key in ("model", "optimizer_applicability", "optimizer_defaults",
                                            "account_sampling", "gradients")}
    schema = global_rng_schema(torch, snapshot)
    if snapshot["phase"] == "C65":
        if (snapshot["role"] != "consumer" or snapshot["point"] != "post_service_load_pre_first_consume"
                or applicability != C65_APPLICABILITY):
            raise NotApplicable("C65 RNG projection requires the exact reviewed role/point/source set")
        # Preserve and validate the complete artifacts. Only these two top-level
        # values' cross-worker equality is inapplicable; no recursive ignoring.
        result["global_rng_record_schema"] = schema
    else:
        result["torch_rng"], result["cuda_rng"] = snapshot["torch_rng"], snapshot["cuda_rng"]
    result["source_boundary"] = snapshot["source_boundary"]
    if snapshot["optimizer_applicability"] == "not_applicable":
        if snapshot["optimizer"] is not None or snapshot["optimizer_id_mapping"]["applicability"] != "not_applicable":
            raise NotApplicable("consumer optimizer not_applicable inconsistent")
        result["optimizer"] = None
    else:
        result["optimizer"] = canonical_optimizer(torch, snapshot["optimizer"], snapshot["optimizer_id_mapping"])
    return result


def compare_initial(torch, reference, candidate, report):
    constraints = json.loads(bound_read(reference.contract["source_constraints"]))
    applicability = {name: binding["sha256"] for name, binding in
                     constraints.get("c65_rng_applicability", {}).items()}
    if applicability != C65_APPLICABILITY:
        raise NotApplicable("fixed C65 applicability provenance missing from source constraints")
    for phase in ("C65", "L65"):
        rows = exact_rows(torch, initial_projection(torch, reference.start(phase), applicability),
                          initial_projection(torch, candidate.start(phase), applicability), "initial/" + phase)
        report["comparisons"].extend(rows)
        report["initial_state_gates"][phase] = all_pass(rows)
        if phase == "C65":
            records = {}
            for label, slot in (("reference", reference), ("candidate", candidate)):
                item = next(row for row in slot.observations(phase) if row["record_kind"] == "initial_state")
                records[label] = {name: item[name] for name in ("file", "bytes", "sha256")}
            report["scope_exclusions"].append({"key": "initial/C65/global_rng_cross_worker_equality",
                "status": "not_applicable", "role": "consumer", "phase": "C65",
                "point": "post_service_load_pre_first_consume", "complete_records_retained": records,
                "schema_and_provenance_checked": True, "fixed_source_evidence": applicability,
                "reason": "Retain full Torch/CUDA records; cross-worker equality is not a C65 behavior prerequisite under the fixed reviewed source set."})
    for label, slot in (("reference", reference), ("candidate", candidate)):
        if slot.slot == 12:
            rows = exact_rows(torch, initial_projection(torch, slot.start("L65")),
                              initial_projection(torch, slot.start("L65-cold")),
                              "initial/" + label + "/cold_post_open_pre_sampling_restore")
            report["comparisons"].extend(rows)
            report["initial_state_gates"][label + "_cold_same_post_open"] = all_pass(rows)


def projection_semantics(row):
    """Only named process-local addresses and the four already-bound thread fields differ."""
    value = dict(row)
    for name in ("projection", "gru_features"):
        value[name] = {key: item for key, item in row[name].items() if key != "storage_pointer"}
    precision = row.get("precision")
    if not isinstance(precision, dict):
        raise NotApplicable("learning projection precision record missing")
    value["precision"] = {key: item for key, item in precision.items()
                          if key not in ("torch_intra_threads", "torch_interop_threads", "omp_threads", "mkl_threads")}
    if value.get("phase") == "L65-cold":
        value["phase"] = "L65"  # same explicit cold learning coordinate as existing capture keys
    return value


def learning_projection_records(slot, coordinates=None):
    phase = "L65-cold" if slot.slot == 12 else "L65"
    path = slot.workers[phase]["directory"] / "captures" / "projection-features.jsonl"
    raw = bounded_bytes(path, 16 << 20)
    slot.cost["additional_metadata_bytes_read"] += len(raw)
    records = {}
    # Projection metadata describes primary observed frames. Derived gradient
    # and coverage records share this prefix but remain in original_gates.
    actual = {key for key, items in slot.index["records"].items()
              if key.startswith("learn/") and any(item["kind"] == "observed" for item in items)}
    expected = actual if coordinates is None else set(coordinates)
    if not expected <= actual:
        raise NotApplicable("requested learning projection is outside actual reference coverage")
    for line in raw.splitlines():
        row = json.loads(line)
        key = "learn/%d/%s" % (row["epoch"], row["sample"])
        # A normal reference has 138 metadata rows, but original stop gates
        # loaded only the common first32 tensor schemas. Do not consume or
        # demand unseen tensor schemas beyond that approved comparison scope.
        if coordinates is not None and key not in expected:
            continue
        if key in records or key not in expected or row["phase"] != phase:
            raise NotApplicable("learning projection coordinate duplicate/outside actual learning")
        if row["batch"] != 0 or row["time"] != 0 or row["train"] is not True or row["grad_enabled"] is not True:
            raise NotApplicable("learning projection is outside the original single-step train/grad mode")
        if slot.learning_coordinates.get(row["sample"]) != (row["account"], row["call"]):
            raise NotApplicable("learning projection account/call differs from frozen registration")
        width = slot.workers[phase]["identity"]["service_identity"]["config"]["width"]
        if row["projection"]["shape"] != [width] or row["gru_features"]["shape"] != [1, 1, width]:
            raise NotApplicable("learning projection is not the original single-frame shape")
        alias = row["same_contiguous_view"]
        if type(alias) is not bool or row["stored_value_objects"] != (1 if alias else 2):
            raise NotApplicable("learning projection/view storage proof missing")
        # original_gates already loaded this event; retain only its small schema,
        # never deserialize the full token/activation capture again for metadata.
        observed = slot.learning_observed_schema.get(key)
        required = {"projection_output"} | (set() if alias else {"gru_features"})
        if observed is None or not required <= observed["fields"] or any(observed["kinds"].get(name) != "activation" for name in required):
            raise NotApplicable("learning projection/features activation absent or reclassified")
        for name in ("projection", "gru_features"):
            layout = row[name]
            if layout["ordinary_tensor"] is not True or layout["dtype"] != "torch.float32":
                raise NotApplicable("learning projection/features dtype/storage differs")
        records[key] = projection_semantics(row)
    if set(records) != expected:
        missing, extra = expected - records.keys(), records.keys() - expected
        examples = lambda keys: [key[:160] for key in sorted(keys)[:3]]
        raise NotApplicable("learning projection missing an actual learning frame; "
                            "slot=%s phase=%s missing=%d %r extra=%d %r" %
                            (slot.slot, phase, len(missing), examples(missing), len(extra), examples(extra)))
    return records


def compare_learning_projection(torch, reference, candidate, report):
    right = learning_projection_records(candidate)
    left = learning_projection_records(reference, right.keys() if candidate.slot in (9, 10) else None)
    for key, row in right.items():
        if key not in left:
            raise NotApplicable("candidate learning projection has no reference coordinate")
        report["comparisons"].extend(exact_rows(torch, left[key], row, "projection-layout/" + key))
    if candidate.slot not in (9, 10) and set(left) != set(right):
        raise NotApplicable("normal learning projection coverage differs")
    report["scope_exclusions"].append({"key": "projection-layout/precision/thread_fields",
        "reason": "Only the original four named thread fields are checked by each frozen worker thread_binding; full precision source records remain stored. Process-local storage_pointer is audit evidence, never a cross-worker value."})


def original_gates(torch, reference, candidate, report):
    """Execute original individual gates first, not an old failed whole report."""
    r, c = reference.index, candidate.index
    rmeta, cmeta = r["identities"][0], c["identities"][0]
    if any(rmeta[key] != cmeta[key] for key in ("observer", "method", "execution_method", "capture_method",
                                              "auxiliary_loss_ast_sha256")):
        raise NotApplicable("original wrapper/method bindings differ")
    stop = c["slot"] in (9, 10)
    compared = set()
    for key, alternatives in c["records"].items():
        if stop and r["slot"] not in (9, 10) and key == "final_checkpoint_fields":
            report["scope_exclusions"].append({"key": key, "reason": "original stop baseline versus its own preupdate state"})
            continue
        canonical = key.replace("binding/L65-cold/", "binding/L65/")
        if canonical not in r["records"]:
            report["comparisons"].append({"path": key, "pass": False, "reason": "reference-key-missing"})
            continue
        original = r["records"][canonical][0]
        # Only this explicit whole optimizer tree is routed to the new consumer.
        # It is not run as exact and then filtered; same-gradient diagnostics
        # (including their optimizer exact rows) are executed below unchanged.
        optimizer_key = canonical in ("update/1/optimizer", "update/2/optimizer")
        if optimizer_key:
            if stop or len(alternatives) != 1 or original["kind"] != "exact" or alternatives[0]["kind"] != "exact":
                raise NotApplicable("unexpected optimizer capture scope/kind")
            compared.add(canonical)
            continue
        expected = reference.load_item(original)
        for actual in alternatives:
            if actual["kind"] != original["kind"]:
                report["comparisons"].append({"path": key, "pass": False, "reason": "capture-kind"})
                continue
            value = candidate.load_item(actual)
            report["comparisons"].extend(compare_capture(torch, expected, value, original["kind"], key))
        compared.add(canonical)
    if not stop:
        report["comparisons"].extend({"path": key, "pass": False, "reason": "candidate-key-missing"}
                                     for key in r["records"].keys() - compared)
        for epoch in (1, 2):
            for suffix in ("gradients", "parameters", "optimizer", "parameter_mapping", "same_gradient_adam"):
                key = "update/%d/%s" % (epoch, suffix)
                if key not in compared:
                    raise NotApplicable("necessary original update evidence missing: " + key)
    elif r["slot"] not in (9, 10):
        required = {key for key in r["records"] if key.startswith(("collect/", "initial/", "binding/"))}
        required.update("learn/1/L65/%03d" % i for i in range(32))
        report["comparisons"].extend({"path": key, "pass": False, "reason": "stop-prefix-missing"}
                                     for key in required - compared)
    compare_learning_projection(torch, reference, candidate, report)
    return stop


def _state_shape(torch, parameter, gradient=None):
    if parameter["dtype"] != "torch.float32":
        raise NotApplicable("parameter outside FP32 coverage")
    state = parameter["state"]
    if state is not None:
        if set(state) != {"step", "exp_avg", "exp_avg_sq"}:
            raise NotApplicable("unexpected Adam state fields")
        for field in ("exp_avg", "exp_avg_sq"):
            tensor = state[field]
            if (not isinstance(tensor, torch.Tensor) or tensor.dtype != torch.float32
                    or tensor.device.type != "cpu" or tensor.layout != torch.strided
                    or list(tensor.shape) != parameter["shape"] or not bool(torch.isfinite(tensor).all())):
                raise NotApplicable("dense finite FP32 moment required")
        step = state["step"]
        if (not isinstance(step, torch.Tensor) or step.numel() != 1
                or not bool(torch.isfinite(step).all()) or float(step) < 0 or float(step) != int(float(step))):
            raise NotApplicable("actual inherited Adam step is invalid")
    if gradient is not None:
        if (not isinstance(gradient, torch.Tensor) or gradient.dtype != torch.float32
                or gradient.layout != torch.strided or gradient.device.type != "cpu"
                or list(gradient.shape) != parameter["shape"] or not bool(torch.isfinite(gradient).all())):
            raise NotApplicable("full dense finite FP32 gradient required")


def compare_updates(torch, reference, candidate, report):
    initial_r, initial_c = reference.start("L65"), candidate.start("L65")
    pre_r = canonical_optimizer(torch, initial_r["optimizer"], initial_r["optimizer_id_mapping"])
    pre_c = canonical_optimizer(torch, initial_c["optimizer"], initial_c["optimizer_id_mapping"])
    previous = {}
    for epoch in (1, 2):
        prefix = "update/%d" % epoch
        grad_r, grad_c = reference.one(prefix + "/gradients"), candidate.one(prefix + "/gradients")
        post_r = canonical_optimizer(torch, reference.one(prefix + "/optimizer"), reference.update_mapping(epoch))
        post_c = canonical_optimizer(torch, candidate.one(prefix + "/optimizer"), candidate.update_mapping(epoch))
        params_r, params_c = reference.one(prefix + "/parameters"), candidate.one(prefix + "/parameters")
        maps_r, maps_c = reference.one(prefix + "/parameter_mapping"), candidate.one(prefix + "/parameter_mapping")
        # Groups/configuration, object aliases/order and all non-moment state
        # fields remain exact.  No moment leaf has been tested/removed as exact.
        rows = []
        rows += exact_rows(torch, pre_r["groups"], pre_c["groups"], prefix + "/groups")
        for label, before, after, mapping in (("reference", pre_r, post_r, maps_r),
                                               ("candidate", pre_c, post_c, maps_c)):
            if before["type"] != "torch.optim.adam.Adam" or after["type"] != "torch.optim.adam.Adam":
                raise NotApplicable("actual optimizer must be exact torch.optim.Adam")
            rows += exact_rows(torch, before["groups"], after["groups"], prefix + "/" + label + "/groups_continuity")
            rows += exact_rows(torch, mapping, [g["names"] for g in after["groups"]],
                               prefix + "/" + label + "/actual_name_mapping")
            rows += exact_rows(torch, before["defaults"], after["defaults"], prefix + "/" + label + "/defaults_continuity")
        names = set(pre_r["parameters"])
        if any(set(value) != names for value in (pre_c["parameters"], post_r["parameters"], post_c["parameters"],
                                                 grad_r, grad_c, params_r, params_c)):
            raise NotApplicable("complete parameter/gradient/mapping name sets differ")
        report["comparisons"].extend(rows)
        report["optimizer_structure_gates"][prefix + "/groups_and_names"] = all_pass(rows)
        if not all_pass(rows):
            return
        for name in sorted(names):
            pr, pc, ar, ac = (state["parameters"][name] for state in (pre_r, pre_c, post_r, post_c))
            path = prefix + "/optimizer/" + name
            discrete = []
            descriptor = lambda p: {k: p[k] for k in ("group", "position", "aliases", "shape", "dtype")}
            discrete += exact_rows(torch, descriptor(pr), descriptor(pc), path + "/structure")
            discrete += exact_rows(torch, descriptor(pr), descriptor(ar), path + "/reference_continuity")
            discrete += exact_rows(torch, descriptor(pc), descriptor(ac), path + "/candidate_continuity")
            gr, gc = grad_r[name], grad_c[name]
            for tensor in (params_r[name], params_c[name]):
                if (not isinstance(tensor, torch.Tensor) or tensor.dtype != torch.float32
                        or tensor.layout != torch.strided or tensor.device.type != "cpu"
                        or list(tensor.shape) != pr["shape"] or not bool(torch.isfinite(tensor).all())):
                    raise NotApplicable("actual parameter outside dense finite FP32 coverage")
            for parameter, gradient in ((pr, gr), (pc, gc), (ar, gr), (ac, gc)):
                _state_shape(torch, parameter, gradient)
            betas, maximize = configuration(pr["configuration"])
            configuration(pc["configuration"])
            if (gr is None) != (gc is None):
                discrete.append({"path": path + "/gradient_presence", "pass": False})
            if (pr["state"] is None) != (pc["state"] is None) or (ar["state"] is None) != (ac["state"] is None):
                discrete.append({"path": path + "/state_presence", "pass": False})
            for label, before, after, gradient in (("reference", pr, ar, gr), ("candidate", pc, ac, gc)):
                if gradient is None:
                    discrete += exact_rows(torch, before["state"], after["state"], path + "/" + label + "/None_unchanged")
                elif after["state"] is None:
                    discrete.append({"path": path + "/" + label + "/state_creation", "pass": False})
                else:
                    step = after["state"]["step"]
                    if before["state"] is None:
                        expected = torch.ones_like(step)
                    else:
                        previous_step = before["state"]["step"]
                        # Follow the actual in-place scalar Tensor increment's
                        # dtype, including inherited steps beyond FP32 2**24.
                        expected = previous_step + 1
                    discrete += exact_rows(torch, expected, step, path + "/" + label + "/step_inheritance")
            if ar["state"] is not None and ac["state"] is not None:
                discrete += exact_rows(torch, ar["state"]["step"], ac["state"]["step"], path + "/step")
            report["comparisons"].extend(discrete)
            report["optimizer_structure_gates"][path] = all_pass(discrete)
            if not all_pass(discrete):
                return
            report["analytic_applicability_gates"][path] = True
            if gr is None:
                # The cross-run bound is carried, not decayed or reset. Each
                # side's unchanged state was just tested exactly.
                report["analytic_moment_gates"][path + "/None_carry"] = True
                continue
            shape = pr["shape"]
            r_m = torch.zeros(shape, dtype=torch.float32) if pr["state"] is None else pr["state"]["exp_avg"]
            r_v = torch.zeros(shape, dtype=torch.float32) if pr["state"] is None else pr["state"]["exp_avg_sq"]
            c_m = torch.zeros(shape, dtype=torch.float32) if pc["state"] is None else pc["state"]["exp_avg"]
            c_v = torch.zeros(shape, dtype=torch.float32) if pc["state"] is None else pc["state"]["exp_avg_sq"]
            old_m, old_v = previous.get(name, (torch.zeros(shape, dtype=torch.float64),
                                              torch.zeros(shape, dtype=torch.float64)))
            next_m, next_v = torch.empty_like(old_m), torch.empty_like(old_v)
            moment_rows = []
            # Fixed-size working chunks, not a sample/acceptance limit.
            for start in range(0, gr.numel(), 16384):
                part = slice(start, min(start + 16384, gr.numel()))
                flat = lambda x: x.reshape(-1)[part]
                m, v, _ = advance(torch, (flat(r_m), flat(r_v)), (flat(c_m), flat(c_v)),
                                  flat(gr), flat(gc), (flat(old_m), flat(old_v)), betas, maximize)
                flat(next_m).copy_(m)
                flat(next_v).copy_(v)
                for field, upper in (("exp_avg", m), ("exp_avg_sq", v)):
                    passed, metrics = difference_within(torch, flat(ar["state"][field]), flat(ac["state"][field]), upper)
                    moment_rows.append(dict(path=path + "/" + field + "/%d" % start,
                                            kind="analytic_moment_v3", pass_=passed, **metrics))
                    moment_rows[-1]["pass"] = moment_rows[-1].pop("pass_")
            previous[name] = next_m, next_v
            report["comparisons"].extend(moment_rows)
            report["analytic_moment_gates"][path] = all_pass(moment_rows)
            if not all_pass(moment_rows):
                return
        pre_r, pre_c = post_r, post_c


def compare_slots_v3(torch, reference_root, candidate_root, contract, method_binding):
    started = time.monotonic()
    cost = {"artifact_loads": 0, "artifact_bytes_loaded": 0, "additional_metadata_bytes_read": 0}
    report = {"version": VERSION, "status": "UNKNOWN", "pass": False,
              "comparison_contract_sha256": method_binding["comparison_contract_sha256"],
              "method_binding": method_binding, "reference_slot": None, "candidate_slot": None,
              "reference_counts": {}, "candidate_counts": {}, "reference_count_gates": {},
              "candidate_count_gates": {}, "initial_state_gates": {}, "optimizer_structure_gates": {},
              "analytic_applicability_gates": {}, "analytic_moment_gates": {},
              "scope_exclusions": [], "comparisons": [], "cost": cost}
    try:
        reference, candidate = (Slot(torch, root, contract, cost) for root in (reference_root, candidate_root))
        report.update(reference_slot=reference.slot, candidate_slot=candidate.slot,
                      reference_counts=reference.index["counts"], candidate_counts=candidate.index["counts"],
                      reference_count_gates=reference.index["count_gates"], candidate_count_gates=candidate.index["count_gates"])
        if (reference.slot, candidate.slot) not in PAIRS:
            raise NotApplicable("pair/reference direction outside the approved sixteen")
        for slot in (reference, candidate):
            if slot.slot == 12 and not isinstance(slot.workers["L65-cold"]["finish"].get("cold_restore_proof"), dict):
                raise NotApplicable("cold missing original sampling-restore proof")
        if not all(report["reference_count_gates"].values()) or not all(report["candidate_count_gates"].values()):
            report["status"] = "FAIL"
            return report
        stop = original_gates(torch, reference, candidate, report)
        if not all_pass(report["comparisons"]):
            report["status"] = "FAIL"
            return report
        reference.semantics(report, "reference")
        candidate.semantics(report, "candidate")
        if not all_pass(report["comparisons"]):
            report["status"] = "FAIL"
            return report
        if stop:
            report["scope_exclusions"].append({"key": "initial_sidecars_and_two_updates",
                                                "reason": "approved stop prefix uses original own baseline; zero Adam"})
        else:
            compare_initial(torch, reference, candidate, report)
            if not all_pass(report["comparisons"]):
                report["status"] = "FAIL"
                return report
            compare_updates(torch, reference, candidate, report)
        report["pass"] = all_pass(report["comparisons"]) and all(
            all(report[name].values()) for name in ("initial_state_gates", "optimizer_structure_gates",
                                                    "analytic_applicability_gates", "analytic_moment_gates"))
        report["status"] = "PASS" if report["pass"] else "FAIL"
    except (NotApplicable, KeyError, ValueError, TypeError, OSError, RuntimeError) as error:
        report["status"], report["pass"] = "UNKNOWN", False
        report["analytic_applicability_gates"]["complete_evidence_and_finite_domain"] = False
        report["comparisons"].append({"path": "evidence_or_applicability", "pass": False,
                                       "status": "UNKNOWN", "reason": type(error).__name__ + ": " + str(error)})
    finally:
        cost.update(comparison_seconds=time.monotonic() - started,
                    artifact_bytes_note="logical bytes per load; hash reads included separately by loader, physical I/O Unknown",
                    peak_memory="Unknown", forward=0, backward=0, actual_adam=0, reference_adam=0)
    return report


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    if len(argv) != 5 or argv[0] != "compare":
        raise SystemExit("usage: analytic_moment_compare.py compare <reference> <candidate> <report> <contract>")
    _, reference, candidate, destination, contract_path = argv
    started = time.monotonic()
    try:
        contract, binding = read_contract(contract_path)
        import torch
        for filename, module_name in (("adam.py", "torch.optim.adam"),
                                      ("optimizer.py", "torch.optim.optimizer")):
            actual = Path(sys.modules[module_name].__file__).read_bytes()
            if hashlib.sha256(actual).hexdigest() != contract["sources"]["optimizer"][filename]["sha256"]:
                raise NotApplicable("installed torch optimizer source differs from reviewed source")
        torch.set_num_threads(2)
        report = compare_slots_v3(torch, reference, candidate, contract, binding)
    except (NotApplicable, ValueError, TypeError, KeyError, OSError, RuntimeError) as error:
        report = {"version": VERSION, "status": "UNKNOWN", "pass": False,
                  "comparison_contract_sha256": None,
                  "reference_slot": None, "candidate_slot": None, "method_binding": {},
                  "reference_counts": {}, "candidate_counts": {}, "reference_count_gates": {},
                  "candidate_count_gates": {}, "initial_state_gates": {}, "optimizer_structure_gates": {},
                  "analytic_applicability_gates": {"frozen_contract": False}, "analytic_moment_gates": {},
                  "scope_exclusions": [], "comparisons": [{"path": "contract", "pass": False,
                                                             "status": "UNKNOWN", "reason": str(error)}], "cost": {}}
    report["cost"]["entry_seconds_before_report_write"] = time.monotonic() - started
    write_json_new(destination, report)
    print(json.dumps({"version": VERSION, "status": report["status"], "pass": report["pass"],
                      "reference_slot": report["reference_slot"], "candidate_slot": report["candidate_slot"],
                      "report": str(destination)}))
    return {"PASS": 0, "FAIL": 1, "UNKNOWN": 2}[report["status"]]


if __name__ == "__main__":
    raise SystemExit(main())
