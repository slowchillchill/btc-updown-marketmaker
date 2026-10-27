"""Finite CPU-only artificial evidence for the explicit #377 comparator.

No production model, checkpoint, capture, CUDA, forward/backward or Adam.step.
Saved .pt files below are tiny hand-constructed data in this fixture's own root.
"""
import argparse
import copy
from fractions import Fraction
import hashlib
import io
import json
import math
from pathlib import Path
import sys
import time
import unittest
from unittest.mock import patch


PYTHON_ROOT = Path(__file__).resolve().parents[2] / "python"
sys.path.insert(0, str(PYTHON_ROOT))
import analytic_moment_bound as bound
import analytic_moment_compare as consumer
from performance_capture import (CaptureWriter, _observation_metadata, compare_value,
                                 gradient_summary, projection_feature_evidence)
from performance_validation import full_work_counts


def cpu_config():
    return {"lr": 0.01, "betas": (0.5, 0.5), "eps": 1e-8, "weight_decay": 0,
            "amsgrad": False, "maximize": False, "foreach": None, "fused": None,
            "capturable": False, "differentiable": False, "decoupled_weight_decay": False}


def mapping(torch, state, gradients=None, ids=(73, 11)):
    entries = []
    for position, (name, identifier, shape, aliases) in enumerate(
            (("p", ids[0], [2], ["p", "p_alias"]), ("q", ids[1], [1], ["q"]))):
        gradient = None if gradients is None else gradients[name]
        entries.append({"position": position, "state_id": identifier, "parameter_name": name,
                        "aliases": aliases, "shape": shape, "dtype": "torch.float32",
                        "requires_grad": True, "has_state": identifier in state["state"],
                        "state_fields": sorted(state["state"].get(identifier, {})),
                        "gradient": {"present": gradient is not None,
                                     "zero": None if gradient is None else not bool(torch.count_nonzero(gradient))}})
    return {"applicability": "present", "optimizer_type": "torch.optim.adam.Adam",
            "groups": [{"group_index": 0, "serialized_ids": list(ids), "runtime_parameter_count": 2,
                        "entries": entries, "configuration": _observation_metadata(torch, cpu_config())}],
            "defaults": _observation_metadata(torch, cpu_config()), "runtime_dispatch": "unknown",
            "unmapped_state_ids": [], "group_length_mismatches": [], "unnamed_runtime_parameters": [],
            "named_parameters_outside_optimizer": []}


def states(torch, candidate=False, ids=(73, 11), none_second=False):
    tensor = lambda value: torch.tensor(value, dtype=torch.float32, device="cpu")
    initial = {"state": {ids[0]: {"step": tensor(7), "exp_avg": tensor([0.125, -0.25]),
                                  "exp_avg_sq": tensor([0.5, 0.25])}},
               "param_groups": [dict(cpu_config(), params=list(ids))]}
    current, post, gradients = copy.deepcopy(initial), [], []
    for epoch in (1, 2):
        delta = 2 ** -20 if candidate else 0.0
        grads = {"p": tensor([0.25 + delta, -0.5 + delta]), "q": None if epoch == 1 else tensor([0])}
        if none_second and epoch == 2:
            grads["p"] = None
        following = copy.deepcopy(current)
        for name, identifier in zip(("p", "q"), ids):
            gradient = grads[name]
            if gradient is None:
                continue
            before = current["state"].get(identifier)
            m = torch.zeros_like(gradient) if before is None else before["exp_avg"]
            v = torch.zeros_like(gradient) if before is None else before["exp_avg_sq"]
            # Independent small dyadic arithmetic, deliberately no optimizer.
            # The chosen betas/inputs have hand-checkable two-step results.
            following["state"][identifier] = {
                "step": tensor(1 if before is None else float(before["step"]) + 1),
                "exp_avg": ((m.double() + gradient.double()) / 2).float(),
                "exp_avg_sq": ((v.double() + gradient.double().square()) / 2).float()}
        gradients.append(grads)
        post.append(following)
        current = following
    return initial, post, gradients


def initial_snapshot(torch, phase, state, ids=(73, 11)):
    tensor = lambda value: torch.tensor(value, dtype=torch.float32, device="cpu")
    learner = phase != "C65"
    point = {"C65": "post_service_load_pre_first_consume", "L65": "post_learn_open_pre_collection_and_update",
             "L65-cold": "post_learn_open_pre_collection_sampling_restore"}[phase]
    absent = {"applicability": "not_applicable", "optimizer_type": None, "groups": [], "defaults": None,
              "runtime_dispatch": "unknown", "unmapped_state_ids": [], "group_length_mismatches": [],
              "unnamed_runtime_parameters": [], "named_parameters_outside_optimizer": []}
    return {"version": "b-runtime-state-observation-v1", "role": "learner" if learner else "consumer",
            "phase": phase, "point": point,
            "model": {"p": tensor([1, 2]), "p_alias": tensor([1, 2]), "q": tensor([3]), "persistent_buffer": tensor([4])},
            "optimizer": copy.deepcopy(state) if learner else None,
            "optimizer_defaults": cpu_config() if learner else None,
            "optimizer_applicability": "present" if learner else "not_applicable",
            "optimizer_id_mapping": mapping(torch, state, ids=ids) if learner else absent,
            "torch_rng": torch.tensor([1, 2, 3], dtype=torch.uint8),
            "cuda_rng": {"initialized": False, "scope": "not_applicable", "states": []},
            "account_sampling": {}, "gradients": {"p": None, "q": None},
            "source_boundary": {"group_id": "artificial-group" if learner else None,
                                "sequence": 2 if learner else None,
                                "checkpoint_sha256": "a" * 64 if learner else None}}


def write_learning_frames(torch, writer, phase, stopped):
    """Hand-built CPU values through the real writer; no model or gradient call.

    The 65+4 coordinates and two epochs exercise the current primary/derived
    record contract. Counts below describe artificial input, never real work.
    """
    samples = [{"name": "L65/%03d" % i, "account": "artificial", "call": i + 1}
               for i in range(65)]
    samples += [{"name": case, "account": "artificial-" + case, "call": 1}
                for case in ("F-N", "F-P", "F-R", "F-S")]
    for epoch in ((1,) if stopped else (1, 2)):
        for ordinal, sample in enumerate(samples[:32] if stopped else samples):
            key = "learn/%d/%s" % (epoch, sample["name"])
            projected = torch.tensor([ordinal / 128, epoch / 8], dtype=torch.float32)
            features = projected.reshape(1, 1, -1)
            evidence = projection_feature_evidence(projected, features)
            writer.write(key, {"values": {"projection_output": [projected]},
                               "kinds": {"projection_output": "activation"}}, "observed")
            writer.write_metadata("projection-features.jsonl", {
                "account": sample["account"], "call": sample["call"], "batch": 0, "time": 0,
                "epoch": epoch, "phase": phase, "sample": sample["name"],
                "train": True, "grad_enabled": True,
                "precision": {"torch_intra_threads": 2, "torch_interop_threads": 1,
                              "omp_threads": "2", "mkl_threads": "2", "device": "cpu"},
                **evidence})
            if sample["name"].startswith("F-"):
                for component in ("actor", "critic", "loss"):
                    gradients = {name: torch.tensor([0.25], dtype=torch.float32)
                                 for name in ("option.bias", "decoder.weight", "bit.bias")}
                    writer.write(key + "/component_gradient/" + component, gradients, "gradient")
                    summary = gradient_summary(torch, gradients)
                    required = {"finite": all(value["finite"] for value in summary.values())}
                    if component == "actor" and sample["name"] in ("F-N", "F-P", "F-R"):
                        required["actor"] = not summary["option.bias"]["zero"]
                        if sample["name"] in ("F-P", "F-R"):
                            required.update(decoder=not summary["decoder.weight"]["zero"],
                                            bit=not summary["bit.bias"]["zero"])
                    writer.write(key + "/gradient_summary/" + component,
                                 {"parameters": summary, "required": required,
                                  "pass": all(required.values())}, "coverage")


def make_slot(torch, root, slot, contract, candidate=False, ids=(73, 11), none_second=False):
    root.mkdir()
    variant, forward, adam = consumer.SLOTS[slot]
    initial, posts, gradients = states(torch, candidate, ids, none_second)
    phases = ["C65", "L65", "L65-cold"] if slot == 12 else ["C65", "L65"]
    for phase in phases:
        directory = root / phase
        directory.mkdir()
        identity = {"slot": slot, "phase": phase, "variant": variant,
                    "service_identity": {"torch": "2.8.0+cu128", "config": {"width": 2}},
                    "source": {"files": {k: v["sha256"] for k, v in contract["sources"]["implementations"][variant].items()}},
                    "observer": {k: v["sha256"] for k, v in contract["sources"]["observer"].items()},
                    "method": {"sha256": consumer.METHODS["validation"]}, "execution_method": "artificial-fixed",
                    "capture_method": "artificial-fixed", "auxiliary_loss_ast_sha256": "b" * 64}
        (directory / "identity.json").write_text(json.dumps(identity))
        finish = {"phase": phase, "stop_notified": slot in (9, 10),
                  "cold_restore_proof": {"pass": True,
                     "comparisons": [{"path": "sampling_restore/accounts/artificial/rng_state/0", "pass": True}],
                     "expected_calls": {"artificial": 65},
                     "result": {"restored": True, "calls": 65, "accounts": ["artificial"]}}
                     if phase == "L65-cold" else None}
        if slot in (9, 10) and phase == "L65":
            finish["stop_state_unchanged"] = [{"path": "stop_state/parameters/p", "pass": True},
                                               {"path": "stop_state/optimizer/step", "pass": True}]
            finish["learning_sampling_unchanged"] = [{"path": "sampling/artificial/rng", "pass": True}]
        (directory / "finish.json").write_text(json.dumps(finish))
        writer = CaptureWriter(torch, directory / "captures", 1 << 20)
        if slot not in (9, 10):
            snapshot = initial_snapshot(torch, phase, initial, ids)
            writer.write_initial_observation(snapshot, {key: snapshot[key] for key in
                ("version", "role", "phase", "point", "optimizer_id_mapping")} |
                {"record_kind": "initial_state", "slot": slot, "event_monotonic": time.monotonic()})
        writer.write("binding/" + phase + "/samples",
                     [{"name": "L65/%03d" % i, "account": "artificial", "call": i + 1}
                      for i in range(65)], "exact")
        writer.write("binding/" + phase + "/fixtures",
                     [{"case": case, "frame": {"account": "artificial-" + case, "call": 1}}
                      for case in ("F-N", "F-P", "F-R", "F-S")], "exact")
        if phase == "C65":
            writer.write("collect/artifical", torch.tensor([0.5]), "activation")
        updating = phase == ("L65-cold" if slot == 12 else "L65")
        if updating:
            write_learning_frames(torch, writer, phase, slot in (9, 10))
            writer.write("learning_sampling_state", {"artificial": {"calls": 65,
                         "rng": torch.tensor([1, 2, 3], dtype=torch.uint8)}}, "exact")
        if adam and updating:
            for epoch in (1, 2):
                prefix = "update/%d" % epoch
                writer.write(prefix + "/gradients", gradients[epoch - 1], "gradient")
                writer.write(prefix + "/parameters", {"p": torch.tensor([1.0, 2.0]), "q": torch.tensor([3.0])}, "parameter")
                writer.write(prefix + "/optimizer", posts[epoch - 1], "exact")
                writer.write(prefix + "/parameter_mapping", [["p", "q"]], "exact")
                writer.write(prefix + "/same_gradient_adam", [
                    {"path": "parameters/p", "kind": "parameter", "pass": True},
                    {"path": "optimizer", "kind": "exact", "pass": True}], "diagnostic")
                writer.write_metadata("runtime-observations.jsonl", {
                    "version": "b-runtime-state-observation-v1", "record_kind": "optimizer_id_mapping",
                    "point": "post_actual_update", "phase": phase, "epoch": epoch,
                    "gradient_observation_point": "pre_update",
                    "mapping": mapping(torch, posts[epoch - 1], gradients[epoch - 1], ids)})
        writer.close()
    # These are explicitly synthetic report inputs, not measured model calls.
    counts = full_work_counts(slot)
    counts.update(actual_adam_completed=adam, reference_adam_completed=adam,
                  production_frame_calls=130 if variant != "R0" else 0,
                  production_sampling_restore=1 if slot == 12 else 0,
                  production_sampling_restore_completed=1 if slot == 12 else 0)
    (root / "work.jsonl").write_text(json.dumps({"counts": counts}) + "\n")


class ArtificialChecks(unittest.TestCase):
    serial = 0

    def directory(self, name):
        type(self).serial += 1
        return self.root / ("%02d-" % self.serial + name)

    def pair(self, candidate_slot=2, candidate=True):
        r, c = self.directory("reference"), self.directory("candidate")
        make_slot(self.torch, r, 1, self.contract)
        make_slot(self.torch, c, candidate_slot, self.contract, candidate)
        return r, c

    def compare(self, r, c):
        report = consumer.compare_slots_v3(self.torch, r, c, self.contract, self.binding)
        self.comparison_costs.append(report["cost"])
        return report

    def rewrite_capture(self, slot, key, mutate):
        index = consumer.capture_index(slot)
        item = index["records"][key][0]
        value = consumer.load_capture(self.torch, item)
        value = mutate(value)
        self.torch.save(value, item["path"])
        indexpath = item["path"].parent / "index.jsonl"
        rows = [json.loads(line) for line in indexpath.read_text().splitlines()]
        for row in rows:
            if row["key"] == key:
                data = item["path"].read_bytes()
                row.update(bytes=len(data), sha256=hashlib.sha256(data).hexdigest())
        indexpath.write_text("".join(json.dumps(row) + "\n" for row in rows))

    def rewrite_initial(self, slot, phase, mutate):
        directory = slot / phase / "captures"
        artifact, metadata = directory / "initial-state.pt", directory / "runtime-observations.jsonl"
        value = self.torch.load(artifact, map_location="cpu", weights_only=True)
        mutate(value)
        self.torch.save(value, artifact)
        raw = artifact.read_bytes()
        rows = [json.loads(line) for line in metadata.read_text().splitlines()]
        for row in rows:
            if row["record_kind"] == "initial_state":
                row.update(bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest())
        metadata.write_text("".join(json.dumps(row) + "\n" for row in rows))

    def test_01_upward_primitives_and_u32(self):
        t = self.torch
        values = [0.0, 2 ** -149, 2 ** -126, 0.1, 1.0, 2 ** 100]
        for a, b in zip(values, reversed(values)):
            add, mul = float(bound.add_up(t, a, b)), float(bound.mul_up(t, a, b))
            self.assertGreaterEqual(Fraction(add), Fraction(a) + Fraction(b))
            self.assertGreaterEqual(Fraction(mul), Fraction(a) * Fraction(b))
        self.assertGreater(float(bound.mul_up(t, math.ulp(0.0), 0.5)), 0)
        self.assertEqual(float(bound.mul_up(t, 0, 4)), 0)
        self.assertGreaterEqual(Fraction(bound.one_minus_up(0.9)), 1 - Fraction(0.9))
        for value in values[:-1]:
            self.assertGreater(float(bound.u32(t, value)), value)
        with self.assertRaises(bound.NotApplicable):
            bound.u32(t, bound.F32_MAX)

    def test_02_independent_rho_and_no_post_fit(self):
        t = self.torch
        m, v, g = t.tensor([0.125]), t.tensor([0.5]), t.tensor([0.25])
        rm, rv = bound.rho(t, m, v, g, 0.5)
        u, tau = Fraction(1, 2 ** 24), Fraction(1, 2 ** 126)
        gamma = lambda n: n * u / (1 - n * u)
        expected_m = 2 * gamma(6) * (Fraction(1, 8) + Fraction(1, 4)) + 7 * (1 + u) ** 6 * tau
        expected_v = gamma(5) * (Fraction(1, 4) + Fraction(1, 32)) + (1 + u) ** 5 * (tau * 6 + tau ** 2)
        self.assertGreaterEqual(Fraction(float(rm)), expected_m)
        self.assertGreaterEqual(Fraction(float(rv)), expected_v)
        self.assertNotIn("post", consumer.advance.__code__.co_varnames)
        passed, _ = bound.difference_within(t, t.tensor([0.0]), t.tensor([1.0]), t.tensor([1.0], dtype=t.float64))
        self.assertTrue(passed)
        self.assertFalse(bound.difference_within(t, t.tensor([0.0]), t.tensor([1.0]),
                                                 t.tensor([math.nextafter(1.0, 0)], dtype=t.float64))[0])

    def test_03_two_updates_nonzero_initial_and_missing_state(self):
        r, c = self.pair()
        report = self.compare(r, c)
        self.assertTrue(report["pass"], json.dumps(report))
        self.assertEqual(report["status"], "PASS")
        self.assertTrue(report["analytic_moment_gates"]["update/2/optimizer/p"])
        self.assertTrue(report["analytic_moment_gates"]["update/1/optimizer/q/None_carry"])
        self.assertTrue(report["optimizer_structure_gates"]["update/2/optimizer/q"])
        records = consumer.capture_index(c)["records"]
        primary = [key for key, items in records.items() if key.startswith("learn/")
                   and items[0]["kind"] == "observed"]
        derived = [key for key in records if "/component_gradient/" in key or "/gradient_summary/" in key]
        self.assertEqual(len(primary), 138)
        self.assertEqual(len(derived), 48)
        self.assertTrue(any(row["path"].startswith("projection-layout/") for row in report["comparisons"]))
        (self.root / "positive-two-update-report.json").write_text(json.dumps(report, indent=2))
        # Cross-run moments genuinely differ; no old failed report is filtered.
        rs = consumer.load_capture(self.torch, consumer.capture_index(r)["records"]["update/1/optimizer"][0])
        cs = consumer.load_capture(self.torch, consumer.capture_index(c)["records"]["update/1/optimizer"][0])
        self.assertFalse(all(row["pass"] for row in compare_value(self.torch, rs, cs, "exact", "old_cross_exact")))

    def test_04_original_gradient_parameter_and_diagnostic_not_rescued(self):
        for key, mutate in (
            ("update/1/gradients", lambda v: dict(v, p=v["p"] + 1)),
            ("update/1/parameters", lambda v: dict(v, p=v["p"] + 1)),
            ("update/1/same_gradient_adam", lambda v: [dict(row, **{"pass": False}) for row in v]),
            ("learn/1/F-N/component_gradient/actor", lambda v: {key: value + 1 for key, value in v.items()}),
            ("learn/1/F-N/gradient_summary/actor", lambda v: dict(v, **{"pass": False}))):
            r, c = self.pair()
            self.rewrite_capture(c, key, mutate)
            report = self.compare(r, c)
            self.assertFalse(report["pass"])
            self.assertEqual(report["status"], "FAIL")
            self.assertEqual(report["analytic_moment_gates"], {})

    def test_05_state_inheritance_and_bound_negative(self):
        for field, value in (("step", 1.0), ("exp_avg", 100.0), ("exp_avg_sq", 100.0)):
            r, c = self.pair()
            def mutate(state):
                state["state"][73][field].fill_(value)
                return state
            self.rewrite_capture(c, "update/2/optimizer", mutate)
            report = self.compare(r, c)
            self.assertFalse(report["pass"])
            self.assertEqual(report["status"], "FAIL")

    def test_06_initial_mapping_missing_and_wrong_point(self):
        for mutation in ("mapping", "point", "missing", "model"):
            r, c = self.pair()
            directory = c / "L65/captures"
            path = directory / "runtime-observations.jsonl"
            rows = [json.loads(line) for line in path.read_text().splitlines()]
            if mutation == "missing":
                rows = [row for row in rows if row["record_kind"] != "initial_state"]
            elif mutation == "point":
                rows[0]["point"] = "post_update"
            elif mutation == "mapping":
                rows[0]["optimizer_id_mapping"]["groups"][0]["entries"][0]["state_id"] = 11
            else:
                artifact = directory / "initial-state.pt"
                value = self.torch.load(artifact, map_location="cpu", weights_only=True)
                value["model"]["persistent_buffer"].add_(1)
                self.torch.save(value, artifact)
                raw = artifact.read_bytes()
                rows[0].update(bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest())
            path.write_text("".join(json.dumps(row) + "\n" for row in rows))
            report = self.compare(r, c)
            self.assertFalse(report["pass"])
            self.assertEqual(report["status"], "FAIL" if mutation == "model" else "UNKNOWN")

    def test_07_coverage_and_none_zero_distinctions(self):
        config = cpu_config()
        self.assertEqual(consumer.configuration(config), ((0.5, 0.5), False))
        for key, bad in (("fused", True), ("weight_decay", 0.1), ("capturable", True),
                         ("differentiable", True), ("amsgrad", True), ("betas", [0.5, 1])):
            with self.assertRaises(bound.NotApplicable):
                consumer.configuration(dict(config, **{key: bad}))
        r, c = self.pair()
        self.rewrite_capture(c, "update/1/gradients", lambda value: dict(value, q=self.torch.tensor([0.0])))
        self.assertFalse(self.compare(r, c)["pass"])
        with self.assertRaises(bound.NotApplicable):
            bound.rho(self.torch, self.torch.tensor([math.inf]), self.torch.tensor([0.0]), self.torch.tensor([0.0]), 0.5)

    def test_08_stop_and_cold_corresponding_points(self):
        r, stop = self.pair(9)
        report = self.compare(r, stop)
        self.assertTrue(report["pass"], json.dumps(report))
        self.assertEqual(report["initial_state_gates"], {})
        self.assertEqual(report["analytic_moment_gates"], {})
        a, b = self.directory("stop-A"), self.directory("stop-B")
        make_slot(self.torch, a, 9, self.contract)
        make_slot(self.torch, b, 10, self.contract)
        self.assertTrue(self.compare(a, b)["pass"])
        r, cold = self.pair(12)
        report = self.compare(r, cold)
        self.assertTrue(report["pass"], json.dumps(report))
        self.assertTrue(report["initial_state_gates"]["candidate_cold_same_post_open"])
        (self.root / "positive-cold-report.json").write_text(json.dumps(report, indent=2))

    def test_09_contract_real_cli_and_output_not_aliasing(self):
        r, c = self.pair()
        output = self.root / "artificial-cli-report.json"
        with patch("sys.stdout", new=io.StringIO()):
            rc = consumer.main(["compare", str(r), str(c), str(output), str(self.contract_path)])
        report = json.loads(output.read_text())
        self.comparison_costs.append(report["cost"])
        self.assertEqual(rc, 0, json.dumps(report))
        self.assertEqual(report["comparison_contract_sha256"], self.binding["comparison_contract_sha256"])
        self.assertEqual(report["status"], "PASS")
        self.assertTrue(report["pass"])
        # Changing the predecessor state must change step-2 provenance, not be
        # accepted as a reset M0/V0; all artifacts above remain independent files.
        self.assertEqual(report["reference_counts"]["actual_adam"], 2)
        with self.assertRaises(bound.NotApplicable):
            consumer.canonical_optimizer(self.torch, states(self.torch)[0],
                dict(mapping(self.torch, states(self.torch)[0]), unmapped_state_ids=[999]))

    def test_10_none_carries_prior_nonzero_cross_run_bound(self):
        r, c = self.directory("None-reference"), self.directory("None-candidate")
        make_slot(self.torch, r, 1, self.contract, none_second=True)
        make_slot(self.torch, c, 2, self.contract, candidate=True, none_second=True)
        report = self.compare(r, c)
        self.assertTrue(report["pass"], json.dumps(report))
        self.assertTrue(report["analytic_moment_gates"]["update/2/optimizer/p/None_carry"])
        def violate_unchanged(state):
            state["state"][73]["exp_avg"].add_(2 ** -20)
            return state
        self.rewrite_capture(c, "update/2/optimizer", violate_unchanged)
        report = self.compare(r, c)
        self.assertFalse(report["pass"])
        self.assertEqual(report["status"], "FAIL")

    def test_11_old_gate_reference_direction_and_finite_domain(self):
        t = self.torch
        r, c = t.tensor([1000.0]), t.tensor([1001.0009765625])
        self.assertFalse(compare_value(t, r, c, "gradient")[0]["pass"])
        self.assertTrue(compare_value(t, c, r, "gradient")[0]["pass"])
        self.assertGreaterEqual(float(bound.gradient_envelope(t, c)), float((r.double() - c.double()).abs()))
        with self.assertRaises(bound.NotApplicable):
            bound.add_up(t, sys.float_info.max, sys.float_info.max)
        # An inherited FP32 step increments in its actual dtype; no Python
        # float coercion may manufacture a missing +1 at this boundary.
        step = t.tensor(float(2 ** 24), dtype=t.float32)
        self.assertEqual(float(step + 1), float(step))

    def test_12_stop_cold_missing_existing_proof_is_unknown(self):
        for slot, phase, key in ((9, "L65", "stop_state_unchanged"),
                                 (9, "L65", "learning_sampling_unchanged"),
                                 (12, "L65-cold", "cold_restore_proof")):
            r, c = self.pair(slot)
            finishpath = c / phase / "finish.json"
            value = json.loads(finishpath.read_text())
            del value[key]
            finishpath.write_text(json.dumps(value))
            report = self.compare(r, c)
            self.assertFalse(report["pass"])
            self.assertEqual(report["status"], "UNKNOWN")
        # Both stop slots losing a common frame must not pass by intersection.
        a, b = self.directory("stop-missing-A"), self.directory("stop-missing-B")
        make_slot(self.torch, a, 9, self.contract)
        make_slot(self.torch, b, 10, self.contract)
        for slotroot in (a, b):
            index = slotroot / "L65/captures/index.jsonl"
            rows = [json.loads(line) for line in index.read_text().splitlines()]
            index.write_text("".join(json.dumps(row) + "\n" for row in rows if row["key"] != "learn/1/L65/031"))
        report = self.compare(a, b)
        self.assertFalse(report["pass"])
        self.assertEqual(report["status"], "UNKNOWN")
        # The primary Tensor still exists: a missing projection row must fail.
        r, c = self.pair()
        projection = c / "L65/captures/projection-features.jsonl"
        rows = [json.loads(line) for line in projection.read_text().splitlines()]
        projection.write_text("".join(json.dumps(row) + "\n" for row in rows
                                     if not (row["epoch"] == 1 and row["sample"] == "L65/000")))
        report = self.compare(r, c)
        self.assertFalse(report["pass"])
        self.assertEqual(report["status"], "UNKNOWN")

    def test_13_c65_globals_retained_applicable_schema_and_learner_exact(self):
        t = self.torch
        r, c = self.pair()
        def complete_cuda(value):
            value["cuda_rng"] = {"initialized": True, "scope": "actual_initialized_service_device",
                                 "states": [{"device": 0, "state": t.tensor([4, 5, 6], dtype=t.uint8)}]}
        self.rewrite_initial(r, "C65", complete_cuda)
        self.rewrite_initial(c, "C65", complete_cuda)
        def different_globals(value):
            value["torch_rng"].add_(1)
            value["cuda_rng"]["states"][0]["state"].add_(1)
        self.rewrite_initial(c, "C65", different_globals)
        report = self.compare(r, c)
        self.assertTrue(report["pass"], json.dumps(report))
        self.assertEqual(set(report["method_binding"]),
                         {"comparison_contract_sha256", "method", "source_constraints_sha256"})
        exclusion = next(row for row in report["scope_exclusions"]
                         if row["key"] == "initial/C65/global_rng_cross_worker_equality")
        self.assertEqual(exclusion["status"], "not_applicable")
        self.assertEqual(exclusion["role"], "consumer")
        self.assertEqual(exclusion["point"], "post_service_load_pre_first_consume")
        self.assertEqual(exclusion["fixed_source_evidence"], consumer.C65_APPLICABILITY)
        self.assertTrue(exclusion["complete_records_retained"]["candidate"]["sha256"])
        (self.root / "c65-applicability-positive-report.json").write_text(json.dumps(report, indent=2))
        # A different persistent model buffer is still an initial exact failure.
        self.rewrite_initial(c, "C65", lambda value: value["model"]["persistent_buffer"].add_(1))
        self.assertEqual(self.compare(r, c)["status"], "FAIL")
        for omitted in ("torch_rng", "applicable_cuda_state"):
            r, c = self.pair()
            self.rewrite_initial(c, "C65", complete_cuda)
            def omit(value):
                if omitted == "torch_rng":
                    del value["torch_rng"]
                else:
                    value["cuda_rng"]["states"] = []
            self.rewrite_initial(c, "C65", omit)
            report = self.compare(r, c)
            self.assertFalse(report["pass"])
            self.assertEqual(report["status"], "UNKNOWN")
        # Same phase restriction: normal learner and cold globals stay exact.
        for slot, phase in ((2, "L65"), (12, "L65-cold")):
            r, c = self.pair(slot)
            self.rewrite_initial(c, phase, lambda value: value["torch_rng"].add_(1))
            report = self.compare(r, c)
            self.assertFalse(report["pass"])
            self.assertEqual(report["status"], "FAIL")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--evidence-root", type=Path, required=True)
    parser.add_argument("--contract", type=Path, required=True)
    parser.add_argument("--stop-cold-only", action="store_true")
    parser.add_argument("--c65-rng-only", action="store_true")
    parser.add_argument("--current-interface-only", action="store_true")
    args = parser.parse_args()
    args.evidence_root.mkdir(parents=True, exist_ok=False)
    import torch
    torch.set_num_threads(2)
    torch.set_num_interop_threads(1)
    calls = []
    def forbidden(*unused, **kwargs):
        calls.append("forbidden CUDA/Adam access")
        raise AssertionError(calls[-1])
    ArtificialChecks.torch = torch
    ArtificialChecks.root = args.evidence_root
    ArtificialChecks.contract_path = args.contract
    ArtificialChecks.contract, ArtificialChecks.binding = consumer.read_contract(args.contract)
    ArtificialChecks.comparison_costs = []
    patches = [patch.object(torch.cuda, name, forbidden) for name in (
        "get_rng_state", "get_rng_state_all", "is_available", "is_initialized", "current_device", "_lazy_init")]
    patches.append(patch.object(torch.optim.Adam, "step", forbidden))
    patches.extend((patch.object(torch.optim.Adam, "__init__", forbidden),
                    patch.object(torch.Tensor, "backward", forbidden),
                    patch.object(torch.autograd, "grad", forbidden)))
    for item in patches:
        item.start()
    started = time.monotonic()
    try:
        if args.current_interface_only:
            names = ("test_03_two_updates_nonzero_initial_and_missing_state",
                     "test_04_original_gradient_parameter_and_diagnostic_not_rescued",
                     "test_05_state_inheritance_and_bound_negative",
                     "test_06_initial_mapping_missing_and_wrong_point",
                     "test_07_coverage_and_none_zero_distinctions",
                     "test_08_stop_and_cold_corresponding_points",
                     "test_09_contract_real_cli_and_output_not_aliasing",
                     "test_10_none_carries_prior_nonzero_cross_run_bound",
                     "test_12_stop_cold_missing_existing_proof_is_unknown",
                     "test_13_c65_globals_retained_applicable_schema_and_learner_exact")
            suite = unittest.TestSuite(ArtificialChecks(name) for name in names)
        elif args.c65_rng_only:
            suite = unittest.TestSuite([ArtificialChecks(
                "test_13_c65_globals_retained_applicable_schema_and_learner_exact")])
        elif args.stop_cold_only:
            suite = unittest.TestSuite(ArtificialChecks(name) for name in (
                "test_08_stop_and_cold_corresponding_points",
                "test_09_contract_real_cli_and_output_not_aliasing",
                "test_12_stop_cold_missing_existing_proof_is_unknown"))
        else:
            suite = unittest.defaultTestLoader.loadTestsFromTestCase(ArtificialChecks)
        result = unittest.TextTestRunner(verbosity=2, failfast=True).run(suite)
    finally:
        for item in reversed(patches):
            item.stop()
    elapsed = time.monotonic() - started
    summary = {"version": "b-analytic-cpu-artificial-check-v1", "tests": result.testsRun,
               "failures": len(result.failures), "errors": len(result.errors), "pass": result.wasSuccessful(),
               "assertion_seconds": elapsed, "torch": str(torch.__version__), "threads": [2, 1],
               "cuda_or_adam_step_calls": calls, "production_models": 0, "real_capture_reads": 0,
               "artificial_comparer_calls": len(ArtificialChecks.comparison_costs),
               "comparer_subprocesses": 0,
               "artificial_artifact_loads": sum(row["artifact_loads"] for row in ArtificialChecks.comparison_costs),
               "artificial_artifact_logical_bytes": sum(row["artifact_bytes_loaded"] for row in ArtificialChecks.comparison_costs),
               "artificial_metadata_logical_bytes": sum(row["additional_metadata_bytes_read"] for row in ArtificialChecks.comparison_costs)}
    (args.evidence_root / "result.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary))
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main())
