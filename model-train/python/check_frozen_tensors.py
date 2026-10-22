"""单次独立CPU只读核对两臂原提交与导出；零Policy、零RNG恢复、零前向。"""
import hashlib
import json
from pathlib import Path
import sys


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def main():
    import torch
    torch.set_num_threads(2)
    config = json.loads(Path(sys.argv[1]).read_text())
    reports = []
    for path in config["exports"]:
        export = json.loads(Path(path).read_text())
        before = sha(export["checkpoint"])
        checkpoint = torch.load(export["checkpoint"], map_location="cpu", weights_only=True)
        frozen = torch.load(export["output_model"], map_location="cpu", weights_only=True)
        assert before == export["checkpoint_sha256"] == sha(export["checkpoint"])
        assert checkpoint["sequence"] == export["sequence"]
        assert checkpoint["identity"]["model_sha256"] == config["initial_model_sha256"]
        assert checkpoint["model"].keys() == frozen["state_dict"].keys()
        for name, value in checkpoint["model"].items():
            other = frozen["state_dict"][name]
            assert value.dtype == other.dtype and value.shape == other.shape and torch.equal(value, other), name
        state = checkpoint["optimizer"]["state"]
        assert state and any(torch.count_nonzero(s["exp_avg"]).item() for s in state.values())
        assert all(s["step"].item() == 2 for s in state.values())
        assert frozen["frozen_source"]["target_version"] == checkpoint["target_version"]
        reports.append({"source_checkpoint_sha256": before, "frozen_sha256": sha(export["output_model"]),
                        "tensor_count": len(checkpoint["model"]), "all_tensors_equal": True,
                        "optimizer_state_count": len(state), "adam_step": 2, "nonzero_first_moment": True,
                        "sequence": checkpoint["sequence"], "source_unchanged": True})
    with Path(sys.argv[2]).open("x") as output:
        output.write(json.dumps({"arms": reports, "policy_constructions": 0, "rng_initializations": 0,
                                          "forwards": 0, "updates": 0}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
