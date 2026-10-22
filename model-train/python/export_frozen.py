"""从完整学习提交只读导出消费者包；不构造Policy，不初始化或恢复RNG。"""
import hashlib
import json
from pathlib import Path
import sys


def file_sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def export_frozen(config, torch_module=None):
    # 仅显式执行导出才装入Torch；配置检查和源码测试不导入模型运行环境。
    if torch_module is None:
        import torch as torch_module
    source = Path(config["checkpoint"])
    if file_sha256(source) != config["checkpoint_sha256"]:
        raise ValueError("导出源checkpoint身份不符")
    checkpoint = torch_module.load(source, map_location="cpu", weights_only=True)
    if checkpoint["version"] != "ppo-group-transaction-v1":
        raise ValueError("未知学习提交格式")
    for key in ("sequence", "model_version", "encoding_version", "target_version", "trajectory_version"):
        if checkpoint[key] != config[key]:
            raise ValueError("导出源提交/模型/目标身份不符: " + key)
    if checkpoint["identity"]["config"] != config["shape"]:
        raise ValueError("导出源模型形状/初始化身份不符")
    implementations = checkpoint["source_implementations"]
    if not implementations or any(v != config["source_implementation"] for v in implementations):
        raise ValueError("导出源程序身份不符")
    binding = {key: config[key] for key in ("checkpoint_sha256", "sequence", "source_implementation", "model_version", "encoding_version", "target_version", "trajectory_version", "shape")}
    package = {"version": checkpoint["model_version"], "encoding_version": checkpoint["encoding_version"],
               "config": checkpoint["identity"]["config"], "state_dict": checkpoint["model"],
               "frozen_source": binding}
    # 禁止覆盖已有产物；不写源checkpoint/optimizer/消费历史。
    with Path(config["output_model"]).open("xb") as stream:
        torch_module.save(package, stream)
    return {"output_model": config["output_model"], "sha256": file_sha256(config["output_model"]),
            "frozen_source": binding, "forwards": 0, "updates": 0,
            "policy_constructions": 0, "rng_initializations": 0}


def main():
    with Path(sys.argv[1]).open(encoding="utf-8") as stream:
        config = json.load(stream)
    if len(sys.argv) == 3 and sys.argv[2] == "--print-config":
        print(json.dumps(config, ensure_ascii=False))
        return
    print(json.dumps(export_frozen(config), ensure_ascii=False))


if __name__ == "__main__":
    main()
