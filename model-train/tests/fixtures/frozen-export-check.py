"""正式导出入口的stdlib协议替身：零Torch、零模型、零张量解析。"""
import ast
import hashlib
from pathlib import Path
import runpy
import sys
import tempfile
import types

source=Path(sys.argv[1])
module=runpy.run_path(str(source))
# 实际模块只在export_frozen执行且未注入替身时导入torch。
assert "torch" not in sys.modules
calls=[]
with tempfile.TemporaryDirectory() as root:
    checkpoint_path=Path(root)/"checkpoint.pt"
    checkpoint_path.write_bytes(b"opaque-original-checkpoint")
    implementation={"git_commit":"source-commit","source_identity_sha256":"1"*64,"cargo_lock_sha256":"2"*64}
    model={"parameter":object()}
    checkpoint={"version":"ppo-group-transaction-v1","sequence":2,"model_version":"b-gru-actor-critic-gtc-v2","encoding_version":"b-public-token-log1p-v1","target_version":"confirmed-finite-j-special-v1","trajectory_version":"learning-trajectory-special-v1","identity":{"config":{"width":48}},"source_implementations":[implementation],"model":model}
    config={key:checkpoint[key] for key in ("sequence","model_version","encoding_version","target_version","trajectory_version")}
    config.update(checkpoint=str(checkpoint_path),checkpoint_sha256=module["file_sha256"](checkpoint_path),shape={"width":48},source_implementation=implementation,output_model=str(Path(root)/"frozen.pt"))
    def load(path,**kwargs):
        calls.append((str(path),kwargs))
        assert kwargs=={"map_location":"cpu","weights_only":True}
        return checkpoint
    def save(package,stream):
        assert package["state_dict"] is model
        assert package["config"] is checkpoint["identity"]["config"]
        assert package["frozen_source"]["target_version"]==checkpoint["target_version"]
        stream.write(b"opaque-frozen-package")
    fake=types.SimpleNamespace(load=load,save=save)
    result=module["export_frozen"](config,fake)
    assert result["policy_constructions"]==result["rng_initializations"]==result["forwards"]==result["updates"]==0
    assert module["file_sha256"](checkpoint_path)==config["checkpoint_sha256"]
    bad=dict(config,sequence=3,output_model=str(Path(root)/"must-not-exist.pt"))
    try:module["export_frozen"](bad,fake)
    except ValueError:pass
    else:raise AssertionError("不同提交身份未拒绝")
    assert not Path(bad["output_model"]).exists()
for path in source.parent.glob("*.py"):
    ast.parse(path.read_text())
print("正式导出协议/源不变/无Policy或RNG调用已核；实际Torch张量同值待单独签发")
