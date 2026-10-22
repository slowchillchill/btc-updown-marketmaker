"""仅抽取正式open方法，以元数据替身核对v1在load_state前拒绝；不导入torch。"""
import ast
import contextlib
from pathlib import Path
import sys
import tempfile
import types

source = ast.parse(Path(sys.argv[1]).read_text())
cls = next(node for node in source.body if isinstance(node, ast.ClassDef) and node.name == "LearningEngine")
method = next(node for node in cls.body if isinstance(node, ast.FunctionDef) and node.name == "open")
loaded = []
model = types.SimpleNamespace(parameters=lambda: (), load_state_dict=lambda *a, **kw: loaded.append("model"))
optimizer = types.SimpleNamespace(load_state_dict=lambda *a: loaded.append("optimizer"))
config = {"learning_rate": .0003, "checkpoint_directory": ""}
checkpoint = {"config": config, "model_version": "fixture", "version": "ppo-group-transaction-v1",
              "encoding_version": "b-public-token-log1p-v1", "target_version": "confirmed-finite-j-v1",
              "trajectory_version": "learning-trajectory-v1"}
torch = types.ModuleType("torch")
torch.optim = types.SimpleNamespace(Adam=lambda *a, **kw: optimizer)
torch.serialization = types.SimpleNamespace(safe_globals=lambda _: contextlib.nullcontext())
torch.load = lambda *a, **kw: checkpoint
torch.Tensor = type("NeverTensor", (), {})
version = types.ModuleType("torch.torch_version")
version.TorchVersion = str
sys.modules["torch"] = torch
sys.modules["torch.torch_version"] = version
namespace = {"torch": torch, "Path": Path}
helpers = [node for node in source.body if isinstance(node, ast.FunctionDef) and node.name in ("learning_config_semantics", "learning_group_semantics")]
exec(compile(ast.fix_missing_locations(ast.Module(body=helpers + [method], type_ignores=[])), "production-open", "exec"), namespace)
with tempfile.TemporaryDirectory() as root:
    config["checkpoint_directory"] = root
    Path(root, "checkpoint.pt").write_text("仅元数据替身，不是模型")
    learner = types.SimpleNamespace(optimizer=None, model=model, service=types.SimpleNamespace(identity={"version":"fixture"}, device=types.SimpleNamespace(type="cpu")), meter=lambda *a: None)
    request = {"config": config, "group": {"id":"new-id-does-not-bypass", "target_version":"confirmed-finite-j-v1", "trajectory_version":"learning-trajectory-v2"}}
    try:
        namespace["open"](learner, request)
    except ValueError as error:
        assert str(error) == "恢复配置或模型版本不同"
    else:
        raise AssertionError("旧v1 checkpoint被静默加载")
    assert loaded == []
# 已消费同id仍完整比较group；更改语义不能绕过原协议。
learner = types.SimpleNamespace(optimizer=optimizer, config=config, consumed={"g":{"id":"g","trajectory_version":"learning-trajectory-v1"}}, sequence=1)
try:
    namespace["open"](learner, {"config":config,"group":{"id":"g","trajectory_version":"learning-trajectory-v2"},"account_weights":{}})
except ValueError as error:
    assert str(error) == "同一消费身份对应不同轨迹/目标"
else:
    raise AssertionError("旧消费身份被改写")
print("原open方法：旧版本在load_state前拒绝，旧消费身份不迁移；零模型")
# 原base/v2不能借新group id进入special；原open在state_dict前比较完整数值与目标。
checkpoint.update(config=config, trajectory_version="learning-trajectory-v2", target_version="confirmed-finite-j-v1")
with tempfile.TemporaryDirectory() as root:
    config["checkpoint_directory"] = root
    Path(root,"checkpoint.pt").write_text("元数据替身")
    special = dict(config, special={"version":"D-0375-special-v1","alpha":1})
    learner = types.SimpleNamespace(optimizer=None,model=model,service=types.SimpleNamespace(identity={"version":"fixture"},device=types.SimpleNamespace(type="cpu")),meter=lambda *a:None)
    try:
        namespace["open"](learner,{"config":special,"group":{"id":"new-special-id","target_version":"confirmed-finite-j-special-v1","trajectory_version":"learning-trajectory-special-v1"}})
    except ValueError as error:
        assert str(error)=="恢复配置或模型版本不同"
    else:raise AssertionError("原base优化器被静默迁移到special")
    assert loaded==[]
learner=types.SimpleNamespace(optimizer=optimizer,config=config,consumed={"same":{"id":"same","target_version":"confirmed-finite-j-v1","trajectory_version":"learning-trajectory-v2"}},sequence=1)
try:
    namespace["open"](learner,{"config":config,"group":{"id":"same","target_version":"confirmed-finite-j-special-v1","trajectory_version":"learning-trajectory-special-v1"},"account_weights":{}})
except ValueError as error:assert str(error)=="同一消费身份对应不同轨迹/目标"
else:raise AssertionError("同id换专项归属/目标未拒绝")
print("原base及同id异目标均拒绝专项热迁移；仍零模型")

# 同id下改变实际数值，原消费身份比较仍拒绝；不以入口硬锁代替绑定。
group={"id":"numeric","target_version":"confirmed-finite-j-special-v1","binding":{"parameters":{"special":{"safe_notrade_credit_pusd":"0.01"}}}}
changed={**group,"binding":{"parameters":{"special":{"safe_notrade_credit_pusd":"0.02"}}}}
learner=types.SimpleNamespace(optimizer=optimizer,config=config,consumed={"numeric":group},sequence=1)
try:
    namespace["open"](learner,{"config":config,"group":changed,"account_weights":{}})
except ValueError as error:assert str(error)=="同一消费身份对应不同轨迹/目标"
else:raise AssertionError("同id实际数值变化未拒绝")
print("专项数值保留于同id完整消费身份；零模型")
