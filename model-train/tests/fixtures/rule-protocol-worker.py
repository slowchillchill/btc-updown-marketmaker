"""仅验证冷启动/恢复协议，无torch、模型、优化器或SIM实现。"""
import json
from pathlib import Path
import sys

control = json.loads(Path(sys.argv[2]).read_text())
log = Path(control["log"])
assert Path(control["sealed_rules"]).is_file(), "查询封存必须先于worker启动"
if "effective_config" in control:
    assert Path(control["effective_config"]).is_file(), "有效恢复配置必须先于worker启动"

def record(value):
    with log.open("a") as stream:
        stream.write(json.dumps(value) + "\n")

for line in sys.stdin:
    request = json.loads(line)
    if "hello" in request:
        record({"phase": "worker_start"})
        response = {"ready": True}
    else:
        results = []
        for item in request["batch"]:
            record(item)
            if item["phase"] == "learn_open":
                if control.get("already_committed"):
                    assert item["group"] == control["expected_group"], "恢复必须保留同一组身份"
                results.append({"already_committed": control.get("already_committed", False)})
            elif item["phase"] == "metrics":
                results.append({"forwards": 0, "optimizer_steps": 0, "commits": 0})
            else:
                raise AssertionError("本夹具不允许真实模型/学习请求: " + item["phase"])
        response = {"batch": results}
    print(json.dumps(response), flush=True)
