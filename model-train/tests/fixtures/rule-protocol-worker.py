"""仅验证冷启动/恢复协议，无torch、模型、优化器或SIM实现。"""
import json
from pathlib import Path
import sys
import time

control = json.loads(Path(sys.argv[2]).read_text())
log = Path(control["log"])
assert Path(control["sealed_rules"]).is_file(), "查询封存必须先于worker启动"
if "effective_config" in control:
    assert Path(control["effective_config"]).is_file(), "有效恢复配置必须先于worker启动"

def record(value):
    with log.open("a") as stream:
        stream.write(json.dumps(value) + "\n")

begins = 0
for line in sys.stdin:
    request = json.loads(line)
    if "hello" in request:
        record({"phase": "worker_start"})
        time.sleep(control.get("handshake_delay_ms", 0) / 1000)
        response = {"ready": True}
    else:
        results = []
        for item in request["batch"]:
            record(item)
            if item["phase"] == "learn_open":
                time.sleep(control.get("learn_open_delay_ms", 0) / 1000)
                if control.get("already_committed"):
                    assert item["group"] == control["expected_group"], "恢复必须保留同一组身份"
                results.append({"already_committed": control.get("already_committed", False)})
            elif item["phase"] == "metrics":
                results.append({"forwards": 0, "optimizer_steps": 0, "commits": 0, "scripted_begins": begins})
            elif control.get("timing_protocol"):
                if item["phase"] == "begin":
                    begins += 1
                    if begins == 1:
                        if "first_begin_release" in control:
                            Path(control["first_begin_received"]).write_text("收到begin，响应仍由测试控制")
                            while not Path(control["first_begin_release"]).exists():
                                time.sleep(.001)
                        time.sleep(control.get("first_begin_delay_ms", 0) / 1000)
                if control.get("fail_phase") == item["phase"]:
                    results.append({"error": "timing-scripted-failure"})
                elif item["phase"] == "begin":
                    results.append({"option": 0, "log_probability": 0.0, "heads": 0,
                                    "head_trace": [], "value": 0.0, "behavior": {}, "forward": {}})
                elif item["phase"] in ("learn_epoch", "learn_step", "learn_end_epoch", "learn_commit"):
                    results.append({"sequence": 1})
                else:
                    raise AssertionError("非本夹具协议: " + item["phase"])
            else:
                raise AssertionError("本夹具不允许真实模型/学习请求: " + item["phase"])
        response = {"batch": results}
    print(json.dumps(response), flush=True)
