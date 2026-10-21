"""stdlib替身只记录真实消费者请求并固定NoTrade；不导入神经库，不执行学习。"""
import json
from pathlib import Path
import sys

log = Path(sys.argv[2])
for line in sys.stdin:
    request = json.loads(line)
    if "hello" in request:
        response = {"ready": True}
    else:
        results = []
        for item in request["batch"]:
            with log.open("a") as stream:
                stream.write(json.dumps(item) + "\n")
            if item["phase"] == "begin":
                results.append({"option": 0, "log_probability": 0.0, "heads": 0,
                                "head_trace": [], "value": 0.0, "behavior": {}, "forward": {}})
            elif item["phase"] == "metrics":
                results.append({"forwards": 0, "optimizer_steps": 0, "commits": 0})
            else:
                raise AssertionError("禁止模型/条件头/学习请求: " + item["phase"])
        response = {"batch": results}
    print(json.dumps(response), flush=True)
