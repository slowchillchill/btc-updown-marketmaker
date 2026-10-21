"""stdlib协议替身：没有torch导入、模型或学习事务。"""
import json
import sys
calls = {key: 0 for key in ("inference", "teacher", "common_teacher")}
for line in sys.stdin:
    r = json.loads(line)
    if r["phase"] == "hello":
        result = {"ready": True, "identity": "stdlib-no-model"}
    elif r["phase"] == "metrics":
        result = {"calls": calls, "optimizer_steps": 0, "commits": 0}
    elif r["phase"] == "diagnostic_step":
        branch, frame = r["branch"], r["frame"]
        if frame.get("inject_error"):
            print(json.dumps({"error": "intentional-protocol-failure"}), flush=True)
            break
        assert frame["call"] == calls[branch] + 1
        calls[branch] += 1
        # 中间调用新采样Pair；下一调用仍继续，原冻结NoTrade概率独立。
        result = {"branch": branch, "call": frame["call"], "sampled_option_not_executed": 1,
                  "frozen_joint_logp": -0.7, "trace": {"hidden_after": calls[branch]}}
    else:
        raise RuntimeError("不允许的学习协议")
    print(json.dumps(result), flush=True)
