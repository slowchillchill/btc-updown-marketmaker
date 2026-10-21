"""有限零更新诊断；复用真实推理/teacher，不打开学习事务。"""
import json
import sys
import time


def serve(model_path, device, threads):
    # 仅显式model模式启动本脚本；配置打印和输入比较不导入torch。
    import torch
    from model_worker import Service
    from learning_engine import LearningEngine
    service = Service(model_path, device, threads)
    engines = {name: LearningEngine(service) for name in ("teacher", "common_teacher")}
    hidden = {name: None for name in engines}
    calls = {name: 0 for name in ("inference", "teacher", "common_teacher")}
    active = {}
    trace_d2h_bytes = 0
    trace_d2h_copies = 0
    frozen_score_d2h_bytes = 0
    frozen_score_d2h_copies = 0
    original_encode = service.model.encode
    original_choose = service.choose

    def encode(*args):
        value = original_encode(*args)
        active["encoding"] = value[0]
        return value

    def choose(logits, allowed, account):
        active["option_scores"] = logits
        return original_choose(logits, allowed, account)

    service.model.encode = encode
    service.choose = choose
    service.model.input.register_forward_pre_hook(lambda m, args: active.update(gru_input_concatenated=args[0]))
    service.model.input.register_forward_hook(lambda m, args, result: active.update(gru_input=result))
    service.model.gru.register_forward_pre_hook(lambda m, args: active.update(hidden_before=args[1]))
    service.model.gru.register_forward_hook(lambda m, args, result: active.update(latent=result[0], hidden_after=result[1]))
    service.model.value.register_forward_hook(lambda m, args, result: active.update(value=result))

    def values(value):
        nonlocal trace_d2h_bytes, trace_d2h_copies
        if value.device.type == "cuda":
            trace_d2h_bytes += value.numel() * value.element_size()
            trace_d2h_copies += 1
        return value.detach().cpu().tolist()

    for line in sys.stdin:
        try:
            request = json.loads(line)
            phase = request.get("phase")
            if phase == "hello":
                if request["model_sha256"] != service.identity["model_sha256"]:
                    raise ValueError("初始模型身份不符")
                if any(request["encoding"][k] != service.config[k] for k in ("field_buckets", "category_buckets")):
                    raise ValueError("编码配置不符")
                response = {"ready": True, "identity": service.identity}
            elif phase == "metrics":
                response = {"service": service.snapshot(), "branch_calls": calls,
                            "trace_d2h_bytes": trace_d2h_bytes,
                            "trace_d2h_copies": trace_d2h_copies,
                            "frozen_score_d2h_bytes": frozen_score_d2h_bytes,
                            "frozen_score_d2h_copies": frozen_score_d2h_copies,
                            "transfer_scope": "逻辑张量传输字节；service、额外trace、冻结评分分列，非PCIe硬件实测",
                            "backwards": 0, "optimizer_steps": 0, "commits": 0}
            elif phase == "diagnostic_step":
                branch, frame = request["branch"], request["frame"]
                if branch not in calls or frame["call"] != calls[branch] + 1:
                    raise ValueError("诊断分支必须从call1顺序推进")
                options, selected = frame["options"], frame["selected_option"]
                if options[selected]["kind"] != "NoTrade" or frame["rpc"]:
                    raise ValueError("未知：仅支持冻结NoTrade且无条件RPC")
                active.clear()
                calls[branch] += 1  # 发起前计数，失败/在途不重试。
                started = time.perf_counter()
                if branch == "inference":
                    with torch.inference_mode():
                        sampled = service.begin([dict(frame, account="diagnostic-inference", seed=request["seed"])])[0]
                    # 新样本不构成动作义务；清除仅该诊断账户的未使用条件上下文。
                    # 账户hidden/calls/RNG仍为真实begin结果，不撤回或覆盖。
                    service.pending.pop("diagnostic-inference", None)
                    if active["option_scores"].device.type == "cuda":
                        frozen_score_d2h_bytes += active["option_scores"].numel() * 8
                        frozen_score_d2h_copies += 1
                    scores = active["option_scores"].detach().to(device="cpu", dtype=torch.float64)
                    old_logp = float(torch.log_softmax(scores, dim=0)[selected])
                    sampled_option = sampled["option"]
                else:
                    engine = engines[branch]
                    before = hidden[branch] if hidden[branch] is not None else engine.zeros()
                    with torch.enable_grad():
                        logp, value, after, terms = engine.teacher(frame, before, trace=True)
                    hidden[branch] = after  # 数值连续，保留真实grad/train递推；不反向。
                    scores = terms[0]["logits"]
                    active["option_scores"] = scores
                    old_logp = float(logp.detach())
                    sampled_option = None
                trace = {k: (v if isinstance(v, list) else values(v)) for k, v in active.items()}
                # 原input线性层收到concat；后半正是本次实际pooled市场表示。
                concat = trace["gru_input_concatenated"]
                trace["pooled"] = concat[len(concat) // 2:]
                response = {"branch": branch, "call": frame["call"], "frozen_option": selected,
                            "sampled_option_not_executed": sampled_option, "frozen_joint_logp": old_logp,
                            "trace": trace, "elapsed_seconds": time.perf_counter() - started}
                # 切断仅用于诊断输出的引用；不会再次前向。
                active.clear()
            else:
                raise ValueError("诊断协议不支持学习或其他动作")
            print(json.dumps(response, ensure_ascii=False, allow_nan=False), flush=True)
        except Exception as error:
            print(json.dumps({"error": str(error), "branch_calls": calls}, ensure_ascii=False), flush=True)
            return 1
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 5 or sys.argv[1] != "serve":
        raise SystemExit("用法: model_diagnostic.py serve <原初始模型> <设备> <线程数>")
    raise SystemExit(serve(sys.argv[2], sys.argv[3], int(sys.argv[4])))
