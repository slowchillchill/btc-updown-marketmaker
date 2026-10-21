"""D-0371：精确历史动作的可微概率、整轮PPO、TBPTT及单组原子学习提交。"""
import hashlib
import json
import math
import os
from pathlib import Path
import torch


def gradient_norm(parameters):
    values = [p.grad.detach().float().square().sum() for p in parameters if p.grad is not None]
    return float(torch.stack(values).sum().sqrt()) if values else 0.0


def learning_config_semantics(config):
    """仅检查点存放位置不属于数值兼容性；保留其他键和原记录。"""
    return {key: value for key, value in config.items() if key != "checkpoint_directory"}


def learning_group_semantics(group):
    """仅投影binding.parameters的同一位置字段，不递归忽略其他路径。"""
    result = dict(group)
    binding = group.get("binding")
    if isinstance(binding, dict):
        binding = dict(binding)
        parameters = binding.get("parameters")
        if isinstance(parameters, dict):
            binding["parameters"] = learning_config_semantics(parameters)
        result["binding"] = binding
    return result


class LearningEngine:
    def __init__(self, service):
        self.service = service
        self.model = service.model
        self.optimizer = None
        self.config = None
        self.consumed = {}
        self.sequence = 0
        self.epoch = 0
        self.hidden = {}
        self.pending_loss = {}
        self.counts = {}
        self.group = None
        self.report = []
        self.weights = None
        self.audit = False

    def meter(self, name, amount=1):
        self.service.metrics[name] = self.service.metrics.get(name, 0) + amount

    def teacher(self, frame, hidden, trace=False):
        """沿原选项/整数域/价格与属性前缀计算；既不重采，也不反推float金额域。"""
        s, model = self.service, self.model
        model.train(torch.is_grad_enabled())
        self.meter("recompute_forwards")  # 发起前计量，故障/在途不隐藏工作。
        encoded, nbytes = model.encode([frame["global"]] + frame["markets"], s.device)
        if s.device.type == "cuda":
            s.metrics["h2d_bytes"] += nbytes
            s.metrics["h2d_copies"] += 1
        pooled = encoded[1:].mean(0) if len(encoded) > 1 else torch.zeros_like(encoded[0])
        features = model.input(torch.cat([encoded[0], pooled]))
        output, following = model.gru(features.reshape(1, 1, -1), hidden)
        latent = output[0, 0]
        value = model.value(latent).squeeze()
        options = frame["options"]
        selected = frame["selected_option"]
        terms = []

        def probability(logits, allowed, choice):
            if choice not in allowed:
                raise ValueError("历史动作不属于其冻结精确条件域")
            # 与采集路径相同FP64概率投影，cast保留梯度；单值logp=0仍继续原条件状态。
            logits64 = logits.to(device="cpu", dtype=torch.float64)
            if s.device.type == "cuda":
                s.metrics["d2h_bytes"] += logits64.numel() * 8
                s.metrics["d2h_copies"] += 1
            result = torch.log_softmax(logits64[allowed], dim=0)[allowed.index(choice)]
            self.meter("recompute_heads")
            if trace:
                terms.append({"logits": logits64.detach().tolist(), "allowed": allowed,
                              "selected": choice, "logp": float(result.detach())})
            return result

        scores = []
        kinds = []
        for option in options:
            kind = 0 if option["kind"] == "NoTrade" else (1 if option["kind"] == "Pair" else 2 + option["side"])
            market = torch.zeros_like(latent) if kind == 0 else encoded[1 + option["market"]]
            scores.append(model.option(torch.cat([latent, market])).squeeze() + model.option_bias[kind])
            kinds.append(kind)
        logp = probability(torch.stack(scores), list(range(len(options))), selected)
        if kinds[selected] == 0:
            return logp, value, following, terms
        option = options[selected]
        context = torch.tanh(latent + encoded[1 + option["market"]])

        def tensor(values):
            if s.device.type == "cuda":
                s.metrics["h2d_bytes"] += 16
                s.metrics["h2d_copies"] += 1
            return torch.tensor(values, dtype=torch.float32, device=s.device)

        def integer(maximum, field, actual):
            nonlocal logp
            prefix = 0
            for position in reversed(range(maximum.bit_length())):
                allowed = [0, 1] if prefix + (1 << position) <= maximum else [0]
                bit = (actual >> position) & 1
                if len(allowed) > 1:
                    feature = tensor([position / 100, field / 6, prefix / (maximum + 1), math.log1p(maximum) / 100])
                    logits = model.bit(torch.cat([context, feature])) + model.bit_bias[field, position]
                    logp = logp + probability(logits, allowed, bit)
                elif bit != 0:
                    raise ValueError("历史整数前缀越界")
                prefix += bit << position
            if prefix != actual or not 0 <= actual <= maximum:
                raise ValueError("历史整数与冻结完整排名不符")

        for item in frame["rpc"]:
            request, response = item["request"], item["response"]
            phase = request["phase"]
            if phase == "post":
                allowed = [int(p) for p in request["allowed"]]
                actual = int(response["post_only"])
                feature = tensor([0, 5 / 6, request["side"], 0])
                logits = model.bit(torch.cat([context, feature])) + model.bit_bias[5, 0]
                logp = logp + probability(logits, allowed, actual)
                context = model.decoder(tensor([5 / 6, request["ordinal"], request["side"], actual]), context)
            elif phase == "price":
                field = request["ordinal"] if kinds[selected] == 1 else 2
                actual = response["index"] - 1
                integer(request["maximum"] - 1, field, actual)
                context = model.decoder(tensor([field / 6, actual / request["maximum"], request["side"], 0]), context)
            elif phase == "quantity":
                integer(int(request["count"]) - 1, 3 if kinds[selected] == 1 else 4, int(response["rank"]))
            else:
                raise ValueError("未知历史条件头")
        return logp, value, following, terms

    def zeros(self):
        s = self.service
        return torch.zeros(self.model.config["layers"], 1, self.model.config["width"], device=s.device)

    def checkpoint_state(self):
        def cpu(value):
            if isinstance(value, torch.Tensor):
                result = value.detach().cpu()
                if self.service.device.type == "cuda" and value.device.type == "cuda":
                    self.meter("checkpoint_d2h_bytes", result.numel() * result.element_size())
                return result
            if isinstance(value, dict):
                return {k: cpu(v) for k, v in value.items()}
            if isinstance(value, (list, tuple)):
                return type(value)(cpu(v) for v in value)
            return value
        return {"version": "ppo-group-transaction-v1", "model_version": self.service.identity["version"],
                "encoding_version": "b-public-token-log1p-v1", "target_version": "confirmed-finite-j-v1",
                "trajectory_version": self.group["trajectory_version"], "model": cpu(self.model.state_dict()),
                "optimizer": cpu(self.optimizer.state_dict()), "config": self.config,
                "consumed": self.consumed, "sequence": self.sequence,
                "torch_rng": torch.get_rng_state(),
                "cuda_rng": torch.cuda.get_rng_state_all() if self.service.device.type == "cuda" else [],
                "account_sampling_rng": {k: v["rng"].get_state() for k, v in self.service.accounts.items()},
                "scheduler": None, "identity": self.service.identity}

    def open(self, request, audit=False):
        self.audit = audit
        self.hidden, self.pending_loss, self.counts, self.report = {}, {}, {}, []
        self.epoch = 0
        if audit:
            return {"audit": True}
        config = request["config"]
        if self.optimizer is None:
            self.config = config
            self.optimizer = torch.optim.Adam(self.model.parameters(), lr=config["learning_rate"])
            path = Path(config["checkpoint_directory"]) / "checkpoint.pt"
            if path.exists():
                # 本轮首个工程checkpoint把版本str子类落盘；只允许已核官方类，仍weights_only。
                from torch.torch_version import TorchVersion
                with torch.serialization.safe_globals([TorchVersion]):
                    checkpoint = torch.load(path, map_location=self.service.device, weights_only=True)
                def tensor_bytes(value):
                    if isinstance(value, torch.Tensor):
                        return value.numel() * value.element_size() if value.device.type == "cuda" else 0
                    if isinstance(value, dict):
                        return sum(tensor_bytes(v) for v in value.values())
                    if isinstance(value, (tuple, list)):
                        return sum(tensor_bytes(v) for v in value)
                    return 0
                self.meter("checkpoint_load_h2d_tensor_bytes", tensor_bytes(checkpoint))
                if (learning_config_semantics(checkpoint["config"]) != learning_config_semantics(config) or checkpoint["model_version"] != self.service.identity["version"]
                        or checkpoint["version"] != "ppo-group-transaction-v1"
                        or checkpoint["encoding_version"] != "b-public-token-log1p-v1"
                        or checkpoint["target_version"] != request["group"]["target_version"]
                        or checkpoint["trajectory_version"] != request["group"]["trajectory_version"]):
                    raise ValueError("恢复配置或模型版本不同")
                self.model.load_state_dict(checkpoint["model"], strict=True)
                self.optimizer.load_state_dict(checkpoint["optimizer"])
                self.consumed, self.sequence = checkpoint["consumed"], checkpoint["sequence"]
                torch.set_rng_state(checkpoint["torch_rng"].cpu())
                if checkpoint["cuda_rng"]:
                    torch.cuda.set_rng_state_all([v.cpu() for v in checkpoint["cuda_rng"]])
                self.service.identity["checkpoint_sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
                self.service.identity["optimizer_updates_since_commit"] = 0
        elif config != self.config:
            raise ValueError("同一学习器不能静默改变生效配置")
        self.group = request["group"]
        self.weights = request["account_weights"]
        existing = self.consumed.get(self.group["id"])
        if existing is not None:
            if learning_group_semantics(existing) != learning_group_semantics(self.group):
                raise ValueError("同一消费身份对应不同轨迹/目标")
            return {"already_committed": True, "sequence": self.sequence}
        return {"already_committed": False, "sequence": self.sequence}

    def start_epoch(self):
        self.epoch += 1
        self.hidden, self.pending_loss, self.counts, self.report = {}, {}, {}, []
        self.optimizer.zero_grad(set_to_none=True)
        return {"epoch": self.epoch}

    def step(self, request, audit=False):
        frame = request["frame"]
        key = frame["account"]
        before = self.hidden.get(key, self.zeros())
        if audit:
            with torch.no_grad():
                logp,value,after,terms=self.teacher(frame,before,trace=True)
        else:
            logp,value,after,terms=self.teacher(frame,before)
        self.hidden[key] = after
        if audit:
            self.hidden[key] = after.detach()
            return {"logp": float(logp.detach()), "value": float(value.detach()), "terms": terms,
                    "hidden": after.detach().cpu().flatten().tolist()}
        label = request["label"]
        scale = self.config["money_scale_pusd"]
        advantage = label["advantage"] / scale
        ratio = (logp - label["old_joint_logp"]).exp()
        actor = -torch.minimum(ratio * advantage,
                               torch.clamp(ratio, 1 - self.config["epsilon"], 1 + self.config["epsilon"]) * advantage)
        critic = 0.5 * self.config["value_weight"] * ((value - label["critic_target"]) / scale).square()
        loss = self.weights[key] * (actor.to(value.device) + critic)
        self.pending_loss.setdefault(key, []).append(loss)
        self.counts[key] = self.counts.get(key, 0) + 1
        result = {"epoch": self.epoch, "account": key, "call": frame["call"], "logp": float(logp.detach()),
                  "ratio": float(ratio.detach()), "value": float(value.detach()),
                  "actor_loss": float(actor.detach()), "critic_loss": float(critic.detach())}
        self.report.append(result)
        if self.counts[key] % self.config["tbptt_steps"] == 0:
            torch.stack(self.pending_loss[key]).sum().backward()
            self.pending_loss[key] = []
            self.hidden[key] = after.detach()
        return result

    def end_epoch(self):
        for losses in self.pending_loss.values():
            if losses:
                torch.stack(losses).sum().backward()
        self.pending_loss = {}
        m = self.model
        result = {"epoch": self.epoch, "samples": self.report,
                  "gradients": {"gru": gradient_norm(m.gru.parameters()), "encoder": gradient_norm(m.token.parameters()),
                                "actor": gradient_norm(list(m.option.parameters()) + [m.option_bias]),
                                "critic": gradient_norm(m.value.parameters()), "decoder": gradient_norm(m.decoder.parameters())}}
        self.meter("optimizer_steps")
        self.optimizer.step()  # 一轮一次；所有块之间参数不变。
        self.service.identity["optimizer_updates_since_commit"] = self.service.identity.get("optimizer_updates_since_commit", 0) + 1
        return result

    def commit(self, request):
        path = Path(self.config["checkpoint_directory"])
        path.mkdir(parents=True, exist_ok=True)
        if request.get("fault") == "before_commit":
            self.fault_witness(request["witness"], "before_commit")
            raise RuntimeError("injected-before-commit-fault")
        # 单文件包含全部学习状态、随机状态及消费版本；原子rename是唯一提交边界。
        self.consumed[self.group["id"]] = self.group
        self.sequence += 1
        temporary = path / "checkpoint.incomplete"
        with temporary.open("wb") as stream:
            torch.save(self.checkpoint_state(), stream)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path / "checkpoint.pt")
        immutable=path / ("committed-%06d.pt" % self.sequence)
        if not immutable.exists():
            os.link(path / "checkpoint.pt",immutable)
        descriptor = os.open(path, os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        self.meter("successful_commits")
        self.service.identity["checkpoint_sha256"] = hashlib.sha256((path / "checkpoint.pt").read_bytes()).hexdigest()
        self.service.identity["optimizer_updates_since_commit"] = 0
        if request.get("fault") == "after_commit":
            self.fault_witness(request["witness"], "after_commit")
            raise RuntimeError("injected-after-commit-ack-fault")
        return {"committed": True, "sequence": self.sequence,
                "checkpoint_sha256": self.service.identity["checkpoint_sha256"]}

    def fault_witness(self, destination, phase):
        with Path(destination).open("x") as stream:
            json.dump({"phase": phase, "group": self.group, "sequence": self.sequence,
                       "metrics": self.service.snapshot()}, stream, ensure_ascii=False, indent=2)

    def gradient_reference(self, request):
        outputs = []
        model = self.model
        modes = ["full_bptt", "tbptt"] if request["kind"] == "bptt" else ["critic_only"]
        for mode in modes:
            model.zero_grad(set_to_none=True)
            hidden = self.zeros()
            losses = []
            for i, (frame, label) in enumerate(zip(request["frames"], request["labels"])):
                logp, value, hidden, _ = self.teacher(frame, hidden)
                losses.append(((value - label["critic_target"]) / request["money_scale_pusd"]).square() / 2
                              if mode == "critic_only" else logp.to(value.device) + value / request["money_scale_pusd"])
                if mode == "tbptt" and (i + 1) % request["tbptt_steps"] == 0:
                    torch.stack(losses).sum().backward()
                    losses = []
                    hidden = hidden.detach()
            if losses:
                torch.stack(losses).sum().backward()
            outputs.append({"mode": mode, "hidden": hidden.detach().cpu().flatten().tolist(),
                            "gru_gradient": torch.cat([p.grad.flatten() for p in model.gru.parameters()]).detach().cpu().tolist(),
                            "norms": {"gru": gradient_norm(model.gru.parameters()), "encoder": gradient_norm(model.token.parameters()),
                                      "critic": gradient_norm(model.value.parameters()),
                                      "actor": gradient_norm(list(model.option.parameters()) + [model.option_bias]),
                                      "decoder": gradient_norm(model.decoder.parameters())}})
        model.zero_grad(set_to_none=True)
        return {"references": outputs, "optimizer_steps": 0}

    def dispatch(self, request):
        phase = request["phase"]
        if phase == "learn_open":
            return self.open(request)
        if phase == "audit_open":
            return self.open(request, audit=True)
        if phase == "learn_epoch":
            return self.start_epoch()
        if phase == "learn_step":
            return self.step(request)
        if phase == "audit_step":
            return self.step(request, audit=True)
        if phase == "learn_end_epoch":
            return self.end_epoch()
        if phase == "learn_commit":
            return self.commit(request)
        if phase == "gradient_reference":
            return self.gradient_reference(request)
        raise ValueError("未知学习请求")
