"""D-0371：精确历史动作的可微概率、整轮PPO、TBPTT及单组原子学习提交。"""
import hashlib
import json
import math
import os
from pathlib import Path
import sys
import time
import torch
from input_cache import GroupInputCache


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
        self.cache = GroupInputCache(service)
        self.sequence_observer = None
        self.device_labels = None

    def device_learning(self):
        return (getattr(self.service, "performance", None) or {}).get("mode") == "device_v1"

    def meter(self, name, amount=1):
        self.service.metrics[name] = self.service.metrics.get(name, 0) + amount

    def check_cancel(self):
        check = getattr(self.service, "check_cancel", None)
        if check is not None:
            check()

    def _teacher_trunk(self, frame, hidden):
        """原逐帧encode/两级mean/vector投影/单步GRU；每帧只执行一次。"""
        s, model = self.service, self.model
        model.train(torch.is_grad_enabled())
        self.meter("recompute_forwards")  # 发起前计量，故障/在途不隐藏工作。
        encoded, nbytes = model.encode([frame["global"]] + frame["markets"], s.device)
        if s.device.type == "cuda" and nbytes:
            s.metrics["h2d_bytes"] += nbytes
            s.metrics["h2d_copies"] += 1
        pooled = encoded[1:].mean(0) if len(encoded) > 1 else torch.zeros_like(encoded[0])
        features = model.input(torch.cat([encoded[0], pooled]))
        output, following = model.gru(features.reshape(1, 1, -1), hidden)
        latent = output[0, 0]
        value = model.value(latent).squeeze()
        return encoded, latent, value, following

    def teacher(self, frame, hidden, trace=False):
        """沿原历史条件头；主干结果与所有条件头共用，不重采动作。"""
        encoded, latent, value, following = self._teacher_trunk(frame, hidden)
        logp, terms = self._teacher_heads(frame, encoded, latent, trace=trace,
                                         probability_device=self.service.device if self.device_learning() else "cpu")
        return logp, value, following, terms

    def _teacher_heads(self, frame, encoded, latent, trace=False, probability_device="cpu", tensor_trace=False):
        """共同历史条件链；B只改变概率投影设备，不改CPU采样、整数域或前缀顺序。"""
        s, model = self.service, self.model
        options = frame["options"]
        selected = frame["selected_option"]
        terms = []

        def probability(logits, allowed, choice):
            if choice not in allowed:
                raise ValueError("历史动作不属于其冻结精确条件域")
            # 与采集路径相同FP64概率投影，cast保留梯度；单值logp=0仍继续原条件状态。
            logits64 = logits.to(device=probability_device, dtype=torch.float64)
            if s.device.type == "cuda" and logits64.device.type == "cpu":
                s.metrics["d2h_bytes"] += logits64.numel() * 8
                s.metrics["d2h_copies"] += 1
            result = torch.log_softmax(logits64[allowed], dim=0)[allowed.index(choice)]
            self.meter("recompute_heads")
            if tensor_trace:
                terms.append({"logits": logits64.detach(), "allowed": allowed,
                              "selected": choice, "logp": result.detach()})
            elif trace:
                terms.append({"logits": logits64.detach().tolist(), "allowed": allowed,
                              "selected": choice, "logp": float(result.detach())})
                if logits64.device.type == "cuda":
                    self.meter("d2h_bytes", (logits64.numel() + 1) * 8)
                    self.meter("d2h_copies", 2)
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
            return logp, terms
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
        return logp, terms

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
                "encoding_version": "b-public-token-log1p-v1", "target_version": self.group["target_version"],
                "trajectory_version": self.group["trajectory_version"], "model": cpu(self.model.state_dict()),
                "optimizer": cpu(self.optimizer.state_dict()), "config": self.config,
                "source_implementations": [candidate["implementation_identity"] for day in self.group.get("binding", {}).get("consumer", {}).get("days", []) for candidate in day["candidates"]],
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
        self.device_labels = None
        existing = self.consumed.get(self.group["id"])
        if existing is not None:
            if learning_group_semantics(existing) != learning_group_semantics(self.group):
                raise ValueError("同一消费身份对应不同轨迹/目标")
            return {"already_committed": True, "sequence": self.sequence}
        self.cache.begin_group(self.group["id"])
        return {"already_committed": False, "sequence": self.sequence}

    def start_epoch(self):
        self.check_cancel()
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
            self.check_cancel()
        return result

    def run_epoch(self):
        if self.device_learning():
            return self.run_device_epoch()
        if self.cache.schedule is None:
            raise ValueError("内部学习前必须封存当前组输入和标签")
        self.start_epoch()
        started = last_progress = time.monotonic()
        total = len(self.cache.schedule)
        progress_every = (getattr(self.service, "performance", None) or {}).get("progress_seconds", 900)
        for number, (account, index) in enumerate(self.cache.schedule, 1):
            self.step({"frame": self.cache.frame(account, index),
                       "label": self.cache.labels[account][index]})
            now = time.monotonic()
            if now - last_progress >= progress_every:
                print(json.dumps({"stage": "learning_epoch", "epoch": self.epoch,
                                  "account": account, "samples": number, "total_samples": total,
                                  "tbptt_segments": sum(count // self.config["tbptt_steps"]
                                                        for count in self.counts.values()),
                                  "elapsed_seconds": now - started}, ensure_ascii=False),
                      file=sys.stderr, flush=True)
                last_progress = now
        self.check_cancel()
        return self.end_epoch()

    def _prepare_device_labels(self):
        if self.device_labels is not None:
            return
        # 原Python binary64除法只在封存标签时求一次，随后有效advantage与old logp均在GPU FP64。
        scale = self.config["money_scale_pusd"]
        values = [[self.cache.labels[key][index]["old_joint_logp"],
                   self.cache.labels[key][index]["advantage"] / scale]
                  for key, index in self.cache.schedule]
        with torch.inference_mode(False), torch.no_grad():
            self.device_labels = torch.tensor(values, dtype=torch.float64,
                                              device=self.service.device).reshape(-1, 2)
        size = self.device_labels.numel() * self.device_labels.element_size()
        self.service.metrics["learning_label_device_bytes"] = size
        if size:
            self.meter("learning_label_h2d_bytes", size)
            self.meter("learning_label_h2d_copies")
            self.meter("h2d_bytes", size)
            self.meter("h2d_copies")

    def _device_forward(self, start, stop):
        """只形成原设备loss图；TBPTT反向仍由原片段/epoch控制，不能在此更新。"""
        entries = self.cache.schedule[start:stop]
        frames = [self.cache.frame(key, index) for key, index in entries]
        key = entries[0][0]
        observer = self.sequence_observer
        rows = []
        scale, epsilon = self.config["money_scale_pusd"], self.config["epsilon"]
        for offset, ((_, index), frame) in enumerate(zip(entries, frames)):
            if observer is not None:
                # 每帧是一次真实[1,1,W]调用；不把完整片段伪装成一次sequence。
                observer.begin_sequence([frame], [{"batch": 0, "time": 0}])
            before = self.hidden.get(key)
            if before is None:
                before = self.zeros()
            teacher_frame = self.cache.frame_for_teacher(frame)
            frame_encoded, latent, value, following = self._teacher_trunk(teacher_frame, before)
            self.hidden[key] = following
            next_position = start + offset + 1
            if next_position < len(self.cache.schedule):
                next_account, next_index = self.cache.schedule[next_position]
                self.cache.prefetch_block([self.cache.frame(next_account, next_index)])
            logp, terms = self._teacher_heads(frame, frame_encoded, latent,
                                             probability_device=self.service.device,
                                             tensor_trace=observer is not None)
            label = self.cache.labels[key][index]
            old_logp, advantage = self.device_labels[start + offset].unbind()
            ratio = (logp - old_logp).exp()
            actor = -torch.minimum(ratio * advantage,
                                   torch.clamp(ratio, 1 - epsilon, 1 + epsilon) * advantage)
            # 保留原FP32 value与Python标量运算的转换、除法和乘法顺序。
            critic = 0.5 * self.config["value_weight"] * ((value - label["critic_target"]) / scale).square()
            loss = self.weights[key] * (actor.to(value.device) + critic)
            self.pending_loss.setdefault(key, []).append(loss)
            self.counts[key] = self.counts.get(key, 0) + 1
            rows.append(torch.stack((logp.detach(), ratio.detach(), value.detach().to(torch.float64),
                                     actor.detach(), critic.detach().to(torch.float64))))
            if observer is not None:
                observer.sequence_sample(frame, label, {"logp": logp, "value": value, "ratio": ratio,
                                                        "actor": actor, "critic": critic, "loss": loss}, terms)
                observer.end_sequence()
        # 正常路径仅按片段导出必要报告，不在每个条件头/样本上同步取标量。
        report_values = torch.stack(rows).cpu().tolist()
        self.meter("learning_report_d2h_bytes", len(rows) * 5 * 8)
        self.meter("learning_report_d2h_copies")
        self.meter("d2h_bytes", len(rows) * 5 * 8)
        self.meter("d2h_copies")
        for frame, values in zip(frames, report_values):
            self.report.append({"epoch": self.epoch, "account": key, "call": frame["call"],
                                "logp": values[0], "ratio": values[1], "value": values[2],
                                "actor_loss": values[3], "critic_loss": values[4]})
        return key, following

    def _device_segment(self, start, stop, next_frames):
        # 仅主干恢复逐帧；设备标签/heads/loss和原32步反向时点保持。
        # 取数只包当前帧，下一实际帧raw预取不改变本帧数值算子或反向边界。
        key, following = self._device_forward(start, stop)
        if self.counts[key] % self.config["tbptt_steps"] == 0:
            torch.stack(self.pending_loss[key]).sum().backward()
            self.pending_loss[key] = []
            self.hidden[key] = following.detach()
            self.cache.release_account_columns(key)
            self.check_cancel()

    def run_device_epoch(self):
        if self.cache.schedule is None:
            raise ValueError("内部学习前必须封存当前组输入和标签")
        if self.service.device.type != "cuda":
            raise ValueError("device_v1的学习要求CUDA设备")
        self.start_epoch()
        self._prepare_device_labels()
        started = last_progress = time.monotonic()
        schedule = self.cache.schedule
        total, position = len(schedule), 0
        progress_every = (self.service.performance or {}).get("progress_seconds", 900)
        while position < total:
            self.check_cancel()
            account = schedule[position][0]
            remaining = self.config["tbptt_steps"] - self.counts.get(account, 0) % self.config["tbptt_steps"]
            stop = position
            while stop < total and stop - position < remaining and schedule[stop][0] == account:
                stop += 1
            self._device_segment(position, stop, None)
            position = stop
            now = time.monotonic()
            if now - last_progress >= progress_every:
                print(json.dumps({"stage": "learning_epoch", "epoch": self.epoch,
                                  "account": account, "samples": position, "total_samples": total,
                                  "tbptt_segments": sum(count // self.config["tbptt_steps"]
                                                        for count in self.counts.values()),
                                  "elapsed_seconds": now - started}, ensure_ascii=False),
                      file=sys.stderr, flush=True)
                last_progress = now
        self.check_cancel()
        return self.end_epoch()

    def end_epoch(self):
        self.check_cancel()
        for account, losses in self.pending_loss.items():
            if losses:
                torch.stack(losses).sum().backward()
                if self.device_learning():
                    self.cache.release_account_columns(account)
                self.check_cancel()
        self.pending_loss = {}
        m = self.model
        result = {"epoch": self.epoch, "samples": self.report,
                  "gradients": {"gru": gradient_norm(m.gru.parameters()), "encoder": gradient_norm(m.token.parameters()),
                                "actor": gradient_norm(list(m.option.parameters()) + [m.option_bias]),
                                "critic": gradient_norm(m.value.parameters()), "decoder": gradient_norm(m.decoder.parameters())}}
        self.check_cancel()
        self.meter("optimizer_steps")
        self.optimizer.step()  # 一轮一次；所有块之间参数不变。
        self.service.identity["optimizer_updates_since_commit"] = self.service.identity.get("optimizer_updates_since_commit", 0) + 1
        self.check_cancel()
        return result

    def commit(self, request):
        self.check_cancel()
        path = Path(self.config["checkpoint_directory"])
        path.mkdir(parents=True, exist_ok=True)
        if request.get("fault") == "before_commit":
            self.fault_witness(request["witness"], "before_commit")
            raise RuntimeError("injected-before-commit-fault")
        # 单文件包含全部学习状态、随机状态及消费版本；原子rename是唯一提交边界。
        self.check_cancel()
        following_consumed = {**self.consumed, self.group["id"]: self.group}
        following_sequence = self.sequence + 1
        state = self.checkpoint_state()
        state["consumed"] = following_consumed
        state["sequence"] = following_sequence
        temporary = path / "checkpoint.incomplete"
        with temporary.open("wb") as stream:
            torch.save(state, stream)
            stream.flush()
            os.fsync(stream.fileno())
        self.check_cancel()
        os.replace(temporary, path / "checkpoint.pt")
        self.consumed, self.sequence = following_consumed, following_sequence
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
        self.cache.release()
        if self.device_learning():
            self.device_labels = None
            self.service.metrics["learning_label_device_bytes"] = 0
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
        if phase == "learn_cache_status":
            return self.cache.status()
        if phase == "learn_cache_frame":
            if self.cache.group_id is None:
                raise ValueError("冷重建前必须打开可缓存的学习组")
            frame = self.cache.prepare_frame(request.get("frame", request))
            return {"cached": True, "frame": self.cache.record_frame(frame)}
        if phase == "learn_prepare":
            return self.cache.prepare(request["labels"], request["schedule"])
        if phase == "learn_run_epoch":
            return self.run_epoch()
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
