"""#181真实Actor/Critic消费者：公共经济归Rust，学习按显式整组事务进行。"""
import hashlib
import json
import math
import select
import os
import resource
import sys
import time

import torch
from torch import nn

MODEL_VERSION = "b-gru-actor-critic-gtc-v2"
ENCODING_VERSION = "b-public-token-log1p-v1"


class Policy(nn.Module):
    def __init__(self, config):
        super().__init__()
        self.config = config
        width = config["width"]
        self.field = nn.Embedding(config["field_buckets"], width)
        self.category = nn.Embedding(config["category_buckets"], width)
        self.kind = nn.Embedding(4, width)
        self.numeric = nn.Linear(1, width)
        self.token = nn.Sequential(nn.Linear(width, width), nn.Tanh())
        self.input = nn.Linear(width * 2, width)
        self.gru = nn.GRU(width, width, config["layers"], batch_first=True)
        self.value = nn.Linear(width, 1)
        self.option = nn.Linear(width * 2, 1)
        self.option_bias = nn.Parameter(torch.zeros(4))
        self.decoder = nn.GRUCell(4, width)
        self.bit = nn.Linear(width + 4, 2)
        # 六个字段：配对腿0/腿1价、修复价、配对/修复数量、post_only。
        self.bit_bias = nn.Parameter(torch.zeros(6, 100, 2))

    def encode_columns(self, columns, lengths):
        """同一原token数学；各sequence单独均值，市场均值仍由调用方等权处理。"""
        fields, categories, numeric, kinds = columns
        tokens = self.token(self.field(fields) + self.category(categories)
                            + self.kind(kinds) + self.numeric(numeric))
        pooled = []
        offset = 0
        for length in lengths:
            pooled.append(tokens[offset:offset + length].mean(0))
            offset += length
        return torch.stack(pooled)

    def encode(self, sequences, device):
        lengths = [len(s) for s in sequences]
        if sequences and hasattr(sequences[0], "columns"):
            columns = [sequence.columns(device) for sequence in sequences]
            values = (torch.cat([c[0] for c in columns]),
                      torch.cat([c[1] for c in columns]),
                      torch.cat([c[2] for c in columns]),
                      torch.cat([c[3] for c in columns]))
            return self.encode_columns(values, lengths), sum(c[4] for c in columns)
        rows = [row for sequence in sequences for row in sequence]
        tensor = torch.tensor(rows, dtype=torch.float32, device=device)
        values = (tensor[:, 0].to(torch.int64), tensor[:, 1].to(torch.int64),
                  tensor[:, 2:3], tensor[:, 3].to(torch.int64))
        return self.encode_columns(values, lengths), tensor.numel() * tensor.element_size()


def initialize(config_path, model_path):
    with open(config_path, encoding="utf-8") as stream:
        config = json.load(stream)
    torch.manual_seed(config["initialization_seed"])
    model = Policy(config)
    if config["initialization"] == "controlled_synthetic_v1":
        # 明确工程初始化，不读取账户/时刻来脚本发单。所有动作仍由真实条件头采样。
        with torch.no_grad():
            model.bit.weight.mul_(0.01)
            model.bit.bias.zero_()
            targets = [39, 69, 59, 999, 9999]  # 0基：0.40、0.70、0.60、10/100份。
            for field, target in enumerate(targets):
                for position in range(100):
                    bit = (target >> position) & 1
                    model.bit_bias[field, position, bit] = 10.0
                    model.bit_bias[field, position, 1 - bit] = -10.0
            model.option.weight.mul_(0.01)
            model.option.bias.zero_()
            model.option_bias.copy_(torch.tensor([-0.2, 0.3, 0.4, 0.4]))
    elif config["initialization"] != "random_v1":
        raise ValueError("未知初始化方式")
    torch.save({"version": MODEL_VERSION, "encoding_version": ENCODING_VERSION,
                "config": config, "state_dict": model.state_dict()}, model_path)
    print(json.dumps({"model": model_path, "sha256": sha256(model_path),
                      "parameters": sum(p.numel() for p in model.parameters()),
                      "config": config}, ensure_ascii=False))


def sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as stream:
        for block in iter(lambda: stream.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


class Service:
    def __init__(self, path, device, threads):
        started = time.perf_counter()
        torch.set_num_threads(threads)
        if "B_MODEL_INTEROP_THREADS" in os.environ:
            torch.set_num_interop_threads(int(os.environ["B_MODEL_INTEROP_THREADS"]))
        self.performance = None
        self.native_domain = None
        self.cancel_fd = int(os.environ["B_MODEL_CANCEL_FD"]) if "B_MODEL_CANCEL_FD" in os.environ else None
        self.cancelled = False
        if self.cancel_fd is not None:
            os.set_blocking(self.cancel_fd, False)
        self.device = torch.device(device)
        package = torch.load(path, map_location="cpu", weights_only=True)
        if package["version"] != MODEL_VERSION or package["encoding_version"] != ENCODING_VERSION:
            raise ValueError("模型/编码版本不匹配")
        self.frozen_source = package.get("frozen_source")
        self.config = package["config"]
        self.model = Policy(self.config)
        self.model.load_state_dict(package["state_dict"], strict=True)
        self.model.eval().to(self.device)
        self.accounts = {}
        from learning_engine import LearningEngine
        self.learning = LearningEngine(self)
        self.pending = {}
        self.metrics = {"forwards": 0, "forward_batches": 0, "maximum_forward_batch": 0,
                        "heads": 0, "h2d_bytes": 0, "d2h_bytes": 0,
                        "h2d_copies": 0, "d2h_copies": 0,
                        "begin_seconds": 0.0, "decode_seconds": 0.0,
                        "load_seconds": time.perf_counter() - started,
                        "model_state_h2d_bytes": sum(t.numel() * t.element_size() for t in self.model.state_dict().values()) if self.device.type == "cuda" else 0}
        self.identity = {"frozen_source": self.frozen_source, "model_sha256": sha256(path), "version": MODEL_VERSION,
                         "torch": str(torch.__version__), "device": str(self.device),
                         "cuda_arch_list": torch.cuda.get_arch_list() if self.device.type == "cuda" else [],
                         "config": self.config,
                         "numerical_runtime": {"default_dtype": str(torch.get_default_dtype()),
                             "parameter_dtype": str(next(self.model.parameters()).dtype),
                             "torch_intra_threads": torch.get_num_threads(),
                             "torch_interop_threads": torch.get_num_interop_threads(),
                             "omp_threads": os.environ.get("OMP_NUM_THREADS"),
                             "mkl_threads": os.environ.get("MKL_NUM_THREADS"),
                             "cuda_matmul_allow_tf32": torch.backends.cuda.matmul.allow_tf32,
                             "cudnn_allow_tf32": torch.backends.cudnn.allow_tf32,
                             "cudnn_deterministic": torch.backends.cudnn.deterministic,
                             "cudnn_benchmark": torch.backends.cudnn.benchmark}}

    def sync(self):
        if self.device.type == "cuda":
            torch.cuda.synchronize(self.device)

    def check_cancel(self):
        if self.cancel_fd is not None and not self.cancelled:
            ready, _, _ = select.select([self.cancel_fd], [], [], 0)
            if ready:
                os.read(self.cancel_fd, 4096)
                self.cancelled = True  # 控制端EOF也停止，不能在监督者消失后继续。
        if self.cancelled:
            raise InterruptedError("模型收到独立停止请求；未提交状态不视为成功")

    def frame(self, requests):
        """一帧一次IPC；条件域仍由同份Rust函数按已选前缀计算。"""
        prepared = [self.learning.cache.prepare_frame(request) for request in requests]
        with torch.inference_mode():
            begins = self.begin(prepared)
            output = []
            for request, begin in zip(prepared, begins):
                self.check_cancel()
                selected = begin["option"]
                option = request["options"][selected]
                rpc = []
                if option["kind"] != "NoTrade":
                    market = request["domain_context"]["markets"][option["market"]]
                    sides = [0, 1] if option["kind"] == "Pair" else [option["side"]]
                    context = {"handle": self.native_domain.open(request["native_rules"]),
                               "cash": request["domain_context"]["cash"],
                               "ticks": market["ticks"], "max_prices": market["max_prices"],
                               "sides": sides, "selected": []}
                    for ordinal, side in enumerate(sides):
                        self.check_cancel()
                        context["ordinal"] = ordinal
                        allowed = self.native_domain.request(dict(context, op="post"))["allowed"]
                        item = {"phase": "post", "account": request["account"], "call": request["call"],
                                "allowed": allowed, "ordinal": ordinal, "side": side}
                        response = self.decode(item)
                        rpc.append({"request": item, "response": response})
                        context["post_only"] = response["post_only"]
                        maximum = self.native_domain.request(dict(context, op="price"))["maximum"]
                        item = {"phase": "price", "account": request["account"], "call": request["call"],
                                "maximum": maximum, "ordinal": ordinal, "side": side}
                        response = self.decode(item)
                        rpc.append({"request": item, "response": response})
                        context["selected"].append({"price_index": response["index"],
                                                    "tick": market["ticks"][side],
                                                    "post_only": context["post_only"]})
                    count = self.native_domain.request(dict(context, op="quantity"))["count"]
                    item = {"phase": "quantity", "account": request["account"], "call": request["call"], "count": count}
                    rpc.append({"request": item, "response": self.decode(item)})
                record = {key: request[key] for key in ("account", "call", "global", "markets", "options")}
                joint_logp = begin["log_probability"]
                for item in rpc:
                    joint_logp += item["response"]["log_probability"]
                record.update({"selected_option": selected,
                               "rpc": [{"request": item["request"], "response": {
                                   "post_only": item["response"].get("post_only"),
                                   "index": item["response"].get("index"),
                                   "rank": item["response"].get("rank")}} for item in rpc],
                               "old_joint_logp": joint_logp,
                               "old_value": begin["value"]})
                if self.learning.group is not None:
                    self.learning.cache.record_frame(record)
                output.append({"begin": begin, "rpc": rpc})
            return output

    def account(self, key, seed):
        if key not in self.accounts:
            generator = torch.Generator(device="cpu")
            generator.manual_seed(seed)
            hidden = torch.zeros(self.config["layers"], 1, self.config["width"], device=self.device)
            self.accounts[key] = {"hidden": hidden, "rng": generator, "calls": 0}
        return self.accounts[key]

    def choose(self, logits, allowed, account):
        if len(allowed) == 1:
            return allowed[0], 0.0, 0
        cpu = logits.detach().to(device="cpu", dtype=torch.float64)
        if self.device.type == "cuda":
            self.metrics["d2h_bytes"] += cpu.numel() * 8
            self.metrics["d2h_copies"] += 1
        probabilities = torch.softmax(cpu[allowed], dim=0)
        ordinal = int(torch.multinomial(probabilities, 1, generator=account["rng"]).item())
        logp = float(torch.log_softmax(cpu[allowed], dim=0)[ordinal])
        account["head_trace"].append({"allowed": allowed, "selected": allowed[ordinal],
                                      "logits": cpu.tolist(), "log_probability": logp})
        return allowed[ordinal], logp, 1

    def begin(self, requests):
        self.model.eval()
        started = time.perf_counter()
        keys = [r["account"] for r in requests]
        if len(set(keys)) != len(keys):
            raise ValueError("同一账户不能在一个前向批中出现两次")
        sequences = []
        offsets = []
        accounts = []
        for request in requests:
            key = request["account"]
            if key in self.pending:
                raise ValueError("上一动作未完成时不得再次推进账户GRU")
            account = self.account(key, request["seed"])
            if request["call"] != account["calls"] + 1:
                raise ValueError("账户调用顺序不连续")
            account["head_trace"] = []
            accounts.append(account)
            offsets.append((len(sequences), len(request["markets"])))
            sequences.append(request["global"])
            sequences.extend(request["markets"])
        encoded, byte_count = self.model.encode(sequences, self.device)
        if self.device.type == "cuda" and byte_count:
            self.metrics["h2d_bytes"] += byte_count
            self.metrics["h2d_copies"] += 1
        inputs = []
        for offset, count in offsets:
            markets = encoded[offset + 1:offset + 1 + count]
            pooled = markets.mean(0) if count else torch.zeros_like(encoded[offset])
            inputs.append(self.model.input(torch.cat([encoded[offset], pooled])))
        before = torch.cat([a["hidden"] for a in accounts], dim=1)
        self.metrics["forwards"] += len(requests)
        output, after = self.model.gru(torch.stack(inputs).unsqueeze(1), before)
        self.sync()
        responses = []
        for i, (request, account, (offset, count)) in enumerate(zip(requests, accounts, offsets)):
            account["hidden"] = after[:, i:i + 1].detach().contiguous()
            account["calls"] += 1
            latent = output[i, 0]
            options = request["options"]
            scores = []
            kinds = []
            for option in options:
                kind = 0 if option["kind"] == "NoTrade" else (1 if option["kind"] == "Pair" else 2 + option["side"])
                market = torch.zeros_like(latent) if kind == 0 else encoded[offset + 1 + option["market"]]
                scores.append(self.model.option(torch.cat([latent, market])).squeeze() + self.model.option_bias[kind])
                kinds.append(kind)
            selected, logp, heads = self.choose(torch.stack(scores), list(range(len(scores))), account)
            option = options[selected]
            if kinds[selected] != 0:
                self.pending[request["account"]] = {"call": request["call"], "latent": latent.detach(),
                                                     "kind": kinds[selected], "option": option,
                                                     "context": torch.tanh(latent.detach() + encoded[offset + 1 + option["market"]])}
            norms = torch.stack([before[:, i].norm(), after[:, i].norm()]).to("cpu")
            if self.device.type == "cuda":
                self.metrics["d2h_bytes"] += norms.numel() * norms.element_size()
                self.metrics["d2h_copies"] += 1
            value_cpu=self.model.value(latent).squeeze().to("cpu")
            if self.device.type=="cuda":
                self.metrics["d2h_bytes"]+=value_cpu.numel()*value_cpu.element_size()
                self.metrics["d2h_copies"]+=1
            responses.append({"value":float(value_cpu),"behavior":dict(self.identity),"option": selected, "log_probability": logp, "heads": heads,
                              "head_trace": list(account["head_trace"]),
                              "forward": {"account_call": account["calls"], "batch": len(requests),
                                          "hidden_before_norm": float(norms[0]), "hidden_after_norm": float(norms[1]),
                                          "device": str(self.device)}})
            if request.get("diagnostic_hidden", False):
                hidden_cpu = after[:, i].detach().to("cpu").contiguous()
                responses[-1]["hidden_after_f32"] = hidden_cpu.flatten().tolist()
                if self.device.type == "cuda":
                    self.metrics["d2h_bytes"] += hidden_cpu.numel() * hidden_cpu.element_size()
                    self.metrics["d2h_copies"] += 1
            self.metrics["heads"] += heads
        self.metrics["forward_batches"] += 1
        self.metrics["maximum_forward_batch"] = max(self.metrics["maximum_forward_batch"], len(requests))
        self.metrics["begin_seconds"] += time.perf_counter() - started
        return responses

    def integer(self, maximum, field, pending, account):
        """精确选择[0, maximum]：每条可表示值路径唯一，掩码由Python整数前缀决定。"""
        value = 0
        logp = 0.0
        heads = 0
        for position in reversed(range(maximum.bit_length())):
            allowed = [0, 1] if value + (1 << position) <= maximum else [0]
            if len(allowed) == 1:
                continue
            features = [position / 100.0, field / 6.0, value / (maximum + 1), math.log1p(maximum) / 100.0]
            feature = torch.tensor(features, device=self.device, dtype=torch.float32)
            if self.device.type == "cuda":
                self.metrics["h2d_bytes"] += 16
                self.metrics["h2d_copies"] += 1
            logits = self.model.bit(torch.cat([pending["context"], feature])) + self.model.bit_bias[field, position]
            bit, probability, used = self.choose(logits, allowed, account)
            value += bit << position
            logp += probability
            heads += used
        return value, logp, heads

    def condition(self, pending, values):
        feature = torch.tensor(values, dtype=torch.float32, device=self.device)
        if self.device.type == "cuda":
            self.metrics["h2d_bytes"] += 16
            self.metrics["h2d_copies"] += 1
        pending["context"] = self.model.decoder(feature, pending["context"])

    def decode(self, request):
        started = time.perf_counter()
        key = request["account"]
        pending = self.pending[key]
        if pending["call"] != request["call"]:
            raise ValueError("条件头调用身份不匹配")
        account = self.accounts[key]
        trace_start = len(account["head_trace"])
        if request["phase"] == "post":
            allowed = [int(p) for p in request["allowed"]]
            if not allowed:
                raise ValueError("属性资金域为空")
            feature = torch.tensor([0.0, 5 / 6, request["side"], 0.0], device=self.device)
            if self.device.type == "cuda":
                self.metrics["h2d_bytes"] += 16
                self.metrics["h2d_copies"] += 1
            logits = self.model.bit(torch.cat([pending["context"], feature])) + self.model.bit_bias[5, 0]
            post, logp, heads = self.choose(logits, allowed, account)
            self.condition(pending, [5 / 6, request["ordinal"], request["side"], post])
            result = {"post_only": bool(post), "log_probability": logp, "heads": heads}
        elif request["phase"] == "price":
            field = request["ordinal"] if pending["kind"] == 1 else 2
            value, logp, heads = self.integer(request["maximum"] - 1, field, pending, account)
            self.condition(pending, [field / 6, value / request["maximum"], request["side"], 0])
            result = {"index": value + 1, "log_probability": logp, "heads": heads}
        elif request["phase"] == "quantity":
            count = int(request["count"])
            if count <= 0:
                raise ValueError("数量域为空")
            field = 3 if pending["kind"] == 1 else 4
            value, logp, heads = self.integer(count - 1, field, pending, account)
            del self.pending[key]
            result = {"rank": str(value), "log_probability": logp, "heads": heads}
        else:
            raise ValueError("未知条件头阶段")
        self.sync()
        self.metrics["decode_seconds"] += time.perf_counter() - started
        result["head_trace"] = account["head_trace"][trace_start:]
        self.metrics["heads"] += result["heads"]
        return result

    def collection_sampling_state(self):
        if self.performance is None or self.learning.group is None:
            return None
        return {"version": "b-collection-sampling-v1", "group": self.learning.group["id"],
                "accounts": {key: {"calls": value["calls"],
                                     "rng_state": value["rng"].get_state().tolist()}
                             for key, value in self.accounts.items()}}

    def restore_collection_sampling_state(self, request):
        """仅恢复同一完整采集的采样状态；学习hidden仍由teacher从合法零状态建立。"""
        state, expected = request["state"], request["expected_calls"]
        if self.performance is None or self.learning.group is None:
            raise ValueError("采样恢复需要已打开的新性能学习组")
        if state.get("version") != "b-collection-sampling-v1" or state.get("group") != self.learning.group["id"]:
            raise ValueError("完整采集采样状态的版本或组绑定不同")
        if set(state["accounts"]) != {key for key, count in expected.items() if count}:
            raise ValueError("完整采集采样状态账户集合不同")
        if self.accounts:
            if self.collection_sampling_state() != state:
                raise ValueError("已有账户不能被不同采样状态覆盖")
            return {"restored": False, "already_present": True}
        pending = {}
        for key, value in state["accounts"].items():
            if value["calls"] != expected[key]:
                raise ValueError("完整采集采样状态调用计数不同")
            generator = torch.Generator(device="cpu")
            generator.set_state(torch.tensor(value["rng_state"], dtype=torch.uint8))
            pending[key] = {"calls": value["calls"], "rng": generator,
                            "hidden": torch.zeros(self.config["layers"], 1, self.config["width"], device=self.device)}
        self.accounts = pending
        self.metrics["collection_sampling_restores"] = self.metrics.get("collection_sampling_restores", 0) + 1
        self.metrics["restored_collection_calls"] = sum(expected.values())
        return {"restored": True, "accounts": sorted(pending), "calls": sum(expected.values())}

    def account_snapshot(self, key):
        # 夹具诊断：不建立账户、不调用网络、不推进hidden/RNG或经济状态。
        account = self.accounts[key]
        hidden = account["hidden"].detach().to("cpu").contiguous()
        if self.device.type == "cuda":
            self.metrics["d2h_bytes"] += hidden.numel() * hidden.element_size()
            self.metrics["d2h_copies"] += 1
        return {"account": key, "calls": account["calls"], "hidden": hidden.flatten().tolist(),
                "rng_sha256": hashlib.sha256(bytes(account["rng"].get_state().tolist())).hexdigest()}

    def snapshot(self):
        result = dict(self.metrics)
        result["identity"] = self.identity
        result["rss_peak_kib"] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        result["account_calls"] = {k: v["calls"] for k, v in self.accounts.items()}
        result["pending_accounts"] = list(self.pending)
        sampling = self.collection_sampling_state()
        if sampling is not None:
            result["collection_sampling_state"] = sampling
        if self.native_domain is not None:
            result["native_domain_calls"] = self.native_domain.calls
            result["native_domain_seconds"] = self.native_domain.seconds
        if hasattr(self.learning, "cache"):
            result["input_cache"] = self.learning.cache.status()
        if self.device.type == "cuda":
            result["cuda_allocated_peak_bytes"] = torch.cuda.max_memory_allocated(self.device)
            result["cuda_reserved_peak_bytes"] = torch.cuda.max_memory_reserved(self.device)
        return result

    def run(self):
        from model_protocol import PROTOCOL_VERSION, NativeDomain, read_packet
        binary = False
        stream = sys.stdin.buffer
        try:
            while True:
                if binary:
                    request = read_packet(stream)
                    if request is None:
                        break
                else:
                    line = stream.readline()
                    if not line:
                        break
                    request = json.loads(line)
                if "hello" in request:
                    hello = request["hello"]
                    for key in ("field_buckets", "category_buckets"):
                        if hello["encoding"][key] != self.config[key]:
                            raise ValueError("编码参数与模型不匹配")
                    if hello["encoding_version"] != ENCODING_VERSION:
                        raise ValueError("编码版本不匹配")
                    response = {"ready": True, "identity": self.identity, "load_seconds": self.metrics["load_seconds"]}
                    if "performance" in hello:
                        if hello.get("transport") != PROTOCOL_VERSION:
                            raise ValueError("未知紧凑协议版本")
                        self.performance = hello["performance"]
                        self.native_domain = NativeDomain(self.performance["native_library"])
                        self.identity["execution"] = {"mode": self.performance["mode"],
                                                      "transport": PROTOCOL_VERSION,
                                                      "native_domain": "rust-exact-domain-v1"}
                        response["transport"] = PROTOCOL_VERSION
                        binary = True
                else:
                    batch = request["batch"]
                    results = [None] * len(batch)
                    indices = [i for i, item in enumerate(batch) if item["phase"] == "begin"]
                    if indices:
                        with torch.inference_mode():
                            for index, result in zip(indices, self.begin([batch[i] for i in indices])):
                                results[index] = result
                    indices = [i for i, item in enumerate(batch) if item["phase"] == "frame"]
                    if indices:
                        for index, result in zip(indices, self.frame([batch[i] for i in indices])):
                            results[index] = result
                    for i, item in enumerate(batch):
                        phase = item["phase"]
                        if phase in ("begin", "frame"):
                            continue
                        try:
                            if phase == "learn_restore_collection_sampling_state":
                                results[i] = self.restore_collection_sampling_state(item)
                            elif phase.startswith(("learn_", "audit_")) or phase == "gradient_reference":
                                with torch.enable_grad():
                                    results[i] = self.learning.dispatch(item)
                            elif phase == "metrics":
                                results[i] = self.snapshot()
                            elif phase == "account_snapshot":
                                results[i] = self.account_snapshot(item["account"])
                            else:
                                with torch.inference_mode():
                                    results[i] = self.decode(item)
                        except InterruptedError as error:
                            results[i] = {"error": str(error), "stopped_before_commit": True}
                    response = {"batch": results}
                print(json.dumps(response, ensure_ascii=False, allow_nan=False), flush=True)
        finally:
            if self.native_domain is not None:
                self.native_domain.close()
            if self.cancel_fd is not None:
                os.close(self.cancel_fd)


if __name__ == "__main__":
    if sys.argv[1] == "initialize":
        initialize(sys.argv[2], sys.argv[3])
    elif sys.argv[1] == "serve":
        Service(sys.argv[2], sys.argv[3], int(sys.argv[4])).run()
    else:
        raise SystemExit("用法: model_worker.py initialize <配置> <模型> | serve <模型> <设备> <线程数>")
