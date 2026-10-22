"""#181真实Actor/Critic消费者：公共经济归Rust，学习按显式整组事务进行。"""
import hashlib
import json
import math
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

    def encode(self, sequences, device):
        lengths = [len(s) for s in sequences]
        rows = [row for sequence in sequences for row in sequence]
        tensor = torch.tensor(rows, dtype=torch.float32, device=device)
        fields = tensor[:, 0].to(torch.int64)
        categories = tensor[:, 1].to(torch.int64)
        kinds = tensor[:, 3].to(torch.int64)
        tokens = self.token(self.field(fields) + self.category(categories)
                            + self.kind(kinds) + self.numeric(tensor[:, 2:3]))
        pooled = []
        offset = 0
        for length in lengths:
            pooled.append(tokens[offset:offset + length].mean(0))
            offset += length
        return torch.stack(pooled), tensor.numel() * tensor.element_size()


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
                             "cuda_matmul_allow_tf32": torch.backends.cuda.matmul.allow_tf32,
                             "cudnn_allow_tf32": torch.backends.cudnn.allow_tf32,
                             "cudnn_deterministic": torch.backends.cudnn.deterministic,
                             "cudnn_benchmark": torch.backends.cudnn.benchmark}}

    def sync(self):
        if self.device.type == "cuda":
            torch.cuda.synchronize(self.device)

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
        if self.device.type == "cuda":
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
        if self.device.type == "cuda":
            result["cuda_allocated_peak_bytes"] = torch.cuda.max_memory_allocated(self.device)
            result["cuda_reserved_peak_bytes"] = torch.cuda.max_memory_reserved(self.device)
        return result

    def run(self):
        for line in sys.stdin:
            request = json.loads(line)
            if "hello" in request:
                hello = request["hello"]
                for key in ("field_buckets", "category_buckets"):
                    if hello["encoding"][key] != self.config[key]:
                        raise ValueError("编码参数与模型不匹配")
                if hello["encoding_version"] != ENCODING_VERSION:
                    raise ValueError("编码版本不匹配")
                response={"ready":True,"identity":self.identity,"load_seconds":self.metrics["load_seconds"]}
            else:
                batch=request["batch"]
                results=[None]*len(batch)
                indices=[i for i,r in enumerate(batch) if r["phase"]=="begin"]
                if indices:
                    with torch.inference_mode():
                        for index,result in zip(indices,self.begin([batch[i] for i in indices])):
                            results[index]=result
                for i,item in enumerate(batch):
                    phase=item["phase"]
                    if phase=="begin":
                        continue
                    if phase.startswith(("learn_","audit_")) or phase=="gradient_reference":
                        with torch.enable_grad():
                            results[i]=self.learning.dispatch(item)
                    elif phase=="metrics":
                        results[i]=self.snapshot()
                    elif phase=="account_snapshot":
                        results[i]=self.account_snapshot(item["account"])
                    else:
                        with torch.inference_mode():
                            results[i]=self.decode(item)
                response={"batch":results}
            print(json.dumps(response,ensure_ascii=False,allow_nan=False),flush=True)


if __name__ == "__main__":
    if sys.argv[1] == "initialize":
        initialize(sys.argv[2], sys.argv[3])
    elif sys.argv[1] == "serve":
        Service(sys.argv[2], sys.argv[3], int(sys.argv[4])).run()
    else:
        raise SystemExit("用法: model_worker.py initialize <配置> <模型> | serve <模型> <设备> <线程数>")
