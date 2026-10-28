"""#377 无模型协议夹具：检查真实紧凑读写和独立取消FD，不导入Torch。"""
import json
import os
from pathlib import Path
import select
import struct
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "python"))
from model_protocol import PROTOCOL_VERSION, read_packet

stream = sys.stdin.buffer
hello = json.loads(stream.readline())
assert hello["hello"]["transport"] == PROTOCOL_VERSION
print(json.dumps({"ready": True, "transport": PROTOCOL_VERSION}), flush=True)
while True:
    request = read_packet(stream)
    if request is None:
        break
    responses = []
    for item in request["batch"]:
        if item["phase"] == "wait_cancel":
            descriptor = int(os.environ["B_MODEL_CANCEL_FD"])
            select.select([descriptor], [], [])
            stopped = os.read(descriptor, 1) == b"!"
            responses.append({"stopped": stopped})
        elif item["phase"] == "stopped_error":
            responses.append({"error":"fixture cancellation","stopped_before_commit":True})
        elif item["phase"] == "inspect":
            tokens = item["global"]
            responses.append({"fields": list(struct.unpack("<" + "I" * len(tokens), tokens.fields)),
                              "categories": list(struct.unpack("<" + "I" * len(tokens), tokens.categories)),
                              "numeric_bits": list(struct.unpack("<" + "I" * len(tokens), tokens.numeric)),
                              "kinds": list(tokens.kinds),
                              "market_lengths": [len(value) for value in item["markets"]]})
        else:
            responses.append({"error": "fixture未知phase"})
    print(json.dumps({"batch": responses}), flush=True)
