"""#377 紧凑token传输与同一Rust精确域薄绑定；本模块不装载模型。"""
import ctypes
import json
import struct
import time

PROTOCOL_VERSION = "b-frame-v1"
HEADER = struct.Struct("<4sIQ")


class PackedTokens:
    """单个序列的原始位值列；buffer所有权随memoryview保留。"""
    def __init__(self, data, token_count):
        self.token_count = token_count
        self.retained_bytes = 13 * token_count
        view = memoryview(data)
        if len(view) != self.retained_bytes:
            raise ValueError("紧凑token列长度不匹配")
        n = token_count
        self.fields = view[:4 * n]
        self.categories = view[4 * n:8 * n]
        self.numeric = view[8 * n:12 * n]
        self.kinds = view[12 * n:13 * n]

    def __len__(self):
        return self.token_count


def read_exact(stream, length):
    result = bytearray(length)
    view = memoryview(result)
    offset = 0
    while offset < length:
        count = stream.readinto(view[offset:])
        if not count:
            raise EOFError("紧凑请求在声明长度前结束")
        offset += count
    return result


def read_packet(stream):
    first = stream.read(HEADER.size)
    if not first:
        return None
    if len(first) != HEADER.size:
        raise EOFError("紧凑请求头截断")
    magic, metadata_length, payload_length = HEADER.unpack(first)
    if magic != b"BMF1":
        raise ValueError("未知紧凑请求版本")
    metadata = json.loads(read_exact(stream, metadata_length))
    payload = read_exact(stream, payload_length)
    view = memoryview(payload)
    offset = 0
    batch = metadata["batch"]
    layouts = metadata["token_layouts"]
    if len(batch) != len(layouts):
        raise ValueError("紧凑请求的批次与布局数量不符")
    for item, layout in zip(batch, layouts):
        if layout is None:
            continue
        sequences = []
        for n in [layout["global"]] + layout["markets"]:
            length = 13 * n
            sequences.append(PackedTokens(view[offset:offset + length], n))
            offset += length
        item["global"] = sequences[0]
        item["markets"] = sequences[1:]
    if offset != payload_length:
        raise ValueError("紧凑请求token布局未覆盖声明载荷")
    return metadata


class NativeDomain:
    """只传冻结金额/域标量；96位Decimal、费用和BigUint均留在Rust。"""
    def __init__(self, library):
        self.library = ctypes.CDLL(library)
        self.library.model_train_domain_v1.argtypes = [ctypes.c_void_p, ctypes.c_size_t,
                                                      ctypes.POINTER(ctypes.c_size_t)]
        self.library.model_train_domain_v1.restype = ctypes.c_void_p
        self.library.model_train_domain_free_v1.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
        self.library.model_train_domain_free_v1.restype = None
        self.handles = {}
        self.calls = 0
        self.seconds = 0.0

    def request(self, request):
        raw = json.dumps(request, separators=(",", ":"), ensure_ascii=False).encode()
        input_buffer = ctypes.create_string_buffer(raw)
        length = ctypes.c_size_t()
        started = time.perf_counter()
        pointer = self.library.model_train_domain_v1(input_buffer, len(raw), ctypes.byref(length))
        self.calls += 1
        self.seconds += time.perf_counter() - started
        if not pointer:
            raise RuntimeError("原生精确域没有返回响应")
        try:
            value = json.loads(ctypes.string_at(pointer, length.value))
        finally:
            self.library.model_train_domain_free_v1(pointer, length.value)
        if "error" in value:
            raise ValueError("原生精确域: " + value["error"])
        return value

    def open(self, binding):
        key = (binding["snapshot_path"], binding["expected_file_sha256"],
               json.dumps(binding["implementation_identity"], sort_keys=True))
        if key not in self.handles:
            self.handles[key] = self.request({"op": "open", "rules": binding})["handle"]
        return self.handles[key]

    def close(self):
        for handle in self.handles.values():
            self.request({"op": "close", "handle": handle})
        self.handles.clear()
