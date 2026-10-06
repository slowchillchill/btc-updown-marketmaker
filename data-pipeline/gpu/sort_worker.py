"""DPT冷准备常驻CUDA排序器；stdin/stdout为二进制协议，进度仅写stderr。"""
import json
import struct
import sys
import time

import numpy as np
import torch

from prepare import stable_source_order


def read_exact(stream, count):
    buffer = bytearray(count)
    view = memoryview(buffer)
    position = 0
    while position < count:
        read = stream.readinto(view[position:])
        if not read:
            raise EOFError('GPU排序请求被截断')
        position += read
    return buffer


def main():
    if not torch.cuda.is_available():
        raise RuntimeError('GPU准备需要CUDA，不回退CPU排序')
    source = sys.stdin.buffer
    target = sys.stdout.buffer
    target.write(b'DPTGPU01')
    target.flush()
    processed = 0
    started = time.perf_counter()
    while True:
        header = source.read(16)
        if not header:
            return
        if len(header) != 16:
            header += read_exact(source, 16 - len(header))
        count, width = struct.unpack('<QQ', header)
        batch_started = time.perf_counter()
        lengths_host = torch.from_numpy(np.frombuffer(read_exact(source, count * 8), dtype='<i8'))
        keys_host = torch.from_numpy(np.frombuffer(read_exact(source, count * width), dtype=np.uint8).reshape(count, width))
        # pinned缓冲使大批传输走DMA；材料载荷不来回拷贝，只有键与permutation跨边界。
        lengths = lengths_host.pin_memory().to('cuda', non_blocking=True)
        keys = keys_host.pin_memory().to('cuda', non_blocking=True)
        order, fault = stable_source_order(keys, lengths)
        if bool(fault.any()):
            raise RuntimeError('原来源键长度超请求矩阵，未提交排序结果')
        result = order.to('cpu').numpy().astype('<u8', copy=False)
        target.write(struct.pack('<Q', count))
        target.write(result.tobytes())
        target.flush()
        processed += count
        print(json.dumps({'stage': 'gpu_initial_run_sort', 'batch_rows': count,
                          'processed_rows': processed, 'key_width': width,
                          'batch_seconds': time.perf_counter() - batch_started,
                          'elapsed_seconds': time.perf_counter() - started,
                          'device_peak_allocated_bytes': torch.cuda.max_memory_allocated()}),
              file=sys.stderr, flush=True)
        del keys, lengths, order, fault, result, keys_host, lengths_host


if __name__ == '__main__':
    main()
