"""#377：当前学习组的权重无关输入；驻留输入不保存编码特征或计算图。"""
import struct
import time

import torch


class _BytePool:
    """按块分配普通字节Tensor，训练期间不再写入已经封存的块。"""

    def __init__(self, device, block_bytes, limit=None):
        self.device = torch.device(device)
        self.block_bytes = block_bytes
        self.limit = limit
        self.blocks = []
        self.allocated = 0
        self.used = 0
        self.payload = 0

    def store(self, source):
        size = source.numel()
        offset = (self.used + 3) // 4 * 4
        if not self.blocks or offset + size > self.blocks[-1].numel():
            remaining = None if self.limit is None else self.limit - self.allocated
            if remaining is not None and size > remaining:
                return None
            capacity = max(self.block_bytes, size)
            if remaining is not None:
                capacity = min(capacity, remaining)
            with torch.inference_mode(False), torch.no_grad():
                self.blocks.append(torch.empty(capacity, dtype=torch.uint8, device=self.device))
            self.allocated += capacity
            self.used = offset = 0
        with torch.inference_mode(False), torch.no_grad():
            target = self.blocks[-1].narrow(0, offset, size)
            # A 使用原顺序的同步传输。只有B的实测路径才安排流式预取。
            target.copy_(source, non_blocking=False)
        self.used = offset + size
        self.payload += size
        return target


def _packed_bytes(sequence):
    """保留 u32/u32/f32/u8 的位值；不经过浮点编号或嵌套数字列表。"""
    if hasattr(sequence, "token_count"):
        count = sequence.token_count
        raw = bytearray()
        for name in ("fields", "categories", "numeric", "kinds"):
            raw.extend(getattr(sequence, name).cast("B"))
        return count, raw
    count = len(sequence)
    raw = bytearray(count * 13)
    for index, (field, category, numeric, kind) in enumerate(sequence):
        struct.pack_into("<I", raw, index * 4, field)
        struct.pack_into("<I", raw, count * 4 + index * 4, category)
        struct.pack_into("<f", raw, count * 8 + index * 4, numeric)
        raw[count * 12 + index] = kind
    return count, raw


class TokenSequence:
    """紧凑原始列；调用时只转换算子需要的整数类型，不缓存权重相关值。"""

    def __init__(self, sequence, cache=None):
        count, raw = _packed_bytes(sequence)
        self.token_count = count
        self.retained_bytes = count * 13
        self._cache = cache
        with torch.inference_mode(False), torch.no_grad():
            source = (torch.frombuffer(raw, dtype=torch.uint8) if raw
                      else torch.empty(0, dtype=torch.uint8))
            self._storage = cache._store(source) if cache is not None and count else source

    def __len__(self):
        return self.token_count

    def columns(self, device):
        destination = torch.device(device)
        uploaded = 0
        with torch.inference_mode(False), torch.no_grad():
            raw = self._storage
            if raw.device != destination:
                # 非驻留部分用独立的临时storage；autograd保留它到相应反向结束。
                transferred = raw.to(destination, non_blocking=False)
                # device='cuda'与实际cuda:0可指同一设备；to返回原对象时没有上传。
                if (transferred is not raw and raw.device.type == "cpu"
                        and destination.type == "cuda" and self.retained_bytes):
                    uploaded = self.retained_bytes
                    if self._cache is not None:
                        self._cache._meter("cache_stream_h2d_bytes", uploaded)
                        self._cache._meter("cache_stream_h2d_copies")
                raw = transferred
            count = self.token_count
            # int32视图再扩为int64并保留无符号位值，避免依赖uint32设备算子覆盖。
            fields = raw[:count * 4].view(torch.int32).to(torch.int64).bitwise_and(0xFFFFFFFF)
            categories = raw[count * 4:count * 8].view(torch.int32).to(torch.int64).bitwise_and(0xFFFFFFFF)
            numeric = raw[count * 8:count * 12].view(torch.float32).reshape(count, 1)
            kinds = raw[count * 12:].to(torch.int64)
        return fields, categories, numeric, kinds, uploaded


class _FrameColumnView:
    """Borrow one frame-owned ordinary column slice; original bytes stay read-only."""
    def __init__(self, original, columns):
        self._storage = original._storage
        self.token_count = len(original)
        self.retained_bytes = original.retained_bytes
        self._columns = columns

    def __len__(self):
        return self.token_count

    def columns(self, device):
        # block_columns already charged the actual H2D at submission. These are
        # same-device views, not a second upload attributed to teacher.
        return (*self._columns, 0)


class GroupInputCache:
    """一个学习组的输入池；CPU目标是规划基准，GPU份额满后固定保留原驻留子集。"""

    def __init__(self, service):
        self.service = service
        self.group_id = None
        self.frames = {}
        self.calls = {}
        self.labels = None
        self.schedule = None
        self.cpu_pool = None
        self.gpu_pool = None
        self.host_target = 0
        self.gpu_budget = 0
        self.host_peak = 0
        self.gpu_peak = 0
        self.streaming_capacity = 0
        self.streaming_slots = []
        self.copy_stream = None
        self.next_streaming_slot = 0
        self.prefetched = None
        self.buffer_observer = None
        self.owned_columns = {}
        self.owned_live_bytes = 0
        self.owned_peak_bytes = 0

    def _observe_buffer(self, event, **values):
        if self.buffer_observer is not None:
            # 仅显式短验证包装接收实际对象；生产默认关闭，不逐块写日志。
            self.buffer_observer({"event": event, **values})

    def _meter(self, name, amount=1):
        metrics = self.service.metrics
        metrics[name] = metrics.get(name, 0) + amount

    def _enabled(self):
        config = getattr(self.service, "performance", None)
        return config is not None and config.get("mode") in ("storage_v1", "device_v1")

    def begin_group(self, group_id):
        # hello在LearningEngine构造后才确定performance；这里只建立组生命周期。
        self.release()
        self.group_id = group_id if self._enabled() else None

    def _configure(self):
        if self.cpu_pool is not None:
            return
        config = self.service.performance
        block_bytes = max(4, int(config["streaming_block_bytes"]))
        self.host_target = int(config["host_cache_bytes"])
        if self.service.device.type == "cuda":
            free, _ = torch.cuda.mem_get_info(self.service.device)
            self.gpu_budget = min(int(config["gpu_cache_bytes"]),
                                  max(0, free - int(config["runtime_reserve_bytes"])))
            resident_budget = self.gpu_budget
            if config.get("mode") == "device_v1":
                # 两个传输槽在既定缓存份额内预留；运行余量仍供参数、算子和存活图。
                self.streaming_capacity = min(block_bytes // 4 * 4,
                                              self.gpu_budget // 2 // 4 * 4)
                if self.streaming_capacity < 4:
                    raise RuntimeError("批准的GPU缓存份额不足以容纳两个4字节传输槽")
                resident_budget -= 2 * self.streaming_capacity
            if resident_budget:
                self.gpu_pool = _BytePool(self.service.device, block_bytes, resident_budget)
        self.cpu_pool = _BytePool("cpu", block_bytes)
        self.service.metrics["cache_host_target_bytes"] = self.host_target
        self.service.metrics["cache_gpu_budget_bytes"] = self.gpu_budget
        self.service.metrics["cache_stream_reserved_bytes"] = 2 * self.streaming_capacity
        self.service.metrics["cache_stream_slot_capacity_bytes"] = self.streaming_capacity
        self.service.metrics["cache_gpu_resident_budget_bytes"] = self.gpu_budget - 2 * self.streaming_capacity

    def _store(self, source):
        self._configure()
        resident = self.gpu_pool.store(source) if self.gpu_pool is not None else None
        if resident is not None:
            size = source.numel()
            self._meter("cache_resident_h2d_bytes", size)
            self._meter("cache_resident_h2d_copies")
            # 在实际上传阶段计量。后续columns不会重复把驻留材料计成H2D。
            self._meter("h2d_bytes", size)
            self._meter("h2d_copies")
            result = resident
        else:
            # 当前组直到epoch完成才释放，不能在此等待或裁剪合法帧。
            result = self.cpu_pool.store(source)
        self._resource_metrics()
        return result

    def _resource_metrics(self):
        cpu = self.cpu_pool.allocated if self.cpu_pool is not None else 0
        resident_gpu = self.gpu_pool.allocated if self.gpu_pool is not None else 0
        stream_gpu = len(self.streaming_slots) * self.streaming_capacity
        gpu = resident_gpu + stream_gpu
        pinned = stream_gpu
        self.host_peak = max(self.host_peak, cpu + pinned)
        self.gpu_peak = max(self.gpu_peak, gpu)
        metrics = self.service.metrics
        metrics.update({"cache_host_allocated_bytes": cpu + pinned, "cache_gpu_allocated_bytes": gpu,
                        "cache_host_peak_bytes": self.host_peak, "cache_gpu_peak_bytes": self.gpu_peak,
                        "cache_host_compact_allocated_bytes": cpu,
                        "cache_gpu_resident_allocated_bytes": resident_gpu,
                        "cache_stream_gpu_allocated_bytes": stream_gpu,
                        "cache_stream_pinned_allocated_bytes": pinned,
                        "cache_owned_columns_live_bytes": self.owned_live_bytes,
                        "cache_owned_columns_peak_bytes": self.owned_peak_bytes,
                        "cache_host_payload_bytes": self.cpu_pool.payload if self.cpu_pool is not None else 0,
                        "cache_gpu_payload_bytes": self.gpu_pool.payload if self.gpu_pool is not None else 0})

    def _ensure_streaming_slots(self):
        if self.streaming_slots:
            return
        with torch.inference_mode(False), torch.no_grad():
            self.copy_stream = torch.cuda.Stream(device=self.service.device)
            for number in range(2):
                self.streaming_slots.append({
                    "slot": number,
                    "host": torch.empty(self.streaming_capacity, dtype=torch.uint8, pin_memory=True),
                    "raw": torch.empty(self.streaming_capacity, dtype=torch.uint8,
                                       device=self.service.device),
                    "ready": torch.cuda.Event(), "owned": torch.cuda.Event(),
                    "generation": 0, "owned_generation": 0,
                })
            # 空Tensor也可能复用分配stream此前使用的storage，首次侧stream写入同样要排序。
            compute = torch.cuda.current_stream(self.service.device)
            self.copy_stream.wait_stream(compute)
            for slot in self.streaming_slots:
                slot["raw"].record_stream(self.copy_stream)
        self._resource_metrics()
        self._observe_buffer("allocate", slots=[{"slot": item["slot"], "host": item["host"],
                                                 "raw": item["raw"]} for item in self.streaming_slots],
                             copy_stream=self.copy_stream)

    @staticmethod
    def _copy_column(raw, target, column):
        """raw只作暂存；copy_的目的列有独立storage且不参与输入构造的autograd。"""
        if column in (0, 1):
            target.copy_(raw.view(torch.int32).to(torch.int64).bitwise_and(0xFFFFFFFF))
        elif column == 2:
            target.copy_(raw.view(torch.float32).reshape(-1, 1))
        else:
            target.copy_(raw.to(torch.int64))

    def _submit_streaming_copy(self, slot, used, sample_keys, prefetch):
        previous = slot["generation"]
        if previous != slot["owned_generation"]:
            raise RuntimeError("不能覆盖尚未消费为独占列的预取raw槽")
        with torch.cuda.stream(self.copy_stream):
            if previous:
                self.copy_stream.wait_event(slot["owned"])
                self._observe_buffer("reuse_wait_submitted", slot=slot["slot"],
                                     previous_generation=previous, owned_event=slot["owned"],
                                     copy_stream=self.copy_stream)
            slot["raw"][:used].copy_(slot["host"][:used], non_blocking=True)
            slot["ready"].record(self.copy_stream)
        slot["generation"] += 1
        self._meter("cache_stream_h2d_bytes", used)
        self._meter("cache_stream_h2d_copies")
        self._meter("h2d_bytes", used)
        self._meter("h2d_copies")
        self._observe_buffer("h2d_submitted", slot=slot["slot"], generation=slot["generation"],
                             bytes=used, host=slot["host"], raw=slot["raw"],
                             ready_event=slot["ready"], copy_stream=self.copy_stream,
                             sample_keys=sample_keys, prefetch=prefetch)

    def _consume_streaming_copy(self, slot, used, pieces, columns, sample_keys, prefetch):
        compute = torch.cuda.current_stream(self.service.device)
        compute.wait_event(slot["ready"])
        for column, _, _, count, destination_offset, item_bytes, source_offset in pieces:
            raw = slot["raw"].narrow(0, source_offset, count * item_bytes)
            destination = columns[column].narrow(0, destination_offset, count)
            self._copy_column(raw, destination, column)
        # 所有模型输入都已由独立列持有，后续raw复写不会改变保存给backward的输入版本。
        slot["owned"].record(compute)
        slot["owned_generation"] = slot["generation"]
        self._observe_buffer("owned_copy_submitted", slot=slot["slot"], generation=slot["generation"],
                             bytes=used, raw=slot["raw"], owned_columns=columns,
                             owned_event=slot["owned"], compute_stream=compute,
                             sample_keys=sample_keys, prefetch=prefetch)

    def _block_plan(self, frames):
        sequences, offsets = [], []
        for frame in frames:
            offsets.append((len(sequences), len(frame["markets"])))
            sequences.extend([frame["global"], *frame["markets"]])
        lengths = [len(sequence) for sequence in sequences]
        account = frames[0]["account"]
        if any(frame["account"] != account for frame in frames):
            raise ValueError("一个序列片段不能跨越账户的独立反向边界")
        sample_keys = [(frame["account"], frame["call"]) for frame in frames]
        host_segments, resident_segments = [], []
        destination_offset = 0
        for sequence in sequences:
            count, raw = len(sequence), sequence._storage
            for column, (source_offset, item_bytes) in enumerate(
                    ((0, 4), (count * 4, 4), (count * 8, 4), (count * 12, 1))):
                if count:
                    target = resident_segments if raw.device.type == "cuda" else host_segments
                    target.append((column, raw, source_offset, count, destination_offset, item_bytes))
            destination_offset += count
        return {"lengths": lengths, "offsets": offsets, "account": account, "sample_keys": sample_keys,
                "host": host_segments, "resident": resident_segments}

    def _host_chunks(self, segments):
        """仅排布已有原始列的字节片段；无模型、额外Tensor或特征计算。"""
        used, pieces = 0, []
        for column, raw, source_offset, remaining, destination_offset, item_bytes in segments:
            while remaining:
                aligned = (used + item_bytes - 1) // item_bytes * item_bytes
                available = (self.streaming_capacity - aligned) // item_bytes
                if not available:
                    yield used, pieces
                    used, pieces = 0, []
                    continue
                take = min(remaining, available)
                pieces.append((column, raw, source_offset, take, destination_offset, item_bytes, aligned))
                used = aligned + take * item_bytes
                source_offset += take * item_bytes
                destination_offset += take
                remaining -= take
                if used == self.streaming_capacity:
                    yield used, pieces
                    used, pieces = 0, []
        if pieces:
            yield used, pieces

    def _fill_streaming_slot(self, used, pieces):
        slot = self.streaming_slots[self.next_streaming_slot]
        self.next_streaming_slot = (self.next_streaming_slot + 1) % 2
        if slot["generation"] != slot["owned_generation"]:
            raise RuntimeError("两个raw槽已持有未消费的下一片段，不能追加预取")
        if slot["generation"]:
            # GPU wait不能保护CPU覆写：等待这笔DMA实际读取完pin-host。
            slot["ready"].synchronize()
            self._observe_buffer("host_reuse_ready", slot=slot["slot"],
                                 generation=slot["generation"], ready_event=slot["ready"], completed=True)
        end = 0
        for _, raw, source_offset, count, _, item_bytes, destination_offset in pieces:
            if destination_offset > end:
                slot["host"][end:destination_offset].zero_()
            end = destination_offset + count * item_bytes
            slot["host"][destination_offset:end].copy_(raw.narrow(0, source_offset, count * item_bytes))
        return slot

    def prefetch_block(self, frames):
        """当前GRU提交后，只用原两个raw槽预取下一片段；不提前分配下一张图的列。"""
        if self.prefetched is not None:
            raise RuntimeError("上一片段预取尚未消费")
        plan = self._block_plan(frames)
        if not plan["host"]:
            return
        self._ensure_streaming_slots()
        chunks = iter(self._host_chunks(plan["host"]))
        pending, uploaded = [], 0
        with torch.inference_mode(False), torch.no_grad():
            for _ in range(2):
                item = next(chunks, None)
                if item is None:
                    break
                used, pieces = item
                slot = self._fill_streaming_slot(used, pieces)
                self._submit_streaming_copy(slot, used, plan["sample_keys"], prefetch=True)
                pending.append((slot, slot["generation"], used, pieces))
                uploaded += used
        self.prefetched = {"plan": plan, "chunks": chunks, "pending": pending, "bytes": uploaded}
        self._meter("cache_prefetched_blocks")
        self._meter("cache_prefetch_h2d_bytes", uploaded)
        self._meter("cache_prefetch_h2d_copies", len(pending))
        self.service.metrics["cache_prefetched_raw_bytes"] = uploaded
        self.service.metrics["cache_prefetched_raw_copies"] = len(pending)

    def block_columns(self, frames):
        """B连续片段：传输槽可复用，四列由本片段的真实图独占到原反向边界。"""
        if self.service.device.type != "cuda":
            raise ValueError("device_v1的序列学习要求CUDA设备")
        self._configure()
        # 在当前模型计算前建立侧stream分配依赖，首笔下一片段预取不被当前GRU的分配等待串行化。
        if self.cpu_pool.payload:
            self._ensure_streaming_slots()
        prefetched = self.prefetched
        if prefetched is not None:
            if [(frame["account"], frame["call"]) for frame in frames] != prefetched["plan"]["sample_keys"]:
                raise ValueError("预取输入身份与原学习顺序不同")
            plan = prefetched["plan"]
        else:
            plan = self._block_plan(frames)
        total, account = sum(plan["lengths"]), plan["account"]
        sample_keys = plan["sample_keys"]
        with torch.inference_mode(False), torch.no_grad():
            columns = (torch.empty(total, dtype=torch.int64, device=self.service.device),
                       torch.empty(total, dtype=torch.int64, device=self.service.device),
                       torch.empty((total, 1), dtype=torch.float32, device=self.service.device),
                       torch.empty(total, dtype=torch.int64, device=self.service.device))
            for column, raw, source_offset, count, destination_offset, item_bytes in plan["resident"]:
                self._copy_column(raw.narrow(0, source_offset, count * item_bytes),
                                  columns[column].narrow(0, destination_offset, count), column)
            uploaded = 0
            if plan["host"]:
                self._ensure_streaming_slots()
                if prefetched is not None:
                    for slot, generation, used, pieces in prefetched["pending"]:
                        if slot["generation"] != generation:
                            raise RuntimeError("预取raw代次在消费前被覆盖")
                        self._consume_streaming_copy(slot, used, pieces, columns, sample_keys, prefetch=True)
                    uploaded = prefetched["bytes"]
                    chunks = prefetched["chunks"]
                    self.prefetched = None
                    self.service.metrics["cache_prefetched_raw_bytes"] = 0
                    self.service.metrics["cache_prefetched_raw_copies"] = 0
                else:
                    chunks = self._host_chunks(plan["host"])
                for used, pieces in chunks:
                    slot = self._fill_streaming_slot(used, pieces)
                    self._submit_streaming_copy(slot, used, sample_keys, prefetch=False)
                    self._consume_streaming_copy(slot, used, pieces, columns, sample_keys, prefetch=False)
                    uploaded += used
        size = sum(value.numel() * value.element_size() for value in columns)
        self.owned_columns.setdefault(account, []).append(columns)
        self.owned_live_bytes += size
        self.owned_peak_bytes = max(self.owned_peak_bytes, self.owned_live_bytes)
        self._resource_metrics()
        self._observe_buffer("graph_inputs_retained", account=account, owned_columns=columns,
                             bytes=size, live_bytes=self.owned_live_bytes)
        return {"columns": columns, "sequence_lengths": plan["lengths"], "frame_offsets": plan["offsets"],
                "actual_uploaded_bytes": uploaded}

    def frame_for_teacher(self, frame):
        """Only raw movement changes. Policy.encode still cats one frame's sequences."""
        block = self.block_columns([frame])
        sequences = [frame["global"], *frame["markets"]]
        views, offset = [], 0
        for original, length in zip(sequences, block["sequence_lengths"]):
            views.append(_FrameColumnView(original, tuple(column.narrow(0, offset, length)
                                                          for column in block["columns"])))
            offset += length
        result = dict(frame)
        result["global"], result["markets"] = views[0], views[1:]
        return result

    def release_account_columns(self, account):
        # 只由原账户backward返回后调用。这里只报告图持有释放，不声称GPU已经完成。
        blocks = self.owned_columns.pop(account, ())
        if blocks:
            size = sum(value.numel() * value.element_size() for columns in blocks for value in columns)
            self.owned_live_bytes -= size
            self._observe_buffer("graph_inputs_released", account=account, owned_columns=blocks,
                                 bytes=size, live_bytes=self.owned_live_bytes)
        self._resource_metrics()

    def prepare_frame(self, frame):
        if not self._enabled():
            return frame
        started = time.perf_counter()
        owner = self if self.group_id is not None else None
        with torch.inference_mode(False), torch.no_grad():
            result = dict(frame)
            result["global"] = (frame["global"] if isinstance(frame["global"], TokenSequence)
                                else TokenSequence(frame["global"], owner))
            result["markets"] = [value if isinstance(value, TokenSequence) else TokenSequence(value, owner)
                                 for value in frame["markets"]]
        self._meter("cache_input_prepare_seconds", time.perf_counter() - started)
        return result

    def record_frame(self, frame):
        if self.group_id is None:
            return None
        if self.schedule is not None:
            raise ValueError("学习组输入已封存，不能追加采集帧")
        key, call = frame["account"], frame["call"]
        identities = self.calls.setdefault(key, set())
        if call in identities:
            raise ValueError("同一缓存账户和调用身份重复")
        identities.add(call)
        rows = self.frames.setdefault(key, [])
        rows.append(dict(frame))
        self._meter("cache_recorded_frames")
        return {"account": key, "index": len(rows) - 1, "call": call}

    def status(self):
        return {"group": self.group_id,
                "accounts": {key: len(rows) for key, rows in self.frames.items()},
                "prepared": self.schedule is not None}

    def prepare(self, labels, schedule):
        if self.group_id is None:
            raise ValueError("必须先打开可缓存的学习组")
        ordered = tuple((item["account"], item["index"]) for item in schedule)
        frozen = {key: tuple(values) for key, values in labels.items()}
        if self.schedule is not None:
            if self.schedule != ordered or self.labels != frozen:
                raise ValueError("同一学习组不能替换已封存的标签或顺序")
            return {"prepared": True, "samples": len(ordered), **self.status()}
        keys = set(self.frames) | set(frozen)
        for key in keys:
            if len(self.frames.get(key, ())) != len(frozen.get(key, ())):
                raise ValueError("缓存帧数与冻结标签数不同")
        expected = {(key, index) for key in keys for index in range(len(frozen.get(key, ())))}
        if len(ordered) != len(expected) or set(ordered) != expected:
            raise ValueError("学习顺序必须恰好覆盖本组全部冻结样本")
        self.labels, self.schedule = frozen, ordered
        self._meter("cache_label_preparations")
        return {"prepared": True, "samples": len(ordered), **self.status()}

    def frame(self, account, index):
        return self.frames[account][index]

    def release(self):
        for slot in self.streaming_slots:
            if slot["generation"]:
                # 组结束才销毁两个pin-host/raw storage，完成原复制和读取后再释放。
                slot["ready"].synchronize()
                if slot["owned_generation"]:
                    slot["owned"].synchronize()
        self.frames, self.calls = {}, {}
        self.labels = self.schedule = None
        self.cpu_pool = self.gpu_pool = None
        self.streaming_slots = []
        self.copy_stream = None
        self.streaming_capacity = self.next_streaming_slot = 0
        self.prefetched = None
        self.owned_columns = {}
        self.owned_live_bytes = 0
        self.group_id = None
        if hasattr(self.service, "metrics"):
            self.service.metrics["cache_prefetched_raw_bytes"] = 0
            self.service.metrics["cache_prefetched_raw_copies"] = 0
            self._resource_metrics()
