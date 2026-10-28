# 无模型历史元数据准入工具

`dpt-historical-admission` 读取调用者显式声明的材料，列出初始化候选或验证已冻结的选择。它不选择市场或 Snapshot，不建立账户，不运行模型、规则查询或业务回放。工具成功不证明窗口连续、样本适用或训练已获准。

```bash
cargo build -p data-pipeline-training --release --bin dpt-historical-admission --locked --offline
RAYON_NUM_THREADS=2 dpt-historical-admission inspect config.json new-inspect.jsonl --config-bytes 1048576
RAYON_NUM_THREADS=2 dpt-historical-admission verify selected-config.json new-verify.jsonl --config-bytes 1048576
```

配置形状如下。路径、窗口及上限均由调用者按自己的准确任务填写；数值是接口示例，不构成实际材料运行授权。

```json
{
  "daily_manifests": ["day/manifest.json"],
  "initialization": {
    "files": [{"manifest_path": "prior/manifest.json", "relative_paths": ["metadata.parquet"]}],
    "markets": [],
    "received_start_utc_ms": 1785369601000,
    "received_end_utc_ms": 1785369602000,
    "purpose": "明确的数据用途"
  },
  "active_end_utc_ms": 1785369601600,
  "limits": {
    "batch_rows": 64,
    "batch_bytes": 1048576,
    "input_file_bytes": 16777216,
    "input_rows": 10000,
    "output_bytes": 2097152,
    "source_epochs": 16
  }
}
```

`inspect` 要求 `markets` 为空。它经正式 V6/旧格式转换和公共来源证据函数输出 `metadata_candidate`，明确记录 `before_start`；S 后行也可显示，但不是合法初始化候选。调用者选定后，将 `markets` 改为非空数组，元素为 `{"condition_id":"…","snapshot_source_message_id":"…"}`，再调用 `verify`。后者进入同一 `MetadataInitializationSelectionV1` 内核，保留其同市场、Snapshot 类型、S 前与后续 Existing 选择规则。两个命令分别使用新输出文件，没有自动重试或换选择。

输出为 JSONL。`manifest_declaration` 保存声明的 closure/barrier/ack/gaps 和文件事实；`daily_admission` 表明全部日文件经过正式身份/footer准入；`metadata_candidate` 或 `selected_initialization` 保存必要元数据事实与来源；`selection_verified` 保存原组合 source identity；`window_source` 保存原 `DayIngressV1` 在 `[S,H)` 返回的记录、时标间隔和实际可见 epoch/序号事实。E 只显示为经营截止，不剪掉 E 到 H 的证据。记录间隔和不连续序号不自动等于丢失事件；重启原因、业务连续性及训练准备度没有证据时始终为 `Unknown`。

只有退出码0且最终 `complete` 行为 true 才表示工具完成。输出使用独占新建；失败保留部分文件并非0退出，不能消费为成功。输出字节预算严格作用于包括错误行在内的写出；因此预算耗尽时最后一行可能不完整，也没有成功终态。

资源上限属于工具调用预算，不改变合法材料范围。`--config-bytes` 限制配置读取；原 manifest 继续使用公共10MiB边界。`input_file_bytes/input_rows` 检查全部日文件声明，加上独立初始化遍历（重叠仍计）；正式reader继续核真实文件大小、SHA、footer、schema。`batch_bytes` 作用于正式载荷读取，原窄列索引受行数、来源 epoch 和文件声明预算约束，不承诺所有内部数组均受单批字节值限制。应同时使用调用者的父级 wall/内存监督，并按可用CPU设置 `RAYON_NUM_THREADS`。

完整日准入会哈希所有声明文件；随后原窗口接口扫描必要窄列并解码窗口载荷。初始化文件另行身份校验和解码。报告分别记录声明文件字节、实际解码/窄列行数和重复遍历口径；它没有测量精确内核读取或物理 I/O。不会把工具包装成只读取几份小 metadata，也不输出行情载荷副本。真实材料运行仍须按各自任务授权。

唯一直接人工组合可用以下具名测试运行：

```bash
RAYON_NUM_THREADS=2 cargo test -p data-pipeline-training --release --test historical_admission_cli --locked --offline -- --exact historical_admission_cli_roundtrip --test-threads=1 --nocapture
```

该组合使用正式 typed Parquet writer 生成四行微型人工 V6 Existing/Snapshot，实际运行三次子 CLI：inspect、合法 verify、预期拒绝 S 后 Snapshot。它同时核对 S 后排除、来源绑定、人工间隔仍可见且不变成业务 PASS，不读取真实研究材料。
