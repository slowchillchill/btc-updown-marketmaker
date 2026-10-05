# AutoDL 的 Python / cu128 离线环境

本入口服务 #181 的首次目标环境准备，不启动训练。代码通过既定 Git bundle 流程同步。用户已批准本次
材料准备在目标机直接下载，下载失败或停滞时停止并汇报；也可继续在本机准备后上传。
下载完成后的安装仍完全离线。Rust 材料见 [Rust 离线准备](autodl-rust.md)。

## 依赖来源

`model-train/pyproject.toml` 共同声明 `torch==2.11.0`，使扩展包元数据接受该公开版本的 CPU / CUDA 构建。
CPU 索引和 `uv.lock` 仍固定 `2.11.0+cpu`；GPU 准备读取共同声明，明确选择 `2.11.0+cu128`，
并用原 `uv.lock` 约束其余已锁定包。公开版本声明本身不选择 CUDA 版本，实际构建由各自索引与锁限定。
Python 保持 `3.13.15`，解析工具保持 `uv 0.12.9`，目标为 CPython 3.13、Linux x86_64、glibc 2.35。
`pylock.cu128.toml` 记录 GPU 目标 wheel 的官方 URL 和 SHA-256，不覆盖 CPU 依赖锁。
GPU 所需 CUDA / cuDNN / Triton 依赖由 uv 解析，不用镜像预装版本替代。

源文件哈希仅用于记录。执行时核对实际版本约束；注释变化或脚本修正不使已经校验的安装材料失效。
同一制品使用另一个已核实的官方地址时，保留版本和 SHA。脚本不自动切换来源或更新依赖。

## 本机解析与准备

本机需要 Python 3.11+、curl 和已经核实 SHA 的 uv 0.12.9 可执行文件。下载缓存放在仓库外。
代理由调用者通过环境变量提供；curl 支持既有 `ALL_PROXY` / SOCKS5 配置。本仓库不记录代理地址，
脚本不读取或修改 VPN 全局配置。uv 的索引解析和 curl 的下载分别使用其标准网络机制。

```bash
AUTODL_LOCAL_CACHE="${XDG_CACHE_HOME:-$HOME/.cache}/btc-updown-marketmaker/issue-181-autodl"
python3 model-train/scripts/autodl_environment.py lock \
  --uv "$AUTODL_LOCAL_CACHE/tools/uv-0.12.9" \
  --cache "$AUTODL_LOCAL_CACHE/resolution"
python3 model-train/scripts/autodl_environment.py plan
```

`lock --head-sizes` 可用 curl 查询索引未提供的大小；HEAD 失败时保留未知，不阻止写锁，也不会触发大包下载。
`plan` 区分已知合计与未知材料，不把部分大小当作总量。首次锁应审查后再下载；正常准备直接复用已提交的锁。

```bash
python3 model-train/scripts/autodl_environment.py prepare \
  --bundle "$AUTODL_LOCAL_CACHE/python-cu128-bundle" \
  --jobs 4 \
  --progress-seconds 5
```

`prepare --jobs` 默认 1，可用 4 小规模并发下载独立材料。curl 继承调用者代理，`.part` 文件支持续传。
任何材料失败即停止领取和启动新项，并终止其他活动 curl、保留 `.part`；不自动重试、换版本或来源。
连接超时为 30 秒；持续 60 秒低于 1 字节/秒也按失败停止。每个完成文件检查 SHA-256；已知大小同时检查。
已有正确文件直接复用，损坏文件拒绝覆盖。
每隔指定秒数打印当前字节数和耗时，未知总量明确显示。完成后写 `manifest.json`，记录实际大小、原始
`url` 和本次请求地址 `download_url`。
复用文件保留已有来源记录；没有来源记录的预置文件将 `download_url` 标为 `null`，不冒称本次已下载。
该清单用于审查和交接，安装材料是否有效仍由当前锁要求的制品 SHA 决定。

## 目标机离线安装

先按 AGENTS.md 核对数据盘挂载、容量及两端源码版本。可将整个材料目录上传至
`/root/autodl-tmp/` 下，例如 `/root/autodl-tmp/offline/python-cu128-bundle`；也可在目标机执行以下准备命令。
当脚本从 AutoDL 数据盘工作树运行时，材料目录及文件解析路径必须在同一数据盘边界内。
使用目标机已经核实的 Python 3.12+ 引导解释器执行脚本，其绝对路径由本机执行命令提供：

```bash
cd /root/autodl-tmp/btc-updown-marketmaker
"$AUTODL_BOOTSTRAP_PYTHON" model-train/scripts/autodl_environment.py prepare \
  --bundle /root/autodl-tmp/offline/python-cu128-bundle \
  --jobs 4 --pypi-mirror https://mirrors.aliyun.com/pypi
"$AUTODL_BOOTSTRAP_PYTHON" model-train/scripts/autodl_environment.py install \
  --bundle /root/autodl-tmp/offline/python-cu128-bundle
source /root/autodl-tmp/environments/issue-181-cu128.sh
```

`--pypi-mirror` 只把官方 `files.pythonhosted.org/packages/...` 地址映射到显式镜像基地址的
`/packages/...`，不更改版本、文件名或原官方 SHA。Torch / Triton 的官方下载地址和 CPython GitHub
地址保持。镜像缺失或校验失败时停止；此参数不用于 Cargo、代码同步或其他资源。

安装使用 `/root/autodl-tmp/tools/python-3.13.15`、`tools/uv-0.12.9` 和项目 `model-train/.venv`。
先核对上传材料，再由锁生成版本与哈希要求，使用 `--offline --no-index --only-binary --require-hashes` 安装。
不会安装到系统 Python，也不会替换镜像环境。已有虚拟环境须使用同一 Python 版本；此入口不清除额外包，
项目原生扩展由后续构建维护。

生成的环境文件把 Python、uv、Torch、Triton、CUDA 缓存和临时目录放在数据盘；
`CARGO_HOME=/root/autodl-tmp/cargo`、`CARGO_TARGET_DIR=/root/autodl-tmp/build/issue-181`，
Rust 工具前缀为 `/root/autodl-tmp/tools/rust-1.95.0`。后续编译和运行前均先加载该环境文件。

此入口只完成依赖安装和依赖一致性检查。原生扩展编译、实际模型的 GPU 前向/反向/更新/保存加载、
显存与端到端耗时仍由后续短验证完成，不能据此声称训练环境或业务验收已经通过。
