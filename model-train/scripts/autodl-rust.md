# AutoDL 的 Rust 离线构建材料

用于 #181 的目标环境准备。源码通过 Git bundle 同步；本文件涉及的工具和依赖先在本机取得，
再通过 SSH 上传。服务器不访问 crates.io 或外部代码仓库。

Rust 采用本机实际使用的 `1.95.0`，目标为 `x86_64-unknown-linux-gnu`。项目包含三个 Cargo
工作区，准备材料时同时使用它们的锁文件；不只打包根工作区。

## 本机准备

在准确任务工作树根目录运行，`TASK_CACHE` 指向本任务的仓库外材料目录：

```bash
mkdir -p "$TASK_CACHE/rust"
cargo vendor --locked --versioned-dirs \
  --sync model-train/native-inputs/Cargo.toml \
  --sync model-train/rust-ort-validation/Cargo.toml \
  "$TASK_CACHE/rust/vendor" > "$TASK_CACHE/rust/vendor-config.toml"
sha256sum Cargo.lock model-train/native-inputs/Cargo.lock \
  model-train/rust-ort-validation/Cargo.lock > "$TASK_CACHE/rust/Cargo.lock.sha256"
tar -czf "$TASK_CACHE/rust/rust-vendor.tar.gz" -C "$TASK_CACHE/rust" vendor
```

从 [Rust 官方独立安装包](https://static.rust-lang.org/dist/rust-1.95.0-x86_64-unknown-linux-gnu.tar.xz)
下载工具链，并用同路径的 `.sha256` 文件核对。此次官方 SHA-256 为
`2e0338f18ecbaa4a0f631b9e80e8b8e26bb6fe77dd5454fba8a70cf96c1e84a1`。
同时记录本次 vendor 压缩包的 SHA-256，与源码提交及三个锁文件一起保存。

## 目标机安装与使用

将上述工具链、摘要和 vendor 压缩包上传至 `/root/autodl-tmp/transfers/`。
在该目录核对摘要后执行：

```bash
mkdir -p /root/autodl-tmp/tmp /root/autodl-tmp/tools /root/autodl-tmp/cargo
mkdir -p /root/autodl-tmp/dependencies/rust-1.95.0 /root/autodl-tmp/build/issue-181
tar -xJf /root/autodl-tmp/transfers/rust-1.95.0-x86_64-unknown-linux-gnu.tar.xz \
  -C /root/autodl-tmp/tmp
sh /root/autodl-tmp/tmp/rust-1.95.0-x86_64-unknown-linux-gnu/install.sh \
  --prefix=/root/autodl-tmp/tools/rust-1.95.0 \
  --without=rust-docs --disable-ldconfig
tar -xzf /root/autodl-tmp/transfers/rust-vendor.tar.gz \
  -C /root/autodl-tmp/dependencies/rust-1.95.0
```

使用 `/root/autodl-tmp/cargo/config.toml` 保存以下目标环境配置：

```toml
[source.crates-io]
replace-with = "vendored-sources"

[source.vendored-sources]
directory = "/root/autodl-tmp/dependencies/rust-1.95.0/vendor"

[net]
offline = true
```

构建时显式设置 `CARGO_HOME=/root/autodl-tmp/cargo`、
`CARGO_TARGET_DIR=/root/autodl-tmp/build/issue-181` 和 `TMPDIR=/root/autodl-tmp/tmp`，
并把 `/root/autodl-tmp/tools/rust-1.95.0/bin` 放在当前任务的 PATH 前端。
Python 扩展还须使用同一项目环境的解释器，不能借用镜像中的另一套 Python。
所有 Cargo 构建仍使用 `--locked --offline`。三个工作区分别验证；其中任意一个通过不代表其他工作区通过。

以上配置只用于该 AutoDL 项目环境，不修改本机全局 Rust，也不修改实盘机的构建策略。
