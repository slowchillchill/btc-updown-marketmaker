#!/usr/bin/env python3
"""#181 的 CPython/cu128 离线材料准备与目标机安装；不启动训练。"""

from __future__ import annotations

import argparse
from concurrent.futures import CancelledError, ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shlex
import subprocess
import tarfile
import threading
import time
import tomllib
import urllib.parse
import zipfile


MODEL_ROOT = Path(__file__).resolve().parents[1]
GPU_LOCK = MODEL_ROOT / "pylock.cu128.toml"
PYTHON_VERSION = "3.13.15"
UV_VERSION = "0.12.9"
TARGET_PLATFORM = "x86_64-manylinux_2_35"
AUTODL_ROOT = Path("/root/autodl-tmp")
PYTHON_ARCHIVE = {
    "url": "https://github.com/astral-sh/python-build-standalone/releases/download/20260901/cpython-3.13.15%2B20260901-x86_64-unknown-linux-gnu-install_only_stripped.tar.gz",
    "sha256": "8a689a077337bea6d1c4bc0b7df1d52fcaa28f5f67e50df8bf417c1e3f9d8874",
    "size": 34810677,
}
UV_WHEEL = {
    "url": "https://files.pythonhosted.org/packages/37/4b/cd04809c7ad5149faac55160925bd67aab43f803f6c01be1c32aec7d24d9/uv-0.12.9-py3-none-manylinux_2_17_x86_64.manylinux2014_x86_64.whl",
    "sha256": "5badfd805fd88bf99b4b4f044f6e8f762f1892cab27477f4427bb473e93dd049",
    "size": 20056367,
}


def log(message: str) -> None:
    print(message, flush=True)


def digest(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def normalized(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def source_inputs() -> tuple[list[str], dict[str, str], dict[str, str]]:
    project = tomllib.loads((MODEL_ROOT / "pyproject.toml").read_text())
    cpu_lock = tomllib.loads((MODEL_ROOT / "uv.lock").read_text())
    if project["project"]["requires-python"] != f"=={PYTHON_VERSION}":
        raise ValueError("项目 Python 版本已变化，须重新审查离线材料")
    if project["tool"]["uv"]["required-version"] != f"=={UV_VERSION}":
        raise ValueError("项目 uv 版本已变化，须重新审查离线材料")
    dependencies = project["project"]["dependencies"]
    if dependencies.count("torch==2.11.0") != 1:
        raise ValueError("项目须声明 torch==2.11.0，GPU 锁明确选用 2.11.0+cu128")
    direct = ["torch==2.11.0+cu128" if x == "torch==2.11.0" else x for x in dependencies]
    preserved = {
        normalized(p["name"]): p["version"]
        for p in cpu_lock["package"]
        if "registry" in p["source"] and normalized(p["name"]) != "torch"
    }
    metadata = {
        "python-version": PYTHON_VERSION,
        "uv-version": UV_VERSION,
        "python-platform": TARGET_PLATFORM,
        "source-pyproject-sha256": digest(MODEL_ROOT / "pyproject.toml"),
        "source-uv-lock-sha256": digest(MODEL_ROOT / "uv.lock"),
    }
    return direct, preserved, metadata


def read_lock(path: Path = GPU_LOCK) -> dict:
    lock = tomllib.loads(path.read_text())
    direct, preserved, metadata = source_inputs()
    recorded = lock.get("tool", {}).get("autodl", {})
    for key in ("python-version", "uv-version", "python-platform"):
        if recorded.get(key) != metadata[key]:
            raise ValueError(f"GPU 锁目标约束不一致：{key}")
    if lock["requires-python"] != f"=={PYTHON_VERSION}":
        raise ValueError("GPU 锁 Python 约束不匹配")
    packages = {normalized(p["name"]): p for p in lock["packages"]}
    if len(packages) != len(lock["packages"]):
        raise ValueError("GPU 锁存在重复包")
    expected = dict(preserved)
    for requirement in direct:
        name, version = requirement.split("==")
        expected[normalized(name)] = version
    for name, version in expected.items():
        if packages.get(name, {}).get("version") != version:
            raise ValueError(f"GPU 锁未保留固定版本：{name}=={version}")
    for package in packages.values():
        if not package.get("wheels") or "sdist" in package:
            raise ValueError(f"只允许目标平台 wheel：{package['name']}")
        for wheel in package["wheels"]:
            parsed = urllib.parse.urlsplit(wheel["url"])
            if parsed.scheme != "https" or parsed.hostname not in {
                "files.pythonhosted.org", "download.pytorch.org", "download-r2.pytorch.org"
            }:
                raise ValueError("GPU 锁包含未核实的 wheel 来源")
            if not re.fullmatch(r"[0-9a-f]{64}", wheel["hashes"]["sha256"]) or (
                "size" in wheel and wheel["size"] <= 0
            ):
                raise ValueError("GPU 锁中的大小或 SHA-256 无效")
    return lock


def run(command: list[str], *, env: dict | None = None, cwd: Path | None = None) -> None:
    log("执行：" + shlex.join(command))
    subprocess.run(command, check=True, env=env, cwd=cwd)


def head_size(url: str) -> int | None:
    """大小只供计划使用；curl 继承调用者代理，不把 HEAD 成功作为锁定条件。"""
    result = subprocess.run(
        ["curl", "--disable", "--fail", "--location", "--silent", "--show-error",
         "--head", "--connect-timeout", "30", "--max-time", "60", "--url", url],
        text=True, capture_output=True,
    )
    lengths = re.findall(r"^content-length:\s*(\d+)\s*$", result.stdout, re.I | re.M)
    if result.returncode == 0 and lengths and int(lengths[-1]) > 0:
        return int(lengths[-1])
    log(f"HEAD 未取得大小（curl={result.returncode}），保留未知：{url}")
    return None


def lock_dependencies(uv: Path, cache: Path, head_sizes: bool) -> None:
    direct, preserved, metadata = source_inputs()
    actual = subprocess.check_output([str(uv), "--version"], text=True).split()
    if actual[:2] != ["uv", UV_VERSION]:
        raise ValueError(f"需要 uv {UV_VERSION}，实际为 {' '.join(actual)}")
    cache.mkdir(parents=True, exist_ok=True)
    incoming = cache / "requirements.in"
    constraints = cache / "constraints.txt"
    resolved = cache / "pylock.resolved.toml"
    incoming.write_text("\n".join(direct) + "\n")
    constraints.write_text("\n".join(f"{n}=={v}" for n, v in sorted(preserved.items())) + "\n")
    # 显式输入、索引和缓存；不读取调用者的 uv/pip 索引环境配置。
    env = {k: v for k, v in os.environ.items() if not k.startswith(("UV_", "PIP_"))}
    env["UV_PYTHON_DOWNLOADS"] = "never"
    run([
        str(uv), "pip", "compile", str(incoming), "-c", str(constraints),
        "--output-file", str(resolved), "--format", "pylock.toml",
        "--python-version", PYTHON_VERSION, "--python-platform", TARGET_PLATFORM,
        "--torch-backend", "cu128", "--only-binary", ":all:", "--no-python-downloads",
        "--cache-dir", str(cache / "uv-cache"), "--no-header", "--no-annotate",
        "--no-sources", "--default-index", "https://pypi.org/simple", "--quiet",
    ], env=env, cwd=cache)
    value = tomllib.loads(resolved.read_text())
    lines = [
        '# 由 scripts/autodl_environment.py lock 从项目精确依赖与 CPU 锁生成。',
        'lock-version = "1.0"', f'created-by = "uv {UV_VERSION}"',
        f'requires-python = "=={PYTHON_VERSION}"', "", "[tool.autodl]",
    ]
    lines += [f"{key} = {json.dumps(val)}" for key, val in metadata.items()]
    for package in value["packages"]:
        lines += ["", "[[packages]]", f'name = {json.dumps(package["name"])}',
                  f'version = {json.dumps(package["version"])}']
        if "marker" in package:
            lines.append(f'marker = {json.dumps(package["marker"])}')
        lines.append("wheels = [")
        for wheel in package["wheels"]:
            size = wheel.get("size")
            if size is None and head_sizes:
                size = head_size(wheel["url"])
            size_field = "" if size is None else f", size = {size}"
            lines.append("    { url = " + json.dumps(wheel["url"]) + size_field + ", hashes = { sha256 = "
                         + json.dumps(wheel["hashes"]["sha256"]) + " } },")
        lines.append("]")
    candidate = cache / "pylock.cu128.toml"
    candidate.write_text("\n".join(lines) + "\n")
    read_lock(candidate)
    GPU_LOCK.write_text(candidate.read_text())
    log(f"已写入 {GPU_LOCK}；{len(value['packages'])} 个包；请审查锁差异后 prepare")


def artifacts(lock: dict) -> list[dict]:
    result = []
    for package in lock["packages"]:
        for wheel in package["wheels"]:
            result.append({"url": wheel["url"], "sha256": wheel["hashes"]["sha256"],
                           "size": wheel.get("size"), "directory": "wheels"})
    result += [{**PYTHON_ARCHIVE, "directory": "bootstrap"}, {**UV_WHEEL, "directory": "bootstrap"}]
    for item in result:
        item["filename"] = urllib.parse.unquote(urllib.parse.urlsplit(item["url"]).path.rsplit("/", 1)[1])
        if Path(item["filename"]).name != item["filename"] or item["filename"] in {"", ".", ".."}:
            raise ValueError("材料 URL 包含非法文件名")
    return result


def verified(path: Path, item: dict) -> bool:
    return path.is_file() and (item["size"] is None or path.stat().st_size == item["size"]) and digest(path) == item["sha256"]


class DownloadBatch:
    """同一批材料共享首个失败；领取任务和启动 curl 与取消互斥。"""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.error: BaseException | None = None
        self.active: set[subprocess.Popen] = set()

    def start(self, command: list[str]) -> subprocess.Popen:
        with self.lock:
            if self.error is not None:
                raise CancelledError("另一材料失败，下载批次已停止")
            process = subprocess.Popen(command, stdout=subprocess.PIPE, text=True)
            self.active.add(process)
            return process

    def finished(self, process: subprocess.Popen) -> None:
        with self.lock:
            self.active.discard(process)

    def fail(self, error: BaseException) -> bool:
        with self.lock:
            if self.error is None:
                self.error = error
                for process in self.active:
                    if process.poll() is None:
                        try:
                            process.terminate()
                        except ProcessLookupError:
                            pass
                return True
            return False


def download(item: dict, destination: Path, progress_seconds: float, batch: DownloadBatch | None = None,
             retry_sources: bool = False) -> int:
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        if not verified(destination, item):
            raise ValueError(f"已有材料哈希/大小不符，保留现场：{destination}")
        log(f"已核对，复用：{destination.name}")
        return destination.stat().st_size
    partial = destination.with_name(destination.name + ".part")
    started = time.monotonic()
    urls = item.get("download_urls", [item.get("download_url", item["url"])])
    if not retry_sources:
        urls = urls[:1]
    item["attempted_urls"] = []
    for index, url in enumerate(urls):
        command = ["curl", "--disable", "--fail", "--location", "--silent", "--show-error",
                   "--connect-timeout", "30", "--speed-limit", "1", "--speed-time", "60",
                   "--continue-at", "-", "--output", str(partial), "--write-out", "%{http_code}", "--url", url]
        if retry_sources:
            command += ["--retry", "3", "--retry-connrefused"]
        process = batch.start(command) if batch is not None else subprocess.Popen(command, stdout=subprocess.PIPE, text=True)
        item["attempted_urls"].append(url)
        item["download_url"] = url
        log(f"{destination.name}：来源 {index + 1}/{len(urls)}，{url}")
        try:
            try:
                while True:
                    try:
                        result = process.wait(timeout=progress_seconds)
                        break
                    except subprocess.TimeoutExpired:
                        received = partial.stat().st_size if partial.exists() else 0
                        total = item["size"] if item["size"] is not None else "总量未知"
                        log(f"{destination.name}: {received}/{total} 字节，已耗时 {time.monotonic() - started:.1f}s")
                status = (process.stdout.read() if process.stdout is not None else "").strip()
            except BaseException:
                process.terminate()
                process.wait()
                raise
        finally:
            if process.stdout is not None:
                process.stdout.close()
            if batch is not None:
                batch.finished(process)
        if result != 0:
            http_status = int(status) if status.isdigit() else 0
            item.setdefault("attempt_errors", []).append({"url": url, "curl_code": result, "http_status": http_status})
            # 不对写盘、参数、TLS 证书校验等本地/安全错误换源。
            recoverable = result in {5, 6, 7, 16, 18, 28, 35, 52, 55, 56, 92} or (
                result == 22 and http_status in {403, 404, 408, 429, 500, 502, 503, 504}
            )
            if retry_sources and recoverable and index + 1 < len(urls):
                log(f"该来源无法完成下载（curl={result}, HTTP={http_status}）；保留 .part，尝试下一批准来源")
                continue
            raise subprocess.CalledProcessError(result, command)
        if not verified(partial, item):
            raise ValueError(f"下载材料哈希/大小不符，保留 .part：{partial}")
        received = partial.stat().st_size
        partial.replace(destination)
        log(f"完成 {destination.name}: {received} 字节，{time.monotonic() - started:.1f}s，SHA-256 通过")
        return received
    raise ValueError(f"材料没有下载来源：{destination.name}")


def manifest(lock: dict) -> dict:
    # 仅记录来源；复用和安装直接以当前锁中的制品 SHA 为准。
    return {"format": 1, "gpu_lock_sha256": digest(GPU_LOCK), "artifacts": artifacts(lock)}


def plan(lock: dict) -> None:
    items = artifacts(lock)
    total = sum(item["size"] or 0 for item in items)
    unknown = sum(item["size"] is None for item in items)
    log(f"{len(lock['packages'])} 个包，{len(items) - 2} 个 wheel；Python/uv 各 1 份")
    label = "已知材料合计" if unknown else "下载总量"
    log(f"{label}：{total} 字节（{total / 1024**3:.3f} GiB）；不包含 Rust、源码、数据及解压空间")
    if unknown:
        log(f"另有 {unknown} 份材料大小未知，总下载量尚不能确定")
    for item in sorted(items, key=lambda x: x["size"] or 0, reverse=True):
        size = str(item["size"]) if item["size"] is not None else "未知"
        log(f"{size:>10}  {item['filename']}")


def download_url(url: str, pypi_mirror: str | None) -> str:
    if pypi_mirror is None:
        return url
    mirror = urllib.parse.urlsplit(pypi_mirror)
    if mirror.scheme != "https" or not mirror.hostname or mirror.username or mirror.password or mirror.query or mirror.fragment:
        raise ValueError("PyPI 镜像须为不含凭据、查询或片段的显式 HTTPS 基地址")
    original = urllib.parse.urlsplit(url)
    if original.hostname == "files.pythonhosted.org" and original.path.startswith("/packages/"):
        return pypi_mirror.rstrip("/") + original.path
    return url


def download_candidates(url: str, pypi_mirror: str | None, retry_sources: bool) -> list[str]:
    primary = download_url(url, pypi_mirror)
    if not retry_sources:
        return [primary]
    original = urllib.parse.urlsplit(url)
    if original.hostname == "files.pythonhosted.org" and original.path.startswith("/packages/"):
        candidates = ([primary] if pypi_mirror else []) + [
            download_url(url, "https://mirrors.tuna.tsinghua.edu.cn/pypi/web"), url,
        ]
    elif original.hostname == "download-r2.pytorch.org":
        candidates = [url, urllib.parse.urlunsplit(original._replace(netloc="download.pytorch.org"))]
    else:
        candidates = [url]
    return list(dict.fromkeys(candidates))


def prepare(lock: dict, bundle: Path, progress_seconds: float, jobs: int = 1,
            pypi_mirror: str | None = None, retry_sources: bool = False) -> None:
    if jobs < 1:
        raise ValueError("并发下载数须至少为 1")
    target = MODEL_ROOT.is_relative_to(AUTODL_ROOT)
    if target:
        bundle = on_data_disk(bundle)
    bundle.mkdir(parents=True, exist_ok=True)
    expected = manifest(lock)
    expected["artifacts"].sort(key=lambda item: item["directory"] != "bootstrap")
    for item in expected["artifacts"]:
        item["download_urls"] = download_candidates(item["url"], pypi_mirror, retry_sources)
        item["download_url"] = item["download_urls"][0]
    previous = bundle / "manifest.json"
    if target:
        previous = on_data_disk(previous)
    previous_sources = {}
    if previous.exists():
        try:
            for item in json.loads(previous.read_text())["artifacts"]:
                previous_sources[(item["filename"], item["sha256"])] = item.get("download_url", item["url"])
        except (OSError, ValueError, KeyError, TypeError):
            log("旧来源记录无法读取；已有材料仍按当前锁逐项校验，未知来源不补造")
    plan(lock)
    policy = "curl 最多重试 3 次临时错误，再尝试批准来源；校验错误立即停止" if retry_sources else "任一失败即停止本批，不自动重试或切换来源"
    log(f"并发下载数：{jobs}；{policy}")
    pending = iter(enumerate(expected["artifacts"], 1))
    batch = DownloadBatch()
    failure_record = {}

    def worker() -> None:
        try:
            while True:
                with batch.lock:
                    if batch.error is not None:
                        return
                    entry = next(pending, None)
                if entry is None:
                    return
                index, item = entry
                destination = bundle / item["directory"] / item["filename"]
                if target:
                    destination = on_data_disk(destination)
                    on_data_disk(destination.with_name(destination.name + ".part"))
                reused = destination.exists()
                log(f"材料 {index}/{len(expected['artifacts'])}：{item['filename']}；请求地址：{item['download_url']}")
                item["size"] = download(item, destination, progress_seconds, batch, retry_sources)
                item["reused"] = reused
                if reused:
                    item["download_url"] = previous_sources.get((item["filename"], item["sha256"]))
        except CancelledError:
            pass
        except BaseException as error:
            if batch.fail(error):
                failure_record.update(material=item, partial_path=str(destination.with_name(destination.name + ".part")))

    with ThreadPoolExecutor(max_workers=jobs) as pool:
        futures = [pool.submit(worker) for _ in range(jobs)]
        try:
            for future in futures:
                future.result()
        except BaseException as error:
            batch.fail(error)
    if batch.error is not None:
        failure_record["error"] = str(batch.error)
        failure_path = bundle / "download-failure.json"
        if target:
            failure_path = on_data_disk(failure_path)
        try:
            failure_path.write_text(json.dumps(failure_record, ensure_ascii=False, indent=2) + "\n")
            log(f"失败材料与来源记录：{failure_path}")
        except OSError as error:
            log(f"无法写入失败记录：{error}；原始错误继续上报")
        raise batch.error
    previous.write_text(json.dumps(expected, ensure_ascii=False, indent=2) + "\n")
    log(f"材料准备完成：{bundle}；manifest SHA-256：{digest(previous)}")


def on_data_disk(path: Path) -> Path:
    # resolve 同时拒绝经软链接绕出指定数据盘根目录。
    resolved = path.resolve()
    if not resolved.is_relative_to(AUTODL_ROOT) or resolved == AUTODL_ROOT:
        raise ValueError(f"项目路径须位于 {AUTODL_ROOT}/ 内：{path}")
    return resolved


def target_environment(bundle: Path, venv: Path, uv_dir: Path) -> dict[str, str]:
    # standalone 的 sysconfig LIBDIR 保留 /install/lib；使用本入口实际安装布局。
    python_library = on_data_disk(AUTODL_ROOT / f"tools/python-{PYTHON_VERSION}/python/lib")
    library_paths = {
        key: str(python_library) + (":" + os.environ[key] if os.environ.get(key) else "")
        for key in ("LIBRARY_PATH", "LD_LIBRARY_PATH")
    }
    paths = {
        "UV_CACHE_DIR": AUTODL_ROOT / "cache/uv", "XDG_CACHE_HOME": AUTODL_ROOT / "cache",
        "PIP_CACHE_DIR": AUTODL_ROOT / "cache/pip", "TMPDIR": AUTODL_ROOT / "tmp",
        "TORCH_HOME": AUTODL_ROOT / "cache/torch", "TORCH_EXTENSIONS_DIR": AUTODL_ROOT / "cache/torch-extensions",
        "TRITON_CACHE_DIR": AUTODL_ROOT / "cache/triton", "CUDA_CACHE_PATH": AUTODL_ROOT / "cache/cuda",
        "PYTHONPYCACHEPREFIX": AUTODL_ROOT / "cache/pycache", "CARGO_HOME": AUTODL_ROOT / "cargo",
        "CARGO_TARGET_DIR": AUTODL_ROOT / "build/issue-181", "RUSTUP_HOME": AUTODL_ROOT / "tools/rustup",
        "UV_PYTHON_INSTALL_DIR": AUTODL_ROOT / "tools/uv-python",
    }
    for path in paths.values():
        on_data_disk(path).mkdir(parents=True, exist_ok=True)
    return {
        **{k: str(v) for k, v in paths.items()}, "TEMP": str(paths["TMPDIR"]), "TMP": str(paths["TMPDIR"]),
        **library_paths,
        "VIRTUAL_ENV": str(venv), "PYO3_PYTHON": str(venv / "bin/python"),
        "UV_PYTHON_DOWNLOADS": "never", "UV_OFFLINE": "1", "PIP_NO_INDEX": "1",
        "PIP_FIND_LINKS": str(bundle / "wheels"), "PYTHONNOUSERSITE": "1", "CARGO_NET_OFFLINE": "true",
        "PATH": f"{venv}/bin:{uv_dir}:{AUTODL_ROOT}/tools/rust-1.95.0/bin:" + os.environ.get("PATH", "/usr/bin:/bin"),
    }


def install(lock: dict, bundle: Path) -> None:
    if platform.system() != "Linux" or platform.machine() != "x86_64":
        raise ValueError("本入口仅安装到已核实的 Linux x86_64 AutoDL 目标")
    bundle = on_data_disk(bundle)
    on_data_disk(MODEL_ROOT)
    expected = manifest(lock)
    for item in expected["artifacts"]:
        path = on_data_disk(bundle / item["directory"] / item["filename"])
        log(f"核对上传材料：{path.name}")
        if not verified(path, item):
            raise ValueError(f"上传材料哈希/大小不符：{path}")
    python_dir = on_data_disk(AUTODL_ROOT / f"tools/python-{PYTHON_VERSION}")
    uv_dir = on_data_disk(AUTODL_ROOT / f"tools/uv-{UV_VERSION}")
    venv = on_data_disk(MODEL_ROOT / ".venv")
    env_values = target_environment(bundle, venv, uv_dir)
    env = {k: v for k, v in os.environ.items() if not k.startswith(("UV_", "PIP_"))}
    env.update(env_values)
    python = on_data_disk(python_dir / "python/bin/python3.13")
    if not python.exists():
        python_dir.mkdir(parents=True, exist_ok=True)
        archive = next(x for x in expected["artifacts"] if x["url"] == PYTHON_ARCHIVE["url"])
        log("解压已校验的 CPython，禁止归档路径越界")
        with tarfile.open(bundle / "bootstrap" / archive["filename"], "r:gz") as source:
            source.extractall(python_dir, filter="data")
        python = on_data_disk(python_dir / "python/bin/python3.13")
    uv_dir.mkdir(parents=True, exist_ok=True)
    uv_item = next(x for x in expected["artifacts"] if x["url"] == UV_WHEEL["url"])
    uv = on_data_disk(uv_dir / "uv")
    with zipfile.ZipFile(bundle / "bootstrap" / uv_item["filename"]) as wheel:
        members = [x for x in wheel.namelist() if x.endswith("/uv") and "/scripts/" in x]
        if len(members) != 1:
            raise ValueError("uv wheel 缺少唯一的 uv 可执行文件")
        uv.write_bytes(wheel.read(members[0]))
    uv.chmod(0o755)
    version = subprocess.check_output([str(python), "-I", "-c", "import platform; print(platform.python_version())"], env=env, text=True).strip()
    if version != PYTHON_VERSION:
        raise ValueError(f"Python 实际版本不一致：{version}")
    if not venv.exists():
        run([str(uv), "venv", str(venv), "--python", str(python), "--offline", "--no-python-downloads"], env=env)
    on_data_disk(venv / "bin/python")
    version = subprocess.check_output([str(venv / "bin/python"), "-I", "-c", "import platform; print(platform.python_version())"], env=env, text=True).strip()
    if version != PYTHON_VERSION:
        raise ValueError(f"已有虚拟环境 Python 版本不一致：{version}")
    requirements = on_data_disk(bundle / "requirements.offline.txt")
    requirements.write_text("\n".join(
        f"{p['name']}=={p['version']} " + " ".join(f"--hash=sha256:{w['hashes']['sha256']}" for w in p["wheels"])
        for p in lock["packages"]
    ) + "\n")
    run([str(uv), "pip", "install", "--python", str(venv / "bin/python"), "--offline", "--no-index",
         "--find-links", str(bundle / "wheels"), "--only-binary", ":all:", "--require-hashes", "--no-deps",
         "-r", str(requirements)], env=env)
    run([str(uv), "pip", "check", "--python", str(venv / "bin/python"), "--offline"], env=env)
    activation = on_data_disk(AUTODL_ROOT / "environments/issue-181-cu128.sh")
    activation.parent.mkdir(parents=True, exist_ok=True)
    activation.write_text("# 由 autodl_environment.py 生成；仅供当前目标机 source。\n" + "\n".join(
        f"export {key}={shlex.quote(value)}" for key, value in env_values.items()
    ) + "\n")
    log(f"Python/GPU 依赖安装完成。运行前 source {activation}")
    log("尚未编译项目原生扩展，尚未验证实际模型 GPU 前向/反向；这些由后续短验证报告。")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    resolver = sub.add_parser("lock", help="本机小型元数据解析；不下载 Python/GPU 大包")
    resolver.add_argument("--uv", required=True, type=Path)
    resolver.add_argument("--cache", required=True, type=Path)
    resolver.add_argument("--head-sizes", action="store_true", help="可选：curl 经调用者代理补查缺失大小；失败仍保留未知")
    sub.add_parser("plan", help="只读打印锁定材料及下载字节数")
    for name in ("prepare", "install"):
        command = sub.add_parser(name, help="按锁下载材料" if name == "prepare" else "AutoDL 目标机完全离线安装")
        command.add_argument("--bundle", required=True, type=Path)
        if name == "prepare":
            command.add_argument("--progress-seconds", type=float, default=5.0)
            command.add_argument("--jobs", type=int, default=1, help="独立材料并发数，默认 1")
            command.add_argument("--pypi-mirror", help="显式 PyPI 制品镜像基地址，例如 https://mirrors.aliyun.com/pypi")
            command.add_argument("--retry-sources", action="store_true", help="启用本次批准的 curl 临时重试和同制品候选来源；默认关闭")
    args = parser.parse_args()
    try:
        if args.command == "lock":
            lock_dependencies(args.uv.resolve(), args.cache.resolve(), args.head_sizes)
        else:
            lock = read_lock()
            if args.command == "plan":
                plan(lock)
            elif args.command == "prepare":
                if args.progress_seconds <= 0:
                    raise ValueError("进度输出间隔须为正数")
                prepare(lock, args.bundle.resolve(), args.progress_seconds, args.jobs, args.pypi_mirror, args.retry_sources)
            else:
                install(lock, args.bundle.resolve())
    except (OSError, ValueError, KeyError, subprocess.CalledProcessError) as error:
        parser.exit(1, f"停止：{error}\n材料与失败现场已保留；不自动换版本或改用联网安装。\n")


if __name__ == "__main__":
    main()
