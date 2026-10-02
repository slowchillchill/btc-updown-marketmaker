#!/usr/bin/env python3
"""用 GNU time 执行薄试制工具；1秒采样任务材料目录，保留原stderr及精确终值。"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time


def inventory(root):
    files = []
    for directory, _, names in os.walk(root):
        for name in names:
            path = Path(directory) / name
            try:
                st = path.stat()
            except FileNotFoundError:
                continue  # 排序器可能在两次系统调用之间删除临时run。
            files.append({"path": str(path.relative_to(root)), "logical_bytes": st.st_size,
                          "allocated_bytes": st.st_blocks * 512})
    return files


def totals(files):
    return {"files": len(files),
            "logical_bytes": sum(f["logical_bytes"] for f in files),
            "allocated_bytes": sum(f["allocated_bytes"] for f in files)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("config", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    config = json.loads(args.config.read_text())
    root = Path(config["work_root"])
    args.output.mkdir(exist_ok=False)
    machine = {"meminfo_before": Path("/proc/meminfo").read_text(),
               "disk_available_bytes_before": os.statvfs(args.output).f_bavail * os.statvfs(args.output).f_frsize}
    started = time.monotonic()
    with (args.output / "stdout.log").open("w") as stdout, (args.output / "stderr.log").open("w") as stderr:
        process = subprocess.Popen(["/usr/bin/time", "-v", "-o", str(args.output / "gnu-time.txt"),
                                    str(args.binary), str(args.config), str(args.output / "trial.private.json")],
                                   stdout=stdout, stderr=stderr)
        print(f"GNU time PID {process.pid}", flush=True)
        samples = []
        while True:
            samples.append({"elapsed_seconds": time.monotonic() - started, **totals(inventory(root))})
            code = process.poll()
            if code is not None:
                break
            time.sleep(1)
    files = inventory(root)
    for item in files:
        h = hashlib.sha256()
        with (root / item["path"]).open("rb") as source:
            for block in iter(lambda: source.read(1024 * 1024), b""):
                h.update(block)
        item["sha256"] = h.hexdigest()
    output = {"exit_code": code, "monitor_wall_seconds": time.monotonic()-started,
              "sample_interval_seconds": 1, "peak_note": "采样峰值仅为观测下界；遍历不是原子快照",
              "sampled_peak_logical_bytes_lower_bound": max(s["logical_bytes"] for s in samples),
              "sampled_peak_allocated_bytes_lower_bound": max(s["allocated_bytes"] for s in samples),
              "samples": samples, "final_exact": totals(files), "final_files": files,
              "final_subdirectories": {name: totals([f for f in files if f["path"].startswith(name+"/")])
                                      for name in ("merge", "public", "hidden")}, "machine": machine}
    (args.output / "resources.private.json").write_text(json.dumps(output, ensure_ascii=False, indent=2)+"\n")
    raise SystemExit(code)


if __name__ == "__main__":
    main()
