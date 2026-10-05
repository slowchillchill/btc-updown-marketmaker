#!/usr/bin/env python3
"""将明确指定的UTC日从用户ZIP移交到AutoDL数据盘；只核对传输，不生成训练根或评价分区。"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from datetime import date, timedelta
import hashlib
import json
from pathlib import Path, PurePosixPath
import shutil
import stat
import threading
import time
import zipfile


def contained_member(name: str) -> PurePosixPath:
    path = PurePosixPath(name)
    if path.is_absolute() or ".." in path.parts or "\\" in name:
        raise ValueError(f"ZIP路径越界：{name}")
    return path


def extract(archive: Path, output: Path, first: date, last: date, workers: int, progress: float) -> dict:
    days = [(first + timedelta(days=i)).isoformat() for i in range((last - first).days + 1)]
    initial = archive.stat()
    started = time.monotonic()
    report = {"schema": "autodl_training_archive_handoff_v1", "status": "started",
              "archive": str(archive), "archive_bytes": initial.st_size,
              "source_utc_days": days, "output": str(output), "workers": workers,
              "scope": "选定源文件解包、ZIP CRC和原日清单SHA-256/大小核对；非行情质量、DPT、成员分区或正式训练验收"}
    output.mkdir(parents=True, exist_ok=False)
    try:
        with zipfile.ZipFile(archive) as zipped:
            infos = [entry for entry in zipped.infolist()
                     if entry.filename.split("/", 1)[0] in days and not entry.is_dir()]
            members = {}
            for entry in infos:
                path = contained_member(entry.filename)
                if stat.S_ISLNK(entry.external_attr >> 16) or str(path) in members:
                    raise ValueError(f"选定ZIP成员为符号链接或重名：{entry.filename}")
                members[str(path)] = entry
            expected = {}
            manifests = []
            for day in days:
                prefix = f"{day}/typed_raw/utc_day={day}/"
                manifest_name = prefix + "manifest.json"
                manifest = json.loads(zipped.read(members[manifest_name]))
                if manifest["utc_day"] != day:
                    raise ValueError(f"日清单日期与来源路径不同：{day}")
                manifests.append({"utc_day": day, "schema_version": manifest["schema_version"],
                                  "state": manifest["state"], "declared_files": len(manifest["files"]),
                                  "manifest_sha256": hashlib.sha256(zipped.read(members[manifest_name])).hexdigest()})
                for entry in manifest["files"]:
                    relative = contained_member(entry["path"])
                    name = prefix + str(relative)
                    if name in expected or name not in members:
                        raise ValueError(f"日清单文件重复或缺失：{name}")
                    expected[name] = (entry["bytes"], entry["sha256"])
            required = sum(entry.file_size for entry in infos)
            if shutil.disk_usage(output).free < required:
                raise OSError("数据盘可用容量不足以解包选定文件")
            report.update({"selected_files": len(infos), "selected_uncompressed_bytes": required,
                           "manifests": manifests, "manifest_declared_files": len(expected),
                           "zip_only_files": sorted(set(members) - set(expected)
                                                    - {f"{day}/typed_raw/utc_day={day}/manifest.json" for day in days})})
            completed_files = completed_bytes = checked_files = 0
            last_progress = started
            stopped = threading.Event()
            print(f"解包开始：{len(days)}个UTC日，{len(infos)}文件，{required}字节", flush=True)

            def copy(entry):
                if stopped.is_set():
                    raise RuntimeError("因同批文件核对失败停止解包")
                name = str(contained_member(entry.filename))
                target = output.joinpath(*PurePosixPath(name).parts)
                target.parent.mkdir(parents=True, exist_ok=True)
                digest = hashlib.sha256()
                size = 0
                # 新目录与普通文件写入不创建ZIP中的符号链接；ZipExtFile在读完时核对CRC。
                with zipped.open(entry) as source, target.open("xb") as sink:
                    while True:
                        if stopped.is_set():
                            raise RuntimeError("因同批文件核对失败停止解包")
                        block = source.read(8 * 1024 * 1024)
                        if not block:
                            break
                        sink.write(block)
                        digest.update(block)
                        size += len(block)
                if name in expected and (size, digest.hexdigest()) != expected[name]:
                    raise ValueError(f"解包内容与原日清单大小/SHA-256不符：{name}")
                return size, int(name in expected)

            with ThreadPoolExecutor(max_workers=workers) as pool:
                pending = {pool.submit(copy, entry) for entry in infos}
                while pending:
                    ready, pending = wait(pending, timeout=progress, return_when=FIRST_COMPLETED)
                    try:
                        for future in ready:
                            size, checked = future.result()
                            completed_files += 1
                            completed_bytes += size
                            checked_files += checked
                    except BaseException as error:
                        print(f"解包停止：{type(error).__name__}: {error}", flush=True)
                        stopped.set()
                        for future in pending:
                            future.cancel()
                        raise
                    now = time.monotonic()
                    if now - last_progress >= progress or not pending:
                        print(f"解包进度：{completed_files}/{len(infos)}文件，"
                              f"{completed_bytes}/{required}字节，已耗时{now-started:.1f}秒", flush=True)
                        last_progress = now
            final = archive.stat()
            if (final.st_size, final.st_mtime_ns) != (initial.st_size, initial.st_mtime_ns):
                raise RuntimeError("解包期间源ZIP发生变化，不能签发本轮传输核对结果")
            report.update({"status": "transfer_verified", "completed_files": completed_files,
                           "completed_bytes": completed_bytes, "sha256_checked_files": checked_files})
    except BaseException as error:
        report.update({"status": "failed", "error": f"{type(error).__name__}: {error}"})
        raise
    finally:
        report["wall_seconds"] = time.monotonic() - started
        (output / "archive-handoff.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--first-day", type=date.fromisoformat, required=True)
    parser.add_argument("--last-day", type=date.fromisoformat, required=True)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--progress-seconds", type=float, default=10)
    args = parser.parse_args()
    data_root = Path("/root/autodl-tmp")
    archive, output = args.archive.resolve(), args.output_dir.resolve()
    if not archive.is_relative_to(data_root) or not output.is_relative_to(data_root) or output == data_root:
        parser.error("AutoDL输入与新输出目录均须位于/root/autodl-tmp下")
    if args.first_day > args.last_day or args.workers < 1 or not args.progress_seconds > 0:
        parser.error("日期须有序，工作线程数和进度间隔须为正")
    result = extract(archive, output, args.first_day, args.last_day, args.workers, args.progress_seconds)
    print(f"源文件传输核对完成：{result['completed_files']}文件；报告 {output / 'archive-handoff.json'}", flush=True)


if __name__ == "__main__":
    main()
