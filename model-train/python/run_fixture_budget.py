"""工程作业监督；不改变SIM时间、策略动作或服务调度。"""
import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import time


def process_group_rss(group):
    samples = {}
    for path in Path("/proc").iterdir():
        if not path.name.isdecimal():
            continue
        try:
            pid = int(path.name)
            if os.getpgid(pid) != group:
                continue
            fields = {}
            for line in (path / "status").read_text().splitlines():
                key, _, value = line.partition(":")
                fields[key] = value.strip()
            samples[pid] = {"name": fields["Name"],
                            "rss_kib": int(fields.get("VmRSS", "0 kB").split()[0]),
                            "hwm_kib": int(fields.get("VmHWM", "0 kB").split()[0])}
        except (FileNotFoundError, ProcessLookupError, PermissionError):
            continue
    return samples


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--wall-seconds", type=float, required=True)
    parser.add_argument("--evidence", required=True)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if not (0 < args.wall_seconds <= 300):
        raise SystemExit("本轮单功能批/成本档上限为300秒")
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    evidence = Path(args.evidence)
    evidence.mkdir()
    started = time.perf_counter()
    peak = 0
    per_process = {}
    timed_out = False
    with (evidence / "stdout.log").open("w") as output, (evidence / "stderr.log").open("w") as errors:
        process = subprocess.Popen(command, stdout=output, stderr=errors, start_new_session=True)
        while process.poll() is None:
            samples = process_group_rss(process.pid)
            peak = max(peak, sum(s["rss_kib"] for s in samples.values()))
            for pid, sample in samples.items():
                old = per_process.setdefault(str(pid), {"name": sample["name"], "sampled_rss_peak_kib": 0,
                                                        "observed_hwm_peak_kib": 0})
                old["sampled_rss_peak_kib"] = max(old["sampled_rss_peak_kib"], sample["rss_kib"])
                old["observed_hwm_peak_kib"] = max(old["observed_hwm_peak_kib"], sample["hwm_kib"])
            if time.perf_counter() - started >= args.wall_seconds:
                timed_out = True
                os.killpg(process.pid, signal.SIGKILL)
                break
            time.sleep(0.05)
        code = process.wait()
    result = {"command": command, "wall_seconds": time.perf_counter() - started,
              "wall_limit_seconds": args.wall_seconds, "timed_out": timed_out,
              "exit_code": code, "sampled_concurrent_group_rss_peak_kib": peak,
              "processes": per_process, "sample_interval_seconds": 0.05,
              "scope": "父Rust与Python工作进程组；采样峰是观测下界，非硬件容量保证"}
    (evidence / "measurement.json").write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps(result, ensure_ascii=False))
    raise SystemExit(124 if timed_out else code)


if __name__ == "__main__":
    main()
