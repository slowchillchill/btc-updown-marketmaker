"""专项工程监督的stdlib子进程反例；不导入Torch，不执行模型。"""
import json
from pathlib import Path
import runpy
import signal
import subprocess
import sys
import tempfile

source = Path(sys.argv[1]).resolve()
command = runpy.run_path(str(source))["command"]
with tempfile.TemporaryDirectory() as temporary:
    root = Path(temporary)
    command(root, "complete", [sys.executable, "-c", "print('cpu-only')"])
    assert json.loads((root / "complete.started.json").read_text())["status"] == "started"
    assert json.loads((root / "complete.command.json").read_text())["native_exit_code"] == 0
    script = "import pathlib,runpy,sys;f=runpy.run_path(sys.argv[1])['command'];f(pathlib.Path(sys.argv[2]),'limited',[sys.executable,'-c','import time;time.sleep(5)'],wall_limit_seconds=.05)"
    process = subprocess.Popen([sys.executable, "-c", script, str(source), str(root)], start_new_session=True,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    output, errors = process.communicate(timeout=10)
    assert process.returncode == -signal.SIGKILL, (output, errors)
    started = json.loads((root / "limited.started.json").read_text())
    unfinished = json.loads((root / "limited.command.json").read_text())
    assert started["package_pgid"] == process.pid
    assert started["stage_wall_limit_seconds"] == .05
    assert unfinished["status"] == "unfinished" and unfinished["native_exit_code"] is None
    assert unfinished["processes_before_stop"]
assert "torch" not in sys.modules
print("阶段启动留证、到限unfinished及准确独立PG强停已核；零Torch/模型")
