"""Real process-group cleanup after foreground supervisor interruption."""
import json
import os
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

ROOT=Path(__file__).resolve().parents[2]


class SupervisionTests(unittest.TestCase):
    def test_sigterm_reaps_stage_and_records_actual_interruption(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            wrapper=root/'run_source_health_columnar_once.py'
            wrapper.write_bytes((ROOT/'baseline-mm/research/run_source_health_columnar_once.py').read_bytes())
            # The stand-in only reports its native PID and waits. It neither
            # reads source data nor performs a health prepare/study.
            (root/'research_source_health_columnar.py').write_text("import os,time\nprint(os.getpid(),flush=True)\ntime.sleep(30)\n")
            config=root/'config.json';config.write_text(json.dumps(dict(run_root=str(root/'new-run'))))
            logs=root/'logs'
            mount=json.loads(subprocess.check_output(['findmnt','-T',str(root),'-J','-o','TARGET'],text=True))['filesystems'][0]['target']
            process=subprocess.Popen([sys.executable,str(wrapper),str(config),str(root),mount,str(logs)],stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
            child_pid=None
            try:
                deadline=time.monotonic()+5
                while time.monotonic()<deadline:
                    output=logs/'prepare.stdout'
                    if output.exists() and output.read_text().strip():
                        child_pid=int(output.read_text());break
                    time.sleep(0.01)
                self.assertIsNotNone(child_pid,'stand-in stage did not become ready')
                process.send_signal(signal.SIGTERM)
                stdout,stderr=process.communicate(timeout=5)
                self.assertEqual(process.returncode,1,stderr)
                result=json.loads((logs/'prepare.exit.json').read_text())
                self.assertEqual(result['supervisor_signal'],signal.SIGTERM)
                self.assertEqual(result['supervisor_interruption'],'InterruptedError')
                self.assertTrue(result['terminated_by_signal'])
                self.assertEqual(result['returncode'],-signal.SIGTERM)
                self.assertFalse((logs/'study.command.json').exists())
                # An adopted child may briefly remain as a zombie. It cannot
                # execute or write; PID1 owns its final reap.
                status=Path(f'/proc/{child_pid}/status')
                if status.exists():
                    self.assertIn('Z (zombie)',status.read_text())
                self.assertEqual(json.loads(stdout),result)
            finally:
                if process.poll() is None:
                    process.send_signal(signal.SIGTERM);process.wait(timeout=5)
                if child_pid is not None:
                    try:os.kill(child_pid,signal.SIGTERM)
                    except ProcessLookupError:pass


if __name__=='__main__':
    unittest.main()
