"""Check console filtering, retained diagnostics, and failure propagation."""

from pathlib import Path
import os
import subprocess
import sys
import unittest


ROOT = Path(__file__).resolve().parents[1]


class StageRunnerTests(unittest.TestCase):
    def run_stage(self, code, verbose=False):
        env = dict(os.environ, PIPELINE_VERBOSE="1" if verbose else "0")
        return subprocess.run(
            [sys.executable, str(ROOT / "scripts/run_stage.py"), "runner-test",
             sys.executable, "-c", code],
            capture_output=True, text=True, env=env,
        )

    def test_only_application_messages_reach_console(self):
        result = self.run_stage(
            'print("Spark startup noise"); print("[silver] Saved output")'
        )
        self.assertEqual(result.returncode, 0)
        self.assertIn("[silver] Saved output", result.stdout)
        self.assertNotIn("Spark startup noise", result.stdout + result.stderr)
        self.assertIn("Completed in", result.stdout)

    def test_failure_retains_output_and_exit_code(self):
        result = self.run_stage('print("engine diagnostics"); raise ValueError("bad input")')
        self.assertEqual(result.returncode, 1)
        self.assertIn("FAILED", result.stderr)
        self.assertIn("ValueError: bad input", result.stderr)
        log_path = Path(result.stderr.split("Details: ", 1)[1].strip())
        self.assertIn("engine diagnostics", log_path.read_text())
        self.assertIn("Traceback", log_path.read_text())
        self.assertNotIn("engine diagnostics", result.stdout)

    def test_verbose_mode_streams_raw_output(self):
        result = self.run_stage('print("Spark startup noise")', verbose=True)
        self.assertEqual(result.returncode, 0)
        self.assertIn("Spark startup noise", result.stdout)

    def test_missing_command_is_reported(self):
        result = subprocess.run(
            [sys.executable, str(ROOT / "scripts/run_stage.py"), "runner-test",
             "/nonexistent/cps853-command"], capture_output=True, text=True,
        )
        self.assertEqual(result.returncode, 1)
        self.assertIn("FAILED", result.stderr)
        self.assertIn("Details:", result.stderr)


if __name__ == "__main__":
    unittest.main(verbosity=2)
