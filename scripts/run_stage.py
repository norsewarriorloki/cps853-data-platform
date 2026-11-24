"""Run a pipeline command with concise output and retained diagnostics."""

from datetime import datetime
import os
from pathlib import Path
import re
import subprocess
import sys
import time
from uuid import uuid4


APPLICATION_LINE = re.compile(
    r"^\[(silver|gold|dimensions|producer|consumer|quarantine|rolling|validation)\]"
)


def main():
    stage, *command = sys.argv[1:]
    if not command:
        raise SystemExit("Usage: run_stage.py <stage> <command> [arguments...]")
    log_dir = Path(__file__).resolve().parents[1] / ".logs"
    log_dir.mkdir(exist_ok=True)
    safe_stage = re.sub(r"[^a-zA-Z0-9_-]", "_", stage)
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    log_path = log_dir / f"{stamp}-{safe_stage}-{uuid4().hex[:8]}.log"
    verbose = os.getenv("PIPELINE_VERBOSE") == "1"
    started = time.monotonic()
    print(f"\n[{stage}] Starting", flush=True)
    error_hint = None
    with log_path.open("w", buffering=65536) as log:
        try:
            with subprocess.Popen(
                command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, encoding="utf-8", errors="replace",
            ) as process:
                for line in process.stdout:
                    log.write(line)
                    if verbose or APPLICATION_LINE.match(line):
                        print(line, end="", flush=True)
                    if re.match(r"^[\w.]+(?:Error|Exception):", line):
                        error_hint = line.strip()
                code = process.wait()
        except OSError as error:
            log.write(str(error) + "\n")
            error_hint = str(error)
            code = 1
    elapsed = time.monotonic() - started
    if code:
        print(f"[{stage}] FAILED after {elapsed:.1f}s (exit {code})", file=sys.stderr)
        if error_hint:
            print(f"[{stage}] {error_hint}", file=sys.stderr)
        print(f"[{stage}] Details: {log_path}", file=sys.stderr)
    else:
        print(f"[{stage}] Completed in {elapsed:.1f}s", flush=True)
    return code if code >= 0 else 128 - code


if __name__ == "__main__":
    sys.exit(main())
