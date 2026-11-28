#!/usr/bin/env python3
"""Run each admin-console concurrency fuzzer in its own process and save logs."""

import argparse
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time


TARGETS = [
    "FuzzAdminBackendConnections",
    "FuzzAdminClientQuantiles",
    "FuzzAdminPreparedStatements",
    "FuzzAdminClientRouting",
]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--go", default="go", help="Go executable")
    parser.add_argument("--seconds", type=int, default=30, help="Fuzzing seconds per target")
    parser.add_argument("--parallel", type=int, default=2, help="Fuzz workers per target")
    parser.add_argument("--output", type=Path, help="Artifact directory (default: a new /tmp directory)")
    parser.add_argument("--targets", nargs="+", choices=TARGETS, default=TARGETS)
    args = parser.parse_args()
    if not 1 <= args.seconds <= 600 or not 1 <= args.parallel <= 8:
        parser.error("seconds must be 1..600 and parallel must be 1..8")

    root = Path(__file__).resolve().parents[3]
    output = args.output or Path(tempfile.mkdtemp(prefix="spqr-adminconsole-fuzz-"))
    output = output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    env = dict(os.environ)
    env.setdefault("GOFLAGS", "-mod=mod")
    env.setdefault("GOMAXPROCS", "4")
    env["GORACE"] = "halt_on_error=1"
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
    dirty = subprocess.check_output(["git", "status", "--porcelain"], cwd=root, text=True)
    report = {"revision": revision, "worktree_status": dirty, "runs": []}
    print(f"Artifacts: {output}", flush=True)

    for target in args.targets:
        command = [
            args.go, "test", "-race", "-tags=adminconsolestress", "-run=^$",
            f"-fuzz=^{target}$", f"-fuzztime={args.seconds}s", f"-parallel={args.parallel}",
            f"-timeout={args.seconds + 120}s", "./test/stress/adminconsole",
        ]
        log_path = output / f"{target}.log"
        started = time.monotonic()
        with log_path.open("w") as log:
            try:
                result = subprocess.run(command, cwd=root, env=env, stdout=log,
                                        stderr=subprocess.STDOUT, timeout=args.seconds + 180)
                returncode = result.returncode
            except subprocess.TimeoutExpired:
                returncode = None
        contents = log_path.read_text(errors="replace")
        if returncode is None:
            status = "timeout"
        elif "WARNING: DATA RACE" in contents or "race detected during execution" in contents:
            status = "race"
        elif "fatal error:" in contents or "panic:" in contents:
            status = "panic"
        elif returncode == 0:
            status = "ok"
        else:
            status = "failed"
        run = {"target": target, "status": status, "returncode": returncode,
               "elapsed_seconds": round(time.monotonic() - started, 2),
               "command": command, "log": log_path.name}
        report["runs"].append(run)
        (output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
        print(f"{target}: {status} ({run['elapsed_seconds']}s), {log_path}", flush=True)
    return int(any(run["status"] != "ok" for run in report["runs"]))


if __name__ == "__main__":
    raise SystemExit(main())
