#!/usr/bin/env python3
"""Create test flows in the local Metaflow backend for benchmarking.

Idempotent: skips flow creation if a flow already has at least the
required number of runs. Use --force to recreate regardless.
"""

import argparse
import subprocess
import sys
import tempfile
import os

# Flow that succeeds with artifacts and stdout/stderr
GOOD_FLOW = '''
from metaflow import FlowSpec, step

class BenchmarkFlow(FlowSpec):
    @step
    def start(self):
        import sys, time
        self.data_size = 1000
        self.config = {"batch_size": 32, "lr": 0.001}
        print("Starting BenchmarkFlow")
        print("Processing config:", self.config)
        print("Warning: using default parameters", file=sys.stderr)
        self.next(self.process)

    @step
    def process(self):
        import sys
        self.results = [i * 2 for i in range(self.data_size)]
        self.accuracy = 0.95
        self.metrics = {"precision": 0.94, "recall": 0.96, "f1": 0.95}
        print(f"Processed {self.data_size} items")
        print(f"Accuracy: {self.accuracy}")
        print("Some debug info on stderr", file=sys.stderr)
        self.next(self.end)

    @step
    def end(self):
        self.final_score = self.accuracy
        self.summary = f"Processed {self.data_size} items with accuracy {self.accuracy}"
        print(f"Done: {self.summary}")

if __name__ == "__main__":
    BenchmarkFlow()
'''

# Flow that fails (for testing failure detection)
FAILING_FLOW = '''
from metaflow import FlowSpec, step

class DiagnoseFlow(FlowSpec):
    @step
    def start(self):
        import sys
        self.input_count = 500
        print("Starting DiagnoseFlow")
        print("Initialization warnings", file=sys.stderr)
        self.next(self.validate)

    @step
    def validate(self):
        import sys
        self.validated = True
        print(f"Validated {self.input_count} inputs")
        print("Validation stderr output", file=sys.stderr)
        self.next(self.train)

    @step
    def train(self):
        import os
        # Fail based on env var
        if os.environ.get("FAIL_RUN") == "1":
            raise ValueError("Training diverged: loss became NaN at epoch 42")
        self.model_weights = [0.1, 0.2, 0.3]
        self.loss = 0.05
        print(f"Training complete, loss={self.loss}")
        self.next(self.end)

    @step
    def end(self):
        self.final_status = "completed"
        print(f"DiagnoseFlow finished with status: {self.final_status}")

if __name__ == "__main__":
    DiagnoseFlow()
'''


def run_flow(flow_code: str, flow_name: str, env: dict | None = None, run_count: int = 1):
    """Write flow to temp file and run it."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
        f.write(flow_code)
        f.flush()
        path = f.name

    # Base environment: ensure USERNAME is set so Metaflow can identify the user.
    base_env = os.environ.copy()
    if not base_env.get("USERNAME"):
        base_env["USERNAME"] = base_env.get("USER") or base_env.get("LOGNAME") or "benchmark"

    for i in range(run_count):
        run_label = f"{flow_name} run {i+1}/{run_count}"
        extra_env = {}
        if env and i < run_count:
            extra_env = env
        current_env = base_env.copy()
        current_env.update(extra_env)

        result = subprocess.run(
            [sys.executable, path, "run"],
            capture_output=True,
            text=True,
            env=current_env,
        )
        if result.returncode == 0:
            print(f"  {run_label}: OK")
        else:
            print(f"  {run_label}: FAILED (expected)" if extra_env.get("FAIL_RUN") else f"  {run_label}: FAILED (unexpected)")
            if result.stderr:
                # Print last 3 lines of stderr for debugging
                lines = result.stderr.strip().splitlines()
                for line in lines[-3:]:
                    print(f"    {line}")

    os.unlink(path)


# StatusTestFlow: for disambiguation benchmarks and hard timing tasks.
#
# State design:  4 successful + 2 exception-killed (finished=False) runs.
# Run order:     4 successes first, then 2 killed — most recent run is unfinished.
#
# Timing design: start (~0.1s) is always faster than end (~1s sleep).
# This guarantees a clear winner for hard_slowest_step without ties.
STATUS_TEST_FLOW = '''
from metaflow import FlowSpec, step

class StatusTestFlow(FlowSpec):
    @step
    def start(self):
        import os, sys
        self.label = "status_test"
        print("StatusTestFlow starting")
        print("Startup diagnostics", file=sys.stderr)
        if os.environ.get("FAIL_RUN") == "1":
            raise RuntimeError("Deliberate failure: run marked failed for disambiguation testing")
        self.next(self.end)

    @step
    def end(self):
        import time
        # Sleep makes this step clearly the slowest (vs ~0.1s for start).
        # Required so hard_slowest_step has an unambiguous answer.
        time.sleep(1)
        self.result = "completed"
        self.success_flag = True
        print("StatusTestFlow finished successfully")

if __name__ == "__main__":
    StatusTestFlow()
'''


def _flow_has_runs(flow_name: str, min_runs: int) -> bool:
    """Return True if the flow already has at least min_runs runs."""
    try:
        from metaflow import Flow, namespace as _ns
        _ns(None)
        flow = Flow(flow_name)
        runs = []
        for r in flow:
            runs.append(r)
            if len(runs) >= min_runs:
                return True
        return False
    except Exception:
        return False


def main():
    parser = argparse.ArgumentParser(description="Create benchmark test flows.")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Recreate flows even if they already have enough runs.",
    )
    args = parser.parse_args()

    print("Setting up test data for benchmarks...")
    if args.force:
        print("  --force: will create runs regardless of existing data")
    print()

    # BenchmarkFlow: 5 successful runs
    if args.force or not _flow_has_runs("BenchmarkFlow", 5):
        print("Creating BenchmarkFlow (5 successful runs)...")
        run_flow(GOOD_FLOW, "BenchmarkFlow", run_count=5)
    else:
        print("BenchmarkFlow: already has enough runs, skipping.")
    print()

    # DiagnoseFlow: 10 runs (3 success + 2 fail + 5 success)
    if args.force or not _flow_has_runs("DiagnoseFlow", 10):
        print("Creating DiagnoseFlow (3 successful + 2 failed + 5 successful runs)...")
        run_flow(FAILING_FLOW, "DiagnoseFlow", run_count=3)
        run_flow(FAILING_FLOW, "DiagnoseFlow", env={"FAIL_RUN": "1"}, run_count=2)
        run_flow(FAILING_FLOW, "DiagnoseFlow", run_count=5)
    else:
        print("DiagnoseFlow: already has enough runs, skipping.")
    print()

    # StatusTestFlow: 4 successful + 2 exception-killed (finished=False)
    # The end step sleeps 1s so hard_slowest_step has a clear winner.
    if args.force or not _flow_has_runs("StatusTestFlow", 6):
        print("Creating StatusTestFlow (4 successful + 2 exception-killed runs)...")
        run_flow(STATUS_TEST_FLOW, "StatusTestFlow", run_count=4)
        run_flow(STATUS_TEST_FLOW, "StatusTestFlow", env={"FAIL_RUN": "1"}, run_count=2)
    else:
        print("StatusTestFlow: already has enough runs, skipping.")
        print("  NOTE: if you need the end-step sleep for hard_slowest_step timing,")
        print("  re-run with --force to create fresh runs with the updated flow code.")
    print()

    # Verify
    print("Verifying data...")
    from metaflow import Flow, namespace as _ns
    _ns(None)
    benchmark_flows = ["BenchmarkFlow", "DiagnoseFlow", "StatusTestFlow"]
    for name in benchmark_flows:
        try:
            flow = Flow(name)
            runs = list(flow)[:15]
            finished = [r for r in runs if r.finished]
            successful = sum(1 for r in finished if r.successful)
            failed = sum(1 for r in finished if not r.successful)
            unfinished = len(runs) - len(finished)
            print(f"  {name}: {len(runs)} runs ({successful} finished-ok, {failed} finished-fail, {unfinished} unfinished)")
        except Exception as e:
            print(f"  {name}: ERROR — {e}")

    print()
    print("Test data setup complete!")


if __name__ == "__main__":
    main()
