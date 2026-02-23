"""Discover real flows with enough runs/failures for benchmark test data."""

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class TestContext:
    """Context discovered from the live Metaflow backend for parameterizing tasks."""

    flow_name: str = ""
    run_pathspec: str = ""
    task_pathspec: str = ""
    step_name: str = ""
    artifact_name: str = ""
    failed_flow_name: str = ""


def discover_flows(min_runs: int = 3, max_flows: int = 5) -> list[dict]:
    """Scan Metaflow() for flows with sufficient data for benchmarking.

    Returns list of dicts with keys: name, num_runs, has_failure, run_ids.
    """
    from metaflow import Metaflow, Flow

    results = []
    for flow_obj in Metaflow():
        flow_name = flow_obj.id
        try:
            flow = Flow(flow_name)
        except Exception:
            continue

        runs = []
        has_failure = False
        for run in flow:
            runs.append(run)
            if run.finished and not run.successful:
                has_failure = True
            if len(runs) >= 20:
                break

        if len(runs) >= min_runs:
            results.append({
                "name": flow_name,
                "num_runs": len(runs),
                "has_failure": has_failure,
                "run_ids": [r.id for r in runs[:10]],
            })
        if len(results) >= max_flows:
            break

    return results


def build_test_context(flows: list[dict]) -> TestContext:
    """Pick the best flow(s) and probe for step names, artifacts, failures.

    Returns a TestContext with all fields populated from live data.
    """
    from metaflow import Flow, Run, Step

    ctx = TestContext()
    if not flows:
        return ctx

    # Pick the flow with the most runs as primary
    primary = max(flows, key=lambda f: f["num_runs"])
    ctx.flow_name = primary["name"]

    # Find a failed flow (may be the same)
    for f in flows:
        if f["has_failure"]:
            ctx.failed_flow_name = f["name"]
            break
    if not ctx.failed_flow_name:
        ctx.failed_flow_name = ctx.flow_name  # fallback

    # Probe the most recent run for steps, tasks, artifacts
    flow = Flow(ctx.flow_name)
    for run in flow:
        if not run.finished:
            continue
        ctx.run_pathspec = run.pathspec

        for step in run:
            ctx.step_name = step.id
            for task in step:
                ctx.task_pathspec = task.pathspec
                # Find an artifact
                for art in task:
                    if not art.id.startswith("_"):
                        ctx.artifact_name = art.id
                        break
                if ctx.artifact_name:
                    break
            if ctx.artifact_name:
                break
        break

    return ctx
