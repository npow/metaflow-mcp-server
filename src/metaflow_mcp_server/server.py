"""
Metaflow MCP Server.

Exposes Metaflow run inspection as MCP tools. Works with any configured
backend (local, service, spin, mli) -- uses the same client API and config
system as the `metaflow` CLI.

Configuration is inherited from the Python environment:
  - If nflx-metaflow is installed, Netflix defaults (mli, s3) are used.
  - If only metaflow is installed, OSS defaults (local) are used.
  - Override via ~/.metaflowconfig/config.json or METAFLOW_* env vars.

Usage:
  metaflow-mcp-server
  python -m metaflow_mcp_server.server
"""

import json
import re
import traceback
from datetime import datetime, timezone
from functools import wraps

from mcp.server import FastMCP

mcp = FastMCP("metaflow")

# Set namespace to None so we can see all runs (not just the current user's).
# This is important for querying production runs triggered by schedulers
# like Maestro, Argo, or Step Functions, which run in different namespaces.
from metaflow import namespace

namespace(None)


# ── Helpers ───────────────────────────────────────────────────


def _json(obj):
    return json.dumps(obj, indent=2, default=str)


def _handle_errors(fn):
    """Catch exceptions and return structured error JSON instead of crashing."""

    @wraps(fn)
    def wrapper(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            return _json(
                {
                    "error": type(e).__name__,
                    "message": str(e),
                    "traceback": traceback.format_exc()[-1000:],
                }
            )

    return wrapper


def _parse_dt(s):
    """Parse an ISO datetime string, assuming UTC if no timezone is given."""
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _ensure_tz(dt):
    """Ensure a datetime has timezone info (assume UTC if missing)."""
    if dt is not None and dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _duration(start, end):
    """Compute duration in seconds between two datetimes, or None."""
    if start and end:
        s = _ensure_tz(start)
        e = _ensure_tz(end)
        return round((e - s).total_seconds(), 1)
    return None


def _filter_log(text, head=None, tail=None, pattern=None):
    """Apply head/tail/pattern filters to log text."""
    if not text:
        return text
    lines = text.splitlines(keepends=True)
    if pattern:
        lines = [l for l in lines if re.search(pattern, l)]
    if tail is not None:
        lines = lines[-tail:]
    elif head is not None:
        lines = lines[:head]
    return "".join(lines)


# ── Configuration ─────────────────────────────────────────────


def _get_username() -> str | None:
    """Return the current Metaflow username, or None if not determinable."""
    import os
    import getpass

    # Metaflow sets METAFLOW_USER, otherwise fall back to OS user
    return os.environ.get("METAFLOW_USER") or getpass.getuser() or None


@mcp.tool()
@_handle_errors
def get_config() -> str:
    """Show current Metaflow configuration.

    Returns the active metadata provider, datastore, namespace, and profile.
    Also returns the user's default namespace (e.g. "user:npow") -- pass this
    as the namespace parameter to list_flows/search_runs/get_latest_failure to
    scope results to only your own runs.
    Use this first to understand what backend you're connected to.
    """
    import os

    from metaflow.client import get_metadata
    from metaflow.metaflow_config import (
        DEFAULT_DATASTORE,
        DEFAULT_ENVIRONMENT,
        DEFAULT_METADATA,
    )

    username = _get_username()
    return _json(
        {
            "metadata_provider": get_metadata(),
            "active_namespace": "global (None -- all runs visible)",
            "default_namespace": f"user:{username}" if username else None,
            "default_datastore": DEFAULT_DATASTORE,
            "default_metadata": DEFAULT_METADATA,
            "default_environment": DEFAULT_ENVIRONMENT,
            "profile": os.environ.get("METAFLOW_PROFILE", "(not set)"),
        }
    )


# ── Flow Discovery ───────────────────────────────────────────


@mcp.tool()
@_handle_errors
def list_flows(last_n: int = 50, namespace: str | None = None) -> str:
    """List available Metaflow flows.

    Returns flow names visible in the given namespace.
    Use this to discover flows before searching for runs.

    Args:
        last_n: Max number of flows to return (default 50).
        namespace: Metaflow namespace to scope results (e.g. "user:npow").
                   Use get_config to find your default_namespace.
                   If omitted, returns all flows visible globally.
    """
    import metaflow as mf

    if namespace:
        mf.namespace(namespace)
    try:
        flows = []
        for flow in mf.Metaflow():
            if len(flows) >= last_n:
                break
            flows.append(flow.id)
        return _json({"flows": flows, "count": len(flows), "namespace": namespace or "global"})
    finally:
        if namespace:
            mf.namespace(None)  # restore global namespace


# ── Run Discovery ─────────────────────────────────────────────


@mcp.tool()
@_handle_errors
def search_runs(
    flow_name: str,
    last_n: int = 5,
    status: str | None = None,
    created_after: str | None = None,
    created_before: str | None = None,
    tags: list[str] | None = None,
    namespace: str | None = None,
) -> str:
    """Find recent runs of a flow with optional filters.

    Args:
        flow_name: Name of the flow class (e.g. "MyFlow").
        last_n: Max number of matching runs to return (default 5).
        status: Filter by status: "successful", "failed", or "running".
        created_after: ISO datetime -- only runs created after this time (e.g. "2024-01-15" or "2024-01-15T10:30:00").
        created_before: ISO datetime -- only runs created before this time.
        tags: Only include runs that have all of these user tags.
        namespace: Metaflow namespace to scope results (e.g. "user:npow").
                   Use get_config to find your default_namespace.
    """
    import metaflow as mf

    if namespace:
        mf.namespace(namespace)
    try:
        flow = mf.Flow(flow_name)
    finally:
        if namespace:
            mf.namespace(None)

    after_dt = _parse_dt(created_after) if created_after else None
    before_dt = _parse_dt(created_before) if created_before else None

    runs = []
    scanned = 0
    MAX_SCAN = 200

    for run in flow:
        scanned += 1
        if scanned > MAX_SCAN:
            break

        created = _ensure_tz(run.created_at)

        # Runs are reverse-chronological: stop once past the time window.
        if after_dt and created < after_dt:
            break

        if before_dt and created > before_dt:
            continue

        if status:
            if status == "successful" and not run.successful:
                continue
            elif status == "failed" and not (run.finished and not run.successful):
                continue
            elif status == "running" and run.finished:
                continue

        if tags:
            user_tags = run.user_tags
            if not all(t in user_tags for t in tags):
                continue

        runs.append(
            {
                "pathspec": run.pathspec,
                "id": run.id,
                "successful": run.successful,
                "finished": run.finished,
                "finished_at": str(run.finished_at) if run.finished_at else None,
                "created_at": str(run.created_at),
                "tags": sorted(run.user_tags),
            }
        )

        if len(runs) >= last_n:
            break

    return _json({"flow": flow_name, "count": len(runs), "runs": runs})


@mcp.tool()
@_handle_errors
def get_run(pathspec: str) -> str:
    """Get detailed status of a run including per-step breakdown.

    Args:
        pathspec: Run pathspec like "FlowName/RunID".
    """
    from metaflow import Run

    run = Run(pathspec)
    steps = []
    for step in run:
        tasks = []
        for task in step:
            tasks.append(
                {
                    "id": task.id,
                    "successful": task.successful,
                    "finished": task.finished,
                    "created_at": str(task.created_at),
                    "finished_at": str(task.finished_at) if task.finished_at else None,
                    "duration_seconds": _duration(task.created_at, task.finished_at),
                }
            )
        steps.append(
            {"step": step.id, "created_at": str(step.created_at), "tasks": tasks}
        )

    return _json(
        {
            "pathspec": run.pathspec,
            "successful": run.successful,
            "finished": run.finished,
            "created_at": str(run.created_at),
            "finished_at": str(run.finished_at) if run.finished_at else None,
            "duration_seconds": _duration(run.created_at, run.finished_at),
            "tags": sorted(run.user_tags),
            "steps": steps,
        }
    )


# ── Task Inspection ───────────────────────────────────────────


@mcp.tool()
@_handle_errors
def get_task_logs(
    pathspec: str,
    stdout: bool = True,
    stderr: bool = True,
    tail: int | None = None,
    head: int | None = None,
    pattern: str | None = None,
) -> str:
    """Get stdout/stderr logs for a specific task.

    Args:
        pathspec: Task pathspec like "FlowName/RunID/StepName/TaskID".
        stdout: Include stdout (default true).
        stderr: Include stderr (default true).
        tail: Return only the last N lines of each log.
        head: Return only the first N lines of each log (ignored if tail is set).
        pattern: Regex pattern -- return only lines matching this pattern.
    """
    from metaflow import Task

    task = Task(pathspec)
    result = {"pathspec": pathspec}
    if stdout:
        result["stdout"] = _filter_log(
            task.stdout, head=head, tail=tail, pattern=pattern
        )
    if stderr:
        result["stderr"] = _filter_log(
            task.stderr, head=head, tail=tail, pattern=pattern
        )
    return _json(result)


@mcp.tool()
@_handle_errors
def list_artifacts(pathspec: str) -> str:
    """List all artifacts produced by a task (or the first task of a step).

    Returns artifact names and metadata without loading data.
    Use get_artifact to retrieve actual values.

    Args:
        pathspec: Task pathspec like "FlowName/RunID/StepName/TaskID",
                  or step pathspec like "FlowName/RunID/StepName" (uses first task).
    """
    from metaflow import Step, Task

    parts = pathspec.split("/")
    if len(parts) == 3:
        step = Step(pathspec)
        task = next(iter(step))
    else:
        task = Task(pathspec)

    artifacts = []
    for art in task:
        artifacts.append(
            {
                "name": art.id,
                "sha": art.sha,
                "created_at": str(art.created_at),
            }
        )
    return _json({"pathspec": task.pathspec, "artifacts": artifacts})


@mcp.tool()
@_handle_errors
def get_artifact(pathspec: str, name: str) -> str:
    """Get the value of a data artifact from a task.

    Args:
        pathspec: Task pathspec like "FlowName/RunID/StepName/TaskID".
        name: Artifact name (e.g. "model", "result").
    """
    from metaflow import Task

    task = Task(pathspec)
    artifact = task[name]
    value = artifact.data
    return _json(
        {
            "pathspec": pathspec,
            "name": name,
            "type": type(value).__name__,
            "value": repr(value),
        }
    )


# ── Compound Operations ──────────────────────────────────────


@mcp.tool()
@_handle_errors
def get_latest_failure(flow_name: str, last_n_runs: int = 20, namespace: str | None = None) -> str:
    """Find failed runs and return error details.

    Scans recent runs, finds all failures, and returns the failing
    step/task with exception and stderr for each.

    Args:
        flow_name: Name of the flow.
        last_n_runs: How many recent runs to scan (default 20).
        namespace: Metaflow namespace to scope results (e.g. "user:npow").
                   Use get_config to find your default_namespace.
    """
    import metaflow as mf

    if namespace:
        mf.namespace(namespace)
    try:
        flow = mf.Flow(flow_name)
    finally:
        if namespace:
            mf.namespace(None)

    failures = []
    scanned = 0
    for run in flow:
        scanned += 1
        if scanned > last_n_runs:
            break
        if not (run.finished and not run.successful):
            continue

        failure = {
            "run": run.pathspec,
            "created_at": str(run.created_at),
            "failing_step": None,
            "failing_task": None,
            "exception": None,
            "stderr_tail": None,
        }

        for step in run:
            for task in step:
                if task.finished and not task.successful:
                    failure["failing_step"] = step.id
                    failure["failing_task"] = task.pathspec
                    failure["exception"] = (
                        repr(task.exception) if task.exception else None
                    )
                    failure["stderr_tail"] = (task.stderr or "")[-2000:]
                    break
            if failure["failing_task"]:
                break

        if not failure["failing_task"]:
            failure["note"] = "Run failed but could not identify failing task"
        failures.append(failure)

    return _json(
        {
            "flow": flow_name,
            "runs_scanned": scanned,
            "failures_found": len(failures),
            "failures": failures,
        }
    )


@mcp.tool()
@_handle_errors
def search_artifacts(
    flow_name: str,
    artifact_name: str,
    last_n_runs: int = 5,
    step_name: str | None = None,
) -> str:
    """Search for a named artifact across recent runs of a flow.

    Scans recent runs to find which tasks produced an artifact with the
    given name. Does not load artifact data. Use get_artifact to retrieve values.

    Note: for runs with many parallel tasks this may be slow. Use step_name
    to narrow the search.

    Args:
        flow_name: Name of the flow class.
        artifact_name: Name of the artifact to search for (e.g. "model", "accuracy").
        last_n_runs: Number of recent runs to scan (default 5).
        step_name: Only search within this step (e.g. "train"). Recommended for large flows.
    """
    from metaflow import Flow

    flow = Flow(flow_name)
    results = []
    scanned = 0
    for run in flow:
        scanned += 1
        if scanned > last_n_runs:
            break
        for step in run:
            if step_name and step.id != step_name:
                continue
            for task in step:
                for art in task:
                    if art.id == artifact_name:
                        results.append(
                            {
                                "task": task.pathspec,
                                "step": step.id,
                                "run": run.pathspec,
                                "created_at": str(art.created_at),
                                "sha": art.sha,
                            }
                        )
                        break  # Found in this task, move to next

    return _json(
        {
            "flow": flow_name,
            "artifact_name": artifact_name,
            "runs_scanned": scanned,
            "matches_found": len(results),
            "matches": results,
        }
    )


@mcp.tool()
@_handle_errors
def get_recent_runs(
    namespace: str,
    last_n_flows: int = 20,
    last_n_runs_per_flow: int = 3,
    status: str | None = None,
) -> str:
    """Find the most recent runs across all flows in a namespace.

    Use this when the user asks about "my last run" or "my recent runs" without
    specifying a flow name. Scans all flows in the namespace and returns runs
    sorted by creation time (newest first).

    Args:
        namespace: Metaflow namespace to scope results (e.g. "user:npow").
                   Use get_config to find your default_namespace.
        last_n_flows: How many flows to scan (default 20).
        last_n_runs_per_flow: How many recent runs to check per flow (default 3).
        status: Filter by status: "successful", "failed", or "running".
    """
    import metaflow as mf

    mf.namespace(namespace)
    try:
        flows = list(mf.Metaflow())[:last_n_flows]
        all_runs = []
        for flow in flows:
            count = 0
            for run in flow:
                if count >= last_n_runs_per_flow:
                    break
                count += 1
                if status:
                    if status == "successful" and not run.successful:
                        continue
                    elif status == "failed" and not (run.finished and not run.successful):
                        continue
                    elif status == "running" and run.finished:
                        continue
                all_runs.append(
                    {
                        "pathspec": run.pathspec,
                        "flow": flow.id,
                        "successful": run.successful,
                        "finished": run.finished,
                        "created_at": run.created_at,
                        "finished_at": run.finished_at,
                        "duration_seconds": _duration(run.created_at, run.finished_at),
                        "tags": sorted(run.user_tags),
                    }
                )
    finally:
        mf.namespace(None)

    all_runs.sort(key=lambda r: _ensure_tz(r["created_at"]) if r["created_at"] else datetime.min.replace(tzinfo=timezone.utc), reverse=True)
    for r in all_runs:
        r["created_at"] = str(r["created_at"])
        r["finished_at"] = str(r["finished_at"]) if r["finished_at"] else None

    return _json(
        {
            "namespace": namespace,
            "flows_scanned": len(flows),
            "runs_found": len(all_runs),
            "runs": all_runs,
        }
    )


def main():
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
