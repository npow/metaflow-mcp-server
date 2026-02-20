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
import traceback
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


# ── Configuration ─────────────────────────────────────────────


@mcp.tool()
@_handle_errors
def get_config() -> str:
    """Show current Metaflow configuration.

    Returns the active metadata provider, datastore, namespace, and profile.
    Use this first to understand what backend you're connected to.
    """
    import os

    from metaflow.client import get_metadata
    from metaflow.metaflow_config import (
        DEFAULT_DATASTORE,
        DEFAULT_ENVIRONMENT,
        DEFAULT_METADATA,
    )

    return _json(
        {
            "metadata_provider": get_metadata(),
            "namespace": "global (None -- all runs visible)",
            "default_datastore": DEFAULT_DATASTORE,
            "default_metadata": DEFAULT_METADATA,
            "default_environment": DEFAULT_ENVIRONMENT,
            "profile": os.environ.get("METAFLOW_PROFILE", "(not set)"),
        }
    )


# ── Run Discovery ─────────────────────────────────────────────


@mcp.tool()
@_handle_errors
def search_runs(flow_name: str, last_n: int = 5) -> str:
    """Find recent runs of a flow.

    Args:
        flow_name: Name of the flow class (e.g. "MyFlow").
        last_n: Max number of runs to return (default 5).
    """
    from metaflow import Flow

    flow = Flow(flow_name)
    runs = []
    for run in flow:
        if len(runs) >= last_n:
            break
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
                    "finished_at": str(task.finished_at) if task.finished_at else None,
                }
            )
        steps.append({"step": step.id, "tasks": tasks})

    return _json(
        {
            "pathspec": run.pathspec,
            "successful": run.successful,
            "finished": run.finished,
            "finished_at": str(run.finished_at) if run.finished_at else None,
            "tags": sorted(run.user_tags),
            "steps": steps,
        }
    )


# ── Task Inspection ───────────────────────────────────────────


@mcp.tool()
@_handle_errors
def get_task_logs(pathspec: str, stdout: bool = True, stderr: bool = True) -> str:
    """Get stdout/stderr logs for a specific task.

    Args:
        pathspec: Task pathspec like "FlowName/RunID/StepName/TaskID".
        stdout: Include stdout (default true).
        stderr: Include stderr (default true).
    """
    from metaflow import Task

    task = Task(pathspec)
    result = {"pathspec": pathspec}
    if stdout:
        result["stdout"] = task.stdout
    if stderr:
        result["stderr"] = task.stderr
    return _json(result)


@mcp.tool()
@_handle_errors
def list_artifacts(pathspec: str) -> str:
    """List all artifacts produced by a task (or the first task of a step).

    Args:
        pathspec: Task pathspec like "FlowName/RunID/StepName/TaskID",
                  or step pathspec like "FlowName/RunID/StepName" (uses first task).
    """
    from metaflow import Task, Step

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
                "type": type(art.data).__name__,
                "size": repr(art.data)[:80],
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
def get_latest_failure(flow_name: str) -> str:
    """Find the most recent failed run and return its error details.

    Finds the failed run, identifies the failing step/task, and returns
    the exception and stderr in one call. Scans the most recent 20 runs.

    Args:
        flow_name: Name of the flow.
    """
    from metaflow import Flow

    flow = Flow(flow_name)
    scanned = 0
    for run in flow:
        scanned += 1
        if scanned > 20:
            break
        if run.finished and not run.successful:
            for step in run:
                for task in step:
                    if task.finished and not task.successful:
                        return _json(
                            {
                                "run": run.pathspec,
                                "failing_step": step.id,
                                "failing_task": task.pathspec,
                                "exception": repr(task.exception)
                                if task.exception
                                else None,
                                "stderr_tail": (task.stderr or "")[-2000:],
                            }
                        )
            return _json(
                {
                    "run": run.pathspec,
                    "failing_step": None,
                    "failing_task": None,
                    "note": "Run failed but could not identify failing task",
                }
            )

    return _json(
        {"message": "No failed runs found in the last %d runs of %s" % (scanned, flow_name)}
    )


def main():
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
