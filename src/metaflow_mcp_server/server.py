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
        return round((e - s).total_seconds(), 2)
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


def _extract_text_from_html(html: str) -> str:
    """Extract visible text content from HTML, stripping tags and scripts."""
    from html.parser import HTMLParser

    class _TextExtractor(HTMLParser):
        def __init__(self):
            super().__init__()
            self._pieces: list[str] = []
            self._skip = False

        def handle_starttag(self, tag, attrs):
            if tag in ("script", "style"):
                self._skip = True

        def handle_endtag(self, tag):
            if tag in ("script", "style"):
                self._skip = False

        def handle_data(self, data):
            if not self._skip:
                text = data.strip()
                if text:
                    self._pieces.append(text)

    extractor = _TextExtractor()
    extractor.feed(html)
    return "\n".join(extractor._pieces)


def _resolve_tasks_for_cards(pathspec: str):
    """Resolve a run/step/task pathspec to a list of (Task, label) pairs for card lookup."""
    from metaflow import Run, Step, Task

    parts = pathspec.split("/")
    if len(parts) == 4:
        task = Task(pathspec)
        return [(task, task.pathspec)]
    elif len(parts) == 3:
        step = Step(pathspec)
        task = next(iter(step))
        return [(task, task.pathspec)]
    elif len(parts) == 2:
        run = Run(pathspec)
        tasks = []
        for step in run:
            for task in step:
                tasks.append((task, task.pathspec))
                break  # first task per step only
        return tasks
    else:
        raise ValueError(
            f"Invalid pathspec '{pathspec}': expected FlowName/RunID, "
            "FlowName/RunID/StepName, or FlowName/RunID/StepName/TaskID"
        )


def _build_comparison_html(entries: list[dict]) -> str:
    """Build a side-by-side HTML comparison page from card entries."""
    import html as html_module

    n = len(entries)
    iframe_blocks = []
    for entry in entries:
        escaped = html_module.escape(entry["html"], quote=True)
        label = html_module.escape(entry["task"])
        card_type = html_module.escape(entry.get("card_type") or "unknown")
        iframe_blocks.append(
            f'<div class="card-col">'
            f'<div class="card-label">{label} ({card_type})</div>'
            f'<iframe srcdoc="{escaped}" sandbox="allow-scripts allow-same-origin"></iframe>'
            f"</div>"
        )

    return (
        "<!DOCTYPE html>\n<html>\n<head>\n"
        '<meta charset="utf-8">\n'
        "<title>Metaflow Card Comparison</title>\n"
        "<style>\n"
        "* { margin: 0; padding: 0; box-sizing: border-box; }\n"
        "body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #f5f5f5; }\n"
        "h1 { padding: 16px 24px; background: #1a1a2e; color: white; font-size: 18px; }\n"
        ".container { display: flex; gap: 8px; padding: 8px; height: calc(100vh - 56px); }\n"
        ".card-col { flex: 1; min-width: 400px; display: flex; flex-direction: column; }\n"
        ".card-label { background: #2d2d44; color: white; padding: 8px 12px; font-size: 13px;\n"
        "             border-radius: 6px 6px 0 0; font-family: monospace; }\n"
        "iframe { flex: 1; border: 1px solid #ddd; border-top: none; border-radius: 0 0 6px 6px;\n"
        "         background: white; width: 100%; }\n"
        "</style>\n</head>\n<body>\n"
        f"<h1>Metaflow Card Comparison ({n} cards)</h1>\n"
        '<div class="container">\n'
        + "\n".join(iframe_blocks)
        + "\n</div>\n</body>\n</html>"
    )


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

    Returns artifact names, data types, and metadata.
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
        try:
            art_type = type(art.data).__name__
        except Exception:
            art_type = "unknown"
        artifacts.append(
            {
                "name": art.id,
                "type": art_type,
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


# ── Card Inspection ───────────────────────────────────────────


@mcp.tool()
@_handle_errors
def list_cards(
    pathspec: str,
    card_type: str | None = None,
    card_id: str | None = None,
) -> str:
    """List cards attached to a run, step, or task.

    Cards are visual reports (HTML) produced by Metaflow steps, often
    containing plots, tables, and metrics. Use this to discover available
    cards before retrieving them with get_card.

    For a run pathspec, scans all steps (first task per step).
    For a step pathspec, uses the first task.
    For a task pathspec, uses that exact task.

    Args:
        pathspec: Run ("FlowName/RunID"), step ("FlowName/RunID/StepName"),
                  or task ("FlowName/RunID/StepName/TaskID") pathspec.
        card_type: Only list cards of this type (e.g. "default").
        card_id: Only list cards with this ID.
    """
    from metaflow.cards import get_cards

    tasks = _resolve_tasks_for_cards(pathspec)
    all_cards = []
    for task, label in tasks:
        cards = get_cards(task, id=card_id, type=card_type)
        for card in cards:
            all_cards.append(
                {
                    "task": label,
                    "type": card.type,
                    "id": card.id,
                    "hash": card.hash,
                }
            )

    return _json(
        {
            "pathspec": pathspec,
            "card_count": len(all_cards),
            "cards": all_cards,
        }
    )


@mcp.tool()
@_handle_errors
def get_card(
    pathspec: str,
    card_index: int = 0,
    card_type: str | None = None,
    card_id: str | None = None,
) -> str:
    """Get a Metaflow card's content and save it as a viewable HTML file.

    Retrieves the card HTML from the datastore, saves it to a temp file
    you can open in your browser, and returns extracted text content for
    analysis.

    Use list_cards first to discover available cards.

    Args:
        pathspec: Step ("FlowName/RunID/StepName") or task
                  ("FlowName/RunID/StepName/TaskID") pathspec.
        card_index: Which card to retrieve if multiple exist (default 0).
        card_type: Filter cards by type before selecting by index.
        card_id: Filter cards by ID before selecting by index.
    """
    import tempfile

    from metaflow.cards import get_cards

    tasks = _resolve_tasks_for_cards(pathspec)
    if not tasks:
        return _json({"error": f"No tasks found for pathspec '{pathspec}'"})

    task, label = tasks[0]
    cards = get_cards(task, id=card_id, type=card_type)

    if len(cards) == 0:
        return _json({"error": "No cards found", "pathspec": pathspec, "task": label})

    if card_index >= len(cards):
        return _json(
            {
                "error": f"Card index {card_index} out of range (found {len(cards)} cards)",
                "pathspec": pathspec,
                "task": label,
            }
        )

    card = cards[card_index]
    html = card.get()

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".html", delete=False, prefix="metaflow_card_"
    ) as f:
        f.write(html)
        file_path = f.name

    text_content = _extract_text_from_html(html)
    max_text_len = 10_000
    if len(text_content) > max_text_len:
        text_content = text_content[:max_text_len] + "\n... (truncated)"

    return _json(
        {
            "pathspec": pathspec,
            "task": label,
            "card_type": card.type,
            "card_id": card.id,
            "card_hash": card.hash,
            "html_file": file_path,
            "html_size_bytes": len(html),
            "text_content": text_content,
        }
    )


@mcp.tool()
@_handle_errors
def compare_cards(
    pathspecs: list[str] | None = None,
    flow_name: str | None = None,
    step_name: str | None = None,
    run_ids: list[str] | None = None,
    card_type: str | None = None,
    card_id: str | None = None,
    card_index: int = 0,
) -> str:
    """Compare Metaflow cards across multiple runs side by side.

    Creates an HTML comparison page and saves it to a temp file you can
    open in your browser. Also returns text summaries of each card for
    analysis.

    Two ways to specify which cards to compare:

    Option A -- provide a list of step/task pathspecs directly:
        pathspecs=["MyFlow/100/validate", "MyFlow/101/validate"]

    Option B -- provide flow_name + step_name + run_ids (shorthand):
        flow_name="MyFlow", step_name="validate", run_ids=["100", "101"]
        Resolves each to "MyFlow/{run_id}/{step_name}" (first task).

    Args:
        pathspecs: List of step or task pathspecs to compare.
        flow_name: Flow name (used with step_name + run_ids).
        step_name: Step name (used with flow_name + run_ids).
        run_ids: List of run IDs to compare (used with flow_name + step_name).
        card_type: Filter cards by type before selecting.
        card_id: Filter cards by ID before selecting.
        card_index: Which card to use if multiple match (default 0).
    """
    import tempfile

    from metaflow.cards import get_cards

    if pathspecs:
        resolved = pathspecs
    elif flow_name and step_name and run_ids:
        resolved = [f"{flow_name}/{rid}/{step_name}" for rid in run_ids]
    else:
        return _json(
            {"error": "Provide either 'pathspecs' or all of 'flow_name', 'step_name', and 'run_ids'."}
        )

    if len(resolved) < 2:
        return _json({"error": "Need at least 2 pathspecs to compare."})

    entries = []
    errors = []
    for spec in resolved:
        try:
            tasks = _resolve_tasks_for_cards(spec)
            if not tasks:
                errors.append({"pathspec": spec, "error": "No tasks found"})
                continue
            task, label = tasks[0]
            cards = get_cards(task, id=card_id, type=card_type)
            if len(cards) == 0:
                errors.append({"pathspec": spec, "error": "No cards found"})
                continue
            idx = min(card_index, len(cards) - 1)
            card = cards[idx]
            html = card.get()
            entries.append(
                {
                    "pathspec": spec,
                    "task": label,
                    "card_type": card.type,
                    "card_id": card.id,
                    "html": html,
                }
            )
        except Exception as e:
            errors.append({"pathspec": spec, "error": str(e)})

    if not entries:
        return _json({"error": "No cards could be loaded", "details": errors})

    comparison_html = _build_comparison_html(entries)

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".html", delete=False, prefix="metaflow_compare_"
    ) as f:
        f.write(comparison_html)
        file_path = f.name

    summaries = []
    for entry in entries:
        text = _extract_text_from_html(entry["html"])
        if len(text) > 3000:
            text = text[:3000] + "\n... (truncated)"
        summaries.append(
            {
                "pathspec": entry["pathspec"],
                "task": entry["task"],
                "card_type": entry["card_type"],
                "text_content": text,
            }
        )

    return _json(
        {
            "comparison_file": file_path,
            "cards_compared": len(entries),
            "summaries": summaries,
            "errors": errors if errors else None,
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
def get_source_code(
    pathspec: str,
    file_path: str | None = None,
) -> str:
    """Get the source code from a Metaflow run's code package.

    Every Metaflow run that executes remotely stores a snapshot of the code.
    Use this to inspect the exact code that was used in a run.

    Without file_path, returns the main FlowSpec source file and lists all
    files in the code package. With file_path, returns the content of that
    specific file from the package.

    Args:
        pathspec: Run pathspec like "FlowName/RunID", or task pathspec
                  like "FlowName/RunID/StepName/TaskID".
        file_path: Optional path of a specific file within the code package.
                   If omitted, returns the main flow file and a listing of
                   all files in the package.
    """
    from metaflow import Run, Task

    parts = pathspec.split("/")
    if len(parts) == 2:
        run = Run(pathspec)
        code = run.code
    elif len(parts) == 4:
        task = Task(pathspec)
        code = task.code
    else:
        return _json({"error": "pathspec must be FlowName/RunID or FlowName/RunID/StepName/TaskID"})

    if code is None:
        return _json({"error": "No code package found for this run/task. Code is only stored for runs with remote steps."})

    if file_path:
        # Return content of a specific file
        try:
            content = None
            tarball = code.tarball
            for member in tarball.getmembers():
                if member.name == file_path or member.name.endswith("/" + file_path):
                    f = tarball.extractfile(member)
                    if f:
                        content = f.read().decode("utf-8", errors="replace")
                    break
            if content is None:
                return _json({"error": f"File '{file_path}' not found in code package"})
            return _json({
                "pathspec": pathspec,
                "file_path": file_path,
                "content": content,
            })
        except Exception as e:
            return _json({"error": f"Failed to extract file: {e}"})
    else:
        # Return main flowspec + file listing
        file_list = []
        try:
            tarball = code.tarball
            for member in tarball.getmembers():
                if member.isfile():
                    file_list.append({
                        "name": member.name,
                        "size": member.size,
                    })
        except Exception:
            pass  # tarball listing failed, still return flowspec

        return _json({
            "pathspec": pathspec,
            "main_file": code.info.get("script", "unknown"),
            "flowspec": code.flowspec,
            "files": file_list,
        })


@mcp.tool()
@_handle_errors
def get_recent_runs(
    namespace: str,
    last_n_flows: int = 20,
    last_n_runs_per_flow: int = 3,
    status: str | None = None,
) -> str:
    """Find the most recent runs across all flows in a namespace.

    Use this when no specific flow name is given and you need to find what the
    user ran recently. Scans all flows in the namespace and returns runs sorted
    by creation time (newest first).

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
        all_runs = []
        flows_scanned = 0
        for flow in mf.Metaflow():
            if flows_scanned >= last_n_flows:
                break
            flows_scanned += 1
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
            "flows_scanned": flows_scanned,
            "runs_found": len(all_runs),
            "runs": all_runs,
        }
    )


def get_tool_schemas() -> list[dict]:
    """Return name, signature, and docstring for all registered MCP tools.

    Intended for use by code-generation approaches that need to discover
    available API functions at runtime without a hardcoded module path.

    Example usage:
        from metaflow_mcp_server.server import get_tool_schemas
        for fn in get_tool_schemas():
            print(fn['name'], fn['signature'])
    """
    import inspect

    tool_fns = [
        get_config,
        list_flows,
        search_runs,
        get_run,
        get_task_logs,
        list_artifacts,
        get_artifact,
        list_cards,
        get_card,
        compare_cards,
        get_latest_failure,
        search_artifacts,
        get_recent_runs,
        get_source_code,
    ]
    schemas = []
    for fn in tool_fns:
        schemas.append({
            "name": fn.__name__,
            "signature": str(inspect.signature(fn)),
            "docstring": (fn.__doc__ or "").strip(),
        })
    return schemas


def search_tool_schemas(keyword: str) -> list[dict]:
    """Search registered MCP tools by keyword.

    Returns schemas for tools whose name or docstring contains the keyword
    (case-insensitive substring match). Use this to discover which API
    functions are relevant before calling them — the full schema list is
    not available without searching.

    Args:
        keyword: Search term, e.g. "artifact", "run", "failure", "log".

    Example usage:
        from metaflow_mcp_server.server import search_tool_schemas
        for fn in search_tool_schemas('artifact'):
            print(fn['name'], fn['signature'])
            print(fn['docstring'][:200])
    """
    kw = keyword.lower()
    return [
        s for s in get_tool_schemas()
        if kw in s["name"].lower() or kw in s["docstring"].lower()
    ]


def main():
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
