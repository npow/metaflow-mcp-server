"""10 benchmark task definitions with reference answer functions."""

import json
from dataclasses import dataclass
from typing import Callable, Optional

from benchmarks.discover import TestContext


@dataclass
class BenchmarkTask:
    """A single benchmark task definition."""

    id: str
    category: str  # simple, medium, complex
    prompt_template: str  # may contain {flow_name}, {run}, etc.
    reference_fn: Callable[[TestContext], str]
    skip_reason: Optional[str] = None  # set if context is insufficient


def _ref_simple_config(ctx: TestContext) -> str:
    from metaflow.client import get_metadata
    from metaflow.metaflow_config import DEFAULT_DATASTORE
    return json.dumps({
        "metadata_provider": get_metadata(),
        "default_datastore": DEFAULT_DATASTORE,
    })


def _ref_simple_list_runs(ctx: TestContext) -> str:
    from metaflow import Flow
    flow = Flow(ctx.flow_name)
    runs = []
    for run in flow:
        runs.append({
            "pathspec": run.pathspec,
            "successful": run.successful,
            "finished": run.finished,
        })
        if len(runs) >= 3:
            break
    return json.dumps(runs, default=str)


def _ref_medium_run_details(ctx: TestContext) -> str:
    from metaflow import Run
    run = Run(ctx.run_pathspec)
    steps = []
    for step in run:
        tasks = []
        for task in step:
            tasks.append({"id": task.id, "successful": task.successful})
        steps.append({"step": step.id, "tasks": tasks})
    return json.dumps({"pathspec": run.pathspec, "successful": run.successful, "steps": steps}, default=str)


def _ref_medium_task_logs(ctx: TestContext) -> str:
    from metaflow import Task
    task = Task(ctx.task_pathspec)
    return json.dumps({
        "pathspec": ctx.task_pathspec,
        "stdout": (task.stdout or "")[:500],
        "stderr": (task.stderr or "")[:500],
    })


def _ref_medium_artifact_inspect(ctx: TestContext) -> str:
    from metaflow import Task
    task = Task(ctx.task_pathspec)
    artifacts = [{"name": a.id, "type": type(a.data).__name__} for a in task]
    value = repr(task[ctx.artifact_name].data) if ctx.artifact_name else "N/A"
    return json.dumps({"artifacts": artifacts, "artifact_value": value[:500]}, default=str)


def _ref_complex_latest_failure(ctx: TestContext) -> str:
    from metaflow import Flow
    flow = Flow(ctx.failed_flow_name)
    for run in flow:
        if run.finished and not run.successful:
            for step in run:
                for task in step:
                    if task.finished and not task.successful:
                        return json.dumps({
                            "run": run.pathspec,
                            "failing_step": step.id,
                            "exception": repr(task.exception) if task.exception else None,
                        }, default=str)
            return json.dumps({"run": run.pathspec, "note": "failed but no failing task found"})
    return json.dumps({"message": "no failed runs found"})


def _ref_complex_success_rate(ctx: TestContext) -> str:
    from metaflow import Flow
    flow = Flow(ctx.flow_name)
    runs = []
    for run in flow:
        runs.append(run)
        if len(runs) >= 10:
            break
    finished = [r for r in runs if r.finished]
    successful = [r for r in finished if r.successful]
    rate = len(successful) / len(finished) if finished else 0.0
    return json.dumps({
        "flow": ctx.flow_name,
        "total_runs": len(runs),
        "total_finished": len(finished),
        "successful": len(successful),
        "success_rate": round(rate, 2),
    })


def _ref_complex_compare_runs(ctx: TestContext) -> str:
    from metaflow import Flow
    flow = Flow(ctx.flow_name)
    runs = []
    for run in flow:
        if run.finished:
            runs.append(run)
        if len(runs) >= 2:
            break
    if len(runs) < 2:
        return json.dumps({"error": "not enough finished runs to compare"})
    comparison = []
    for run in runs:
        steps = [step.id for step in run]
        comparison.append({"pathspec": run.pathspec, "successful": run.successful, "steps": steps})
    return json.dumps(comparison, default=str)


def _ref_complex_artifact_diff(ctx: TestContext) -> str:
    from metaflow import Flow
    flow = Flow(ctx.flow_name)
    successful_runs = []
    for run in flow:
        if run.finished and run.successful:
            successful_runs.append(run)
        if len(successful_runs) >= 2:
            break
    if len(successful_runs) < 2:
        return json.dumps({"error": "not enough successful runs"})
    results = []
    for run in successful_runs:
        # Get artifacts from last step (by finished_at) first task
        steps = list(run)
        last_step = max(steps, key=lambda s: s.finished_at or "") if steps else None
        if last_step:
            for task in last_step:
                arts = {a.id: repr(a.data)[:200] for a in task if not a.id.startswith("_")}
                results.append({"run": run.pathspec, "step": last_step.id, "artifacts": arts})
                break
    return json.dumps(results, default=str)


def _ref_simple_list_flows(ctx: TestContext) -> str:
    from metaflow import Metaflow
    flows = [flow.id for flow in Metaflow()]
    return json.dumps({"flows": flows, "count": len(flows)})


def _ref_medium_filtered_runs(ctx: TestContext) -> str:
    from metaflow import Flow
    flow = Flow(ctx.flow_name)
    runs = []
    for run in flow:
        if run.successful:
            runs.append({
                "pathspec": run.pathspec,
                "successful": True,
                "created_at": str(run.created_at),
            })
        if len(runs) >= 3:
            break
    return json.dumps(runs, default=str)


def _ref_medium_bounded_logs(ctx: TestContext) -> str:
    from metaflow import Task
    task = Task(ctx.task_pathspec)
    stderr = task.stderr or ""
    lines = stderr.splitlines()
    last_10 = lines[-10:] if len(lines) > 10 else lines
    return json.dumps({
        "pathspec": ctx.task_pathspec,
        "stderr_tail": "\n".join(last_10),
        "total_lines": len(lines),
    })


def _ref_medium_run_timing(ctx: TestContext) -> str:
    from metaflow import Run
    run = Run(ctx.run_pathspec)
    steps = []
    for step in run:
        for task in step:
            dur = None
            if task.created_at and task.finished_at:
                s = task.created_at
                e = task.finished_at
                if s.tzinfo is None:
                    from datetime import timezone
                    s = s.replace(tzinfo=timezone.utc)
                if e.tzinfo is None:
                    from datetime import timezone
                    e = e.replace(tzinfo=timezone.utc)
                dur = round((e - s).total_seconds(), 1)
            steps.append({
                "step": step.id,
                "task": task.id,
                "duration_seconds": dur,
            })
            break  # first task per step
    return json.dumps({"pathspec": run.pathspec, "steps": steps}, default=str)


def _ref_complex_artifact_search(ctx: TestContext) -> str:
    from metaflow import Flow
    flow = Flow(ctx.flow_name)
    results = []
    scanned = 0
    for run in flow:
        scanned += 1
        if scanned > 5:
            break
        for step in run:
            for task in step:
                for art in task:
                    if art.id == ctx.artifact_name:
                        results.append({
                            "task": task.pathspec,
                            "step": step.id,
                            "run": run.pathspec,
                        })
                        break
    return json.dumps({
        "artifact_name": ctx.artifact_name,
        "runs_scanned": scanned,
        "matches": results,
    }, default=str)


def _ref_disambig_count_run_states(ctx: TestContext) -> str:
    """Count StatusTestFlow runs by exact state: unfinished / succeeded / failed.

    Key insight: in Metaflow, exception-killed runs have finished=False — they look
    identical to currently-running runs. There is no finished=True, successful=False
    state for local-backend exception failures. Models must use finished flag correctly.
    """
    from metaflow import Flow
    flow = Flow(ctx.status_flow_name)
    all_runs = list(flow)
    unfinished = [r for r in all_runs if not r.finished]
    succeeded = [r for r in all_runs if r.finished and r.successful]
    failed = [r for r in all_runs if r.finished and not r.successful]
    return json.dumps({
        "flow": ctx.status_flow_name,
        "total_runs": len(all_runs),
        "unfinished_or_active": len(unfinished),
        "finished_successfully": len(succeeded),
        "finished_with_failure": len(failed),
    })


def _ref_disambig_most_recent_state(ctx: TestContext) -> str:
    """State of the most recent run of StatusTestFlow.

    The most recent run has finished=False (exception-killed), which is the same
    API state as a currently-executing run. The correct answer is 'unfinished',
    not 'failed'.
    """
    from metaflow import Flow
    flow = Flow(ctx.status_flow_name)
    run = next(iter(flow))  # iteration order = newest first
    if run.finished and run.successful:
        status = "finished_successfully"
    elif run.finished and not run.successful:
        status = "finished_with_failure"
    else:
        status = "unfinished_or_active"
    return json.dumps({
        "run": run.pathspec,
        "finished": run.finished,
        "successful": run.successful,
        "status": status,
    })


def _ref_disambig_unfinished_not_failed(ctx: TestContext) -> str:
    """StatusTestFlow 5 most recent: unfinished runs must NOT be counted as failed.

    The 2 most recent runs are exception-killed (finished=False). A naive model that
    checks only successful=False will count them as failures — this is wrong.
    The correct answer is: 2 unfinished, 3 succeeded, 0 failed.
    """
    from metaflow import Flow
    flow = Flow(ctx.status_flow_name)
    runs = []
    for run in flow:
        runs.append(run)
        if len(runs) >= 5:
            break
    unfinished = [r.pathspec for r in runs if not r.finished]
    finished_ok = [r.pathspec for r in runs if r.finished and r.successful]
    finished_fail = [r.pathspec for r in runs if r.finished and not r.successful]
    return json.dumps({
        "flow": ctx.status_flow_name,
        "runs_examined": len(runs),
        "unfinished_or_active": len(unfinished),
        "finished_successfully": len(finished_ok),
        "finished_with_failure": len(finished_fail),
    })


def _ref_disambig_success_rate_finished_only(ctx: TestContext) -> str:
    """StatusTestFlow: success rate computed only over FINISHED runs.

    If a model includes unfinished runs in the denominator, the success rate
    will be wrong. Correct: 4 finished, 4 successful → 100% success rate.
    """
    from metaflow import Flow
    flow = Flow(ctx.status_flow_name)
    all_runs = list(flow)
    finished = [r for r in all_runs if r.finished]
    successful = [r for r in finished if r.successful]
    rate = round(len(successful) / len(finished), 2) if finished else 0.0
    return json.dumps({
        "flow": ctx.status_flow_name,
        "total_runs_all_states": len(all_runs),
        "total_finished": len(finished),
        "total_successful_among_finished": len(successful),
        "success_rate_among_finished": rate,
    })


def _ref_complex_debug_flow(ctx: TestContext) -> str:
    from metaflow import Flow
    flow = Flow(ctx.flow_name)
    runs = []
    for run in flow:
        runs.append(run)
        if len(runs) >= 10:
            break
    finished = [r for r in runs if r.finished]
    successful = [r for r in finished if r.successful]
    rate = len(successful) / len(finished) if finished else 0.0
    latest_error = None
    for run in finished:
        if not run.successful and latest_error is None:
            for step in run:
                for task in step:
                    if task.finished and not task.successful and task.exception:
                        latest_error = {
                            "run": run.pathspec,
                            "step": step.id,
                            "exception": repr(task.exception),
                        }
                        break
                if latest_error:
                    break
    return json.dumps({
        "flow": ctx.flow_name,
        "total_runs": len(runs),
        "total_finished": len(finished),
        "successful": len(successful),
        "success_rate": round(rate, 2),
        "latest_error": latest_error,
    }, default=str)


def _ref_hard_slowest_step(ctx: TestContext) -> str:
    """2 most recent finished runs: which step was slowest (by first-task duration)?"""
    from metaflow import Flow
    from datetime import timezone
    flow = Flow(ctx.flow_name)
    result = []
    for run in flow:
        if not run.finished:
            continue
        steps_with_dur = []
        for step in run:
            for task in step:
                dur = None
                if task.created_at and task.finished_at:
                    s = task.created_at
                    e = task.finished_at
                    if s.tzinfo is None:
                        s = s.replace(tzinfo=timezone.utc)
                    if e.tzinfo is None:
                        e = e.replace(tzinfo=timezone.utc)
                    dur = round((e - s).total_seconds(), 1)
                steps_with_dur.append({"step": step.id, "duration_seconds": dur})
                break  # first task per step only
        # Sort: slowest first; break ties alphabetically by step name.
        steps_with_dur.sort(
            key=lambda x: (-(x["duration_seconds"] if x["duration_seconds"] is not None else 0), x["step"]),
        )
        slowest = steps_with_dur[0] if steps_with_dur else None
        result.append({
            "run": run.pathspec,
            "slowest_step": slowest["step"] if slowest else None,
            "slowest_step_duration_seconds": slowest["duration_seconds"] if slowest else None,
            "all_steps": steps_with_dur,
        })
        if len(result) >= 2:
            break
    return json.dumps({"flow": ctx.flow_name, "runs": result}, default=str)


def _ref_hard_artifact_timeline(ctx: TestContext) -> str:
    """Fetch an artifact from each of 3 recent successful runs, report oldest-first."""
    from metaflow import Flow
    flow = Flow(ctx.flow_name)
    successful_runs = []
    for run in flow:
        if run.finished and run.successful:
            successful_runs.append(run)
        if len(successful_runs) >= 3:
            break
    # successful_runs is newest-first; reverse for chronological order
    values = []
    for run in reversed(successful_runs):
        val = None
        for step in run:
            for task in step:
                for art in task:
                    if art.id == ctx.artifact_name:
                        val = repr(art.data)[:200]
                        break
                if val is not None:
                    break
            if val is not None:
                break
        values.append({"run": run.pathspec, "value": val})
    return json.dumps({
        "flow": ctx.flow_name,
        "artifact": ctx.artifact_name,
        "values_oldest_first": values,
    }, default=str)


def _ref_hard_steps_per_flow(ctx: TestContext) -> str:
    """For each flow, count steps in its most recent run; report which has most."""
    from metaflow import Metaflow, Flow
    flows_data = []
    for flow_obj in Metaflow():
        try:
            flow = Flow(flow_obj.id)
            run = next(iter(flow), None)
            if run is None:
                continue
            steps = list(run)
            flows_data.append({
                "flow": flow_obj.id,
                "run": run.pathspec,
                "step_count": len(steps),
                "steps": [s.id for s in steps],
            })
        except Exception:
            continue
        # no limit — include all flows
    most_steps_flow = max(flows_data, key=lambda x: x["step_count"])["flow"] if flows_data else None
    return json.dumps({
        "flows": flows_data,
        "most_steps_flow": most_steps_flow,
    }, default=str)


def build_tasks(ctx: TestContext) -> list[BenchmarkTask]:
    """Build the 10 benchmark tasks, parameterized by discovered test context."""
    tasks = [
        BenchmarkTask(
            id="simple_config",
            category="simple",
            prompt_template="What Metaflow backend am I connected to? Show the metadata provider and datastore.",
            reference_fn=_ref_simple_config,
        ),
        BenchmarkTask(
            id="simple_list_flows",
            category="simple",
            prompt_template="List the available Metaflow flows. Show their names.",
            reference_fn=_ref_simple_list_flows,
        ),
        BenchmarkTask(
            id="simple_list_runs",
            category="simple",
            prompt_template="List the last 3 runs of the flow '{flow_name}'. Show their pathspecs and whether they succeeded.",
            reference_fn=_ref_simple_list_runs,
            skip_reason=None if ctx.flow_name else "no flow discovered",
        ),
        BenchmarkTask(
            id="medium_run_details",
            category="medium",
            prompt_template="Show the step-by-step breakdown for run '{run}'. Include task statuses.",
            reference_fn=_ref_medium_run_details,
            skip_reason=None if ctx.run_pathspec else "no run discovered",
        ),
        BenchmarkTask(
            id="medium_task_logs",
            category="medium",
            prompt_template="Show the stdout and stderr logs for task '{task}'.",
            reference_fn=_ref_medium_task_logs,
            skip_reason=None if ctx.task_pathspec else "no task discovered",
        ),
        BenchmarkTask(
            id="medium_artifact_inspect",
            category="medium",
            prompt_template="List the artifacts produced by task '{task}' (include each artifact's data type), then show the value of artifact '{artifact}'.",
            reference_fn=_ref_medium_artifact_inspect,
            skip_reason=None if (ctx.task_pathspec and ctx.artifact_name) else "no artifact discovered",
        ),
        BenchmarkTask(
            id="medium_filtered_runs",
            category="medium",
            prompt_template="List the last 3 successful runs of flow '{flow_name}'. Show their pathspecs and creation times.",
            reference_fn=_ref_medium_filtered_runs,
            skip_reason=None if ctx.flow_name else "no flow discovered",
        ),
        BenchmarkTask(
            id="medium_bounded_logs",
            category="medium",
            prompt_template="Show the last 10 lines of stderr for task '{task}'.",
            reference_fn=_ref_medium_bounded_logs,
            skip_reason=None if ctx.task_pathspec else "no task discovered",
        ),
        BenchmarkTask(
            id="medium_run_timing",
            category="medium",
            prompt_template="Show the duration of each step in run '{run}'. Report the step name and how long its first task took in seconds.",
            reference_fn=_ref_medium_run_timing,
            skip_reason=None if ctx.run_pathspec else "no run discovered",
        ),
        BenchmarkTask(
            id="complex_latest_failure",
            category="complex",
            prompt_template="Find the most recent failed run of '{failed_flow}' (a run that finished but was not successful) and show the error details including the failing step and exception. If no failed runs exist, say so.",
            reference_fn=_ref_complex_latest_failure,
            skip_reason=None if ctx.failed_flow_name else "no failed flow discovered",
        ),
        BenchmarkTask(
            id="complex_success_rate",
            category="complex",
            prompt_template="Look at the 10 most recent runs of '{flow_name}'. How many of those 10 have finished? Of the finished ones, how many were successful? Report the counts and success rate.",
            reference_fn=_ref_complex_success_rate,
            skip_reason=None if ctx.flow_name else "no flow discovered",
        ),
        BenchmarkTask(
            id="complex_compare_runs",
            category="complex",
            prompt_template="Compare the steps of the 2 most recent finished runs of '{flow_name}'. Show which steps each run has and whether they succeeded.",
            reference_fn=_ref_complex_compare_runs,
            skip_reason=None if ctx.flow_name else "no flow discovered",
        ),
        BenchmarkTask(
            id="complex_artifact_diff",
            category="complex",
            prompt_template="Compare the artifacts from the 'end' step of the 2 most recent successful runs of '{flow_name}'. Show what changed.",
            reference_fn=_ref_complex_artifact_diff,
            skip_reason=None if ctx.flow_name else "no flow discovered",
        ),
        BenchmarkTask(
            id="complex_artifact_search",
            category="complex",
            prompt_template="Search the last 5 runs of '{flow_name}' to find which tasks produced an artifact named '{artifact}'. Report the task pathspecs where it was found.",
            reference_fn=_ref_complex_artifact_search,
            skip_reason=None if (ctx.flow_name and ctx.artifact_name) else "no artifact discovered",
        ),
        BenchmarkTask(
            id="complex_debug_flow",
            category="complex",
            prompt_template="Investigate '{flow_name}': Get the 10 most recent runs (regardless of whether they have finished). Among those 10 runs, report how many have finished, how many of those finished successfully, the success rate among finished runs, and whether any finished run has an error.",
            reference_fn=_ref_complex_debug_flow,
            skip_reason=None if ctx.flow_name else "no flow discovered",
        ),
        # --- Hard tasks (multi-hop, aggregation) ---
        BenchmarkTask(
            id="hard_slowest_step",
            category="hard",
            prompt_template=(
                "Get the 2 most recent finished runs of '{flow_name}'. "
                "For each run, report: the run pathspec, the name of the step with the "
                "longest first-task duration, and that duration in seconds. "
                "List all steps with their durations too. "
                "If steps are tied in duration, pick the one that comes first alphabetically."
            ),
            reference_fn=_ref_hard_slowest_step,
            skip_reason=None if ctx.flow_name else "no flow discovered",
        ),
        BenchmarkTask(
            id="hard_artifact_timeline",
            category="hard",
            prompt_template=(
                "Fetch the value of artifact '{artifact}' from each of the last 3 successful "
                "runs of '{flow_name}'. Report the values from oldest run to newest run, "
                "including the run pathspec and artifact value for each."
            ),
            reference_fn=_ref_hard_artifact_timeline,
            skip_reason=None if (ctx.flow_name and ctx.artifact_name) else "no artifact discovered",
        ),
        BenchmarkTask(
            id="hard_steps_per_flow",
            category="hard",
            prompt_template=(
                "For each available Metaflow flow, retrieve its most recent run and count "
                "how many steps it has. Report each flow's name, step count, and step names. "
                "Which flow has the most steps?"
            ),
            reference_fn=_ref_hard_steps_per_flow,
        ),
        # --- Disambiguation tasks ---
        # These specifically test the ability to distinguish between:
        # (a) currently running/unfinished (finished=False),
        # (b) finished successfully (finished=True, successful=True), and
        # (c) finished with failure (finished=True, successful=False).
        # All expected answers are unique integers or binary flags — no fuzzy matching needed.
        BenchmarkTask(
            id="disambig_count_run_states",
            category="disambiguation",
            prompt_template=(
                "For flow '{status_flow}', examine ALL its runs and categorize them into exactly three buckets:\n"
                "1. Currently running or unfinished (finished=False)\n"
                "2. Finished successfully (finished=True AND successful=True)\n"
                "3. Finished with failure (finished=True AND successful=False)\n"
                "Report the exact count for each bucket."
            ),
            reference_fn=_ref_disambig_count_run_states,
            skip_reason=None if ctx.status_flow_name else "StatusTestFlow not found — run setup_test_data.py",
        ),
        BenchmarkTask(
            id="disambig_most_recent_state",
            category="disambiguation",
            prompt_template=(
                "Look at the most recent run of '{status_flow}'. "
                "Check its 'finished' and 'successful' properties explicitly. "
                "Classify it as one of: (a) currently running or unfinished (finished=False), "
                "(b) finished successfully (finished=True, successful=True), or "
                "(c) finished with failure (finished=True, successful=False). "
                "Report the run pathspec and its exact classification."
            ),
            reference_fn=_ref_disambig_most_recent_state,
            skip_reason=None if ctx.status_flow_name else "StatusTestFlow not found — run setup_test_data.py",
        ),
        BenchmarkTask(
            id="disambig_unfinished_not_failed",
            category="disambiguation",
            prompt_template=(
                "For flow '{status_flow}', look at its 5 most recent runs. "
                "Count how many are: (a) unfinished or active (finished=False), "
                "(b) finished successfully (finished=True AND successful=True), "
                "(c) finished with failure (finished=True AND successful=False). "
                "Report exact counts for each category."
            ),
            reference_fn=_ref_disambig_unfinished_not_failed,
            skip_reason=None if ctx.status_flow_name else "StatusTestFlow not found — run setup_test_data.py",
        ),
        BenchmarkTask(
            id="disambig_success_rate_finished_only",
            category="disambiguation",
            prompt_template=(
                "For flow '{status_flow}', compute the success rate — but only count "
                "runs that have actually FINISHED (finished=True). "
                "Report: total runs across all states, how many have finished=True, "
                "how many of those finished runs were successful, "
                "and the success rate among finished runs only. "
                "Do not include unfinished runs in your success rate calculation."
            ),
            reference_fn=_ref_disambig_success_rate_finished_only,
            skip_reason=None if ctx.status_flow_name else "StatusTestFlow not found — run setup_test_data.py",
        ),
    ]
    return tasks


def render_prompt(task: BenchmarkTask, ctx: TestContext) -> str:
    """Fill in the prompt template with values from the test context."""
    return task.prompt_template.format(
        flow_name=ctx.flow_name,
        run=ctx.run_pathspec,
        task=ctx.task_pathspec,
        step=ctx.step_name,
        artifact=ctx.artifact_name,
        failed_flow=ctx.failed_flow_name,
        status_flow=ctx.status_flow_name,
    )
