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
            prompt_template="List the artifacts produced by task '{task}', then show the value of artifact '{artifact}'.",
            reference_fn=_ref_medium_artifact_inspect,
            skip_reason=None if (ctx.task_pathspec and ctx.artifact_name) else "no artifact discovered",
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
            id="complex_debug_flow",
            category="complex",
            prompt_template="Investigate '{flow_name}': Get the 10 most recent runs. Report how many have finished, how many of those finished successfully, the success rate among finished runs, and whether any finished run has an error.",
            reference_fn=_ref_complex_debug_flow,
            skip_reason=None if ctx.flow_name else "no flow discovered",
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
    )
