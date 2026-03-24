"""CLI entry point and orchestrator for the benchmark suite."""

import argparse
import json
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, fields
from pathlib import Path

from benchmarks.config import MODELS, RELAY_BASE_URL, TaskResult
from benchmarks.discover import discover_flows, build_test_context
from benchmarks.tasks import build_tasks, render_prompt, BenchmarkTask
from benchmarks.approaches import APPROACHES
from benchmarks.harness import run_task
from benchmarks.judge import evaluate_results
from benchmarks.report import save_results, print_summary_table, print_aggregate_table, print_category_table, print_significance_table

# Thread-safe print
_print_lock = threading.Lock()


def _tprint(*args, **kwargs):
    with _print_lock:
        print(*args, **kwargs, flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Benchmark: MCP Direct vs Code Mode vs Skill",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--models",
        nargs="+",
        choices=list(MODELS.keys()),
        default=list(MODELS.keys()),
        help="Models to benchmark (default: all)",
    )
    parser.add_argument(
        "--approaches",
        nargs="+",
        choices=list(APPROACHES.keys()),
        default=list(APPROACHES.keys()),
        help="Approaches to benchmark (default: all)",
    )
    parser.add_argument(
        "--tasks",
        nargs="+",
        default=None,
        help="Task IDs to run (default: all discoverable tasks)",
    )
    parser.add_argument(
        "--trials",
        type=int,
        default=3,
        help="Number of independent runs per (approach, model, task) cell (default: 3)",
    )
    parser.add_argument(
        "--only-flows",
        nargs="+",
        default=None,
        metavar="FLOW",
        help="Restrict Phase 1 discovery to these flow names (e.g. StatusTestFlow DiagnoseFlow)",
    )
    parser.add_argument(
        "--output",
        default="benchmarks/results.json",
        help="Output JSON path (default: benchmarks/results.json)",
    )
    parser.add_argument(
        "--skip-judge",
        action="store_true",
        help="Skip LLM-as-judge correctness evaluation",
    )
    parser.add_argument(
        "--judge-only",
        metavar="RAW_FILE",
        default=None,
        help="Skip Phase 1-3; load raw results from RAW_FILE, judge, and write --output",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print detailed progress",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Max parallel workers (default: 4, to avoid relay 503s)",
    )
    return parser.parse_args()


def _run_worker(
    approach,
    model_name: str,
    model_id: str,
    tasks: list,
    questions: dict[str, str],
    num_trials: int,
    verbose: bool,
) -> list[TaskResult]:
    """Run all tasks × trials for one (approach, model) combo. Called in a thread."""
    worker_results = []
    tag = f"{approach.name}/{model_name}"
    for task in tasks:
        prompt = questions[task.id]
        for trial in range(num_trials):
            trial_tag = f"trial {trial + 1}/{num_trials}" if num_trials > 1 else ""
            _tprint(f"  [{tag}] {task.id} {trial_tag}...")
            result = run_task(
                approach=approach,
                model_id=model_id,
                model_name=model_name,
                task_id=task.id,
                user_prompt=prompt,
                verbose=verbose,
            )
            result.trial = trial
            worker_results.append(result)
            _tprint(
                f"  [{tag}] {task.id} t{trial} -> "
                f"{result.total_tokens} tok, {result.wall_clock_seconds}s, "
                f"${result.estimated_cost_usd:.4f}"
                + (f" ERROR: {result.error}" if result.error else "")
            )
    return worker_results


def _load_raw_results(path: str) -> list[TaskResult]:
    """Load TaskResult objects from a raw JSON file saved after Phase 3."""
    data = json.loads(Path(path).read_text())
    field_names = {f.name for f in fields(TaskResult)}
    return [TaskResult(**{k: v for k, v in item.items() if k in field_names}) for item in data]


def main():
    args = parse_args()

    # Phase 0: Verify relay is running
    print(f"Relay: {RELAY_BASE_URL}")

    # --judge-only: skip Phases 1-3, load raw results, re-judge, write output
    if args.judge_only:
        # Prefer checkpoint over raw file if it exists (checkpoint has partial scores)
        checkpoint_path = args.output.replace(".json", ".checkpoint.json")
        load_path = args.judge_only
        if Path(checkpoint_path).exists():
            load_path = checkpoint_path
            print(f"\nJudge-only mode: resuming from checkpoint {checkpoint_path}")
        else:
            print(f"\nJudge-only mode: loading raw results from {args.judge_only}")
        results = _load_raw_results(load_path)
        already_scored = sum(1 for r in results if r.correctness_score is not None)
        print(f"  Loaded {len(results)} results ({already_scored} already scored)")

        # Rebuild task context to recover questions and references
        from metaflow import namespace as _ns
        _ns(None)
        flows = discover_flows(only_flows=args.only_flows)
        ctx = build_test_context(flows)
        all_tasks = build_tasks(ctx)
        questions: dict[str, str] = {}
        references: dict[str, str] = {}
        for task in all_tasks:
            if task.skip_reason is not None:
                continue
            questions[task.id] = render_prompt(task, ctx)
            try:
                references[task.id] = task.reference_fn(ctx)
            except Exception as e:
                references[task.id] = f"(reference error: {e})"

        checkpoint_path = args.output.replace(".json", ".checkpoint.json")
        print(f"\nPhase 4: Judging correctness ({len(results)} results)...")
        evaluate_results(results, questions, references, verbose=args.verbose,
                         checkpoint_path=checkpoint_path)

        print("\nPhase 5: Reporting...")
        save_results(results, args.output)
        print_summary_table(results)
        print_aggregate_table(results)
        task_categories = {t.id: t.category for t in all_tasks}
        print_category_table(results, task_categories)
        print_significance_table(results)
        return

    # Phase 1: Discover test data — set global namespace first so Metaflow
    # doesn't try to resolve user identity before we override it.
    from metaflow import namespace as _ns
    _ns(None)

    print("\nPhase 1: Discovering flows...")
    flows = discover_flows(only_flows=args.only_flows)
    if not flows:
        print("ERROR: No flows found in the Metaflow backend. Cannot run benchmarks.")
        sys.exit(1)

    print(f"  Found {len(flows)} flows: {[f['name'] for f in flows]}")
    ctx = build_test_context(flows)
    print(f"  Primary flow: {ctx.flow_name}")
    print(f"  Run: {ctx.run_pathspec}")
    print(f"  Task: {ctx.task_pathspec}")
    print(f"  Artifact: {ctx.artifact_name}")
    print(f"  Failed flow: {ctx.failed_flow_name}")

    # Phase 2: Build task suite
    print("\nPhase 2: Building tasks...")
    all_tasks = build_tasks(ctx)
    if args.tasks:
        all_tasks = [t for t in all_tasks if t.id in args.tasks]
    runnable = [t for t in all_tasks if t.skip_reason is None]
    skipped = [t for t in all_tasks if t.skip_reason is not None]
    for t in skipped:
        print(f"  SKIP {t.id}: {t.skip_reason}")
    print(f"  {len(runnable)} tasks ready, {len(skipped)} skipped")

    if not runnable:
        print("ERROR: No runnable tasks. Check your Metaflow backend.")
        sys.exit(1)

    # Compute reference answers and prompts
    questions: dict[str, str] = {}
    references: dict[str, str] = {}
    for task in runnable:
        prompt = render_prompt(task, ctx)
        questions[task.id] = prompt
        try:
            references[task.id] = task.reference_fn(ctx)
        except Exception as e:
            print(f"  WARNING: reference_fn failed for {task.id}: {e}")
            references[task.id] = f"(reference error: {e})"

    # Phase 3: Run benchmarks in parallel (one thread per approach/model combo)
    approach_instances = {name: cls() for name, cls in APPROACHES.items() if name in args.approaches}
    num_workers = len(approach_instances) * len(args.models)
    if args.workers is not None:
        num_workers = min(num_workers, args.workers)
    total_tasks = len(approach_instances) * len(args.models) * len(runnable) * args.trials
    print(f"\nPhase 3: Running benchmarks ({num_workers} workers x {len(runnable)} tasks x {args.trials} trials = {total_tasks} total)...")

    results: list[TaskResult] = []
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = {}
        for approach_name, approach in approach_instances.items():
            for model_name in args.models:
                model_id = MODELS[model_name]
                future = executor.submit(
                    _run_worker,
                    approach, model_name, model_id,
                    runnable, questions, args.trials, args.verbose,
                )
                futures[future] = f"{approach_name}/{model_name}"

        for future in as_completed(futures):
            tag = futures[future]
            try:
                worker_results = future.result()
                results.extend(worker_results)
                _tprint(f"  [{tag}] DONE ({len(worker_results)} tasks)")
            except Exception as e:
                _tprint(f"  [{tag}] FAILED: {e}")

    # Sort results for consistent output: approach, model, task order
    task_order = {t.id: i for i, t in enumerate(runnable)}
    approach_order = {n: i for i, n in enumerate(APPROACHES.keys())}
    model_order = {n: i for i, n in enumerate(MODELS.keys())}
    results.sort(key=lambda r: (approach_order.get(r.approach, 99), model_order.get(r.model, 99), task_order.get(r.task_id, 99)))

    # Save raw results (no scores yet) so judging can be re-run if it crashes
    raw_path = args.output.replace(".json", ".raw.json")
    Path(raw_path).write_text(json.dumps([asdict(r) for r in results], indent=2, default=str))
    print(f"\n  Raw results saved to {raw_path} (re-judge with --judge-only {raw_path})")

    # Phase 4: Judge correctness
    if not args.skip_judge:
        checkpoint_path = raw_path.replace(".raw.json", ".checkpoint.json")
        print(f"\nPhase 4: Judging correctness ({len(results)} results)...")
        evaluate_results(results, questions, references, verbose=args.verbose,
                         checkpoint_path=checkpoint_path)
    else:
        print("\nPhase 4: Skipped (--skip-judge)")

    # Phase 5: Report
    print("\nPhase 5: Reporting...")
    save_results(results, args.output)
    print_summary_table(results)
    print_aggregate_table(results)
    # Category breakdown — shows if disambiguation tasks are harder
    task_categories = {t.id: t.category for t in all_tasks}
    print_category_table(results, task_categories)
    print_significance_table(results)


if __name__ == "__main__":
    main()
