"""JSON output and tabulate summary tables."""

import json
import statistics
from collections import defaultdict
from dataclasses import asdict
from pathlib import Path

from tabulate import tabulate

from benchmarks.config import TaskResult


def save_results(results: list[TaskResult], output_path: str) -> None:
    """Write all results to a JSON file."""
    data = [asdict(r) for r in results]
    Path(output_path).write_text(json.dumps(data, indent=2, default=str))
    print(f"\nResults saved to {output_path}")


def print_summary_table(results: list[TaskResult]) -> None:
    """Print per-task detail table."""
    headers = [
        "Approach", "Model", "Task", "In Tok", "Out Tok", "Total Tok",
        "Time(s)", "Cost($)", "Score", "Error",
    ]
    rows = []
    for r in results:
        rows.append([
            r.approach,
            r.model,
            r.task_id,
            r.input_tokens,
            r.output_tokens,
            r.total_tokens,
            r.wall_clock_seconds,
            f"${r.estimated_cost_usd:.4f}",
            f"{r.correctness_score:.2f}" if r.correctness_score is not None else "—",
            r.error[:30] if r.error else "",
        ])

    print("\n" + "=" * 110)
    print("DETAILED RESULTS")
    print("=" * 110)
    print(tabulate(rows, headers=headers, tablefmt="simple"))


def _stats(values: list[float]) -> dict:
    """Compute min, median, max, mean for a list of values."""
    if not values:
        return {"min": 0, "median": 0, "max": 0, "mean": 0}
    return {
        "min": min(values),
        "median": statistics.median(values),
        "max": max(values),
        "mean": statistics.mean(values),
    }


def print_aggregate_table(results: list[TaskResult]) -> None:
    """Print per-(approach, model) aggregate table with median, min, max."""
    groups: dict[tuple[str, str], list[TaskResult]] = defaultdict(list)
    for r in results:
        groups[(r.approach, r.model)].append(r)

    headers = [
        "Approach", "Model", "Tasks",
        "Tokens (med)", "Tokens (min)", "Tokens (max)",
        "Time (med)", "Time (min)", "Time (max)",
        "Total Cost($)",
        "Score (med)", "Score (min)", "Score (max)",
    ]
    rows = []
    for (approach, model), group in sorted(groups.items()):
        n = len(group)
        tok_stats = _stats([r.total_tokens for r in group])
        time_stats = _stats([r.wall_clock_seconds for r in group])
        total_cost = sum(r.estimated_cost_usd for r in group)

        scored = [r for r in group if r.correctness_score is not None]
        score_stats = _stats([r.correctness_score for r in scored]) if scored else None

        rows.append([
            approach,
            model,
            n,
            f"{tok_stats['median']:.0f}",
            f"{tok_stats['min']:.0f}",
            f"{tok_stats['max']:.0f}",
            f"{time_stats['median']:.1f}",
            f"{time_stats['min']:.1f}",
            f"{time_stats['max']:.1f}",
            f"${total_cost:.4f}",
            f"{score_stats['median']:.2f}" if score_stats else "—",
            f"{score_stats['min']:.2f}" if score_stats else "—",
            f"{score_stats['max']:.2f}" if score_stats else "—",
        ])

    print("\n" + "=" * 130)
    print("AGGREGATE RESULTS")
    print("=" * 130)
    print(tabulate(rows, headers=headers, tablefmt="simple"))
    print()
