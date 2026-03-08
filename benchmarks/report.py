"""JSON output and tabulate summary tables."""

import json
import math
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


def _cell_scores(results: list[TaskResult]) -> dict[tuple[str, str, str], float]:
    """Average correctness scores across trials per (approach, model, task) cell.

    With multiple trials per cell, this collapses them to one representative score
    before any statistical analysis. Single-trial results (trial=0 only) are unchanged.
    """
    raw: dict[tuple[str, str, str], list[float]] = defaultdict(list)
    for r in results:
        if r.correctness_score is not None:
            raw[(r.approach, r.model, r.task_id)].append(r.correctness_score)
    return {k: sum(v) / len(v) for k, v in raw.items()}


def _cell_costs(results: list[TaskResult]) -> dict[tuple[str, str, str], float]:
    """Average cost per trial per (approach, model, task) cell."""
    raw: dict[tuple[str, str, str], list[float]] = defaultdict(list)
    for r in results:
        raw[(r.approach, r.model, r.task_id)].append(r.estimated_cost_usd)
    return {k: sum(v) / len(v) for k, v in raw.items()}


def _cell_tokens(results: list[TaskResult]) -> dict[tuple[str, str, str], float]:
    """Average total tokens per trial per (approach, model, task) cell."""
    raw: dict[tuple[str, str, str], list[float]] = defaultdict(list)
    for r in results:
        raw[(r.approach, r.model, r.task_id)].append(r.total_tokens)
    return {k: sum(v) / len(v) for k, v in raw.items()}


def print_category_table(results: list[TaskResult], tasks_by_id: dict) -> None:
    """Print per-(approach, category) accuracy breakdown plus cost summary.

    Scores are averaged across trials per cell before aggregating by category.
    """
    scores_map = _cell_scores(results)
    costs_map = _cell_costs(results)

    # (approach, category) → list of task-level cell scores (already trial-averaged)
    groups: dict[tuple[str, str], list[float]] = defaultdict(list)
    for (approach, model, task_id), score in scores_map.items():
        cat = tasks_by_id.get(task_id, "unknown")
        groups[(approach, cat)].append(score)

    # per-approach cost: mean cost per cell, summed across all (model, task) cells
    cost_by_approach: dict[str, list[float]] = defaultdict(list)
    for (approach, model, task_id), cost in costs_map.items():
        cost_by_approach[approach].append(cost)

    if not groups:
        return

    categories = sorted(set(cat for _, cat in groups))
    headers = (
        ["Approach"]
        + [f"{c} (avg)" for c in categories]
        + ["overall (avg)", "total $", "$/task"]
    )
    approaches = list(dict.fromkeys(r.approach for r in results))
    rows = []
    for approach in approaches:
        row = [approach]
        all_scores = []
        for cat in categories:
            scores = groups.get((approach, cat), [])
            all_scores.extend(scores)
            row.append(f"{statistics.mean(scores):.2f}" if scores else "—")
        row.append(f"{statistics.mean(all_scores):.2f}" if all_scores else "—")
        costs = cost_by_approach.get(approach, [])
        total = sum(costs)
        per_task = total / len(costs) if costs else 0.0
        row.append(f"${total:.2f}")
        row.append(f"${per_task:.3f}")
        rows.append(row)

    print("\n" + "=" * 120)
    print("SCORES BY CATEGORY  (avg correctness per approach, all models pooled, trials averaged per cell)")
    print("=" * 120)
    print(tabulate(rows, headers=headers, tablefmt="simple"))


def _ci95(scores: list[float]) -> tuple[float, float]:
    """95% CI for a mean using t-distribution approximation (t≈2.0 for n≥30, 2.08 for n=22)."""
    n = len(scores)
    if n < 2:
        mean = scores[0] if scores else 0.0
        return (mean, mean)
    mean = sum(scores) / n
    std = statistics.stdev(scores)
    # t critical value: 2.08 for df=21 (n=22), 2.0 for larger n
    t = 2.08 if n <= 22 else (2.042 if n <= 30 else 2.0)
    margin = t * std / math.sqrt(n)
    return (mean - margin, mean + margin)


def print_aggregate_table(results: list[TaskResult]) -> None:
    """Print per-(approach, model) aggregate table with median, min, max, and 95% CI.

    Statistics are computed over task-level means (trials averaged per cell),
    so each row represents n=tasks data points regardless of trial count.
    """
    scores_map = _cell_scores(results)
    costs_map = _cell_costs(results)
    tokens_map = _cell_tokens(results)

    # Group cell-level data by (approach, model)
    score_groups: dict[tuple[str, str], list[float]] = defaultdict(list)
    cost_groups: dict[tuple[str, str], list[float]] = defaultdict(list)
    tok_groups: dict[tuple[str, str], list[float]] = defaultdict(list)
    for (approach, model, task_id), score in scores_map.items():
        score_groups[(approach, model)].append(score)
    for (approach, model, task_id), cost in costs_map.items():
        cost_groups[(approach, model)].append(cost)
    for (approach, model, task_id), tok in tokens_map.items():
        tok_groups[(approach, model)].append(tok)

    # Preserve approach/model ordering from results
    order = list(dict.fromkeys((r.approach, r.model) for r in results))

    headers = [
        "Approach", "Model", "Tasks",
        "Tokens (med)", "Tokens (max)",
        "$/task",
        "Score (avg)", "95% CI",
        "Score (min)", "Score (max)",
    ]
    rows = []
    for key in order:
        score_vals = score_groups.get(key, [])
        cost_vals = cost_groups.get(key, [])
        tok_vals = tok_groups.get(key, [])
        n = len(score_vals)
        tok_stats = _stats(tok_vals) if tok_vals else None
        score_stats = _stats(score_vals) if score_vals else None
        ci = _ci95(score_vals) if score_vals else None
        avg_cost = sum(cost_vals) / len(cost_vals) if cost_vals else 0.0

        rows.append([
            key[0],
            key[1],
            n,
            f"{tok_stats['median']:.0f}" if tok_stats else "—",
            f"{tok_stats['max']:.0f}" if tok_stats else "—",
            f"${avg_cost:.3f}",
            f"{score_stats['mean']:.3f}" if score_stats else "—",
            f"[{ci[0]:.3f}, {ci[1]:.3f}]" if ci else "—",
            f"{score_stats['min']:.2f}" if score_stats else "—",
            f"{score_stats['max']:.2f}" if score_stats else "—",
        ])

    num_tasks = len({task_id for (_, _, task_id) in scores_map})
    print("\n" + "=" * 120)
    print(f"AGGREGATE RESULTS  (95% CI uses t-distribution, n={num_tasks} tasks per row, trials averaged per cell)")
    print("=" * 120)
    print(tabulate(rows, headers=headers, tablefmt="simple"))
    print()


def print_significance_table(results: list[TaskResult]) -> None:
    """Paired Wilcoxon signed-rank test: approach A vs approach B across tasks.

    Scores are first averaged across trials per (approach, model, task) cell,
    then averaged across models per (approach, task) to produce one score per task.
    Wilcoxon is applied to these task-level paired vectors.
    """
    try:
        from scipy.stats import wilcoxon
    except ImportError:
        print("\n(scipy not available — skipping significance tests)")
        return

    # Step 1: trial-average per (approach, model, task)
    scores_map = _cell_scores(results)

    # Step 2: average across models per (approach, task)
    cell: dict[tuple[str, str], list[float]] = defaultdict(list)
    for (approach, model, task_id), score in scores_map.items():
        cell[(approach, task_id)].append(score)
    task_avg: dict[tuple[str, str], float] = {k: sum(v) / len(v) for k, v in cell.items()}

    # Collect task IDs
    all_tasks = sorted({tid for (_, tid) in task_avg})
    approaches = ["mcp_direct", "skill", "cf_code_mode", "code_mode"]
    # Filter to approaches actually present
    approaches = [a for a in approaches if any((a, t) in task_avg for t in all_tasks)]

    # Build score vectors per approach (only tasks present in all approaches)
    common_tasks = [t for t in all_tasks if all((a, t) in task_avg for a in approaches)]

    vectors: dict[str, list[float]] = {
        a: [task_avg[(a, t)] for t in common_tasks] for a in approaches
    }

    print("\n" + "=" * 90)
    print(f"PAIRWISE SIGNIFICANCE  (Wilcoxon signed-rank, n={len(common_tasks)} tasks, two-sided, trials averaged per cell)")
    print("=" * 90)

    pairs = []
    for i, a in enumerate(approaches):
        for b in approaches[i + 1:]:
            va, vb = vectors[a], vectors[b]
            diff = [x - y for x, y in zip(va, vb)]
            # Wilcoxon requires at least some non-zero differences
            if all(d == 0 for d in diff):
                pairs.append([a, b, "—", "—", "n.s.", "0.000"])
                continue
            try:
                stat, pval = wilcoxon(diff, alternative="two-sided")
            except ValueError:
                pairs.append([a, b, "—", "—", "n.s.", "—"])
                continue
            avg_a = sum(va) / len(va)
            avg_b = sum(vb) / len(vb)
            sig = "***" if pval < 0.001 else ("**" if pval < 0.01 else ("*" if pval < 0.05 else "n.s."))
            pairs.append([a, b, f"{avg_a:.3f}", f"{avg_b:.3f}", sig, f"{pval:.4f}"])

    print(tabulate(pairs, headers=["Approach A", "Approach B", "Avg A", "Avg B", "Sig", "p-value"],
                   tablefmt="simple"))
    print("  Significance: *** p<0.001  ** p<0.01  * p<0.05  n.s. not significant")
