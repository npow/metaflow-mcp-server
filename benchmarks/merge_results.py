"""Merge partial benchmark results into a base results file.

Usage:
    uv run python benchmarks/merge_results.py \\
        --base benchmarks/results_v4.json \\
        --patch benchmarks/results_cfopus_v2.json \\
        --output benchmarks/results_v5.json

Replaces rows in --base where (approach, model) matches any row in --patch,
then re-sorts and prints a summary table.
"""

import argparse
import json
from pathlib import Path


APPROACH_ORDER = ["mcp_direct", "skill", "cf_code_mode", "code_mode"]
MODEL_ORDER = ["haiku", "sonnet", "opus"]


def sort_key(r: dict) -> tuple:
    a = APPROACH_ORDER.index(r["approach"]) if r["approach"] in APPROACH_ORDER else 99
    m = MODEL_ORDER.index(r["model"]) if r["model"] in MODEL_ORDER else 99
    return (a, m, r.get("task_id", ""))


def print_summary(results: list[dict]) -> None:
    from collections import defaultdict

    scores: dict[tuple, list[float]] = defaultdict(list)
    costs: dict[tuple, list[float]] = defaultdict(list)
    for r in results:
        key = (r["approach"], r["model"])
        if r.get("correctness_score") is not None:
            scores[key].append(r["correctness_score"])
        if r.get("estimated_cost_usd") is not None:
            costs[key].append(r["estimated_cost_usd"])

    print(f"\n{'Approach/model':<30} {'n':>4} {'avg':>6}  {'total $':>8}  {'$/task':>7}")
    print("-" * 62)
    for approach in APPROACH_ORDER:
        for model in MODEL_ORDER:
            key = (approach, model)
            if key in scores:
                vals = scores[key]
                c = costs.get(key, [])
                total = sum(c)
                per_task = total / len(c) if c else 0.0
                print(
                    f"  {approach}/{model:<20} {len(vals):>4} {sum(vals)/len(vals):>6.3f}"
                    f"  ${total:>7.2f}  ${per_task:>6.3f}"
                )


def main() -> None:
    parser = argparse.ArgumentParser(description="Merge partial benchmark results.")
    parser.add_argument("--base", required=True, help="Base results JSON (e.g. results_v4.json)")
    parser.add_argument("--patch", required=True, help="New partial results JSON to splice in")
    parser.add_argument("--output", required=True, help="Output merged JSON path")
    parser.add_argument(
        "--by",
        choices=["approach_model", "row"],
        default="approach_model",
        help=(
            "Merge key: 'approach_model' replaces all rows for matching (approach, model); "
            "'row' replaces individual (approach, model, task_id) triplets (default: approach_model)"
        ),
    )
    args = parser.parse_args()

    base = json.loads(Path(args.base).read_text())
    patch = json.loads(Path(args.patch).read_text())

    if args.by == "row":
        patch_keys = {(r["approach"], r["model"], r["task_id"]) for r in patch}
        print(f"Base:  {len(base)} rows")
        print(f"Patch: {len(patch)} rows covering {patch_keys}")
        kept = [r for r in base if (r["approach"], r["model"], r["task_id"]) not in patch_keys]
    else:
        # Determine which (approach, model) combos the patch covers
        patch_keys = {(r["approach"], r["model"]) for r in patch}
        print(f"Base:  {len(base)} rows")
        print(f"Patch: {len(patch)} rows covering {patch_keys}")
        kept = [r for r in base if (r["approach"], r["model"]) not in patch_keys]

    removed = len(base) - len(kept)
    merged = sorted(kept + patch, key=sort_key)

    print(f"Removed {removed} rows from base, added {len(patch)} from patch → {len(merged)} total")

    Path(args.output).write_text(json.dumps(merged, indent=2, default=str))
    print(f"Written to {args.output}")

    print_summary(merged)


if __name__ == "__main__":
    main()
