# Benchmark: MCP Tools vs Code Generation

Do LLMs perform better calling structured MCP tools, or writing code to call APIs? We tested four approaches against 27 Metaflow tasks of increasing complexity across three model sizes and three trials per cell (4 × 3 × 27 × 3 = 972 runs), scored by a two-judge ensemble (Sonnet + Opus).

## Approaches

| Approach | How it works |
|----------|-------------|
| **MCP Direct** | Claude calls the 10 Metaflow MCP tools directly. Minimal system prompt. |
| **Skill** | Identical to MCP Direct, but with a reference document prepended to every prompt. |
| **Code Mode** | No MCP tools. Claude writes Python or Bash against the Metaflow client library. |
| **Search+Execute** | No MCP tools. Inspired by [Cloudflare's search+execute pattern](https://blog.cloudflare.com/building-ai-agents-with-workers/). The model calls `search_tool_schemas(keyword)` to discover relevant API functions before writing code. |

## Tasks

| ID | Category | Prompt (abbreviated) |
|----|----------|-------------|
| `simple_config` | simple | What backend am I connected to? |
| `simple_list_flows` | simple | List available flows |
| `simple_list_runs` | simple | List last 3 runs of a flow |
| `medium_run_details` | medium | Step-by-step breakdown of a run |
| `medium_task_logs` | medium | Show stdout/stderr for a task |
| `medium_artifact_inspect` | medium | List artifacts, show one value |
| `medium_filtered_runs` | medium | List last 3 successful runs |
| `medium_bounded_logs` | medium | Last 10 lines of stderr |
| `medium_run_timing` | medium | Duration of each step in a run |
| `complex_latest_failure` | complex | Find latest failure with error details |
| `complex_success_rate` | complex | Success rate of last 10 runs |
| `complex_compare_runs` | complex | Compare steps of 2 recent runs |
| `complex_artifact_diff` | complex | Compare artifacts across 2 runs |
| `complex_artifact_search` | complex | Search for named artifact across runs |
| `complex_debug_flow` | complex | Count, success rate, and latest error |
| `hard_slowest_step` | hard | Slowest step across 2 recent runs |
| `hard_artifact_timeline` | hard | Artifact value across 3 runs, oldest-first |
| `hard_steps_per_flow` | hard | Which flow has the most steps? |
| `hard_run_census` | hard | Count ALL runs by state |
| `hard_fastest_run` | hard | Fastest of last 5 successful runs |
| `hard_median_run_duration` | hard | Median duration of last 5 successful runs |
| `hard_cross_flow_status` | hard | Status breakdown across multiple flows |
| `hard_slowest_across_runs` | hard | Slowest (run, step) across 3 runs |
| `disambig_count_run_states` | disambiguation | Count ALL runs by exact state |
| `disambig_most_recent_state` | disambiguation | Classify the most recent run's state |
| `disambig_unfinished_not_failed` | disambiguation | Count 5 recent runs by state |
| `disambig_success_rate_finished_only` | disambiguation | Success rate among finished runs only |

Disambiguation tasks specifically test whether the model distinguishes between a crashed run (finished=False) and a currently-running run — they look identical at the API level.

## Results

Correctness scored by a two-judge ensemble (Sonnet + Opus), averaged per result, on a 0.0–1.0 scale against ground truth computed directly from the Metaflow API. Each (approach, model, task) cell is run 3 times; scores are averaged across trials before computing statistics.

### Accuracy by category (all models pooled)

| Approach | Overall | Simple | Medium | Complex | Hard | Disambiguation |
|----------|---------|--------|--------|---------|------|----------------|
| MCP Direct | **1.00** | 1.00 | 1.00 | 0.99 | 0.99 | 1.00 |
| Skill | 0.99 | 1.00 | 1.00 | 1.00 | 0.98 | 1.00 |
| Search+Execute | 0.99 | 1.00 | 1.00 | 1.00 | 0.98 | 1.00 |
| Code Mode | 0.97 | 0.86 | 1.00 | 0.95 | 0.98 | 1.00 |

Tool-based approaches cluster at 0.99+. Code Mode trails at 0.97, with the gap concentrated in Simple (config interpretation) and Complex (failure detection) tasks.

MCP Direct vs Code Mode: consistent 2.7% gap (Wilcoxon p=0.107, n=27 tasks).
MCP Direct vs Skill: tied on accuracy (p=0.68), but Skill costs 6–14% more tokens (p < 0.001).

### Token cost per task (mean)

| Approach | Haiku | Sonnet | Opus |
|----------|-------|--------|------|
| MCP Direct | 1,022 | 668 | 433 |
| Skill | 1,111 | 707 | 491 |
| Code Mode | 1,716 | 562 | 482 |
| Search+Execute | 1,362 | 719 | 839 |

## Running the benchmark

```bash
pip install -e ".[benchmark]"

# Start claude-relay (required for subprocess Claude calls)
pip install claude-relay
CLAUDE_RELAY_CWD=/path/to/metaflow-mcp-server agent-relay serve --port 18082

# Full benchmark (3 trials per cell, 2-judge ensemble)
RELAY_BASE_URL=http://localhost:18082 python -m benchmarks --verbose --only-flows StatusTestFlow DiagnoseFlow BenchmarkFlow

# Quick test: one approach, one model, two tasks, one trial
python -m benchmarks --approaches mcp_direct --models sonnet --tasks simple_config complex_success_rate --trials 1

# Re-run judging only (after a crash)
python -m benchmarks --judge-only benchmarks/results.raw.json
```

Requires `claude-relay` on `http://localhost:18082` (override with `RELAY_BASE_URL`).
