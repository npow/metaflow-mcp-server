# Benchmark: MCP Tools vs Code Generation

Do LLMs perform better calling structured MCP tools, or writing code to call APIs? We tested four approaches against 22 Metaflow tasks of increasing complexity across three model sizes (264 runs total).

## Approaches

| Approach | How it works |
|----------|-------------|
| **MCP Direct** | Claude calls the 10 Metaflow MCP tools directly. Minimal system prompt. |
| **Skill** | Identical to MCP Direct, but with a detailed Metaflow reference document prepended to every prompt. |
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
| `disambig_count_run_states` | disambiguation | Count ALL runs by exact state |
| `disambig_most_recent_state` | disambiguation | Classify the most recent run's state |
| `disambig_unfinished_not_failed` | disambiguation | Count 5 recent runs by state |
| `disambig_success_rate_finished_only` | disambiguation | Success rate among finished runs only |

Disambiguation tasks specifically test whether the model distinguishes between a crashed run (finished=False) and a currently-running run — they look identical at the API level.

## Results

Correctness scored by LLM-as-judge (Claude) on a 0.0–1.0 scale against ground truth computed directly from the Metaflow API.

### Accuracy by category (all models pooled)

| Approach | Overall | Simple | Medium | Complex | Hard | Disambiguation |
|----------|---------|--------|--------|---------|------|----------------|
| MCP Direct | **0.98** | 1.00 | 0.94 | 0.99 | 1.00 | 1.00 |
| Skill | **0.98** | 1.00 | 0.97 | 0.99 | 0.97 | 0.98 |
| Search+Execute | 0.95 | 0.94 | 0.99 | 0.93 | 0.89 | 1.00 |
| Code Mode | 0.88 | 0.86 | 0.89 | 0.88 | 0.83 | 0.94 |

The gap between MCP approaches and Code Mode is statistically significant (Wilcoxon signed-rank, p<0.01).

### Cost per task

| Approach | Average | Haiku | Sonnet | Opus |
|----------|---------|-------|--------|------|
| Code Mode | $0.56 | $0.12 | $0.29 | $1.28 |
| MCP Direct | $0.62 | $0.10 | $0.32 | $1.43 |
| Skill | $0.64 | $0.11 | $0.30 | $1.50 |
| Search+Execute | **$1.16** | $0.13 | $0.53 | $2.81 |

## Running the benchmark

```bash
pip install -e ".[benchmark]"

# Start claude-relay (required for subprocess Claude calls)
cd ~/code/claude-relay && uv run agent-relay serve --port 8082

# Full benchmark
python -m benchmarks --verbose

# Quick test: one approach, one model, two tasks
python -m benchmarks --approaches mcp_direct --models sonnet --tasks simple_config complex_success_rate

# Re-run judging only (after a crash)
python -m benchmarks --judge-only benchmarks/results.raw.json
```

Requires `claude-relay` on `http://localhost:8082` (override with `RELAY_BASE_URL`).
