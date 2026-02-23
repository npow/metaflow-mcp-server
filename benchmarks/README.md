# Benchmark: MCP Tools vs Code Generation

Do LLMs perform better calling structured MCP tools, or writing code to call APIs? We tested four approaches against 10 Metaflow tasks of increasing complexity.

## Approaches

| Approach | How it works |
|----------|-------------|
| **MCP Direct** | Claude Code calls the 7 Metaflow MCP tools directly |
| **CF Code Mode** | Cloudflare's [search+execute](https://blog.cloudflare.com/code-mode/) pattern — model discovers the API by writing code that queries the schema server-side, then writes code that calls the discovered functions |
| **Code Mode** | Claude Code writes and executes Python code using the Metaflow client library directly (bypasses MCP) |
| **Skill** | Same as Code Mode, but with the full Metaflow API reference (~4K tokens) embedded in the system prompt |

## Tasks

| ID | Category | Description |
|----|----------|-------------|
| `simple_config` | simple | What backend am I connected to? |
| `simple_list_runs` | simple | List last 3 runs of a flow |
| `medium_run_details` | medium | Step-by-step breakdown of a run |
| `medium_task_logs` | medium | Show stdout/stderr for a task |
| `medium_artifact_inspect` | medium | List artifacts, show one value |
| `complex_latest_failure` | complex | Find latest failure with error details |
| `complex_success_rate` | complex | Success rate of last 10 runs |
| `complex_compare_runs` | complex | Compare steps of 2 recent runs |
| `complex_artifact_diff` | complex | Compare artifacts across runs |
| `complex_debug_flow` | complex | Full investigation: count, rate, errors |

## Results (4 approaches x 3 models x 10 tasks = 120 runs)

```
Approach      Model      Tokens (med)  Tokens (min/max)  Time (med)  Total Cost  Score (med)  Score (min)
------------  -------  --------------  ----------------  ----------  ----------  -----------  -----------
mcp_direct    haiku               576       278 / 1232       45.8s      $0.033         1.00         0.50
mcp_direct    sonnet              669       327 / 1307       52.7s      $0.107         1.00         0.00
mcp_direct    opus                273       152 / 996        45.0s      $0.305         1.00         0.25
cf_code_mode  haiku               750       347 / 5467       50.1s      $0.062         1.00         0.00
cf_code_mode  sonnet             1896       690 / 5575       98.4s      $0.340         1.00         0.00
cf_code_mode  opus                771       323 / 2427       75.8s      $0.753         1.00         0.00
code_mode     haiku               652       318 / 5511       50.9s      $0.063         1.00         0.00
code_mode     sonnet              878       522 / 2299       54.4s      $0.173         1.00         0.50
code_mode     opus                488       259 / 2705       50.5s      $0.595         1.00         0.75
skill         haiku              1221       372 / 5684       54.4s      $0.088         1.00         0.00
skill         sonnet             1029       624 / 1610       63.2s      $0.163         1.00         0.50
skill         opus                576       275 / 1598       59.5s      $0.530         1.00         1.00
```

Correctness scored by LLM-as-judge (Sonnet) on a 0.0-1.0 scale against ground truth from the Metaflow API.

## Takeaways

1. **MCP tools win for small, focused APIs.** MCP Direct uses the fewest tokens and is the cheapest across all models. With only 7 well-designed tools, there's nothing for code generation to compress.

2. **CF Code Mode (Cloudflare's pattern) performed worst.** The search+execute two-phase approach adds overhead without benefit when there are only 7 tools to discover. All three models scored 0.0 on at least one task.

3. **MCP Direct + Haiku is the best value.** $0.033 total for 10 tasks, 45.8s median latency, median score of 1.0.

4. **Skill + Opus is the most reliable.** Perfect minimum score of 1.0 — the only configuration where every task was answered correctly.

5. **Code generation has high variance.** Code Mode token range for Haiku: 318-5511 (17x). MCP Direct: 278-1232 (4.4x). When code fails, the model retries and burns tokens.

6. **The crossover point matters.** CF Code Mode is designed for APIs with hundreds or thousands of endpoints. For 7 tools, it's pure overhead. The threshold where search+execute beats direct tools likely lies well above 7.

## Running the benchmark

```bash
pip install -e ".[benchmark]"

# Start the claude-relay (required)
claude-relay serve

# Full benchmark (12 workers in parallel)
python -m benchmarks --verbose

# Quick test: one approach, one model, two tasks
python -m benchmarks --approaches mcp_direct --models sonnet --tasks simple_config complex_success_rate

# Skip correctness judging
python -m benchmarks --skip-judge
```

Requires `claude-relay` running on `http://localhost:8082` (override with `RELAY_BASE_URL`).
