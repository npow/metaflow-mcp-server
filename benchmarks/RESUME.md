# Resuming the Benchmark

## Quick Start

```bash
# 1. Install deps
cd ~/code/metaflow-mcp-server
pip install -e ".[benchmark]"

# 2. Install and start the relay (separate terminal)
cd ~/code/claude-relay
pip install -e .
claude-relay serve
# Relay runs on http://localhost:8082
# NOTE: server.py has --dangerously-skip-permissions added to build_claude_cmd()

# 3. Run the benchmark
cd ~/code/metaflow-mcp-server
python -m benchmarks --verbose
```

## What This Runs

4 approaches x 3 models x 10 tasks = 120 combinations, 12 parallel workers.
Plus 120 LLM-as-judge evaluations (sequential, through the relay).

Total wall time: ~60-90 minutes.

## CLI Options

```bash
# Full run with judge
python -m benchmarks --verbose

# Skip judge (faster, no correctness scores)
python -m benchmarks --skip-judge --verbose

# Subset
python -m benchmarks --approaches mcp_direct --models sonnet --tasks simple_config
python -m benchmarks --approaches mcp_direct cf_code_mode --models haiku sonnet

# Override relay URL
RELAY_BASE_URL=http://other-host:8082 python -m benchmarks
```

## Known Issue: `complex_debug_flow` and Run 30

DiagnoseFlow has an unfinished Run 30 (S3 access denied). This causes scoring mismatches on:
- `complex_debug_flow` — models report 9 finished out of 10, some also report Run 30's error
- `complex_success_rate` — same counting issue

The prompts and reference functions have been aligned to ask about "the 10 most recent runs" (matching what `search_runs(last_n=10)` returns). But models sometimes still go beyond the prompt and scan deeper or report Run 30's error, causing score deductions.

If you have a flow with 10+ consecutive finished runs, you can force it by editing `discover.py` to hardcode the flow name. Or just accept the ~0.5-0.75 scores on that one task as stochastic variance.

## Output

- `benchmarks/results.json` — full results with all fields
- Aggregate table printed to stdout

## Blog Post

The blog post is at `~/code/npow.github.io/content/posts/mcp-tools-vs-code-generation.md`.
Update the tables with new results after a run. Build with: `cd ~/code/npow.github.io && hugo`

## Files Modified Outside This Repo

- `~/code/claude-relay/src/claude_relay/server.py` — added `--dangerously-skip-permissions` to `build_claude_cmd()` (line 128-129). Without this, Claude Code asks for tool permission instead of executing.
