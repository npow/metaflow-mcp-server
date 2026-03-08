# Give the Model Tools, Not a Terminal

There are two ways to give an AI assistant access to your system:

- Expose your API as structured tools and let the model call them.
- Give the model a terminal and let it write code.

We ran a benchmark to find out which works better.

---

## The Setup

[Metaflow](https://metaflow.org) is Netflix's open-source framework for building ML pipelines. It tracks every run, every step, every artifact, every failure. Exactly the kind of system where you'd want an AI assistant.

We tested four approaches:

- **MCP Direct** — Claude has purpose-built tools wrapping the Metaflow API. Minimal system prompt. Calls tools, answers when it has enough.
- **Skill** — Identical to MCP Direct, but with a detailed reference document describing Metaflow concepts added to every prompt.
- **Code Mode** — No tools. Claude writes Python or Bash against the Metaflow client library.
- **Search+Execute** — A hybrid from a [Cloudflare post](https://blog.cloudflare.com/building-ai-agents-with-workers/) on tool-augmented agents. No tools, but the model can call `search_tool_schemas()` to discover API functions by keyword before writing code.

22 tasks across five difficulty levels:

- **Simple** — basic lookups: what backend am I connected to, list flows, show recent runs
- **Medium** — single-object queries: step breakdown of a run, task logs, artifact values, step timing
- **Complex** — multi-step reasoning: success rate over 10 runs, artifact diff between runs, finding the most recent failure
- **Hard** — aggregation across runs or flows: slowest step, artifact value over time, step count per flow
- **Disambiguation** — run state classification: distinguishing a crashed run from an actively-running one, computing success rate only over finished runs

For each task, we first computed a reference answer by running the Metaflow API directly — no model involved. Then we ran each approach three times per task (to average out stochastic variation) and had three separate judge models score each answer independently. The final score is the mean across judge models, from 0 to 1. The numbers below are averages across tasks, trials, and model sizes.

---

## Results

### Accuracy

| Approach | Overall | Simple | Medium | Complex | Hard | Disambiguation |
|---|---|---|---|---|---|---|
| MCP Direct | **0.96** | 1.00 | 0.92 | 0.98 | 0.93 | 0.99 |
| Skill | **0.97** | 0.99 | 0.94 | 0.98 | 0.96 | 1.00 |
| Search+Execute | **0.97** | 1.00 | 0.92 | 0.97 | 0.99 | 1.00 |
| Code Mode | 0.90 | 0.88 | 0.92 | 0.95 | **0.71** | 0.96 |

The gap between MCP approaches and Code Mode is consistent across all model sizes and task categories — roughly 6–7 percentage points overall, growing to 25 points on hard aggregation tasks. With 22 tasks the effect doesn't reach conventional significance thresholds, but the direction is uniform.

### Cost per task

| Approach | Average | Haiku | Sonnet | Opus |
|---|---|---|---|---|
| Code Mode | $0.71 | $0.20 | $0.32 | $1.59 |
| Skill | $0.72 | $0.12 | $0.36 | $1.67 |
| MCP Direct | $0.73 | $0.12 | $0.38 | $1.69 |
| Search+Execute | **$1.37** | $0.29 | $0.60 | $3.23 |

---

## What the Numbers Mean

### The failure mode matters more than the rate

One task asked: *Count the runs of this flow by state — how many are currently running, how many finished successfully, how many finished with a failure?*

The reference answer: 20 finished successfully, 10 unfinished (not yet complete), 0 failures.

Two code-writing runs from Haiku returned this instead:

> | Category | Count |
> |----------|-------|
> | **Currently running or unfinished** (finished=False) | **0** |
> | **Finished successfully** (finished=True AND successful=True) | **20** |
> | **Finished with failure** (finished=True AND successful=False) | **10** |

The counts are exactly inverted for two of the three categories. Twenty successful runs — correct. Zero unfinished — wrong (there are ten). Ten failures — wrong (there are zero). The model read the right data and applied the wrong logic: it filtered `finished=False` runs out and then called the remaining non-successful ones "failures," missing that non-successful doesn't mean finished-unsuccessfully.

The MCP-based answer to the same question:

> | Category | Count |
> |----------|-------|
> | Currently running or unfinished | **10** |
> | Finished successfully | **20** |
> | Finished with failure | **0** |

The MCP tool encodes the classification logic directly. The model never has to reason through the distinction.

A wrong answer that looks uncertain is easy to catch. A wrong answer with a plausible-looking table backed by real counts is not.

### Hard tasks expose the real gap

For simple and medium tasks, all approaches score within a few points of each other. The gap opens on hard tasks — multi-step aggregations that require querying multiple objects and combining the results.

Code Mode scores 0.71 on hard tasks. MCP approaches score 0.93–0.99.

When the task can be answered with a single query, writing code works fine. When it requires coordinating multiple queries — find the slowest step across all runs, compare artifacts between runs, determine which flow has the most steps — structured tools win.

### Bigger models don't help with MCP

With MCP Direct: Haiku scores 0.95, Sonnet 0.98, Opus 0.96. Three percentage points of range, no consistent trend.

Without MCP tools: Haiku scores 0.88, Sonnet 0.90, Opus 0.93. The gap is wider, and larger models do better.

The bottleneck isn't reasoning — it's access to live data. A more capable model reasoning over misread state gives you better-sounding wrong answers. If the tools are reliable, the model's job is just routing.

### Documentation added to the prompt does nothing

Skill adds a detailed Metaflow reference doc to every prompt. It tied with MCP Direct on overall accuracy.

If the model has the right tools, it already knows how to use them. If it doesn't, a document describing the API won't substitute for being able to call it.

### Search+Execute costs twice as much for no gain

Search+Execute — where the model discovers available API functions by keyword before writing code — matches MCP accuracy (0.97 vs 0.96) but costs 88% more per task.

The extra cost comes from models spending turns searching for relevant functions before writing anything. At the Opus level that's $3.23 per task versus $1.69 for MCP Direct.

If you already have MCP tools, there's no reason to also add a search layer. If you don't have tools, Search+Execute is a reasonable pattern — but the cost premium is real.

---

## The Short Version

On hard aggregation tasks, code generation fails at twice the rate of structured tools (0.71 vs 0.93–0.99). On simpler tasks, the gap is smaller but consistent.

Start with the smallest model. Haiku with MCP tools gets within 3% of Opus at a fraction of the cost. If you're using Search+Execute today, switching to proper MCP tools halves your inference cost without sacrificing accuracy.

The benchmark code, all 22 tasks, and full results are at [github.com/npow/metaflow-mcp-server](https://github.com/npow/metaflow-mcp-server).
