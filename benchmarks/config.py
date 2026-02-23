"""Constants, models, pricing, and data structures for the benchmark suite."""

import os
from dataclasses import dataclass
from typing import Optional

# Relay config — claude-relay proxies to Claude Code CLI
RELAY_BASE_URL = os.environ.get("RELAY_BASE_URL", "http://localhost:8082")
RELAY_API_KEY = "not-needed"  # relay doesn't require an API key

# Model names — passed through relay to claude --model
MODELS = {
    "haiku": "haiku",
    "sonnet": "sonnet",
    "opus": "opus",
}

# Per-million-token pricing (USD)
PRICING = {
    "haiku": {"input": 1.00, "output": 5.00},
    "sonnet": {"input": 3.00, "output": 15.00},
    "opus": {"input": 15.00, "output": 75.00},
}

# Harness limits
MAX_TOKENS = 16384

# Judge model
JUDGE_MODEL = "sonnet"


def estimate_cost(model_id: str, input_tokens: int, output_tokens: int) -> float:
    """Estimate cost in USD for a given model and token counts."""
    rates = PRICING.get(model_id, {"input": 0.0, "output": 0.0})
    return (input_tokens * rates["input"] + output_tokens * rates["output"]) / 1_000_000


@dataclass
class TaskResult:
    """Result of running a single (approach, model, task) combination."""

    approach: str
    model: str
    task_id: str
    input_tokens: int = 0
    output_tokens: int = 0
    total_tokens: int = 0
    wall_clock_seconds: float = 0.0
    num_turns: int = 0
    num_tool_calls: int = 0
    final_answer: str = ""
    correctness_score: Optional[float] = None
    correctness_rationale: str = ""
    estimated_cost_usd: float = 0.0
    error: Optional[str] = None
