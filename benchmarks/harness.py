"""Send prompts through the claude-relay and collect results."""

import time

import anthropic

from benchmarks.approaches.base import Approach
from benchmarks.config import (
    RELAY_BASE_URL,
    RELAY_API_KEY,
    MAX_TOKENS,
    TaskResult,
    estimate_cost,
)


def _make_client() -> anthropic.Anthropic:
    """Create an Anthropic client pointing at the claude-relay."""
    return anthropic.Anthropic(
        base_url=RELAY_BASE_URL,
        api_key=RELAY_API_KEY,
        timeout=300.0,
    )


def run_task(
    approach: Approach,
    model_id: str,
    model_name: str,
    task_id: str,
    user_prompt: str,
    verbose: bool = False,
) -> TaskResult:
    """Run a single (approach, model, task) through the relay.

    The relay proxies to Claude Code, which handles all tool execution
    internally (MCP tools, bash, etc.). We send one request and get
    back the final answer with token usage.
    """
    client = _make_client()
    system_prompt = approach.get_system_prompt()

    input_tokens = 0
    output_tokens = 0
    final_answer = ""
    error = None

    start_time = time.monotonic()

    try:
        if verbose:
            print(f"    Calling relay ({model_id})...", end="", flush=True)

        response = client.messages.create(
            model=model_id,
            max_tokens=MAX_TOKENS,
            system=system_prompt,
            messages=[{"role": "user", "content": user_prompt}],
        )

        input_tokens = response.usage.input_tokens
        output_tokens = response.usage.output_tokens

        # Extract text from response
        text_parts = []
        for block in response.content:
            if block.type == "text":
                text_parts.append(block.text)
        final_answer = "\n".join(text_parts)

        if verbose:
            print(f" ({input_tokens}+{output_tokens} tokens)", flush=True)

    except Exception as e:
        error = f"{type(e).__name__}: {e}"
        if verbose:
            print(f" ERROR: {error}", flush=True)

    wall_clock = time.monotonic() - start_time
    total_tokens = input_tokens + output_tokens
    cost = estimate_cost(model_id, input_tokens, output_tokens)

    return TaskResult(
        approach=approach.name,
        model=model_name,
        task_id=task_id,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        total_tokens=total_tokens,
        wall_clock_seconds=round(wall_clock, 2),
        num_turns=1,
        num_tool_calls=0,  # tool calls happen inside Claude Code
        final_answer=final_answer,
        estimated_cost_usd=round(cost, 4),
        error=error,
    )
