"""LLM-as-judge correctness scoring via the claude-relay."""

import json

import anthropic

from benchmarks.config import RELAY_BASE_URL, RELAY_API_KEY, JUDGE_MODEL, JUDGE_MODELS, TaskResult


JUDGE_SYSTEM = """\
You are a strict judge evaluating whether an AI assistant's answer correctly \
addresses a question about Metaflow workflow data. You will be given:
1. The original question
2. A reference answer (ground truth from the Metaflow API)
3. The candidate answer to evaluate

Score the candidate on a 5-level scale:
- 1.0: Fully correct — all key facts match the reference
- 0.75: Mostly correct — minor omissions or formatting differences but core facts right
- 0.5: Partially correct — some key facts right but significant omissions or errors
- 0.25: Mostly wrong — only trivially correct elements
- 0.0: Completely wrong or no meaningful answer

Focus on factual correctness, not style. The candidate doesn't need to match \
the reference format exactly — it needs to convey the same key information.

Respond with ONLY a JSON object:
{"score": <float>, "rationale": "<brief explanation>"}
"""


def _judge_with_model(model: str, question: str, reference: str, candidate: str) -> tuple[float | None, str]:
    """Score using a single judge model. Returns (score, rationale)."""
    client = anthropic.Anthropic(base_url=RELAY_BASE_URL, api_key=RELAY_API_KEY)
    user_msg = (
        f"## Question\n{question}\n\n"
        f"## Reference Answer (ground truth)\n{reference}\n\n"
        f"## Candidate Answer\n{candidate}\n"
    )
    try:
        response = client.messages.create(
            model=model,
            max_tokens=512,
            system=JUDGE_SYSTEM,
            messages=[{"role": "user", "content": user_msg}],
        )
        text = response.content[0].text.strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[1].rsplit("```", 1)[0].strip()
        result = json.loads(text)
        return float(result["score"]), result.get("rationale", "")
    except Exception as e:
        return None, f"Judge error: {type(e).__name__}: {e}"


def judge_answer(question: str, reference: str, candidate: str, judge_models: list[str] | None = None) -> tuple[float, str]:
    """Score a candidate answer using an ensemble of judge models.

    Runs each judge model independently and returns the mean score.
    Returns (mean_score, rationale) where score is 0.0-1.0.
    """
    if judge_models is None:
        judge_models = JUDGE_MODELS

    scores = []
    rationales = []
    for model in judge_models:
        score, rationale = _judge_with_model(model, question, reference, candidate)
        if score is not None:
            scores.append(score)
            rationales.append(f"[{model}:{score:.2f}] {rationale}")

    if not scores:
        return None, "All judges failed: " + " | ".join(rationales)

    mean_score = sum(scores) / len(scores)
    return mean_score, " | ".join(rationales)


def evaluate_results(
    results: list[TaskResult],
    questions: dict[str, str],
    references: dict[str, str],
    verbose: bool = False,
    checkpoint_path: str | None = None,
    checkpoint_every: int = 50,
) -> list[TaskResult]:
    """Run the judge on all results and populate correctness fields.

    Args:
        results: TaskResults with final_answer populated.
        questions: task_id -> original question.
        references: task_id -> reference answer string.
        checkpoint_path: if set, save progress here every `checkpoint_every` items.
            On the next call with the same checkpoint_path, already-scored items
            (correctness_score is not None) are skipped, resuming from where it
            left off.
        checkpoint_every: how often to save a checkpoint (default: 50).

    Returns the same results list with correctness_score/rationale filled in.
    """
    for i, r in enumerate(results):
        # Resume: skip items that already have a score (e.g. from a checkpoint)
        if r.correctness_score is not None:
            if verbose:
                print(f"  Skipping {i+1}/{len(results)}: {r.approach}/{r.model}/{r.task_id} (already scored)")
            continue

        if r.error:
            r.correctness_score = 0.0
            r.correctness_rationale = f"Skipped: {r.error}"
            continue

        question = questions.get(r.task_id, "")
        reference = references.get(r.task_id, "")

        if verbose:
            print(f"  Judging {i+1}/{len(results)}: {r.approach}/{r.model}/{r.task_id}...", flush=True)

        score, rationale = judge_answer(question, reference, r.final_answer)
        r.correctness_score = score
        r.correctness_rationale = rationale

        if verbose:
            print(f"    Score: {score} — {rationale[:80]}")

        # Checkpoint: persist progress so a crash doesn't require re-judging from scratch
        if checkpoint_path and (i + 1) % checkpoint_every == 0:
            from dataclasses import asdict
            from pathlib import Path
            Path(checkpoint_path).write_text(json.dumps([asdict(r2) for r2 in results], indent=2, default=str))
            if verbose:
                print(f"  [checkpoint saved at {i+1}/{len(results)}]", flush=True)

    return results
