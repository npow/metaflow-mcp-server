"""LLM-as-judge correctness scoring via the claude-relay."""

import json
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

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

Important rules:
- Extra information or analysis in the candidate beyond the reference does NOT reduce \
the score. Only penalize for wrong facts or missing key facts from the reference.
- Timestamp differences of a few seconds due to rounding or display precision are NOT errors.
- If all verifiable facts in the candidate match the reference, score 1.0 even if the \
candidate includes additional unverifiable detail.

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
    max_workers: int = 3,
) -> list[TaskResult]:
    """Run the judge on all results and populate correctness fields.

    Judgments run in parallel (max_workers threads). Each judgment calls all
    JUDGE_MODELS sequentially inside judge_answer, but multiple results are
    judged concurrently, cutting total time by ~max_workers.

    Args:
        results: TaskResults with final_answer populated.
        questions: task_id -> original question.
        references: task_id -> reference answer string.
        checkpoint_path: if set, save progress every `checkpoint_every` completions.
        checkpoint_every: how often to checkpoint (default: 50).
        max_workers: parallel judgment threads (default: 16).

    Returns the same results list with correctness_score/rationale filled in.
    """
    _lock = threading.Lock()
    completed = [0]

    # Pre-score errors (no API call needed)
    pending = []
    for r in results:
        if r.correctness_score is not None:
            continue
        if r.error:
            r.correctness_score = 0.0
            r.correctness_rationale = f"Skipped: {r.error}"
            continue
        pending.append(r)

    already_done = len(results) - len(pending)
    if verbose and already_done:
        print(f"  {already_done} already scored or errored, judging {len(pending)} remaining")

    def _judge_one(r: TaskResult) -> None:
        question = questions.get(r.task_id, "")
        reference = references.get(r.task_id, "")
        score, rationale = judge_answer(question, reference, r.final_answer)
        r.correctness_score = score
        r.correctness_rationale = rationale

        with _lock:
            completed[0] += 1
            n = completed[0]
            if verbose:
                tag = f"{r.approach}/{r.model}/{r.task_id}/t{r.trial}"
                print(f"  [{n}/{len(pending)}] {tag}: {score} — {rationale[:60]}", flush=True)
            if checkpoint_path and n % checkpoint_every == 0:
                from dataclasses import asdict
                from pathlib import Path
                Path(checkpoint_path).write_text(
                    json.dumps([asdict(r2) for r2 in results], indent=2, default=str)
                )
                if verbose:
                    print(f"  [checkpoint at {n}/{len(pending)}]", flush=True)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(_judge_one, r) for r in pending]
        for f in as_completed(futures):
            f.result()  # re-raise any exception

    return results
