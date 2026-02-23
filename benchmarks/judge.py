"""LLM-as-judge correctness scoring via the claude-relay."""

import json

import anthropic

from benchmarks.config import RELAY_BASE_URL, RELAY_API_KEY, JUDGE_MODEL, TaskResult


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


def judge_answer(question: str, reference: str, candidate: str) -> tuple[float, str]:
    """Score a candidate answer against a reference using LLM-as-judge.

    Returns (score, rationale) where score is 0.0-1.0.
    """
    client = anthropic.Anthropic(base_url=RELAY_BASE_URL, api_key=RELAY_API_KEY)

    user_msg = (
        f"## Question\n{question}\n\n"
        f"## Reference Answer (ground truth)\n{reference}\n\n"
        f"## Candidate Answer\n{candidate}\n"
    )

    try:
        response = client.messages.create(
            model=JUDGE_MODEL,
            max_tokens=512,
            system=JUDGE_SYSTEM,
            messages=[{"role": "user", "content": user_msg}],
        )
        text = response.content[0].text.strip()
        # Parse JSON from the response (handle markdown code blocks)
        if text.startswith("```"):
            text = text.split("\n", 1)[1].rsplit("```", 1)[0].strip()
        result = json.loads(text)
        return float(result["score"]), result.get("rationale", "")
    except Exception as e:
        return None, f"Judge error: {type(e).__name__}: {e}"


def evaluate_results(
    results: list[TaskResult],
    questions: dict[str, str],
    references: dict[str, str],
    verbose: bool = False,
) -> list[TaskResult]:
    """Run the judge on all results and populate correctness fields.

    Args:
        results: TaskResults with final_answer populated.
        questions: task_id -> original question.
        references: task_id -> reference answer string.

    Returns the same results list with correctness_score/rationale filled in.
    """
    for i, r in enumerate(results):
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

    return results
