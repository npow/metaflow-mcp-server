"""Skill approach: full API reference in system prompt + bash execution."""

from benchmarks.approaches.base import Approach
from benchmarks.api_docs import generate_api_reference, format_api_reference


class SkillApproach(Approach):
    """Full Metaflow API reference embedded in the system prompt.

    Like Code Mode, but with a complete API reference provided upfront
    so the model doesn't need to rely on training knowledge. Instructs
    the model to write and execute Python code via bash.
    """

    def __init__(self):
        self._api_ref: str | None = None

    @property
    def api_ref(self) -> str:
        if self._api_ref is None:
            entries = generate_api_reference()
            self._api_ref = format_api_reference(entries)
        return self._api_ref

    @property
    def name(self) -> str:
        return "skill"

    def get_system_prompt(self) -> str:
        return (
            "You are a Metaflow assistant. Answer the user's question by writing "
            "and executing Python code that uses the Metaflow client library. "
            "Do NOT use any MCP tools. Instead, use the Bash tool to run Python code.\n\n"
            "Always start your code with:\n"
            "  from metaflow import Metaflow, Flow, Run, Step, Task, namespace\n"
            "  namespace(None)\n\n"
            "Here is the complete Metaflow Python client API reference:\n\n"
            f"{self.api_ref}\n\n"
            "Print results to stdout. Be concise and factual."
        )
