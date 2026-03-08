"""Code Mode: Claude Code writes and executes Python code via Bash."""

from benchmarks.approaches.base import Approach


class CodeModeApproach(Approach):
    """Model uses training knowledge of the Metaflow Python client API.
    No MCP tools. Writes and runs Python via Bash."""

    @property
    def name(self) -> str:
        return "code_mode"

    def get_system_prompt(self) -> str:
        return (
            "You are a Metaflow assistant. Use the Bash tool to run Python code "
            "against the Metaflow client library. Do not use MCP tools.\n"
            "Always begin: from metaflow import Metaflow, Flow, Run, Step, Task, namespace; namespace(None)"
        )
