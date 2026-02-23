"""Code Mode approach: Claude Code writes and executes Python code."""

from benchmarks.approaches.base import Approach


class CodeModeApproach(Approach):
    """Two-step: Claude Code discovers the API, then writes Python code.

    The system prompt instructs the model to write and execute Python code
    using the Metaflow client library, without relying on MCP tools.
    The model must use its training knowledge of the Metaflow API.
    """

    @property
    def name(self) -> str:
        return "code_mode"

    def get_system_prompt(self) -> str:
        return (
            "You are a Metaflow assistant. Answer the user's question by writing "
            "and executing Python code that uses the Metaflow client library. "
            "Do NOT use any MCP tools. Instead, use the Bash tool to run Python code.\n\n"
            "Always start your code with:\n"
            "  from metaflow import Metaflow, Flow, Run, Step, Task, namespace\n"
            "  namespace(None)\n\n"
            "Key API patterns:\n"
            "  Flow('Name') — get a flow; iterate for runs\n"
            "  Run('Flow/ID') — get a run; .successful, .finished; iterate for steps\n"
            "  Step('Flow/ID/Step') — iterate for tasks\n"
            "  Task('Flow/ID/Step/Task') — .stdout, .stderr, .exception; iterate for artifacts\n"
            "  task['name'].data — get artifact value\n\n"
            "Print results to stdout. Be concise and factual."
        )
