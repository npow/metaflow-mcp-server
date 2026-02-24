"""MCP Direct approach: Claude Code uses its configured Metaflow MCP tools."""

from benchmarks.approaches.base import Approach


class MCPDirectApproach(Approach):
    """Baseline: Claude Code calls the 9 Metaflow MCP tools directly.

    Relies on the metaflow-mcp-server being configured as an MCP server
    in Claude Code. The system prompt instructs the model to use those
    tools (and only those tools) to answer the question.
    """

    @property
    def name(self) -> str:
        return "mcp_direct"

    def get_system_prompt(self) -> str:
        return (
            "You are a Metaflow assistant. You have access to Metaflow MCP tools "
            "(get_config, list_flows, search_runs, get_run, get_task_logs, "
            "list_artifacts, get_artifact, get_latest_failure, search_artifacts). "
            "Use ONLY these MCP tools to answer the user's question. "
            "Do NOT write or execute Python code. "
            "Be concise and factual in your response."
        )
