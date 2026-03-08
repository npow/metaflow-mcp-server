"""MCP Direct: Claude Code uses Metaflow MCP tools. No guidance beyond that."""

from benchmarks.approaches.base import Approach


class MCPDirectApproach(Approach):
    """Minimal prompt: just tell the model it has MCP tools and to use them.
    Tool discovery happens through the MCP protocol itself."""

    @property
    def name(self) -> str:
        return "mcp_direct"

    def get_system_prompt(self) -> str:
        return (
            "You are a Metaflow assistant with access to Metaflow MCP tools. "
            "Use those tools to answer the question. Do not write Python code."
        )
