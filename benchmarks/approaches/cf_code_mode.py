"""Cloudflare-style Code Mode: search + execute pattern.

Replicates Cloudflare's two-tool Code Mode approach:
- search(): query the API server for functions matching a keyword.
  The model does NOT receive the full schema list — it must discover
  relevant functions by searching, just as in Cloudflare's design where
  a 2,500-endpoint API is reduced to the handful relevant to each query.
- execute(): write and run code that calls the discovered function.

The system prompt deliberately withholds all schema information. The
model only learns what exists after issuing a search call.
"""

from benchmarks.approaches.base import Approach


class CFCodeModeApproach(Approach):
    """Cloudflare-style: discover API schema via keyword search, then execute."""

    @property
    def name(self) -> str:
        return "cf_code_mode"

    def get_system_prompt(self) -> str:
        return (
            "You are a Metaflow assistant. Use the Bash tool only. Do not use MCP tools.\n\n"
            "You have access to a Metaflow API. You do NOT know which functions are available.\n"
            "Discover them by searching. Always use `uv run python3 -c '...'` to run Python:\n\n"
            "```bash\n"
            "uv run python3 -c '\n"
            "from metaflow_mcp_server.server import search_tool_schemas\n"
            "for fn in search_tool_schemas(\"<keyword>\"):\n"
            "    print(fn[\"name\"], fn[\"signature\"])\n"
            "    print(fn[\"docstring\"][:300])\n"
            "    print()\n"
            "'\n"
            "```\n\n"
            "Then call the discovered function:\n\n"
            "```bash\n"
            "uv run python3 -c '\n"
            "import json\n"
            "from metaflow_mcp_server.server import <fn_name>\n"
            "result = json.loads(<fn_name>(...))\n"
            "print(result)\n"
            "'\n"
            "```\n\n"
            "Search first, then execute. Print your final answer. Be concise."
        )
