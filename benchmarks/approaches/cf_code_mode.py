"""Cloudflare-style Code Mode: search + execute pattern.

Replicates Cloudflare's two-tool Code Mode approach:
- search(): write code that queries the API schema server-side to discover
  relevant functions. The full API spec is NOT in the context window.
- execute(): write code that calls the discovered functions.

For Cloudflare's 2,500-endpoint API, this reduces tool definitions from
1.17M tokens to ~1,000 fixed tokens. For our 7-tool API, the search phase
is overhead — but that's exactly what we're benchmarking.
"""

from benchmarks.approaches.base import Approach


class CFCodeModeApproach(Approach):
    """Cloudflare-style Code Mode: search + execute against MCP tool functions.

    The model gets two capabilities:
    1. Search: write Python code that queries the API schema to discover
       available functions, their signatures, and return types.
    2. Execute: write Python code that calls those functions.

    The API reference is NOT in the system prompt. The model must discover
    it through the search phase, just as in Cloudflare's approach.
    """

    @property
    def name(self) -> str:
        return "cf_code_mode"

    def get_system_prompt(self) -> str:
        return (
            "You are a Metaflow assistant. You interact with a Metaflow API through "
            "a two-phase approach using the Bash tool to run Python code. "
            "Do NOT use any MCP tools directly.\n\n"
            "## Phase 1: Search (discover the API)\n"
            "Write Python code to search the API schema and find relevant functions.\n"
            "The schema is available as a list of dicts, each with keys: "
            "name, signature, docstring, parameters.\n"
            "```python\n"
            "from benchmarks.api_docs import generate_api_schema\n"
            "schema = generate_api_schema()  # returns list of function descriptors\n"
            "# Filter/search schema to find what you need, e.g.:\n"
            "for fn in schema:\n"
            "    if 'run' in fn['name'].lower() or 'run' in fn['docstring'].lower():\n"
            "        print(fn['name'], '-', fn['signature'])\n"
            "        print(fn['docstring'][:200])\n"
            "```\n\n"
            "## Phase 2: Execute (call the API)\n"
            "Write Python code that imports and calls the discovered functions.\n"
            "All functions return JSON strings. Use json.loads() to parse.\n"
            "Chain multiple calls in one script — intermediate results stay in your code.\n"
            "```python\n"
            "import json\n"
            "from metaflow_mcp_server.server import search_runs, get_run\n"
            "runs = json.loads(search_runs(flow_name='MyFlow', last_n=5))\n"
            "for r in runs['runs']:\n"
            "    detail = json.loads(get_run(pathspec=r['pathspec']))\n"
            "    print(detail['pathspec'], detail['successful'])\n"
            "```\n\n"
            "Print your final answer to stdout. Be concise and factual."
        )
