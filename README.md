# Metaflow MCP Server

[![CI](https://github.com/Netflix/metaflow-mcp-server/actions/workflows/ci.yml/badge.svg)](https://github.com/Netflix/metaflow-mcp-server/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/metaflow-mcp-server)](https://pypi.org/project/metaflow-mcp-server/)
[![Python](https://img.shields.io/pypi/pyversions/metaflow-mcp-server)](https://pypi.org/project/metaflow-mcp-server/)
[![License](https://img.shields.io/github/license/Netflix/metaflow-mcp-server)](LICENSE)
[![MCP](https://img.shields.io/badge/MCP-compatible-blue)](https://modelcontextprotocol.io/)

Give your coding agent superpowers over your Metaflow workflows. Instead of writing throwaway scripts to check run status or dig through logs, just ask -- your agent will figure out the rest.

Works with any Metaflow backend: local, S3, Azure, GCS, or Netflix internal.

<p align="center">
  <img src="demo/demo.gif" alt="demo" width="800">
</p>

## Tools

| Tool | Description |
|------|-------------|
| `get_config` | What backend am I connected to? |
| `search_runs` | Find recent runs of any flow |
| `get_run` | Step-by-step breakdown of a run |
| `get_task_logs` | Pull stdout/stderr from a task |
| `list_artifacts` | What did this step produce? |
| `get_artifact` | Grab an artifact's value |
| `get_latest_failure` | What broke and why? |

## Quickstart

```bash
pip install metaflow-mcp-server
claude mcp add --scope user metaflow -- metaflow-mcp-server
```

That's it. Restart Claude Code and start asking questions about your flows.

If Metaflow lives in a specific venv, point to it:

```bash
claude mcp add --scope user metaflow -- /path/to/venv/bin/metaflow-mcp-server
```

For other MCP clients, the server speaks stdio: `metaflow-mcp-server`

## How it works

Wraps the Metaflow client API. Whatever backend your Metaflow is pointed at, the server uses too -- no separate config needed. Sets `namespace(None)` at startup so production runs (Argo, Step Functions, Maestro) are visible alongside your dev runs.

Starts once per session, communicates over stdin/stdout. No daemon, no port.

## License

Apache-2.0
