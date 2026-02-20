"""Tests for the Metaflow MCP server.

These tests validate tool registration and basic functionality.
Tests that require a Metaflow backend are marked with @pytest.mark.integration
and skipped in CI unless METAFLOW_MCP_INTEGRATION=1 is set.
"""

import asyncio
import json
import os

import pytest

from metaflow_mcp_server.server import mcp

INTEGRATION = os.environ.get("METAFLOW_MCP_INTEGRATION") == "1"


@pytest.fixture
def run_tool():
    """Helper to call an MCP tool and return parsed JSON."""

    async def _call(name, args=None):
        result = await mcp.call_tool(name, args or {})
        text = result[0][0].text
        return json.loads(text)

    def _sync_call(name, args=None):
        return asyncio.get_event_loop().run_until_complete(_call(name, args))

    return _sync_call


class TestToolRegistration:
    """All tools should be registered with correct names and schemas."""

    def test_tools_registered(self):
        tools = asyncio.get_event_loop().run_until_complete(mcp.list_tools())
        names = {t.name for t in tools}
        expected = {
            "get_config",
            "search_runs",
            "get_run",
            "get_task_logs",
            "list_artifacts",
            "get_artifact",
            "get_latest_failure",
        }
        assert expected == names

    def test_all_tools_have_descriptions(self):
        tools = asyncio.get_event_loop().run_until_complete(mcp.list_tools())
        for tool in tools:
            assert tool.description, f"{tool.name} has no description"

    def test_search_runs_has_params(self):
        tools = asyncio.get_event_loop().run_until_complete(mcp.list_tools())
        search = next(t for t in tools if t.name == "search_runs")
        props = search.inputSchema["properties"]
        assert "flow_name" in props
        assert "last_n" in props

    def test_get_artifact_has_params(self):
        tools = asyncio.get_event_loop().run_until_complete(mcp.list_tools())
        tool = next(t for t in tools if t.name == "get_artifact")
        props = tool.inputSchema["properties"]
        assert "pathspec" in props
        assert "name" in props


class TestErrorHandling:
    """Tools should return structured errors, not crash."""

    def test_bad_flow_name(self, run_tool):
        result = run_tool("search_runs", {"flow_name": "NonExistent__12345"})
        assert "error" in result
        assert "MetaflowNotFound" in result["error"]

    def test_bad_pathspec(self, run_tool):
        result = run_tool("get_run", {"pathspec": "FakeFlow/99999999"})
        assert "error" in result

    def test_bad_artifact_name(self, run_tool):
        result = run_tool("get_artifact", {"pathspec": "F/1/s/1", "name": "nope"})
        assert "error" in result


@pytest.mark.skipif(not INTEGRATION, reason="Requires Metaflow backend (set METAFLOW_MCP_INTEGRATION=1)")
class TestIntegration:
    """Tests that hit a real Metaflow backend."""

    def test_get_config(self, run_tool):
        result = run_tool("get_config")
        assert "metadata_provider" in result
        assert "default_datastore" in result

    def test_search_and_drill(self, run_tool):
        # Find any flow with runs
        from metaflow import Metaflow

        flows = list(Metaflow())
        if not flows:
            pytest.skip("No flows available")

        flow_name = flows[0].id
        runs = run_tool("search_runs", {"flow_name": flow_name, "last_n": 1})
        assert runs["count"] >= 1

        # Drill into the run
        pathspec = runs["runs"][0]["pathspec"]
        run = run_tool("get_run", {"pathspec": pathspec})
        assert "steps" in run
        assert len(run["steps"]) > 0

        # List artifacts from first step
        first_step = run["steps"][-1]
        task_id = first_step["tasks"][0]["id"]
        step_name = first_step["step"]
        task_path = f"{pathspec}/{step_name}/{task_id}"

        artifacts = run_tool("list_artifacts", {"pathspec": task_path})
        assert "artifacts" in artifacts
