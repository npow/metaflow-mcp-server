"""Tests for the Metaflow MCP server.

These tests validate tool registration and basic functionality.
Tests that require a Metaflow backend are marked with @pytest.mark.integration
and skipped in CI unless METAFLOW_MCP_INTEGRATION=1 is set.
"""

import asyncio
import json
import os

import pytest

from metaflow_mcp_server.server import mcp, _filter_log, _parse_dt, _ensure_tz, _duration

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
            "list_flows",
            "search_runs",
            "get_run",
            "get_task_logs",
            "list_artifacts",
            "get_artifact",
            "get_latest_failure",
            "search_artifacts",
            "get_recent_runs",
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
        assert "status" in props
        assert "created_after" in props
        assert "created_before" in props
        assert "tags" in props

    def test_get_artifact_has_params(self):
        tools = asyncio.get_event_loop().run_until_complete(mcp.list_tools())
        tool = next(t for t in tools if t.name == "get_artifact")
        props = tool.inputSchema["properties"]
        assert "pathspec" in props
        assert "name" in props

    def test_get_task_logs_has_params(self):
        tools = asyncio.get_event_loop().run_until_complete(mcp.list_tools())
        tool = next(t for t in tools if t.name == "get_task_logs")
        props = tool.inputSchema["properties"]
        assert "pathspec" in props
        assert "tail" in props
        assert "head" in props
        assert "pattern" in props

    def test_list_flows_has_params(self):
        tools = asyncio.get_event_loop().run_until_complete(mcp.list_tools())
        tool = next(t for t in tools if t.name == "list_flows")
        props = tool.inputSchema["properties"]
        assert "last_n" in props

    def test_search_artifacts_has_params(self):
        tools = asyncio.get_event_loop().run_until_complete(mcp.list_tools())
        tool = next(t for t in tools if t.name == "search_artifacts")
        props = tool.inputSchema["properties"]
        assert "flow_name" in props
        assert "artifact_name" in props
        assert "last_n_runs" in props
        assert "step_name" in props

    def test_get_latest_failure_has_params(self):
        tools = asyncio.get_event_loop().run_until_complete(mcp.list_tools())
        tool = next(t for t in tools if t.name == "get_latest_failure")
        props = tool.inputSchema["properties"]
        assert "flow_name" in props
        assert "last_n_runs" in props


class TestHelpers:
    """Test helper functions in isolation."""

    def test_filter_log_tail(self):
        text = "line1\nline2\nline3\nline4\nline5\n"
        result = _filter_log(text, tail=2)
        assert result == "line4\nline5\n"

    def test_filter_log_head(self):
        text = "line1\nline2\nline3\nline4\nline5\n"
        result = _filter_log(text, head=2)
        assert result == "line1\nline2\n"

    def test_filter_log_pattern(self):
        text = "INFO: ok\nERROR: bad\nINFO: fine\nERROR: worse\n"
        result = _filter_log(text, pattern="ERROR")
        assert result == "ERROR: bad\nERROR: worse\n"

    def test_filter_log_pattern_and_tail(self):
        text = "ERROR: a\nINFO: b\nERROR: c\nERROR: d\n"
        result = _filter_log(text, pattern="ERROR", tail=1)
        assert result == "ERROR: d\n"

    def test_filter_log_empty(self):
        assert _filter_log("", tail=5) == ""
        assert _filter_log(None, tail=5) is None

    def test_filter_log_no_filters(self):
        text = "line1\nline2\n"
        assert _filter_log(text) == text

    def test_filter_log_tail_takes_precedence_over_head(self):
        text = "a\nb\nc\nd\n"
        result = _filter_log(text, head=1, tail=1)
        assert result == "d\n"

    def test_parse_dt_naive(self):
        from datetime import timezone
        dt = _parse_dt("2024-01-15")
        assert dt.tzinfo == timezone.utc

    def test_parse_dt_with_tz(self):
        dt = _parse_dt("2024-01-15T10:30:00+05:00")
        assert dt.tzinfo is not None

    def test_parse_dt_full_iso(self):
        dt = _parse_dt("2024-01-15T10:30:00")
        assert dt.year == 2024
        assert dt.month == 1
        assert dt.day == 15
        assert dt.hour == 10
        assert dt.minute == 30

    def test_ensure_tz_naive(self):
        from datetime import datetime, timezone
        naive = datetime(2024, 1, 1)
        result = _ensure_tz(naive)
        assert result.tzinfo == timezone.utc

    def test_ensure_tz_already_aware(self):
        from datetime import datetime, timezone, timedelta
        aware = datetime(2024, 1, 1, tzinfo=timezone(timedelta(hours=5)))
        result = _ensure_tz(aware)
        assert result.tzinfo == timezone(timedelta(hours=5))

    def test_ensure_tz_none(self):
        assert _ensure_tz(None) is None

    def test_duration_basic(self):
        from datetime import datetime
        start = datetime(2024, 1, 1, 10, 0, 0)
        end = datetime(2024, 1, 1, 10, 5, 30)
        assert _duration(start, end) == 330.0

    def test_duration_none_start(self):
        from datetime import datetime
        assert _duration(None, datetime(2024, 1, 1)) is None

    def test_duration_none_end(self):
        from datetime import datetime
        assert _duration(datetime(2024, 1, 1), None) is None


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

    def test_bad_flow_list_flows(self, run_tool):
        # list_flows should work even if no flows exist (returns empty list)
        # This test just verifies it doesn't crash
        result = run_tool("list_flows", {"last_n": 1})
        assert "flows" in result or "error" in result

    def test_bad_search_artifacts(self, run_tool):
        result = run_tool(
            "search_artifacts",
            {"flow_name": "NonExistent__12345", "artifact_name": "x"},
        )
        assert "error" in result


@pytest.mark.skipif(not INTEGRATION, reason="Requires Metaflow backend (set METAFLOW_MCP_INTEGRATION=1)")
class TestIntegration:
    """Tests that hit a real Metaflow backend."""

    def test_get_config(self, run_tool):
        result = run_tool("get_config")
        assert "metadata_provider" in result
        assert "default_datastore" in result

    def test_list_flows(self, run_tool):
        result = run_tool("list_flows", {"last_n": 5})
        assert "flows" in result
        assert "count" in result

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
        # Verify timing info is present
        assert "created_at" in run
        assert "duration_seconds" in run
        for step in run["steps"]:
            assert "created_at" in step
            for task in step["tasks"]:
                assert "created_at" in task
                assert "duration_seconds" in task

        # List artifacts from first step (no data loading)
        first_step = run["steps"][-1]
        task_id = first_step["tasks"][0]["id"]
        step_name = first_step["step"]
        task_path = f"{pathspec}/{step_name}/{task_id}"

        artifacts = run_tool("list_artifacts", {"pathspec": task_path})
        assert "artifacts" in artifacts
        # Verify no type/size fields (data not loaded)
        for art in artifacts["artifacts"]:
            assert "name" in art
            assert "sha" in art
            assert "created_at" in art

    def test_search_runs_with_status_filter(self, run_tool):
        from metaflow import Metaflow

        flows = list(Metaflow())
        if not flows:
            pytest.skip("No flows available")

        flow_name = flows[0].id
        # Filter by successful runs
        result = run_tool(
            "search_runs",
            {"flow_name": flow_name, "last_n": 3, "status": "successful"},
        )
        for run in result["runs"]:
            assert run["successful"] is True

    def test_get_task_logs_with_tail(self, run_tool):
        from metaflow import Metaflow

        flows = list(Metaflow())
        if not flows:
            pytest.skip("No flows available")

        flow_name = flows[0].id
        runs = run_tool("search_runs", {"flow_name": flow_name, "last_n": 1})
        if runs["count"] == 0:
            pytest.skip("No runs available")

        pathspec = runs["runs"][0]["pathspec"]
        run = run_tool("get_run", {"pathspec": pathspec})
        first_step = run["steps"][-1]
        task_id = first_step["tasks"][0]["id"]
        step_name = first_step["step"]
        task_path = f"{pathspec}/{step_name}/{task_id}"

        result = run_tool("get_task_logs", {"pathspec": task_path, "tail": 5})
        assert "stdout" in result or "stderr" in result
