"""Tests for the Metaflow MCP server.

These tests validate tool registration and basic functionality.
Tests that require a Metaflow backend are marked with @pytest.mark.integration
and skipped in CI unless METAFLOW_MCP_INTEGRATION=1 is set.
"""

import asyncio
import json
import os
import unittest
from unittest.mock import patch, MagicMock, AsyncMock

import pytest

from metaflow_mcp_server.server import (
    mcp,
    _filter_log,
    _parse_dt,
    _ensure_tz,
    _duration,
    _extract_text_from_html,
    _get_env_from_task,
    _apply_package_filters,
    _get_deployer_impl,
    add_run_tags,
    remove_run_tags,
    list_deployments,
    trigger_run,
    get_triggered_run_status,
    terminate_run,
    run_flow,
    resume_run,
)

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
            "list_cards",
            "get_card",
            "compare_cards",
            "get_latest_failure",
            "search_artifacts",
            "get_recent_runs",
            "get_source_code",
            "get_environment",
            "add_run_tags",
            "remove_run_tags",
            "list_deployments",
            "trigger_run",
            "get_triggered_run_status",
            "terminate_run",
            "run_flow",
            "resume_run",
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
        assert "offset" in props

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

    def test_list_cards_has_params(self):
        tools = asyncio.get_event_loop().run_until_complete(mcp.list_tools())
        tool = next(t for t in tools if t.name == "list_cards")
        props = tool.inputSchema["properties"]
        assert "pathspec" in props
        assert "card_type" in props
        assert "card_id" in props

    def test_get_card_has_params(self):
        tools = asyncio.get_event_loop().run_until_complete(mcp.list_tools())
        tool = next(t for t in tools if t.name == "get_card")
        props = tool.inputSchema["properties"]
        assert "pathspec" in props
        assert "card_index" in props
        assert "card_type" in props
        assert "card_id" in props

    def test_compare_cards_has_params(self):
        tools = asyncio.get_event_loop().run_until_complete(mcp.list_tools())
        tool = next(t for t in tools if t.name == "compare_cards")
        props = tool.inputSchema["properties"]
        assert "pathspecs" in props
        assert "flow_name" in props
        assert "step_name" in props
        assert "run_ids" in props
        assert "card_type" in props
        assert "card_id" in props
        assert "card_index" in props


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

    def test_extract_text_basic(self):
        html = "<html><body><h1>Title</h1><p>Hello world</p></body></html>"
        result = _extract_text_from_html(html)
        assert "Title" in result
        assert "Hello world" in result

    def test_extract_text_strips_script_and_style(self):
        html = "<p>visible</p><script>var x = 1;</script><style>.foo{}</style><p>also visible</p>"
        result = _extract_text_from_html(html)
        assert "visible" in result
        assert "also visible" in result
        assert "var x" not in result
        assert ".foo" not in result

    def test_extract_text_empty(self):
        assert _extract_text_from_html("") == ""


class TestGetEnvironment:
    """Tests for the get_environment tool and _get_env_from_task helper."""

    def test_get_environment_has_params(self):
        tools = asyncio.get_event_loop().run_until_complete(mcp.list_tools())
        tool = next(t for t in tools if t.name == "get_environment")
        props = tool.inputSchema["properties"]
        assert "pathspec" in props
        assert "package_type" in props

    def test_get_environment_invalid_pathspec(self, run_tool):
        result = run_tool("get_environment", {"pathspec": "just_one_part"})
        assert result["error"] == "invalid_pathspec"

    def test_get_environment_bad_task(self, run_tool):
        result = run_tool("get_environment", {"pathspec": "FakeFlow/999/step/0"})
        assert "error" in result

    def test_get_env_from_task_no_metadata(self):
        """Task without conda metadata returns None."""
        from unittest.mock import MagicMock
        task = MagicMock()
        task.metadata = []
        result, source = _get_env_from_task(task, "TestFlow")
        assert result is None
        assert source is None

    def test_get_env_from_task_metadata_only_netflix(self):
        """Task with conda_env_id but no code package falls back to metadata_only."""
        from unittest.mock import MagicMock
        metadata_entry = MagicMock()
        metadata_entry.field = "conda_env_id"
        metadata_entry.value = '["abc123", "def456", "linux-64"]'
        task = MagicMock()
        task.metadata = [metadata_entry]
        result, source = _get_env_from_task(task, "TestFlow")
        assert source == "metadata_only"
        assert result["req_id"] == "abc123"
        assert result["full_id"] == "def456"
        assert result["arch"] == "linux-64"

    def test_get_env_from_task_metadata_only_oss(self):
        """Task with conda_env_prefix but CondaEnvironment import fails falls back to metadata_only."""
        from unittest.mock import MagicMock
        metadata_entry = MagicMock()
        metadata_entry.field = "conda_env_prefix"
        metadata_entry.value = "metaflow/abc123/linux-64"
        task = MagicMock()
        task.metadata = [metadata_entry]
        result, source = _get_env_from_task(task, "TestFlow")
        # Will be metadata_only if OSS CondaEnvironment.get_client_info fails
        # or "oss" if it succeeds — either is valid
        assert source in ("oss", "metadata_only")
        if source == "metadata_only":
            assert result["conda_env_prefix"] == "metaflow/abc123/linux-64"

    def test_get_env_from_task_netflix_with_mock_resolved_env(self):
        """Netflix path with full ResolvedEnvironment extraction."""
        from unittest.mock import MagicMock, patch
        import json as json_mod

        metadata_entries = [
            MagicMock(field="conda_env_id", value='["req123", "full456", "linux-64"]'),
            MagicMock(
                field="code-package",
                value=json_mod.dumps({"ds_type": "s3", "location": "s3://bucket", "sha": "abc"}),
            ),
        ]
        task = MagicMock()
        task.metadata = metadata_entries

        mock_pkg = MagicMock()
        mock_pkg.package_name = "numpy"
        mock_pkg.package_version = "1.26.4"
        mock_pkg.package_detailed_version = "1.26.4-py310"
        mock_pkg.TYPE = "pypi"
        mock_pkg.filename = "numpy-1.26.4.whl"

        mock_resolved = MagicMock()
        mock_resolved.packages = [mock_pkg]
        mock_resolved.env_type.value = "pypi-only"
        mock_resolved.env_id.arch = "linux-64"
        mock_resolved.resolved_on = "2025-01-15T10:30:00"
        mock_resolved.resolved_by = "testuser"
        mock_resolved.co_resolved_archs = ["linux-64"]
        mock_resolved.deps = [MagicMock(__str__=lambda s: "pypi::numpy>=1.0")]
        mock_resolved.sources = [MagicMock(__str__=lambda s: "pypi::https://pypi.org/simple")]

        mock_cached = MagicMock()
        mock_cached.env_for.return_value = mock_resolved

        mock_cached_cls = MagicMock()
        mock_cached_cls.from_dict.return_value = mock_cached

        mock_tar_member = MagicMock()
        mock_tar_member.read.return_value = b'{"dummy": "manifest"}'

        with patch.dict("sys.modules", {
            "metaflow_extensions": MagicMock(),
            "metaflow_extensions.nflx": MagicMock(),
            "metaflow_extensions.nflx.plugins": MagicMock(),
            "metaflow_extensions.nflx.plugins.conda": MagicMock(),
            "metaflow_extensions.nflx.plugins.conda.env_descr": MagicMock(
                CachedEnvironmentInfo=mock_cached_cls
            ),
        }), patch("metaflow.metaflow_config.CONDA_MAGIC_FILE_V2", "conda_v2.cnd", create=True), \
             patch("metaflow.client.filecache.FileCache") as mock_fc_cls:

            mock_fc = MagicMock()
            mock_fc.get_data.return_value = (None, b"fake_tarball")
            mock_fc_cls.return_value = mock_fc

            with patch("tarfile.open") as mock_taropen:
                mock_tar = MagicMock()
                mock_tar.__enter__ = MagicMock(return_value=mock_tar)
                mock_tar.__exit__ = MagicMock(return_value=False)
                mock_tar.extractfile.return_value = mock_tar_member
                mock_taropen.return_value = mock_tar

                result, source = _get_env_from_task(task, "TestFlow")

        assert source == "netflix"
        assert result["req_id"] == "req123"
        assert result["full_id"] == "full456"
        assert result["arch"] == "linux-64"
        assert result["resolved_by"] == "testuser"
        assert result["package_count"] == 1
        assert result["packages"][0]["name"] == "numpy"
        assert result["packages"][0]["version"] == "1.26.4"
        assert result["packages"][0]["type"] == "pypi"
        mock_cached.env_for.assert_called_with("req123", "full456", arch="linux-64")

    def test_apply_package_filters_type(self):
        result = {
            "packages": [
                {"name": "numpy", "type": "pypi"},
                {"name": "python", "type": "conda"},
                {"name": "pandas", "type": "pypi"},
            ]
        }
        _apply_package_filters(result, "pypi", None, None)
        assert result["package_count"] == 2
        assert all(p["type"] == "pypi" for p in result["packages"])

    def test_apply_package_filters_by_name(self):
        result = {
            "packages": [
                {"name": "numpy", "version": "1.26.4", "type": "pypi"},
                {"name": "pandas", "version": "2.1.0", "type": "pypi"},
                {"name": "numpy-base", "version": "1.26.4", "type": "conda"},
            ]
        }
        _apply_package_filters(result, None, "numpy", None)
        assert result["package_count"] == 2
        assert result["filtered_by_name"] == "numpy"
        names = [p["name"] for p in result["packages"]]
        assert "numpy" in names
        assert "numpy-base" in names
        assert "pandas" not in names

    def test_apply_package_filters_by_name_case_insensitive(self):
        result = {
            "packages": [
                {"name": "NumPy", "type": "pypi"},
                {"name": "pandas", "type": "pypi"},
            ]
        }
        _apply_package_filters(result, None, "numpy", None)
        assert result["package_count"] == 1
        assert result["packages"][0]["name"] == "NumPy"

    def test_apply_package_filters_combined(self):
        result = {
            "packages": [
                {"name": "numpy", "type": "pypi"},
                {"name": "numpy-base", "type": "conda"},
                {"name": "pandas", "type": "pypi"},
            ]
        }
        _apply_package_filters(result, "pypi", "numpy", None)
        assert result["package_count"] == 1
        assert result["packages"][0]["name"] == "numpy"

    def test_apply_package_filters_max(self):
        result = {
            "packages": [{"name": f"pkg{i}", "type": "pypi"} for i in range(100)]
        }
        _apply_package_filters(result, None, None, 10)
        assert result["packages_truncated"] is True
        assert result["packages_shown"] == 10
        assert result["packages_total"] == 100
        assert len(result["packages"]) == 10

    def test_apply_package_filters_no_truncation(self):
        result = {"packages": [{"name": "a"}, {"name": "b"}]}
        _apply_package_filters(result, None, None, 10)
        assert "packages_truncated" not in result
        assert result["package_count"] == 2

    def test_apply_package_filters_no_packages_key(self):
        result = {"req_id": "abc"}
        _apply_package_filters(result, "pypi", None, 10)
        assert "packages" not in result

    def test_package_type_filter(self, run_tool):
        """package_type filter is accepted without crashing (even on non-conda tasks)."""
        result = run_tool(
            "get_environment",
            {"pathspec": "FakeFlow/999/step/0", "package_type": "pypi"},
        )
        assert "error" in result


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

    def test_list_flows_pagination_fields(self, run_tool):
        result = run_tool("list_flows", {"last_n": 1, "offset": 0})
        assert "offset" in result
        assert "has_more" in result

    def test_bad_search_artifacts(self, run_tool):
        result = run_tool(
            "search_artifacts",
            {"flow_name": "NonExistent__12345", "artifact_name": "x"},
        )
        assert "error" in result

    def test_list_cards_bad_pathspec(self, run_tool):
        result = run_tool("list_cards", {"pathspec": "FakeFlow/99999999"})
        assert "error" in result

    def test_get_card_bad_pathspec(self, run_tool):
        result = run_tool("get_card", {"pathspec": "FakeFlow/99999999/step"})
        assert "error" in result

    def test_compare_cards_missing_args(self, run_tool):
        result = run_tool("compare_cards", {})
        assert "error" in result

    def test_compare_cards_too_few(self, run_tool):
        result = run_tool("compare_cards", {"pathspecs": ["MyFlow/1/step/0"]})
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
        assert "has_more" in result
        assert "offset" in result

    def test_list_flows_pagination(self, run_tool):
        # Get first page
        page1 = run_tool("list_flows", {"last_n": 2, "offset": 0})
        assert page1["count"] <= 2
        assert page1["offset"] == 0
        if page1["has_more"]:
            # Get second page
            page2 = run_tool("list_flows", {"last_n": 2, "offset": 2})
            assert page2["offset"] == 2
            # Pages should not overlap
            assert set(page1["flows"]).isdisjoint(set(page2["flows"]))

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
