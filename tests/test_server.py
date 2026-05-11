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
    _diff_run_metadata,
    _diff_environments,
    add_run_tags,
    remove_run_tags,
    list_deployments,
    trigger_run,
    get_triggered_run_status,
    terminate_run,
    run_flow,
    resume_run,
    diff_runs,
    get_task_logs,
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
            "diff_runs",
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
        assert "include_html" in props

    def test_get_run_has_summary_param(self):
        tools = asyncio.get_event_loop().run_until_complete(mcp.list_tools())
        tool = next(t for t in tools if t.name == "get_run")
        props = tool.inputSchema["properties"]
        assert "pathspec" in props
        assert "summary" in props

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
        # Fallback must explain *why* the netflix path failed, not silently
        # return note-only — the caller needs to distinguish "ext not
        # installed" from "code-package missing" from real exceptions.
        assert "error" in result

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

    def test_get_env_from_task_oss_path_exception_surfaces_error(self):
        """OSS conda fallback must carry the exception text, not just a generic note."""
        from unittest.mock import MagicMock, patch
        metadata_entry = MagicMock()
        metadata_entry.field = "conda_env_prefix"
        metadata_entry.value = "metaflow/abc123/linux-64"
        task = MagicMock()
        task.metadata = [metadata_entry]

        with patch(
            "metaflow.plugins.pypi.conda_environment.CondaEnvironment.get_client_info",
            side_effect=RuntimeError("manifest corrupt"),
        ):
            result, source = _get_env_from_task(task, "TestFlow")

        assert source == "metadata_only"
        assert result["conda_env_prefix"] == "metaflow/abc123/linux-64"
        assert "manifest corrupt" in result["error"]

    def test_get_env_from_task_oss_path_empty_client_info_surfaces_reason(self):
        """get_client_info returning empty must be distinguishable from an exception."""
        from unittest.mock import MagicMock, patch
        metadata_entry = MagicMock()
        metadata_entry.field = "conda_env_prefix"
        metadata_entry.value = "metaflow/abc123/linux-64"
        task = MagicMock()
        task.metadata = [metadata_entry]

        with patch(
            "metaflow.plugins.pypi.conda_environment.CondaEnvironment.get_client_info",
            return_value=None,
        ):
            result, source = _get_env_from_task(task, "TestFlow")

        assert source == "metadata_only"
        assert "returned empty" in result["error"]

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


class TestSilentFailureSurfacing:
    """Failures inside tools must surface in the response, never be swallowed.

    These tests lock in the contract that helpers expose errors via dedicated
    fields (type_error, files_error, error, partial_failure) rather than
    returning empty results indistinguishable from "no data."
    """

    def test_list_artifacts_surfaces_data_deserialization_error(self):
        """art.data failing → type_error field, not silent 'unknown'."""
        from unittest.mock import MagicMock, patch
        from metaflow_mcp_server.server import list_artifacts

        good_art = MagicMock(id="ok", sha="sha1", created_at="2026-01-01")
        good_art.data = "value"

        bad_art = MagicMock(id="bad", sha="sha2", created_at="2026-01-01")
        type(bad_art).data = property(
            lambda self: (_ for _ in ()).throw(IOError("S3 timeout"))
        )

        task = MagicMock(pathspec="F/1/s/0")
        task.__iter__ = lambda self: iter([good_art, bad_art])

        with patch("metaflow.Task", return_value=task):
            result = json.loads(list_artifacts("F/1/s/0"))

        by_name = {a["name"]: a for a in result["artifacts"]}
        assert by_name["ok"]["type"] == "str"
        assert "type_error" not in by_name["ok"]
        assert by_name["bad"]["type"] == "unknown"
        assert "S3 timeout" in by_name["bad"]["type_error"]

    def test_get_source_code_surfaces_tarball_error(self):
        """tarball iteration failing → files_error field, not silent empty list."""
        from unittest.mock import MagicMock, patch
        from metaflow_mcp_server.server import get_source_code

        code = MagicMock()
        code.flowspec = "from metaflow import FlowSpec\n"
        code.info = {"script": "myflow.py"}
        # Accessing .tarball raises — exercise the new error path.
        type(code).tarball = property(
            lambda self: (_ for _ in ()).throw(RuntimeError("tarball corrupt"))
        )

        run = MagicMock(code=code)
        with patch("metaflow.Run", return_value=run):
            result = json.loads(get_source_code("F/1"))

        assert result["files"] == []
        assert "tarball corrupt" in result["files_error"]
        assert result["flowspec"].startswith("from metaflow")

    def test_search_runs_scan_limit_hit_visible(self):
        """When > 200 runs are scanned with no match, caller must see scan_limit_hit."""
        from datetime import datetime, timezone
        from unittest.mock import MagicMock, patch
        from metaflow_mcp_server.server import search_runs

        dt = datetime(2026, 1, 1, tzinfo=timezone.utc)
        end_dt = datetime(2026, 1, 1, 0, 1, tzinfo=timezone.utc)
        # 250 runs all marked unfinished so the "successful" filter never matches,
        # exhausting the MAX_SCAN budget.
        runs = []
        for i in range(250):
            r = MagicMock(
                pathspec=f"F/{i}", id=str(i), successful=False, finished=False,
                created_at=dt, finished_at=end_dt, user_tags=set(),
            )
            runs.append(r)

        flow = MagicMock()
        flow.__iter__ = lambda self: iter(runs)
        with patch("metaflow.Flow", return_value=flow), \
             patch("metaflow.namespace"):
            result = json.loads(
                search_runs("F", last_n=5, status="successful")
            )

        assert result["count"] == 0
        assert result["scan_limit_hit"] is True
        assert result["scan_limit"] == 200
        assert result["scanned"] == 200

    def test_search_runs_scan_limit_not_hit_when_few_runs(self):
        """Few runs → scan_limit_hit is False and scanned reflects reality."""
        from datetime import datetime, timezone
        from unittest.mock import MagicMock, patch
        from metaflow_mcp_server.server import search_runs

        dt = datetime(2026, 1, 1, tzinfo=timezone.utc)
        end_dt = datetime(2026, 1, 1, 0, 1, tzinfo=timezone.utc)
        runs = [
            MagicMock(
                pathspec=f"F/{i}", id=str(i), successful=True, finished=True,
                created_at=dt, finished_at=end_dt, user_tags=set(),
            )
            for i in range(3)
        ]
        flow = MagicMock()
        flow.__iter__ = lambda self: iter(runs)
        with patch("metaflow.Flow", return_value=flow), \
             patch("metaflow.namespace"):
            result = json.loads(search_runs("F", last_n=10))

        assert result["scan_limit_hit"] is False
        assert result["scanned"] == 3

    def test_get_run_summary_mode_returns_counts_and_failing_tasks_only(self):
        """summary=True returns per-step counts + only the failing tasks."""
        from datetime import datetime, timezone
        from unittest.mock import MagicMock, patch
        from metaflow_mcp_server.server import get_run

        dt = datetime(2026, 1, 1, tzinfo=timezone.utc)
        end_dt = datetime(2026, 1, 1, 0, 1, tzinfo=timezone.utc)

        # 10 tasks in one step: 7 ok, 2 failed, 1 still running.
        tasks = []
        for i in range(7):
            tasks.append(MagicMock(
                id=str(i), successful=True, finished=True,
                created_at=dt, finished_at=end_dt,
            ))
        for i in range(7, 9):
            tasks.append(MagicMock(
                id=str(i), successful=False, finished=True,
                created_at=dt, finished_at=end_dt,
            ))
        tasks.append(MagicMock(
            id="9", successful=False, finished=False,
            created_at=dt, finished_at=None,
        ))

        step = MagicMock(id="train", created_at=dt)
        step.__iter__ = lambda self: iter(tasks)

        run = MagicMock(
            pathspec="F/1", successful=False, finished=True,
            created_at=dt, finished_at=end_dt, user_tags=set(),
        )
        run.__iter__ = lambda self: iter([step])

        with patch("metaflow.Run", return_value=run):
            result = json.loads(get_run("F/1", summary=True))

        assert result["summary"] is True
        assert len(result["steps"]) == 1
        s = result["steps"][0]
        assert s["task_count"] == 10
        assert s["successful_count"] == 7
        assert s["failed_count"] == 2
        assert s["running_count"] == 1
        # Only failing tasks are listed by id — successful ones are folded
        # into counts so the payload stays bounded on huge fan-outs.
        assert {t["id"] for t in s["failed_tasks"]} == {"7", "8"}
        assert "tasks" not in s  # no full task list in summary mode

    def test_get_card_default_excludes_html(self):
        """Default response strips full HTML — agents get text + size hint."""
        from unittest.mock import MagicMock, patch
        from metaflow_mcp_server.server import get_card

        html = "<html><body><h1>Title</h1><p>Some text</p></body></html>"
        card = MagicMock(type="default", id="0", hash="abc")
        card.get.return_value = html

        task = MagicMock(pathspec="F/1/s/0")
        task.__iter__ = lambda self: iter([])  # not used by the resolver path below

        with patch(
            "metaflow_mcp_server.server._resolve_tasks_for_cards",
            return_value=[(task, "F/1/s/0")],
        ), patch("metaflow.cards.get_cards", return_value=[card]):
            result = json.loads(get_card("F/1/s"))

        assert "html" not in result, "HTML must be omitted by default to keep payload small"
        assert "text_content" in result
        assert "Title" in result["text_content"]
        assert result["html_bytes"] == len(html)

    def test_get_card_include_html_true(self):
        """include_html=True restores full HTML for callers that need it."""
        from unittest.mock import MagicMock, patch
        from metaflow_mcp_server.server import get_card

        html = "<html><body><h1>Title</h1></body></html>"
        card = MagicMock(type="default", id="0", hash="abc")
        card.get.return_value = html

        task = MagicMock(pathspec="F/1/s/0")
        with patch(
            "metaflow_mcp_server.server._resolve_tasks_for_cards",
            return_value=[(task, "F/1/s/0")],
        ), patch("metaflow.cards.get_cards", return_value=[card]):
            result = json.loads(get_card("F/1/s", include_html=True))

        assert result["html"] == html
        assert result["html_bytes"] == len(html)

    def test_get_run_default_returns_full_task_detail(self):
        """summary defaults to False → full per-task list preserved."""
        from datetime import datetime, timezone
        from unittest.mock import MagicMock, patch
        from metaflow_mcp_server.server import get_run

        dt = datetime(2026, 1, 1, tzinfo=timezone.utc)
        end_dt = datetime(2026, 1, 1, 0, 1, tzinfo=timezone.utc)
        tasks = [
            MagicMock(
                id=str(i), successful=True, finished=True,
                created_at=dt, finished_at=end_dt,
            )
            for i in range(3)
        ]
        step = MagicMock(id="train", created_at=dt)
        step.__iter__ = lambda self: iter(tasks)
        run = MagicMock(
            pathspec="F/1", successful=True, finished=True,
            created_at=dt, finished_at=end_dt, user_tags=set(),
        )
        run.__iter__ = lambda self: iter([step])

        with patch("metaflow.Run", return_value=run):
            result = json.loads(get_run("F/1"))

        assert result["summary"] is False
        s = result["steps"][0]
        assert "tasks" in s
        assert len(s["tasks"]) == 3
        assert "task_count" not in s
        assert "failed_tasks" not in s

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


# ── Unit tests for write/execute tools ─────────────────────────────────────


class TestTagManagement(unittest.TestCase):
    @patch("metaflow.Run")
    def test_add_run_tags(self, mock_run_cls):
        mock_run = MagicMock()
        mock_run.user_tags = {"existing", "new_tag"}
        mock_run_cls.return_value = mock_run

        result = json.loads(add_run_tags("Flow/123", ["new_tag"]))

        mock_run_cls.assert_called_once_with("Flow/123")
        mock_run.add_tags.assert_called_once_with(["new_tag"])
        assert result["pathspec"] == "Flow/123"
        assert result["added"] == ["new_tag"]
        assert "new_tag" in result["current_tags"]

    @patch("metaflow.Run")
    def test_add_run_tags_multiple(self, mock_run_cls):
        mock_run = MagicMock()
        mock_run.user_tags = {"a", "b", "c"}
        mock_run_cls.return_value = mock_run

        result = json.loads(add_run_tags("Flow/123", ["b", "c"]))

        mock_run.add_tags.assert_called_once_with(["b", "c"])
        assert result["added"] == ["b", "c"]
        assert sorted(result["current_tags"]) == ["a", "b", "c"]

    @patch("metaflow.Run")
    def test_add_run_tags_invalid_pathspec(self, mock_run_cls):
        mock_run_cls.side_effect = Exception("not found")

        result = json.loads(add_run_tags("Bad/Path", ["tag"]))

        assert "error" in result
        assert "message" in result

    @patch("metaflow.Run")
    def test_remove_run_tags(self, mock_run_cls):
        mock_run = MagicMock()
        mock_run.user_tags = {"remaining"}
        mock_run_cls.return_value = mock_run

        result = json.loads(remove_run_tags("Flow/123", ["old_tag"]))

        mock_run_cls.assert_called_once_with("Flow/123")
        mock_run.remove_tags.assert_called_once_with(["old_tag"])
        assert result["pathspec"] == "Flow/123"
        assert result["removed"] == ["old_tag"]
        assert "remaining" in result["current_tags"]

    @patch("metaflow.Run")
    def test_remove_run_tags_invalid_pathspec(self, mock_run_cls):
        mock_run_cls.side_effect = Exception("not found")

        result = json.loads(remove_run_tags("Bad/Path", ["tag"]))

        assert "error" in result
        assert "message" in result

    @patch("metaflow.Run")
    def test_remove_run_tags_current_tags_empty(self, mock_run_cls):
        mock_run = MagicMock()
        mock_run.user_tags = set()
        mock_run_cls.return_value = mock_run

        result = json.loads(remove_run_tags("Flow/123", ["gone"]))

        assert result["removed"] == ["gone"]
        assert result["current_tags"] == []


class TestDeploymentManagement(unittest.TestCase):
    @patch("metaflow.DeployedFlow")
    def test_list_deployments(self, mock_df_cls):
        mock_dep1 = MagicMock()
        mock_dep1.__class__.__name__ = "MaestroDeployedFlow"
        mock_dep1.workflow_id = "proj.TrainFlow"
        mock_dep1.name = "TrainFlow"
        mock_dep1.flow_name = "TrainFlow"
        # attrs that should be skipped when None
        mock_dep1.cluster_name = None
        mock_dep1.workflow_version = "1"
        mock_dep1.identifier = "proj.TrainFlow"

        mock_df_cls.list_deployed_flows.return_value = [mock_dep1]

        with patch("metaflow_mcp_server.server._get_deployer_impl", return_value="maestro"):
            result = json.loads(list_deployments(flow_name="TrainFlow", impl="maestro"))

        assert result["flow_name"] == "TrainFlow"
        assert result["impl"] == "maestro"
        assert result["count"] == 1
        assert result["deployments"][0]["workflow_id"] == "proj.TrainFlow"
        assert result["deployments"][0]["name"] == "TrainFlow"
        # None-valued attrs should be omitted
        assert "cluster_name" not in result["deployments"][0]

    @patch("metaflow.DeployedFlow")
    def test_list_deployments_empty(self, mock_df_cls):
        mock_df_cls.list_deployed_flows.return_value = []

        with patch("metaflow_mcp_server.server._get_deployer_impl", return_value="argo_workflows"):
            result = json.loads(list_deployments())

        assert result["count"] == 0
        assert result["deployments"] == []

    @patch("metaflow.DeployedFlow")
    def test_list_deployments_error(self, mock_df_cls):
        mock_df_cls.list_deployed_flows.side_effect = Exception("backend unavailable")

        result = json.loads(list_deployments(impl="maestro"))

        assert "error" in result

    @patch("metaflow.DeployedFlow")
    def test_trigger_run(self, mock_df_cls):
        mock_triggered = MagicMock()
        mock_triggered.workflow_run_id = "run-42"
        mock_triggered.workflow_id = "proj.TrainFlow"
        mock_triggered.workflow_instance_id = None
        mock_triggered.workflow_version = None
        mock_triggered.status = "running"
        mock_triggered.cluster_name = None
        mock_triggered.maestro_ui = "https://maestro/run-42"
        mock_triggered.metaflow_ui = None

        mock_df = MagicMock()
        mock_df.run.return_value = mock_triggered
        mock_df_cls.from_deployment.return_value = mock_df

        with patch("metaflow_mcp_server.server._get_deployer_impl", return_value="maestro"):
            result = json.loads(trigger_run("proj.TrainFlow", parameters={"lr": "0.01"}, impl="maestro"))

        mock_df_cls.from_deployment.assert_called_once_with("proj.TrainFlow", impl="maestro")
        mock_df.run.assert_called_once_with(lr="0.01")
        assert result["identifier"] == "proj.TrainFlow"
        assert result["action"] == "triggered"
        assert result["workflow_run_id"] == "run-42"
        assert result["maestro_ui"] == "https://maestro/run-42"
        # None-valued attrs should be omitted
        assert "workflow_instance_id" not in result
        assert "metaflow_ui" not in result

    @patch("metaflow.DeployedFlow")
    def test_trigger_run_no_parameters(self, mock_df_cls):
        mock_triggered = MagicMock()
        mock_triggered.workflow_run_id = "run-99"
        mock_triggered.workflow_id = "proj.TrainFlow"
        mock_triggered.workflow_instance_id = None
        mock_triggered.workflow_version = None
        mock_triggered.status = "pending"
        mock_triggered.cluster_name = None
        mock_triggered.maestro_ui = None
        mock_triggered.metaflow_ui = None

        mock_df = MagicMock()
        mock_df.run.return_value = mock_triggered
        mock_df_cls.from_deployment.return_value = mock_df

        with patch("metaflow_mcp_server.server._get_deployer_impl", return_value="maestro"):
            result = json.loads(trigger_run("proj.TrainFlow", impl="maestro"))

        mock_df.run.assert_called_once_with()
        assert result["action"] == "triggered"

    @patch("metaflow.DeployedFlow")
    def test_trigger_run_error(self, mock_df_cls):
        mock_df_cls.from_deployment.side_effect = Exception("deployment not found")

        result = json.loads(trigger_run("bad.identifier"))

        assert "error" in result

    @patch("metaflow.DeployedFlow")
    def test_get_triggered_run_status_with_metaflow_run(self, mock_df_cls):
        mock_mf_run = MagicMock()
        mock_mf_run.pathspec = "TrainFlow/123"
        mock_mf_run.successful = False
        mock_mf_run.finished = False

        mock_triggered = MagicMock()
        mock_triggered.status = "running"
        mock_triggered.workflow_id = "proj.TrainFlow"
        mock_triggered.workflow_instance_id = "inst-7"
        mock_triggered.workflow_run_id = "run-42"
        mock_triggered.workflow_version = None
        mock_triggered.cluster_name = None
        mock_triggered.maestro_ui = None
        mock_triggered.metaflow_ui = None
        mock_triggered.run = mock_mf_run

        mock_df_cls.get_triggered_run.return_value = mock_triggered

        with patch("metaflow_mcp_server.server._get_deployer_impl", return_value="maestro"):
            result = json.loads(get_triggered_run_status("proj.TrainFlow", "run-42", impl="maestro"))

        mock_df_cls.get_triggered_run.assert_called_once_with(
            "proj.TrainFlow", "run-42", impl="maestro"
        )
        assert result["identifier"] == "proj.TrainFlow"
        assert result["run_id"] == "run-42"
        assert result["status"] == "running"
        assert result["metaflow_pathspec"] == "TrainFlow/123"
        assert result["finished"] is False

    @patch("metaflow.DeployedFlow")
    def test_get_triggered_run_status_no_metaflow_run_yet(self, mock_df_cls):
        mock_triggered = MagicMock()
        mock_triggered.status = "pending"
        mock_triggered.workflow_id = "proj.TrainFlow"
        mock_triggered.workflow_instance_id = None
        mock_triggered.workflow_run_id = "run-1"
        mock_triggered.workflow_version = None
        mock_triggered.cluster_name = None
        mock_triggered.maestro_ui = None
        mock_triggered.metaflow_ui = None
        # accessing .run raises (start step not yet begun)
        type(mock_triggered).run = property(lambda self: (_ for _ in ()).throw(RuntimeError("not started")))

        mock_df_cls.get_triggered_run.return_value = mock_triggered

        with patch("metaflow_mcp_server.server._get_deployer_impl", return_value="maestro"):
            result = json.loads(get_triggered_run_status("proj.TrainFlow", "run-1", impl="maestro"))

        assert result["status"] == "pending"
        assert "metaflow_run" in result
        assert "not yet available" in result["metaflow_run"]

    @patch("metaflow.DeployedFlow")
    def test_get_triggered_run_status_error(self, mock_df_cls):
        mock_df_cls.get_triggered_run.side_effect = Exception("run not found")

        result = json.loads(get_triggered_run_status("proj.TrainFlow", "bad-id"))

        assert "error" in result

    @patch("metaflow.DeployedFlow")
    def test_terminate_run(self, mock_df_cls):
        mock_triggered = MagicMock()
        mock_triggered.status = "terminated"

        mock_df_cls.get_triggered_run.return_value = mock_triggered

        with patch("metaflow_mcp_server.server._get_deployer_impl", return_value="maestro"):
            result = json.loads(terminate_run("proj.TrainFlow", "run-42", impl="maestro"))

        mock_df_cls.get_triggered_run.assert_called_once_with(
            "proj.TrainFlow", "run-42", impl="maestro"
        )
        mock_triggered.terminate.assert_called_once()
        assert result["identifier"] == "proj.TrainFlow"
        assert result["run_id"] == "run-42"
        assert result["action"] == "terminated"
        assert result["status"] == "terminated"

    @patch("metaflow.DeployedFlow")
    def test_terminate_run_status_unavailable(self, mock_df_cls):
        mock_triggered = MagicMock(spec=["terminate"])
        mock_triggered.terminate.return_value = None

        mock_df_cls.get_triggered_run.return_value = mock_triggered

        with patch("metaflow_mcp_server.server._get_deployer_impl", return_value="maestro"):
            result = json.loads(terminate_run("proj.TrainFlow", "run-42", impl="maestro"))

        assert result["action"] == "terminated"
        assert "status" not in result

    @patch("metaflow.DeployedFlow")
    def test_terminate_run_error(self, mock_df_cls):
        mock_df_cls.get_triggered_run.side_effect = Exception("run not found")

        result = json.loads(terminate_run("proj.TrainFlow", "bad-id"))

        assert "error" in result


class TestRunnerTools(unittest.TestCase):
    @patch("os.path.isfile", return_value=True)
    @patch("metaflow.Runner")
    def test_run_flow(self, mock_runner_cls, _mock_isfile):
        mock_run = MagicMock()
        mock_run.pathspec = "MyFlow/456"
        mock_run.id = "456"

        mock_executing = MagicMock()
        mock_executing.run = mock_run
        mock_executing.status = "running"

        mock_runner = MagicMock()
        mock_runner.async_run = AsyncMock(return_value=mock_executing)
        mock_runner_cls.return_value = mock_runner

        result = json.loads(asyncio.run(run_flow("myflow.py")))

        mock_runner_cls.assert_called_once_with("myflow.py", show_output=False)
        mock_runner.async_run.assert_called_once_with()
        assert result["flow_file"] == "myflow.py"
        assert result["action"] == "started"
        assert result["pathspec"] == "MyFlow/456"
        assert result["run_id"] == "456"
        assert result["status"] == "running"

    @patch("os.path.isfile", return_value=True)
    @patch("metaflow.Runner")
    def test_run_flow_with_parameters(self, mock_runner_cls, _mock_isfile):
        mock_run = MagicMock()
        mock_run.pathspec = "MyFlow/789"
        mock_run.id = "789"

        mock_executing = MagicMock()
        mock_executing.run = mock_run
        mock_executing.status = "running"

        mock_runner = MagicMock()
        mock_runner.async_run = AsyncMock(return_value=mock_executing)
        mock_runner_cls.return_value = mock_runner

        result = json.loads(asyncio.run(run_flow(
            "myflow.py",
            parameters={"lr": "0.01", "epochs": "10"},
            tags=["experiment"],
            max_workers=4,
        )))

        mock_runner.async_run.assert_called_once_with(
            tag=["experiment"], max_workers=4, lr="0.01", epochs="10"
        )
        assert result["pathspec"] == "MyFlow/789"

    @patch("metaflow.Runner")
    def test_run_flow_error(self, mock_runner_cls):
        mock_runner_cls.side_effect = Exception("file not found")

        result = json.loads(asyncio.run(run_flow("missing.py")))

        assert "error" in result
        assert "message" in result

    @patch("os.path.isfile", return_value=True)
    @patch("metaflow.Runner")
    def test_resume_run(self, mock_runner_cls, _mock_isfile):
        mock_run = MagicMock()
        mock_run.pathspec = "MyFlow/999"
        mock_run.id = "999"

        mock_executing = MagicMock()
        mock_executing.run = mock_run
        mock_executing.status = "running"

        mock_runner = MagicMock()
        mock_runner.async_resume = AsyncMock(return_value=mock_executing)
        mock_runner_cls.return_value = mock_runner

        result = json.loads(asyncio.run(resume_run("myflow.py", "456")))

        mock_runner_cls.assert_called_once_with("myflow.py", show_output=False)
        mock_runner.async_resume.assert_called_once_with(origin_run_id="456")
        assert result["flow_file"] == "myflow.py"
        assert result["action"] == "resumed"
        assert result["origin_run_id"] == "456"
        assert result["pathspec"] == "MyFlow/999"
        assert result["run_id"] == "999"

    @patch("metaflow.Runner")
    def test_resume_run_error(self, mock_runner_cls):
        mock_runner_cls.side_effect = Exception("origin run not found")

        result = json.loads(asyncio.run(resume_run("myflow.py", "bad-id")))

        assert "error" in result
        assert "message" in result


class TestGetDeployerImpl(unittest.TestCase):
    def test_explicit_impl_returned_as_is(self):
        assert _get_deployer_impl("maestro") == "maestro"

    def test_explicit_impl_with_dash_not_normalized(self):
        # explicit arg is returned as-is (no normalization)
        assert _get_deployer_impl("argo-workflows") == "argo-workflows"

    @patch.dict(os.environ, {"METAFLOW_DEFAULT_FROM_DEPLOYMENT_IMPL": "dagobah"})
    def test_env_var_used_when_no_explicit_impl(self):
        assert _get_deployer_impl() == "dagobah"

    @patch.dict(os.environ, {"METAFLOW_DEFAULT_FROM_DEPLOYMENT_IMPL": "argo-workflows"})
    def test_env_var_dash_normalized_to_underscore(self):
        assert _get_deployer_impl() == "argo_workflows"

    @patch.dict(os.environ, {}, clear=True)
    def test_auto_detects_maestro_first(self):
        mock_provider_maestro = MagicMock()
        mock_provider_maestro.TYPE = "maestro"
        mock_provider_argo = MagicMock()
        mock_provider_argo.TYPE = "argo-workflows"

        with patch("metaflow.plugins.DEPLOYER_IMPL_PROVIDERS", [mock_provider_argo, mock_provider_maestro]):
            # Remove env var if set
            os.environ.pop("METAFLOW_DEFAULT_FROM_DEPLOYMENT_IMPL", None)
            result = _get_deployer_impl()

        assert result == "maestro"

    @patch.dict(os.environ, {}, clear=True)
    def test_auto_detects_argo_when_no_maestro(self):
        mock_provider = MagicMock()
        mock_provider.TYPE = "argo-workflows"

        with patch("metaflow.plugins.DEPLOYER_IMPL_PROVIDERS", [mock_provider]):
            os.environ.pop("METAFLOW_DEFAULT_FROM_DEPLOYMENT_IMPL", None)
            result = _get_deployer_impl()

        assert result == "argo_workflows"

    @patch.dict(os.environ, {}, clear=True)
    def test_falls_back_to_argo_when_import_fails(self):
        with patch.dict("sys.modules", {"metaflow.plugins": None}):
            os.environ.pop("METAFLOW_DEFAULT_FROM_DEPLOYMENT_IMPL", None)
            result = _get_deployer_impl()

        assert result == "argo_workflows"


# ── diff_runs tests ──────────────────────────────────────────────────────────


def _make_mock_run(user_tags=None, system_tags=None, successful=True, finished=True,
                   created_at="2024-01-01T00:00:00", steps=None):
    run = MagicMock()
    run.user_tags = set(user_tags or [])
    run.system_tags = set(system_tags or [])
    run.successful = successful
    run.finished = finished
    run.created_at = created_at
    run.__iter__ = MagicMock(return_value=iter(steps or []))
    return run


def _make_mock_step(step_id, artifacts=None):
    step = MagicMock()
    step.id = step_id
    task = MagicMock()
    art_list = []
    for name, value in (artifacts or {}).items():
        art = MagicMock()
        art.id = name
        art.data = value
        art_list.append(art)
    task.__iter__ = MagicMock(return_value=iter(art_list))
    step.__iter__ = MagicMock(return_value=iter([task]))
    return step


class TestDiffRunMetadata(unittest.TestCase):
    def test_identical_runs_no_diffs(self):
        run_a = _make_mock_run(user_tags=["prod"], system_tags=["metaflow_version:2.19"])
        run_b = _make_mock_run(user_tags=["prod"], system_tags=["metaflow_version:2.19"])
        diffs = _diff_run_metadata(run_a, run_b)
        assert diffs == {}

    def test_tag_changes_detected(self):
        run_a = _make_mock_run(user_tags=["v1", "experiment"])
        run_b = _make_mock_run(user_tags=["v2", "experiment"])
        diffs = _diff_run_metadata(run_a, run_b)
        assert "tags" in diffs
        assert "v1" in diffs["tags"]["removed"]
        assert "v2" in diffs["tags"]["added"]

    def test_system_tag_changes_detected(self):
        run_a = _make_mock_run(system_tags=["metaflow_version:2.18", "runtime:dev"])
        run_b = _make_mock_run(system_tags=["metaflow_version:2.19", "runtime:dev"])
        diffs = _diff_run_metadata(run_a, run_b)
        assert "system_tags" in diffs
        assert diffs["system_tags"]["metaflow_version"]["source"] == "2.18"
        assert diffs["system_tags"]["metaflow_version"]["target"] == "2.19"
        assert "runtime" not in diffs["system_tags"]

    def test_parameter_changes_detected(self):
        step_a = _make_mock_step("start", {"lr": 0.01, "epochs": 10})
        step_b = _make_mock_step("start", {"lr": 0.001, "epochs": 10})
        run_a = _make_mock_run(steps=[step_a])
        run_b = _make_mock_run(steps=[step_b])
        diffs = _diff_run_metadata(run_a, run_b)
        assert "parameters" in diffs
        assert "lr" in diffs["parameters"]
        assert "epochs" not in diffs["parameters"]

    def test_parameter_value_truncated(self):
        big_val = "x" * 1000
        step_a = _make_mock_step("start", {"data": big_val})
        step_b = _make_mock_step("start", {"data": "small"})
        run_a = _make_mock_run(steps=[step_a])
        run_b = _make_mock_run(steps=[step_b])
        diffs = _diff_run_metadata(run_a, run_b)
        assert "truncated" in diffs["parameters"]["data"]["source"]

    def test_internal_artifacts_excluded(self):
        step_a = _make_mock_step("start", {"_internal": "x", "name": "flow", "lr": 0.01})
        step_b = _make_mock_step("start", {"_internal": "x", "name": "flow", "lr": 0.01})
        run_a = _make_mock_run(steps=[step_a])
        run_b = _make_mock_run(steps=[step_b])
        diffs = _diff_run_metadata(run_a, run_b)
        assert "parameters" not in diffs


class TestDiffEnvironments(unittest.TestCase):
    def test_both_none_returns_none(self):
        run_a = _make_mock_run()
        run_b = _make_mock_run()
        with patch("metaflow_mcp_server.server._get_env_from_task", return_value=(None, None)):
            result = _diff_environments(run_a, run_b, "Flow")
        assert result is None

    def test_one_has_env_other_doesnt(self):
        run_a = _make_mock_run(steps=[_make_mock_step("start")])
        run_b = _make_mock_run(steps=[_make_mock_step("start")])

        call_count = [0]
        def _side_effect(task, flow_name):
            call_count[0] += 1
            if call_count[0] == 1:
                return {"packages": [{"name": "numpy", "version": "1.26"}]}, "netflix"
            return None, None

        with patch("metaflow_mcp_server.server._get_env_from_task", side_effect=_side_effect):
            result = _diff_environments(run_a, run_b, "Flow")
        assert "no environment" in result["target"]

    def test_package_added(self):
        env_a = {"packages": [{"name": "numpy", "version": "1.26"}]}
        env_b = {"packages": [{"name": "numpy", "version": "1.26"}, {"name": "pandas", "version": "2.0"}]}
        run_a = _make_mock_run(steps=[_make_mock_step("start")])
        run_b = _make_mock_run(steps=[_make_mock_step("start")])

        call_count = [0]
        def _side_effect(task, flow_name):
            call_count[0] += 1
            return (env_a, "netflix") if call_count[0] == 1 else (env_b, "netflix")

        with patch("metaflow_mcp_server.server._get_env_from_task", side_effect=_side_effect):
            result = _diff_environments(run_a, run_b, "Flow")
        assert "pandas" in result["added"]

    def test_package_version_changed(self):
        env_a = {"packages": [{"name": "numpy", "version": "1.25"}]}
        env_b = {"packages": [{"name": "numpy", "version": "1.26"}]}
        run_a = _make_mock_run(steps=[_make_mock_step("start")])
        run_b = _make_mock_run(steps=[_make_mock_step("start")])

        call_count = [0]
        def _side_effect(task, flow_name):
            call_count[0] += 1
            return (env_a, "netflix") if call_count[0] == 1 else (env_b, "netflix")

        with patch("metaflow_mcp_server.server._get_env_from_task", side_effect=_side_effect):
            result = _diff_environments(run_a, run_b, "Flow")
        assert "numpy" in result["changed"]
        assert result["changed"]["numpy"]["source"] == "1.25"
        assert result["changed"]["numpy"]["target"] == "1.26"

    def test_package_removed(self):
        env_a = {"packages": [{"name": "numpy", "version": "1.26"}, {"name": "scipy", "version": "1.0"}]}
        env_b = {"packages": [{"name": "numpy", "version": "1.26"}]}
        run_a = _make_mock_run(steps=[_make_mock_step("start")])
        run_b = _make_mock_run(steps=[_make_mock_step("start")])

        call_count = [0]
        def _side_effect(task, flow_name):
            call_count[0] += 1
            return (env_a, "netflix") if call_count[0] == 1 else (env_b, "netflix")

        with patch("metaflow_mcp_server.server._get_env_from_task", side_effect=_side_effect):
            result = _diff_environments(run_a, run_b, "Flow")
        assert "scipy" in result["removed"]

    def test_no_package_diff_returns_none(self):
        env = {"packages": [{"name": "numpy", "version": "1.26"}]}
        run_a = _make_mock_run(steps=[_make_mock_step("start")])
        run_b = _make_mock_run(steps=[_make_mock_step("start")])

        with patch("metaflow_mcp_server.server._get_env_from_task", return_value=(env, "netflix")):
            result = _diff_environments(run_a, run_b, "Flow")
        assert result is None


class TestDiffRuns(unittest.TestCase):
    @patch("subprocess.run")
    @patch("metaflow.Run")
    def test_diff_runs_no_differences(self, mock_run_cls, mock_subprocess):
        run = _make_mock_run(user_tags=["v1"], system_tags=["metaflow_version:2.19"])
        mock_run_cls.return_value = run
        mock_subprocess.return_value = MagicMock(stdout="", stderr="", returncode=0)

        with patch("metaflow_mcp_server.server._get_env_from_task", return_value=(None, None)):
            result = json.loads(diff_runs("Flow/100", "Flow/101"))

        assert result["source"] == "Flow/100"
        assert result["target"] == "Flow/101"
        assert result["summary"]["sections_with_diffs"] == []
        assert "No differences" in result["summary"]["note"]

    @patch("subprocess.run")
    @patch("metaflow.Run")
    def test_diff_runs_code_diff_present(self, mock_run_cls, mock_subprocess):
        run = _make_mock_run()
        mock_run_cls.return_value = run
        mock_subprocess.return_value = MagicMock(
            stdout="--- a/flow.py\n+++ b/flow.py\n@@ -1 +1 @@\n-old\n+new\n",
            stderr="", returncode=0
        )

        with patch("metaflow_mcp_server.server._get_env_from_task", return_value=(None, None)):
            result = json.loads(diff_runs("Flow/100", "Flow/101"))

        assert "code" in result["sections"]
        assert "-old" in result["sections"]["code"]
        assert "+new" in result["sections"]["code"]

    @patch("subprocess.run")
    @patch("metaflow.Run")
    def test_diff_runs_code_diff_truncated(self, mock_run_cls, mock_subprocess):
        run = _make_mock_run()
        mock_run_cls.return_value = run
        huge_diff = "x" * 30_000
        mock_subprocess.return_value = MagicMock(stdout=huge_diff, stderr="", returncode=0)

        with patch("metaflow_mcp_server.server._get_env_from_task", return_value=(None, None)):
            result = json.loads(diff_runs("Flow/100", "Flow/101"))

        assert "truncated" in result["sections"]["code"]
        assert len(result["sections"]["code"]) < 25_000

    @patch("subprocess.run")
    @patch("metaflow.Run")
    def test_diff_runs_parameter_diff(self, mock_run_cls, mock_subprocess):
        step_a = _make_mock_step("start", {"lr": 0.01})
        step_b = _make_mock_step("start", {"lr": 0.001})
        run_a = _make_mock_run(steps=[step_a])
        run_b = _make_mock_run(steps=[step_b])
        mock_run_cls.side_effect = [run_a, run_b]
        mock_subprocess.return_value = MagicMock(stdout="", stderr="", returncode=0)

        with patch("metaflow_mcp_server.server._get_env_from_task", return_value=(None, None)):
            result = json.loads(diff_runs("Flow/100", "Flow/101"))

        assert "parameters" in result["sections"]
        assert "lr" in result["sections"]["parameters"]

    @patch("subprocess.run")
    @patch("metaflow.Run")
    def test_diff_runs_summary_shows_run_status(self, mock_run_cls, mock_subprocess):
        run_a = _make_mock_run(successful=True)
        run_b = _make_mock_run(successful=False)
        mock_run_cls.side_effect = [run_a, run_b]
        mock_subprocess.return_value = MagicMock(stdout="", stderr="", returncode=0)

        with patch("metaflow_mcp_server.server._get_env_from_task", return_value=(None, None)):
            result = json.loads(diff_runs("Flow/100", "Flow/101"))

        assert result["summary"]["source"]["successful"] is True
        assert result["summary"]["target"]["successful"] is False


# ── get_task_logs hint test ──────────────────────────────────────────────────


class TestGetTaskLogsHint(unittest.TestCase):
    @patch("metaflow.Task")
    def test_hint_shown_when_empty_logs_and_failed(self, mock_task_cls):
        task = MagicMock()
        task.stdout = ""
        task.stderr = ""
        task.successful = False
        mock_task_cls.return_value = task

        result = json.loads(get_task_logs("Flow/1/start/1"))
        assert "hint" in result
        assert "get_bootstrap_failure" in result["hint"]
        assert "Flow/1" in result["hint"]

    @patch("metaflow.Task")
    def test_no_hint_when_logs_exist(self, mock_task_cls):
        task = MagicMock()
        task.stdout = "some output"
        task.stderr = ""
        task.successful = False
        mock_task_cls.return_value = task

        result = json.loads(get_task_logs("Flow/1/start/1"))
        assert "hint" not in result

    @patch("metaflow.Task")
    def test_no_hint_when_successful(self, mock_task_cls):
        task = MagicMock()
        task.stdout = ""
        task.stderr = ""
        task.successful = True
        mock_task_cls.return_value = task

        result = json.loads(get_task_logs("Flow/1/start/1"))
        assert "hint" not in result
