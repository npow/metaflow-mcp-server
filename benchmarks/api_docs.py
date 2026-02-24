"""Introspect the Metaflow client to generate an API reference and keyword search."""

import inspect
from dataclasses import dataclass


@dataclass
class APIEntry:
    """A single API member (method, property, or function)."""

    class_name: str
    member_name: str
    kind: str  # "method", "property", "function"
    signature: str
    docstring: str
    search_text: str  # lowercased concatenation for keyword matching


def _get_members(cls):
    """Extract public methods and properties from a class."""
    entries = []
    for name, obj in inspect.getmembers(cls):
        if name.startswith("_"):
            continue
        class_name = cls.__name__
        if isinstance(obj, property) or isinstance(inspect.getattr_static(cls, name, None), property):
            kind = "property"
            sig = f"{class_name}.{name}"
            doc = (obj.fget.__doc__ if isinstance(obj, property) and obj.fget else "") or ""
        elif callable(obj):
            kind = "method"
            try:
                sig = f"{class_name}.{name}{inspect.signature(obj)}"
            except (ValueError, TypeError):
                sig = f"{class_name}.{name}(...)"
            doc = inspect.getdoc(obj) or ""
        else:
            continue
        search_text = f"{class_name} {name} {kind} {doc}".lower()
        entries.append(APIEntry(class_name, name, kind, sig, doc, search_text))
    return entries


def generate_api_reference() -> list[APIEntry]:
    """Introspect Metaflow client classes and return all API entries."""
    from metaflow import Flow, Run, Step, Task
    from metaflow.client.core import DataArtifact, Metaflow, namespace
    from metaflow.client import get_metadata

    entries = []
    for cls in [Metaflow, Flow, Run, Step, Task, DataArtifact]:
        entries.extend(_get_members(cls))

    # Add standalone functions
    for fn_name, fn in [("namespace", namespace), ("get_metadata", get_metadata)]:
        try:
            sig = f"{fn_name}{inspect.signature(fn)}"
        except (ValueError, TypeError):
            sig = f"{fn_name}(...)"
        doc = inspect.getdoc(fn) or ""
        search_text = f"{fn_name} function {doc}".lower()
        entries.append(APIEntry("(module)", fn_name, "function", sig, doc, search_text))

    return entries


def format_api_reference(entries: list[APIEntry]) -> str:
    """Render API entries as a markdown reference for the skill system prompt."""
    lines = ["# Metaflow Python Client API Reference", ""]
    current_class = None
    for e in entries:
        if e.class_name != current_class:
            current_class = e.class_name
            lines.append(f"## {current_class}")
            lines.append("")
        doc_summary = e.docstring.split("\n")[0] if e.docstring else ""
        lines.append(f"- `{e.signature}` — {doc_summary}")
    lines.append("")
    lines.append("## Common Patterns")
    lines.append("```python")
    lines.append("from metaflow import Metaflow, Flow, Run, Step, Task, namespace")
    lines.append("namespace(None)  # see all runs")
    lines.append("for flow in Metaflow():  # list all flows")
    lines.append("for run in Flow('MyFlow'):  # iterate runs (newest first)")
    lines.append("run.successful, run.finished, run.finished_at  # run status")
    lines.append("for step in run:  # iterate steps")
    lines.append("for task in step:  # iterate tasks")
    lines.append("task.stdout, task.stderr, task.exception  # task details")
    lines.append("for artifact in task:  # iterate artifacts")
    lines.append("task['name'].data  # get artifact value")
    lines.append("```")
    return "\n".join(lines)


def generate_api_schema() -> list[dict]:
    """Return the MCP tool function schemas as a list of dicts.

    This is the server-side schema that CF Code Mode's search phase queries.
    Each dict has: name, signature, docstring, parameters (list of param dicts).
    """
    import inspect as _inspect
    from metaflow_mcp_server.server import (
        get_config, list_flows, search_runs, get_run, get_task_logs,
        list_artifacts, get_artifact, get_latest_failure, search_artifacts,
    )

    schema = []
    for fn in [get_config, list_flows, search_runs, get_run, get_task_logs,
               list_artifacts, get_artifact, get_latest_failure, search_artifacts]:
        # Unwrap the _handle_errors decorator
        inner = fn.__wrapped__ if hasattr(fn, '__wrapped__') else fn
        try:
            sig = _inspect.signature(inner)
        except (ValueError, TypeError):
            sig = "()"
        params = []
        for pname, param in sig.parameters.items():
            params.append({
                "name": pname,
                "type": str(param.annotation) if param.annotation != param.empty else "str",
                "default": str(param.default) if param.default != param.empty else None,
            })
        schema.append({
            "name": fn.__name__,
            "signature": f"{fn.__name__}{sig}",
            "docstring": _inspect.getdoc(inner) or "",
            "parameters": params,
            "module": "metaflow_mcp_server.server",
        })
    return schema


def search_api(query: str, entries: list[APIEntry], top_k: int = 10) -> str:
    """Keyword overlap search over API entries. Returns formatted matches."""
    query_words = set(query.lower().split())
    scored = []
    for e in entries:
        entry_words = set(e.search_text.split())
        overlap = len(query_words & entry_words)
        if overlap > 0:
            scored.append((overlap, e))
    scored.sort(key=lambda x: -x[0])
    results = scored[:top_k]

    if not results:
        return "No matching API entries found."

    lines = []
    for _, e in results:
        lines.append(f"**{e.kind}**: `{e.signature}`")
        if e.docstring:
            lines.append(f"  {e.docstring[:200]}")
        lines.append("")
    return "\n".join(lines)
