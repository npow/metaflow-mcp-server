"""Benchmark approaches: MCP Direct, CF Code Mode, Code Mode, Skill."""

from benchmarks.approaches.mcp_direct import MCPDirectApproach
from benchmarks.approaches.cf_code_mode import CFCodeModeApproach
from benchmarks.approaches.code_mode import CodeModeApproach
from benchmarks.approaches.skill import SkillApproach

APPROACHES = {
    "mcp_direct": MCPDirectApproach,
    "cf_code_mode": CFCodeModeApproach,
    "code_mode": CodeModeApproach,
    "skill": SkillApproach,
}

__all__ = [
    "APPROACHES",
    "MCPDirectApproach",
    "CFCodeModeApproach",
    "CodeModeApproach",
    "SkillApproach",
]
