"""Skill approach: invokes the /metaflow skill, which loads ~/.claude/skills/metaflow/SKILL.md."""

from benchmarks.approaches.base import Approach


class SkillApproach(Approach):
    """Simulates the Claude Code skills.md pattern.

    System prompt is minimal. The /metaflow skill is invoked by prepending
    it to the user message, which causes Claude Code to load
    ~/.claude/skills/metaflow/SKILL.md into context — exactly as a user
    would type `/metaflow <question>` in a real session.
    """

    @property
    def name(self) -> str:
        return "skill"

    def get_system_prompt(self) -> str:
        return "You are a Metaflow assistant."

    def transform_user_prompt(self, prompt: str) -> str:
        return f"/metaflow {prompt}"
