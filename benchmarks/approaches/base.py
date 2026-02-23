"""Abstract base class for benchmark approaches."""

from abc import ABC, abstractmethod


class Approach(ABC):
    """Base class for benchmark approaches.

    Each approach defines a system prompt that controls how Claude Code
    handles the task. Tool execution happens inside Claude Code (via the
    relay), not in our harness.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Short identifier for this approach."""
        ...

    @abstractmethod
    def get_system_prompt(self) -> str:
        """Return the system prompt for this approach."""
        ...
