"""Narrow, opt-in reliability faults for designated test objects only."""

from __future__ import annotations

FAULT_PREFIX = "incoming/__queueforge_faults__/"


class InjectedFault(RuntimeError):
    """A deliberately injected, retryable test failure."""


def fault_point(enabled: bool, key: str, receive_count: int) -> str | None:
    """Return a deterministic failure point for the fixed test namespace."""
    if not enabled or not key.startswith(FAULT_PREFIX):
        return None
    relative = key.removeprefix(FAULT_PREFIX)
    if relative.startswith("persistent/"):
        return "before_processing"
    if relative.startswith("first/") and receive_count == 1:
        return "before_processing"
    if relative.startswith("after-report/") and receive_count == 1:
        return "after_report"
    return None
