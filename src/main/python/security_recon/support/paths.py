"""Shared path helpers for locating project resources."""
from __future__ import annotations

from pathlib import Path


def project_root() -> Path:
    """Return the repository root directory."""
    return Path(__file__).resolve().parents[5]


def resources_root() -> Path:
    """Return the src/main/resources directory."""
    return Path(__file__).resolve().parents[4] / "main" / "resources"


def resource_path(*relative: str) -> Path:
    """Build an absolute path under src/main/resources."""
    return resources_root().joinpath(*relative)
