"""Shared path helpers for locating project resources."""
from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable


def _candidate_resource_roots() -> Iterable[Path]:
    env_dir = os.getenv("SECURITY_RECON_RESOURCES_DIR")
    if env_dir:
        yield Path(env_dir)

    package_dir = Path(__file__).resolve().parents[1] / "resources"
    yield package_dir

    # Local development layout: src/main/resources
    repo_main_resources = Path(__file__).resolve().parents[4] / "main" / "resources"
    yield repo_main_resources

    repo_resources = Path(__file__).resolve().parents[4] / "resources"
    yield repo_resources


def project_root() -> Path:
    """Return the repository root directory when available."""
    return Path(__file__).resolve().parents[5]


def resources_root() -> Path:
    """Resolve the resources directory from known locations."""
    for candidate in _candidate_resource_roots():
        if candidate.exists():
            return candidate
    raise FileNotFoundError("Unable to locate resources directory for security_recon")


def resource_path(*relative: str) -> Path:
    """Build an absolute path under the resolved resources directory."""
    return resources_root().joinpath(*relative)
