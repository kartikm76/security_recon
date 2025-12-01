"""Configuration utilities for loading project settings."""
from __future__ import annotations

import os
from pathlib import Path
from functools import lru_cache
from typing import Any, Dict

import yaml

from .paths import resource_path


@lru_cache(maxsize=1)
def load_config(config_name: str = "application.yml") -> Dict[str, Any]:
    """Load the YAML configuration once and cache the result."""
    env_override = os.getenv("SECURITY_RECON_CONFIG_PATH")
    if env_override:
        config_path = Path(env_override)
    else:
        config_path = resource_path(config_name)

    if not config_path.exists():
        return {}

    with config_path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


__all__ = ["load_config"]
