"""Configuration utilities for loading project settings."""
from __future__ import annotations

from functools import lru_cache
from typing import Any, Dict
from .paths import resource_path
import yaml

@lru_cache(maxsize=1)
def load_config(config_name: str = "application.yml") -> Dict[str, Any]:
    """Load the YAML configuration once and cache the result."""
    config_path = resource_path(config_name)
    if not config_path.exists():
        return {}
    with config_path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}

__all__ = ["load_config"]
