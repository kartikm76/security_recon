"""Project-wide logging helper driven by src/main/resources/application.yml."""
from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

from .paths import resource_path

_DEFAULT_CONFIG_PATH = resource_path("application.yml")
_INITIALIZED = False
_CURRENT_LEVEL = logging.INFO

_BOOL_TRUE = {"1", "true", "yes", "on"}


def _load_logging_config(config_path: Optional[Path] = None) -> Dict[str, Any]:
    path = config_path or _DEFAULT_CONFIG_PATH
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    return data.get("logging", {}) or {}


def _determine_level(config: Dict[str, Any]) -> int:
    env_level = os.getenv("APP_LOG_LEVEL") or os.getenv("DB_LOG_LEVEL")
    if env_level:
        return getattr(logging, env_level.upper(), logging.INFO)

    if "level" in config:
        return getattr(logging, str(config["level"]).upper(), logging.INFO)

    env_debug = os.getenv("APP_DEBUG") or os.getenv("DB_DEBUG")
    if env_debug is not None:
        if str(env_debug).strip().lower() in _BOOL_TRUE:
            return logging.DEBUG
        return logging.INFO

    yaml_debug = config.get("debug") or config.get("db_debug")
    if yaml_debug:
        return logging.DEBUG

    return logging.INFO


def configure_logging(config_path: Optional[Path] = None, force: bool = False) -> None:
    global _INITIALIZED, _CURRENT_LEVEL
    if _INITIALIZED and not force:
        return

    cfg = _load_logging_config(config_path)
    level = _determine_level(cfg)
    _CURRENT_LEVEL = level

    logging.basicConfig(level=level)
    _INITIALIZED = True

    root = logging.getLogger()
    root.setLevel(level)

    extra_fields = {
        "level": logging.getLevelName(level),
        "config_path": str(config_path or _DEFAULT_CONFIG_PATH),
        "debug_flag": cfg.get("debug") or cfg.get("db_debug") or False,
    }
    logging.getLogger(__name__).debug("Logging configured: %s", extra_fields)


def get_logger(name: Optional[str] = None) -> logging.Logger:
    if not _INITIALIZED:
        configure_logging()
    logger = logging.getLogger(name)
    logger.setLevel(_CURRENT_LEVEL)
    return logger


def set_level(level: int) -> None:
    """Manually override the global logging level at runtime."""
    global _CURRENT_LEVEL
    _CURRENT_LEVEL = level
    logging.getLogger().setLevel(level)
