"""Small logging helper that reads settings from ``application.yml``."""
from __future__ import annotations

import logging
from functools import wraps
from typing import Any, Callable, Dict, Optional, TypeVar

from .config import load_config

F = TypeVar("F", bound=Callable[..., Any])


class LoggingManager:
    _initialized: bool = False
    _current_level: int = logging.INFO

    """
    @classmethod tells Python to pass the class object itself as the first argument (cls).
    With that access, the method can look at and update class-level state such as _initialized and _current_level, so the configuration applies to every LoggingManager user without needing instances.
    Because everything is stored at the class level, we never have to instantiate the class; callers just use LoggingManager.configure() / .get_logger() and share the same cached configuration.
    """
    @classmethod
    def configure(cls, *, force: bool = False) -> None:
        if cls._initialized and not force:
            return

        settings = load_config().get("logging", {}) or {}
        level = cls._resolve_level(settings)
        cls._current_level = level

        logging.basicConfig(level=level)
        logging.getLogger().setLevel(level)
        cls._initialized = True

    @classmethod
    def get_logger(cls, name: Optional[str] = None) -> logging.Logger:
        if not cls._initialized:
            cls.configure()
        logger = logging.getLogger(name)
        logger.setLevel(cls._current_level)
        return logger

    @classmethod
    def set_level(cls, level: int) -> None:
        cls._current_level = level
        logging.getLogger().setLevel(level)

    @staticmethod
    def _resolve_level(config: Dict[str, Any]) -> int:
        level_name = config.get("level")
        if level_name:
            return getattr(logging, str(level_name).upper(), logging.INFO)
        if config.get("debug"):
            return logging.DEBUG
        return logging.INFO


def log_calls(level: int = logging.DEBUG) -> Callable[[F], F]:  ## decorator factory to choose the log level (default DEBUG) and it returns a decorator.

    def decorator(func: F) -> F:                                # decorator that wraps the function
        @wraps(func)                                            # preserves metadata of the original function
        def wrapper(*args, **kwargs):                           # actual wrapper function
            logger = LoggingManager.get_logger(func.__module__)
            qualified_name = func.__qualname__
            logger.log(level, "Entering %s", qualified_name)
            try:
                result = func(*args, **kwargs)
                logger.log(level, "Exiting %s", qualified_name)
                return result
            except Exception:  # noqa: BLE001
                logger.exception("Error in %s", qualified_name)
                raise

        return wrapper  # type: ignore[return-value]

    return decorator


def configure_logging(force: bool = False) -> None:
    LoggingManager.configure(force=force)


def get_logger(name: Optional[str] = None) -> logging.Logger:
    return LoggingManager.get_logger(name)


def set_level(level: int) -> None:
    LoggingManager.set_level(level)
