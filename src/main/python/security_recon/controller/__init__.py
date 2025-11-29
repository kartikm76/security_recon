"""Controllers and entrypoints (CLI, UI)."""

from .pipeline_runner_cli import main as pipeline_runner_main

__all__ = ["pipeline_runner_main"]
