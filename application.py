"""Convenience entrypoint so `python application.py` still works."""

from security_recon.controller.pipeline_runner_cli import main

if __name__ == "__main__":
    main()