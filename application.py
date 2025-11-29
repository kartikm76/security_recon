"""Convenience entrypoint so `python application.py` still works."""

from security_recon.controller.pipeline_orchestrator import main

if __name__ == "__main__":
    print("Jai Guruji")
    main()