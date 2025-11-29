"""Entrypoint"""

from datetime import date
from security_recon.controller.pipeline_orchestrator import Orchestration

if __name__ == "__main__":
    try:
        as_of_date = date(2023, 12, 30)
        
        orchestrator = Orchestration()
        orchestrator.pipeline_orchestrator(as_of_date = as_of_date)
        orchestrator.pipeline_orchestrator(as_of_date = as_of_date)
    except Exception as e:        
        print(e)