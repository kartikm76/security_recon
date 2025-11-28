## Setup

```bash
source .venv/bin/activate
python3 -m pip install -r requirements.txt
python3 application.py
```
# Tests
PYTHONPATH=src .venv/bin/python -m pytest -vv
OR
.venv/bin/python -m pytest -vv
.venv/bin/python -m pytest -q
(.venv) /Users/kartikmakker/Kartik_Workspace/Python_Projects/security_recon $  .venv/bin/python -m pytest -q
3 passed in 0.76s
```

## Project layout

```
security_recon/
├── app/
│   ├── backend.py          # FastAPI async service layer
│   ├── cli.py              # CLI runner (python app/cli.py --date ...)
│   ├── streamlit_app.py    # UI layer
│   └── __init__.py
│
├── application.py          # Global entrypoint (optional)
│
├── core/
│   ├── database.py         # Engine/connection builders (MySQL/Postgres)
│   ├── logging.py          # Unified logging configuration
│   └── __init__.py
│
├── parquet/                # Local parquet output (Phase 1)
│
├── pipelines/              # Core recon engine (OO)
│   ├── extract.py          # Repositories (MySQL + Postgres)
│   ├── dictionary.py       # Rule loading (YAML)
│   ├── diff.py             # DiffEngine (attribute comparison)
│   ├── classify.py         # Classification layer (match / explainable)
│   ├── io_parquet.py       # ParquetWriter
│   ├── metrics.py          # MetricsCalculator + MetricsRepository
│   ├── run.py              # ReconciliationEngine (sync)
│   └── __init__.py
│
├── services/
│   ├── database_service.py # Additional helper for DB ops (if needed)
│   └── __init__.py
│
├── resources/
│   ├── application.yml     # DB URLs, parquet path, FastAPI configs
│   └── data_dictionary.yml # transformation + tolerance rules
│
├── requirements.txt
└── README.md
```

This keeps infrastructure code in `core`, business services in `services`, orchestration/entrypoints in `app`, and domain-specific pipelines under `pipelines`.