## Setup

```bash
source .venv/bin/activate
.venv/bin/python -m pip install -e .[dev]
```

## Running the application

Runtime code now lives under `src/main/python/security_recon` and is installed in editable mode, so the package is importable everywhere in the virtualenv once the setup steps above are complete.

```bash
.venv/bin/python application.py
```

### Tests

Primary test suites live in `src/test/python/security_recon_tests`. Run everything with:

```bash
.venv/bin/python -m pytest -q
```

Useful variations:

```bash
.venv/bin/python -m pytest -vv
```

## Project layout

```
security_recon/
├── application.py                       # Convenience entrypoint delegating to controller
├── src/
│   ├── main/
│   │   ├── python/
│   │   │   └── security_recon/
│   │   │       ├── controller/          # CLI/diagnostics entry points
│   │   │       ├── domain/              # Data dictionary rules
│   │   │       ├── integration/         # External adapters (Parquet writer, etc.)
│   │   │       ├── repositories/        # DB-backed repositories
│   │   │       ├── service/             # Pipeline runner and business logic
│   │   │       └── support/             # Logging, database, path utilities
│   │   └── resources/
│   │       ├── application.yml          # DB URLs, parquet settings, logging config
│   │       └── data_dictionary.yml      # Attribute rules
│   └── test/
│       └── python/
│           └── security_recon_tests/
│               └── pipelines/           # Pytest suites mirroring src/main structure
├── pytest.ini
└── README.md
```
