"""Database manager that provisions SQLAlchemy engines from YAML config."""
from __future__ import annotations

import os
from pathlib import Path
from typing import Dict
from urllib.parse import quote_plus

import yaml
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import make_url
from sqlalchemy.orm import scoped_session, sessionmaker

from .logging import get_logger
from .paths import resource_path

logger = get_logger(__name__)


class DatabaseManager:
    """Builds engines and scoped sessions for connections defined in application.yml."""

    def __init__(self, config_name: str = "application.yml"):
        self._config_path = resource_path(config_name)
        self._cfg = self._load_yaml()
        self.engines: Dict[str, Engine] = {}
        self.sessions: Dict[str, scoped_session] = {}
        self._build_all()

    def _load_yaml(self) -> dict:
        if not self._config_path.exists():
            logger.warning("Database config not found at %s", self._config_path)
            return {}
        with self._config_path.open("r", encoding="utf-8") as fh:
            return yaml.safe_load(fh) or {}

    @staticmethod
    def _build_url(conf: dict) -> str:
        if conf.get("url"):
            logger.debug("Using explicit URL for %s", conf.get("driver", "unknown"))
            return conf["url"]

        driver = conf.get("driver", "")
        user = quote_plus(conf.get("user", ""))
        pwd = quote_plus(conf.get("password", ""))
        host = conf.get("host", "localhost")
        port = conf.get("port")
        database = conf.get("database", "")
        port_part = f":{port}" if port else ""

        sanitized = f"{driver}://{user}:***@{host}{port_part}/{database}"
        logger.debug("Building URL -> %s", sanitized)
        return f"{driver}://{user}:{pwd}@{host}{port_part}/{database}"

    def _build_all(self) -> None:
        connections = self._cfg.get("connections", {})
        for name, conf in connections.items():
            env_override = os.getenv(f"{name.upper()}_DATABASE_URL")
            url = env_override or self._build_url(conf)
            if env_override:
                logger.debug(
                    "%s: using DATABASE_URL override (sanitized: %s)",
                    name,
                    self._sanitize_url(env_override),
                )

            connect_args = {}
            if name == "mysql":
                connect_args = {"charset": "utf8mb4"}
            elif name == "postgres":
                connect_args = {"options": "-csearch_path=security_master,public"}
                logger.debug("%s connect args: %s", name, connect_args)

            engine = create_engine(url, pool_pre_ping=True, connect_args=connect_args)
            logger.debug(
                "%s engine initialized: %s",
                name,
                engine.url.render_as_string(hide_password=True),
            )
            self.engines[name] = engine
            self.sessions[name] = scoped_session(sessionmaker(bind=engine))

    @staticmethod
    def _sanitize_url(url: str) -> str:
        try:
            return make_url(url).render_as_string(hide_password=True)
        except Exception:  # noqa: BLE001 - best effort sanitization
            return "***"

    def get_engine(self, name: str) -> Engine:
        return self.engines[name]

    def get_session(self, name: str) -> scoped_session:
        return self.sessions[name]

    def dispose_all(self) -> None:
        for session in self.sessions.values():
            try:
                session.remove()
            except Exception:  # noqa: BLE001 - best effort cleanup
                pass
        for engine in self.engines.values():
            try:
                engine.dispose()
            except Exception:  # noqa: BLE001 - best effort cleanup
                pass


db_manager = DatabaseManager()
mysql_engine = db_manager.get_engine("mysql")
postgres_engine = db_manager.get_engine("postgres")
mysql_db = db_manager.get_session("mysql")
postgres_db = db_manager.get_session("postgres")