"""API configuration loaded from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass


def parse_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y"}


@dataclass(frozen=True)
class SqlServerSettings:
    driver: str
    host: str
    database: str
    trusted_connection: bool
    username: str
    password: str
    encrypt: bool
    trust_server_certificate: bool

    @property
    def connection_string(self) -> str:
        parts = [
            f"DRIVER={{{self.driver}}}",
            f"SERVER={self.host}",
            f"DATABASE={self.database}",
            f"Encrypt={'yes' if self.encrypt else 'no'}",
            f"TrustServerCertificate={'yes' if self.trust_server_certificate else 'no'}",
        ]
        if self.trusted_connection:
            parts.append("Trusted_Connection=yes")
        else:
            parts.extend([f"UID={self.username}", f"PWD={self.password}"])
        return ";".join(parts)


@dataclass(frozen=True)
class ApiSettings:
    environment: str
    host: str
    port: int
    sql_server: SqlServerSettings


def get_settings() -> ApiSettings:
    sql_server = SqlServerSettings(
        driver=os.getenv("SQLSERVER_DRIVER", "ODBC Driver 18 for SQL Server"),
        host=os.getenv("SQLSERVER_HOST", "localhost"),
        database=os.getenv("SQLSERVER_DATABASE", "EnergyWarehouse"),
        trusted_connection=parse_bool(os.getenv("SQLSERVER_TRUSTED_CONNECTION"), True),
        username=os.getenv("SQLSERVER_USERNAME", ""),
        password=os.getenv("SQLSERVER_PASSWORD", ""),
        encrypt=parse_bool(os.getenv("SQLSERVER_ENCRYPT"), True),
        trust_server_certificate=parse_bool(os.getenv("SQLSERVER_TRUST_SERVER_CERTIFICATE"), True),
    )
    return ApiSettings(
        environment=os.getenv("API_ENV", "local"),
        host=os.getenv("API_HOST", "127.0.0.1"),
        port=int(os.getenv("API_PORT", "8000")),
        sql_server=sql_server,
    )
