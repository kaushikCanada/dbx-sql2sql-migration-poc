import os
import pyodbc


def _conn_str(server: str, port: str, database: str, user: str, password: str) -> str:
    return (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={server},{port};"
        f"DATABASE={database};"
        f"UID={user};PWD={password};"
        "Encrypt=yes;TrustServerCertificate=yes;"
    )


def source_conn() -> pyodbc.Connection:
    return pyodbc.connect(
        _conn_str(
            os.environ["SOURCE_SERVER"],
            os.environ.get("SOURCE_PORT", "1433"),
            os.environ["SOURCE_DB"],
            os.environ["SOURCE_USER"],
            os.environ["SOURCE_PASSWORD"],
        )
    )


def target_conn(autocommit: bool = False) -> pyodbc.Connection:
    return pyodbc.connect(
        _conn_str(
            os.environ["TARGET_SERVER"],
            os.environ.get("TARGET_PORT", "1433"),
            os.environ["TARGET_DB"],
            os.environ["TARGET_USER"],
            os.environ["TARGET_PASSWORD"],
        ),
        autocommit=autocommit,
    )
