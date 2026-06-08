from pyspark.sql import DataFrame, SparkSession
from src.spark_session import source_jdbc_url, source_props
import pyodbc


def ct_delta_clients(spark: SparkSession, since_version: int) -> DataFrame:
    query = f"""
        SELECT
            ct.client_id,
            ct.SYS_CHANGE_OPERATION,
            t.fac_id,
            t.mpi_id,
            t.deleted,
            t.admission_date,
            t.discharge_date
        FROM CHANGETABLE(CHANGES dbo.clients, {int(since_version)}) AS ct
        LEFT JOIN dbo.clients t ON ct.client_id = t.client_id
    """
    return spark.read.jdbc(
        url=source_jdbc_url(),
        table=f"({query}) AS delta",
        properties=source_props(),
    )


def get_current_ct_version(conn: pyodbc.Connection) -> int:
    return conn.execute("SELECT CHANGE_TRACKING_CURRENT_VERSION()").fetchone()[0]


def get_min_valid_ct_version(conn: pyodbc.Connection, table: str) -> int:
    return conn.execute(
        f"SELECT CHANGE_TRACKING_MIN_VALID_VERSION(OBJECT_ID('dbo.{table}'))"
    ).fetchone()[0]


# --- Mutation helpers used by tests to drive source changes ---

def insert_client(conn: pyodbc.Connection, fac_id: int, mpi_id: int | None = None) -> int:
    """Insert into the non-IDENTITY source clients table using MAX(PK)+1."""
    cursor = conn.execute("SELECT ISNULL(MAX(client_id), 0) + 1 FROM dbo.clients")
    new_id = cursor.fetchone()[0]
    conn.execute(
        "INSERT INTO dbo.clients (client_id, fac_id, mpi_id, admission_date) "
        "VALUES (?, ?, ?, GETDATE())",
        new_id, fac_id, mpi_id,
    )
    conn.commit()
    return new_id


def update_client(conn: pyodbc.Connection, client_id: int, discharge_date: str) -> None:
    conn.execute(
        "UPDATE dbo.clients SET discharge_date=? WHERE client_id=?",
        discharge_date, client_id,
    )
    conn.commit()


def soft_delete_client(conn: pyodbc.Connection, client_id: int) -> None:
    conn.execute("UPDATE dbo.clients SET deleted='Y' WHERE client_id=?", client_id)
    conn.commit()


def hard_delete_client(conn: pyodbc.Connection, client_id: int) -> None:
    conn.execute("DELETE FROM dbo.clients WHERE client_id=?", client_id)
    conn.commit()
