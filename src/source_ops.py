from pyspark.sql import DataFrame, SparkSession
from src.spark_session import source_jdbc_url, source_props
import pyodbc


def bulk_read_facilities(spark: SparkSession) -> DataFrame:
    return spark.read.jdbc(
        url=source_jdbc_url(),
        table="(SELECT fac_id, name, prov, deleted FROM dbo.facility) AS t",
        properties=source_props(),
    )


def bulk_read_clients(spark: SparkSession) -> DataFrame:
    return spark.read.jdbc(
        url=source_jdbc_url(),
        table="(SELECT client_id, fac_id, first_name, last_name, deleted FROM dbo.clients) AS t",
        properties=source_props(),
    )


def ct_delta_clients(spark: SparkSession, since_version: int) -> DataFrame:
    query = f"""
        SELECT
            ct.client_id,
            ct.SYS_CHANGE_OPERATION,
            t.fac_id,
            t.first_name,
            t.last_name,
            t.deleted
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

def insert_client(conn: pyodbc.Connection, fac_id: int, first: str, last: str) -> int:
    cursor = conn.execute(
        "INSERT INTO dbo.clients (fac_id, first_name, last_name) OUTPUT INSERTED.client_id VALUES (?, ?, ?)",
        fac_id, first, last,
    )
    client_id = cursor.fetchone()[0]
    conn.commit()
    return client_id


def update_client(conn: pyodbc.Connection, client_id: int, first: str, last: str) -> None:
    conn.execute(
        "UPDATE dbo.clients SET first_name=?, last_name=? WHERE client_id=?",
        first, last, client_id,
    )
    conn.commit()


def soft_delete_client(conn: pyodbc.Connection, client_id: int) -> None:
    conn.execute("UPDATE dbo.clients SET deleted='Y' WHERE client_id=?", client_id)
    conn.commit()


def hard_delete_client(conn: pyodbc.Connection, client_id: int) -> None:
    conn.execute("DELETE FROM dbo.clients WHERE client_id=?", client_id)
    conn.commit()
