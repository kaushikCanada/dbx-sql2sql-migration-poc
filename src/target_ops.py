from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from src.spark_session import target_jdbc_url, target_props
import pyodbc


# --- PK reservation ---

def reserve_pk_block(conn: pyodbc.Connection, table: str, block_size: int) -> int:
    if block_size <= 0:
        raise ValueError(f"block_size must be > 0, got {block_size}")
    cursor = conn.cursor()
    cursor.execute(
        "DECLARE @fid INT; "
        "EXEC dbo.reserve_pk_block @table_name=?, @block_size=?, @first_id=@fid OUTPUT; "
        "SELECT @fid;",
        table, block_size,
    )
    while cursor.description is None:
        if not cursor.nextset():
            break
    return cursor.fetchone()[0]


# --- Spark JDBC staging writes ---

def write_facilities_to_staging(df: DataFrame, fac_pk_map: dict[int, int]) -> None:
    mapping = F.create_map([F.lit(x) for pair in fac_pk_map.items() for x in pair])
    staging = (
        df.withColumn("src_fac_id", F.col("fac_id"))
          .withColumn("fac_id", mapping[F.col("src_fac_id")])
          .select("fac_id", "name", "prov", "deleted", "src_fac_id")
    )
    staging.write.jdbc(
        url=target_jdbc_url(),
        table="dbo.facility_staging",
        mode="append",
        properties=target_props(),
    )


def write_clients_to_staging(
    df: DataFrame,
    client_pk_map: dict[int, int],
    fac_pk_map: dict[int, int],
) -> None:
    client_map = F.create_map([F.lit(x) for pair in client_pk_map.items() for x in pair])
    fac_map    = F.create_map([F.lit(x) for pair in fac_pk_map.items() for x in pair])
    staging = (
        df.withColumn("src_client_id", F.col("client_id"))
          .withColumn("client_id", client_map[F.col("src_client_id")])
          .withColumn("fac_id",    fac_map[F.col("fac_id")])
          .select("client_id", "fac_id", "first_name", "last_name", "deleted", "src_client_id")
    )
    staging.write.jdbc(
        url=target_jdbc_url(),
        table="dbo.clients_staging",
        mode="append",
        properties=target_props(),
    )


def write_delta_to_staging(df: DataFrame) -> None:
    staging = (
        df.withColumnRenamed("client_id", "src_client_id")
          .withColumnRenamed("SYS_CHANGE_OPERATION", "operation")
          .select("src_client_id", "operation", "fac_id", "first_name", "last_name", "deleted")
    )
    staging.write.jdbc(
        url=target_jdbc_url(),
        table="dbo.clients_delta_staging",
        mode="append",
        properties=target_props(),
    )


# --- SP calls to move staging → target ---

def load_facility_from_staging(conn: pyodbc.Connection) -> None:
    conn.execute("EXEC dbo.load_facility_from_staging")
    conn.commit()


def load_clients_from_staging(conn: pyodbc.Connection) -> None:
    conn.execute("EXEC dbo.load_clients_from_staging")
    conn.commit()


def apply_clients_delta_from_staging(conn: pyodbc.Connection) -> None:
    conn.execute("EXEC dbo.apply_clients_delta_from_staging")
    conn.commit()
