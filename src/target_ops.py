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
        "DECLARE @fid BIGINT; "
        "EXEC dbo.reserve_pk_block @table_name=?, @block_size=?, @first_id=@fid OUTPUT; "
        "SELECT @fid;",
        table, block_size,
    )
    while cursor.description is None:
        if not cursor.nextset():
            break
    return cursor.fetchone()[0]


def build_pk_map(rows: list, pk_col: str, first_id: int) -> dict[int, int]:
    """Build source→target PK mapping from collected rows and a reserved first_id."""
    return {row[pk_col]: first_id + i for i, row in enumerate(rows)}


# --- Spark JDBC staging writes (one per wave table) ---

def _lit_map(pk_map: dict) -> object:
    return F.create_map([F.lit(x) for pair in pk_map.items() for x in pair])


def write_facilities_to_staging(df: DataFrame, fac_pk_map: dict[int, int]) -> None:
    mapping = _lit_map(fac_pk_map)
    staging = (
        df.withColumn("src_fac_id", F.col("fac_id"))
          .withColumn("fac_id", mapping[F.col("src_fac_id")])
          .select("fac_id", "src_fac_id", "name", "prov", "deleted")
    )
    staging.write.jdbc(
        url=target_jdbc_url(), table="dbo.facility_staging",
        mode="append", properties=target_props(),
    )


def write_mpi_to_staging(df: DataFrame, mpi_pk_map: dict[int, int]) -> None:
    mapping = _lit_map(mpi_pk_map)
    staging = (
        df.withColumn("src_mpi_id", F.col("mpi_id"))
          .withColumn("mpi_id", mapping[F.col("src_mpi_id")])
          .select("mpi_id", "src_mpi_id", "first_name", "last_name",
                  "date_of_birth", "sex", "deleted", "created_by", "created_date")
    )
    staging.write.jdbc(
        url=target_jdbc_url(), table="dbo.mpi_staging",
        mode="append", properties=target_props(),
    )


def write_clients_to_staging(
    df: DataFrame,
    client_pk_map: dict[int, int],
    fac_pk_map: dict[int, int],
    mpi_pk_map: dict[int, int],
) -> None:
    client_map = _lit_map(client_pk_map)
    fac_map    = _lit_map(fac_pk_map)
    mpi_map    = _lit_map(mpi_pk_map)
    staging = (
        df.withColumn("src_client_id", F.col("client_id"))
          .withColumn("client_id", client_map[F.col("src_client_id")])
          .withColumn("fac_id",    fac_map[F.col("fac_id")])
          .withColumn("mpi_id",    mpi_map[F.col("mpi_id")])
          .select("client_id", "src_client_id", "fac_id", "mpi_id",
                  "deleted", "admission_date", "discharge_date", "created_by", "created_date")
    )
    staging.write.jdbc(
        url=target_jdbc_url(), table="dbo.clients_staging",
        mode="append", properties=target_props(),
    )


def write_pho_phys_order_to_staging(
    df: DataFrame,
    phys_order_pk_map: dict[int, int],
    client_pk_map: dict[int, int],
    fac_pk_map: dict[int, int],
) -> None:
    po_map     = _lit_map(phys_order_pk_map)
    client_map = _lit_map(client_pk_map)
    fac_map    = _lit_map(fac_pk_map)
    staging = (
        df.withColumn("src_phys_order_id", F.col("phys_order_id"))
          .withColumn("phys_order_id", po_map[F.col("src_phys_order_id")])
          .withColumn("client_id",     client_map[F.col("client_id")])
          .withColumn("fac_id",        fac_map[F.col("fac_id")])
          .select("phys_order_id", "src_phys_order_id", "client_id", "fac_id",
                  "drug_name", "strength", "directions", "order_date",
                  "active_flag", "deleted", "created_by", "created_date")
    )
    staging.write.jdbc(
        url=target_jdbc_url(), table="dbo.pho_phys_order_staging",
        mode="append", properties=target_props(),
    )


def write_pho_order_schedule_to_staging(
    df: DataFrame,
    os_pk_map: dict[int, int],
    phys_order_pk_map: dict[int, int],
    fac_pk_map: dict[int, int],
) -> None:
    os_map  = _lit_map(os_pk_map)
    po_map  = _lit_map(phys_order_pk_map)
    fac_map = _lit_map(fac_pk_map)
    staging = (
        df.withColumn("src_order_schedule_id", F.col("order_schedule_id"))
          .withColumn("order_schedule_id", os_map[F.col("src_order_schedule_id")])
          .withColumn("phys_order_id",     po_map[F.col("phys_order_id")])
          .withColumn("fac_id",            fac_map[F.col("fac_id")])
          .select("order_schedule_id", "src_order_schedule_id", "phys_order_id", "fac_id",
                  "deleted", "dose_value", "directions",
                  "mon", "tues", "wed", "thurs", "fri", "sat", "sun",
                  "created_by", "created_date")
    )
    staging.write.jdbc(
        url=target_jdbc_url(), table="dbo.pho_order_schedule_staging",
        mode="append", properties=target_props(),
    )


def write_pho_schedule_to_staging(
    df: DataFrame,
    sched_pk_map: dict[int, int],
    os_pk_map: dict[int, int],
    phys_order_pk_map: dict[int, int],
    fac_pk_map: dict[int, int],
) -> None:
    sched_map = _lit_map(sched_pk_map)
    os_map    = _lit_map(os_pk_map)
    po_map    = _lit_map(phys_order_pk_map)
    fac_map   = _lit_map(fac_pk_map)
    staging = (
        df.withColumn("src_schedule_id", F.col("schedule_id"))
          .withColumn("schedule_id",       sched_map[F.col("src_schedule_id")])
          .withColumn("order_schedule_id", os_map[F.col("order_schedule_id")])
          .withColumn("phys_order_id",     po_map[F.col("phys_order_id")])
          .withColumn("fac_id",            fac_map[F.col("fac_id")])
          .select("schedule_id", "src_schedule_id", "order_schedule_id", "phys_order_id",
                  "fac_id", "deleted", "description", "start_time", "dose",
                  "created_by", "created_date")
    )
    staging.write.jdbc(
        url=target_jdbc_url(), table="dbo.pho_schedule_staging",
        mode="append", properties=target_props(),
    )


def write_pho_schedule_details_to_staging(
    df: DataFrame,
    detail_pk_map: dict[int, int],
    sched_pk_map: dict[int, int],
) -> None:
    detail_map = _lit_map(detail_pk_map)
    sched_map  = _lit_map(sched_pk_map)
    staging = (
        df.withColumn("src_pho_schedule_detail_id", F.col("pho_schedule_detail_id"))
          .withColumn("pho_schedule_detail_id", detail_map[F.col("src_pho_schedule_detail_id")])
          .withColumn("pho_schedule_id",        sched_map[F.col("pho_schedule_id")])
          .select("pho_schedule_detail_id", "src_pho_schedule_detail_id",
                  "pho_schedule_id", "schedule_date", "dose", "deleted",
                  "perform_by", "perform_date", "perform_initials",
                  "created_by", "created_date")
    )
    staging.write.jdbc(
        url=target_jdbc_url(), table="dbo.pho_schedule_details_staging",
        mode="append", properties=target_props(),
    )


def write_delta_to_staging(df: DataFrame) -> None:
    staging = (
        df.withColumnRenamed("client_id",             "src_client_id")
          .withColumnRenamed("SYS_CHANGE_OPERATION",  "operation")
          .withColumnRenamed("fac_id",                "src_fac_id")
          .withColumnRenamed("mpi_id",                "src_mpi_id")
          .select("src_client_id", "operation", "src_fac_id", "src_mpi_id",
                  "deleted", "admission_date", "discharge_date")
    )
    staging.write.jdbc(
        url=target_jdbc_url(),
        table="dbo.clients_delta_staging",
        mode="append",
        properties=target_props(),
    )


# --- SP calls to move staging → target (one per table + delta) ---

def load_facility_from_staging(conn: pyodbc.Connection) -> None:
    conn.execute("EXEC dbo.load_facility_from_staging")
    conn.commit()


def load_mpi_from_staging(conn: pyodbc.Connection) -> None:
    conn.execute("EXEC dbo.load_mpi_from_staging")
    conn.commit()


def load_clients_from_staging(conn: pyodbc.Connection) -> None:
    conn.execute("EXEC dbo.load_clients_from_staging")
    conn.commit()


def load_pho_phys_order_from_staging(conn: pyodbc.Connection) -> None:
    conn.execute("EXEC dbo.load_pho_phys_order_from_staging")
    conn.commit()


def load_pho_order_schedule_from_staging(conn: pyodbc.Connection) -> None:
    conn.execute("EXEC dbo.load_pho_order_schedule_from_staging")
    conn.commit()


def load_pho_schedule_from_staging(conn: pyodbc.Connection) -> None:
    conn.execute("EXEC dbo.load_pho_schedule_from_staging")
    conn.commit()


def load_pho_schedule_details_from_staging(conn: pyodbc.Connection) -> None:
    conn.execute("EXEC dbo.load_pho_schedule_details_from_staging")
    conn.commit()


def apply_clients_delta_from_staging(conn: pyodbc.Connection) -> None:
    conn.execute("EXEC dbo.apply_clients_delta_from_staging")
    conn.commit()
