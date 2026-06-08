import os
import shutil
import time
import uuid
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import types as T

from src.spark_session import source_jdbc_url, source_props

# Root path for all mirror Delta tables. Override via env var for testing.
MIRROR_BASE: str = os.environ.get("MIRROR_PATH", "/app/_mirror")

# Set to "true" to skip the drop-and-reload and reuse existing mirror tables.
SKIP_MIRROR_RELOAD: bool = os.environ.get("SKIP_MIRROR_RELOAD", "false").lower() == "true"

# Path of the Delta state table — persists watermark across process restarts.
_STATE_PATH: str = os.path.join(MIRROR_BASE, "_state")

# Tables to snapshot, each entry: (table_name, scoped_sql)
# Facility and cross-facility tables are mirrored without a fac_id filter
# (matching _MIRROR_FAC_EXCLUSIONS in the production notebook).
_MIRROR_TABLES: list[tuple[str, str]] = [
    ("facility", "SELECT fac_id, name, prov, deleted FROM dbo.facility"),
    ("clients",  "SELECT client_id, fac_id, first_name, last_name, deleted FROM dbo.clients"),
]

_STATE_SCHEMA = T.StructType([
    T.StructField("run_id",      T.StringType(),    False),
    T.StructField("ts",          T.StringType(),    False),
    T.StructField("ct_version",  T.LongType(),      False),
    T.StructField("src_fac_ids", T.StringType(),    False),  # comma-separated ints
    T.StructField("table",       T.StringType(),    False),
    T.StructField("status",      T.StringType(),    False),
    T.StructField("rows",        T.LongType(),      True),
    T.StructField("elapsed_sec", T.DoubleType(),    True),
    T.StructField("error",       T.StringType(),    True),
])


def _mirror_path(table: str) -> str:
    return os.path.join(MIRROR_BASE, table)


def _get_ct_version(conn) -> int:
    """Read current CT version from the source via an open pyodbc connection."""
    return conn.execute("SELECT CHANGE_TRACKING_CURRENT_VERSION()").fetchone()[0]


def run_mirror(spark: SparkSession, src_fac_ids: list[int], src_conn) -> list[dict]:
    """
    Snapshot source tables into Delta at MIRROR_BASE, then write a _state
    Delta table recording the CT version at snapshot time.

    Drop-and-reload on every call unless SKIP_MIRROR_RELOAD=true.
    src_fac_ids: list of source fac_id values for the facility being migrated.
    src_conn:    open pyodbc connection to source — used to read CT version.
    Returns a list of result dicts (table, status, rows, elapsed_sec).
    """
    run_id = str(uuid.uuid4())[:8]

    if not SKIP_MIRROR_RELOAD:
        if os.path.exists(MIRROR_BASE):
            shutil.rmtree(MIRROR_BASE)
        os.makedirs(MIRROR_BASE, exist_ok=True)

    # Capture CT version immediately before the first table read so the
    # watermark is conservative — any change at or after this version will
    # be picked up by the subsequent delta phase.
    ct_version = _get_ct_version(src_conn)

    fac_ids_csv = ", ".join(str(f) for f in src_fac_ids + [-1])
    results: list[dict] = []

    for table, base_sql in _MIRROR_TABLES:
        t0 = time.time()
        if "fac_id" in base_sql and table not in ("facility",):
            sql = f"{base_sql} WHERE fac_id IN ({fac_ids_csv})"
        else:
            sql = base_sql

        try:
            df = spark.read.jdbc(
                url=source_jdbc_url(),
                table=f"({sql}) AS t",
                properties=source_props(),
            )
            row_count = df.count()
            df.write.format("delta").mode("overwrite").save(_mirror_path(table))
            elapsed = round(time.time() - t0, 2)
            results.append({
                "run_id": run_id, "table": table, "status": "success",
                "rows": row_count, "elapsed_sec": elapsed, "error": None,
                "ts": datetime.now().isoformat(),
            })
        except Exception as exc:
            elapsed = round(time.time() - t0, 2)
            results.append({
                "run_id": run_id, "table": table, "status": "failed",
                "rows": 0, "elapsed_sec": elapsed, "error": str(exc),
                "ts": datetime.now().isoformat(),
            })

    failed = [r for r in results if r["status"] == "failed"]
    if failed:
        names = ", ".join(r["table"] for r in failed)
        raise RuntimeError(f"Mirror failed for tables: {names}. See results for details.")

    # Write state table — one row per mirrored table, all sharing the same
    # run_id and ct_version watermark.
    state_rows = [
        (
            r["run_id"],
            r["ts"],
            ct_version,
            ",".join(str(f) for f in src_fac_ids),
            r["table"],
            r["status"],
            r.get("rows"),
            r.get("elapsed_sec"),
            r.get("error"),
        )
        for r in results
    ]
    state_df = spark.createDataFrame(state_rows, schema=_STATE_SCHEMA)
    state_df.write.format("delta").mode("overwrite").save(_STATE_PATH)

    return results


def read_mirror(spark: SparkSession, table: str) -> DataFrame:
    """Read a previously mirrored table from Delta storage."""
    path = _mirror_path(table)
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"Mirror table '{table}' not found at {path}. "
            "Run run_mirror() first."
        )
    return spark.read.format("delta").load(path)


def read_mirror_state(spark: SparkSession) -> DataFrame:
    """
    Read the _state Delta table written by the last run_mirror() call.
    Contains one row per mirrored table with run_id, ts, ct_version, and
    per-table status. The delta phase reads ct_version from here instead of
    relying on an in-memory variable.
    """
    if not os.path.exists(_STATE_PATH):
        raise FileNotFoundError(
            f"Mirror state not found at {_STATE_PATH}. "
            "Run run_mirror() first."
        )
    return spark.read.format("delta").load(_STATE_PATH)


def get_watermark_ct_version(spark: SparkSession) -> int:
    """
    Return the CT version captured at the last mirror run.
    This is the starting point for the delta phase.
    """
    state = read_mirror_state(spark)
    row = state.select("ct_version").first()
    if row is None:
        raise RuntimeError("Mirror state table is empty — cannot read ct_version watermark.")
    return int(row["ct_version"])
