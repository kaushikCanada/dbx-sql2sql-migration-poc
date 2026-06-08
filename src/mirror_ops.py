import os
import shutil
import time
import uuid
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import types as T

from src.spark_session import source_jdbc_url, source_props

# Root path for all mirror Delta tables.
MIRROR_BASE: str = os.environ.get("MIRROR_PATH", "/app/_mirror")

# Set "true" to skip drop-and-reload and reuse existing mirror tables.
SKIP_MIRROR_RELOAD: bool = os.environ.get("SKIP_MIRROR_RELOAD", "false").lower() == "true"

# Path of the Delta state table — persists CT watermark across process restarts.
_STATE_PATH: str = os.path.join(MIRROR_BASE, "_state")

# Wave ordering: wave_number → [table_name, ...].
# Determines FK-safe processing order — parents always before children.
WAVE_CONFIG: dict[int, list[str]] = {
    2: ["facility"],
    3: ["mpi", "clients"],
    5: ["pho_phys_order", "pho_order_schedule"],
    6: ["pho_schedule", "pho_schedule_details"],
}

# Mirror queries: table_name → SELECT SQL.
# Separated from wave ordering so each concern lives in one place.
MIRROR_QUERIES: dict[str, str] = {
    "facility": (
        "SELECT fac_id, name, prov, deleted "
        "FROM dbo.facility"
    ),
    "mpi": (
        "SELECT mpi_id, first_name, last_name, date_of_birth, sex, "
        "deleted, created_by, created_date "
        "FROM dbo.mpi"
    ),
    "clients": (
        "SELECT client_id, fac_id, mpi_id, deleted, "
        "admission_date, discharge_date, created_by, created_date "
        "FROM dbo.clients"
    ),
    "pho_phys_order": (
        "SELECT phys_order_id, client_id, fac_id, drug_name, strength, "
        "directions, order_date, active_flag, deleted, created_by, created_date "
        "FROM dbo.pho_phys_order"
    ),
    "pho_order_schedule": (
        "SELECT order_schedule_id, phys_order_id, fac_id, deleted, dose_value, "
        "directions, mon, tues, wed, thurs, fri, sat, sun, created_by, created_date "
        "FROM dbo.pho_order_schedule"
    ),
    "pho_schedule": (
        "SELECT schedule_id, order_schedule_id, phys_order_id, fac_id, deleted, "
        "description, start_time, dose, created_by, created_date "
        "FROM dbo.pho_schedule"
    ),
    "pho_schedule_details": (
        "SELECT pho_schedule_detail_id, pho_schedule_id, schedule_date, dose, "
        "deleted, perform_by, perform_date, perform_initials, created_by, created_date "
        "FROM dbo.pho_schedule_details"
    ),
}

# Flat ordered list of all tables — wave order preserved.
ALL_TABLES: list[str] = [
    table
    for wave in sorted(WAVE_CONFIG)
    for table in WAVE_CONFIG[wave]
]

# Tables that carry a fac_id column and should be scoped to src_fac_ids.
_FAC_SCOPED: frozenset[str] = frozenset({
    "clients", "pho_phys_order", "pho_order_schedule", "pho_schedule",
})

_STATE_SCHEMA = T.StructType([
    T.StructField("run_id",      T.StringType(), False),
    T.StructField("ts",          T.StringType(), False),
    T.StructField("ct_version",  T.LongType(),   False),
    T.StructField("src_fac_ids", T.StringType(), False),
    T.StructField("table",       T.StringType(), False),
    T.StructField("wave",        T.IntegerType(),False),
    T.StructField("status",      T.StringType(), False),
    T.StructField("rows",        T.LongType(),   True),
    T.StructField("elapsed_sec", T.DoubleType(), True),
    T.StructField("error",       T.StringType(), True),
])


def _mirror_path(table: str) -> str:
    return os.path.join(MIRROR_BASE, table)


def _get_ct_version(conn) -> int:
    return conn.execute("SELECT CHANGE_TRACKING_CURRENT_VERSION()").fetchone()[0]


def run_mirror(spark: SparkSession, src_fac_ids: list[int], src_conn) -> list[dict]:
    """
    Snapshot all source tables into Delta at MIRROR_BASE in wave order, then
    write a _state Delta table recording the CT version at snapshot time.

    Processes waves in sorted order (2 → 3 → 5 → 6) so FK dependencies are
    always mirrored before their dependants — directly mirroring the wave
    execution order that Databricks Workflows will enforce in production.

    Returns list of result dicts (table, wave, status, rows, elapsed_sec).
    """
    run_id = str(uuid.uuid4())[:8]

    if not SKIP_MIRROR_RELOAD:
        if os.path.exists(MIRROR_BASE):
            shutil.rmtree(MIRROR_BASE)
        os.makedirs(MIRROR_BASE, exist_ok=True)

    # CT version captured before any reads — conservative watermark ensures
    # the subsequent delta phase picks up every change since snapshot time.
    ct_version = _get_ct_version(src_conn)

    fac_ids_csv = ", ".join(str(f) for f in src_fac_ids + [-1])
    results: list[dict] = []

    for wave in sorted(WAVE_CONFIG):
        for table in WAVE_CONFIG[wave]:
            base_sql = MIRROR_QUERIES[table]
            t0 = time.time()

            # Inject facility scope filter for tables that carry fac_id.
            sql = (
                f"{base_sql} WHERE fac_id IN ({fac_ids_csv})"
                if table in _FAC_SCOPED
                else base_sql
            )

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
                    "run_id": run_id, "table": table, "wave": wave,
                    "status": "success", "rows": row_count,
                    "elapsed_sec": elapsed, "error": None,
                    "ts": datetime.now().isoformat(),
                })
            except Exception as exc:
                elapsed = round(time.time() - t0, 2)
                results.append({
                    "run_id": run_id, "table": table, "wave": wave,
                    "status": "failed", "rows": 0,
                    "elapsed_sec": elapsed, "error": str(exc),
                    "ts": datetime.now().isoformat(),
                })

    failed = [r for r in results if r["status"] == "failed"]
    if failed:
        names = ", ".join(r["table"] for r in failed)
        raise RuntimeError(f"Mirror failed for tables: {names}. See results for details.")

    # Write _state — one row per table, all sharing the same run_id and ct_version.
    state_rows = [
        (
            r["run_id"], r["ts"], ct_version,
            ",".join(str(f) for f in src_fac_ids),
            r["table"], r["wave"],
            r["status"], r.get("rows"), r.get("elapsed_sec"), r.get("error"),
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
    """Read the _state Delta table written by the last run_mirror() call."""
    if not os.path.exists(_STATE_PATH):
        raise FileNotFoundError(
            f"Mirror state not found at {_STATE_PATH}. "
            "Run run_mirror() first."
        )
    return spark.read.format("delta").load(_STATE_PATH)


def get_watermark_ct_version(spark: SparkSession) -> int:
    """Return the CT version captured at the last mirror run."""
    state = read_mirror_state(spark)
    row = state.select("ct_version").first()
    if row is None:
        raise RuntimeError("Mirror state table is empty — cannot read ct_version watermark.")
    return int(row["ct_version"])
