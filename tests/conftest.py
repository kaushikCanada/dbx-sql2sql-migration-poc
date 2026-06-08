import pytest
from src.spark_session import get_spark
from src.connections import source_conn, target_conn
from src.mirror_ops import run_mirror, read_mirror, get_watermark_ct_version
from src.target_ops import (
    reserve_pk_block,
    write_facilities_to_staging,
    write_clients_to_staging,
    load_facility_from_staging,
    load_clients_from_staging,
)


@pytest.fixture(scope="session")
def spark():
    return get_spark()


@pytest.fixture(scope="session")
def src():
    conn = source_conn()
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def tgt():
    conn = target_conn()
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def mirror(spark, src):
    """
    Snapshots source tables into Delta mirror once per session.
    Also writes _state Delta table with ct_version watermark.
    Returns the list of result dicts from run_mirror().
    """
    src_fac_ids = [
        row[0] for row in src.execute("SELECT fac_id FROM dbo.facility").fetchall()
    ]
    return run_mirror(spark, src_fac_ids, src_conn=src)


@pytest.fixture(scope="session")
def bulk_loaded(spark, src, tgt, mirror):
    """
    Runs the full bulk load once for the entire test session.
    Reads from mirror Delta tables (not live source JDBC).
    snap_version is read from the _state Delta watermark — not in-memory.
    Returns (fac_pk_map, client_pk_map, snap_version).
    """
    # --- Facilities ---
    fac_df = read_mirror(spark, "facility")
    fac_rows = fac_df.collect()
    fac_first_id = reserve_pk_block(tgt, "facility", len(fac_rows))
    fac_pk_map = {row["fac_id"]: fac_first_id + i for i, row in enumerate(fac_rows)}

    write_facilities_to_staging(fac_df, fac_pk_map)
    load_facility_from_staging(tgt)

    # --- Clients ---
    client_df = read_mirror(spark, "clients")
    client_rows = client_df.collect()
    client_first_id = reserve_pk_block(tgt, "clients", len(client_rows))
    client_pk_map = {row["client_id"]: client_first_id + i for i, row in enumerate(client_rows)}

    write_clients_to_staging(client_df, client_pk_map, fac_pk_map)
    load_clients_from_staging(tgt)

    # Read CT version watermark from _state Delta table — survives process restarts.
    snap_version = get_watermark_ct_version(spark)

    return fac_pk_map, client_pk_map, snap_version
