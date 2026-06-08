import pytest
from src.spark_session import get_spark
from src.connections import source_conn, target_conn
from src.mirror_ops import run_mirror, read_mirror, get_watermark_ct_version
from src.target_ops import (
    build_pk_map,
    reserve_pk_block,
    write_facilities_to_staging,
    write_mpi_to_staging,
    write_clients_to_staging,
    write_pho_phys_order_to_staging,
    write_pho_order_schedule_to_staging,
    write_pho_schedule_to_staging,
    write_pho_schedule_details_to_staging,
    load_facility_from_staging,
    load_mpi_from_staging,
    load_clients_from_staging,
    load_pho_phys_order_from_staging,
    load_pho_order_schedule_from_staging,
    load_pho_schedule_from_staging,
    load_pho_schedule_details_from_staging,
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
    Snapshots all 7 source tables into Delta mirror once per session in wave order.
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
    Wave-ordered bulk load of all 7 tables from Delta mirror into target.
    Processes waves in dependency order (2→3→5→6) so FKs always resolve.
    Returns dict: table_name→pk_map for all 7 tables, plus "snap_version".
    """
    pk_maps: dict = {}

    # Wave 2: facility
    fac_df   = read_mirror(spark, "facility")
    fac_rows = fac_df.collect()
    pk_maps["facility"] = build_pk_map(
        fac_rows, "fac_id", reserve_pk_block(tgt, "facility", len(fac_rows))
    )
    write_facilities_to_staging(fac_df, pk_maps["facility"])
    load_facility_from_staging(tgt)

    # Wave 3: mpi (no fac_id — global demographics table)
    mpi_df   = read_mirror(spark, "mpi")
    mpi_rows = mpi_df.collect()
    pk_maps["mpi"] = build_pk_map(
        mpi_rows, "mpi_id", reserve_pk_block(tgt, "mpi", len(mpi_rows))
    )
    write_mpi_to_staging(mpi_df, pk_maps["mpi"])
    load_mpi_from_staging(tgt)

    # Wave 3: clients (depends on facility + mpi)
    client_df   = read_mirror(spark, "clients")
    client_rows = client_df.collect()
    pk_maps["clients"] = build_pk_map(
        client_rows, "client_id", reserve_pk_block(tgt, "clients", len(client_rows))
    )
    write_clients_to_staging(client_df, pk_maps["clients"], pk_maps["facility"], pk_maps["mpi"])
    load_clients_from_staging(tgt)

    # Wave 5: pho_phys_order (depends on clients)
    po_df   = read_mirror(spark, "pho_phys_order")
    po_rows = po_df.collect()
    pk_maps["pho_phys_order"] = build_pk_map(
        po_rows, "phys_order_id", reserve_pk_block(tgt, "pho_phys_order", len(po_rows))
    )
    write_pho_phys_order_to_staging(po_df, pk_maps["pho_phys_order"], pk_maps["clients"], pk_maps["facility"])
    load_pho_phys_order_from_staging(tgt)

    # Wave 5: pho_order_schedule (depends on pho_phys_order)
    os_df   = read_mirror(spark, "pho_order_schedule")
    os_rows = os_df.collect()
    pk_maps["pho_order_schedule"] = build_pk_map(
        os_rows, "order_schedule_id", reserve_pk_block(tgt, "pho_order_schedule", len(os_rows))
    )
    write_pho_order_schedule_to_staging(
        os_df, pk_maps["pho_order_schedule"], pk_maps["pho_phys_order"], pk_maps["facility"]
    )
    load_pho_order_schedule_from_staging(tgt)

    # Wave 6: pho_schedule (depends on pho_order_schedule + pho_phys_order)
    sched_df   = read_mirror(spark, "pho_schedule")
    sched_rows = sched_df.collect()
    pk_maps["pho_schedule"] = build_pk_map(
        sched_rows, "schedule_id", reserve_pk_block(tgt, "pho_schedule", len(sched_rows))
    )
    write_pho_schedule_to_staging(
        sched_df, pk_maps["pho_schedule"],
        pk_maps["pho_order_schedule"], pk_maps["pho_phys_order"], pk_maps["facility"]
    )
    load_pho_schedule_from_staging(tgt)

    # Wave 6: pho_schedule_details (depends on pho_schedule)
    detail_df   = read_mirror(spark, "pho_schedule_details")
    detail_rows = detail_df.collect()
    pk_maps["pho_schedule_details"] = build_pk_map(
        detail_rows, "pho_schedule_detail_id",
        reserve_pk_block(tgt, "pho_schedule_details", len(detail_rows))
    )
    write_pho_schedule_details_to_staging(
        detail_df, pk_maps["pho_schedule_details"], pk_maps["pho_schedule"]
    )
    load_pho_schedule_details_from_staging(tgt)

    # CT watermark read from _state Delta — survives process restarts
    pk_maps["snap_version"] = get_watermark_ct_version(spark)

    return pk_maps
