"""
Test 2 — Delta Load

Flow:
  Source changes (insert / update / soft delete / hard delete)
  → Spark JDBC reads CHANGETABLE delta since bulk-load snapshot
  → Spark JDBC writes to clients_delta_staging (stores src_fac_id, src_mpi_id)
  → pyodbc calls apply_clients_delta_from_staging SP
  → SP resolves src FK IDs to target IDs via facility/mpi src_* lookups
  → SP applies I/U/D to target clients table
"""
import pytest
from src.source_ops import (
    ct_delta_clients,
    get_current_ct_version,
    insert_client,
    update_client,
    soft_delete_client,
    hard_delete_client,
)
from src.target_ops import write_delta_to_staging, apply_clients_delta_from_staging


def test_delta_insert(spark, src, tgt, bulk_loaded):
    snap = bulk_loaded["snap_version"]

    new_src_id = insert_client(src, fac_id=1)

    delta_df = ct_delta_clients(spark, snap)
    delta_rows = {r["client_id"]: r["SYS_CHANGE_OPERATION"] for r in delta_df.collect()}
    assert delta_rows.get(new_src_id) == "I"

    write_delta_to_staging(delta_df)
    apply_clients_delta_from_staging(tgt)

    cursor = tgt.cursor()
    cursor.execute(
        "SELECT src_client_id FROM dbo.clients WHERE src_client_id = ?", new_src_id
    )
    row = cursor.fetchone()
    assert row is not None, f"Inserted client src_client_id={new_src_id} not found on target"


def test_delta_update(spark, src, tgt, bulk_loaded):
    snap = bulk_loaded["snap_version"]

    update_client(src, client_id=1, discharge_date="2023-12-31")

    delta_df = ct_delta_clients(spark, snap)
    delta_rows = {r["client_id"]: r["SYS_CHANGE_OPERATION"] for r in delta_df.collect()}
    assert delta_rows.get(1) == "U"

    write_delta_to_staging(delta_df)
    apply_clients_delta_from_staging(tgt)

    cursor = tgt.cursor()
    cursor.execute("SELECT discharge_date FROM dbo.clients WHERE src_client_id = 1")
    row = cursor.fetchone()
    assert row is not None
    assert str(row[0])[:10] == "2023-12-31"


def test_delta_soft_delete(spark, src, tgt, bulk_loaded):
    snap = bulk_loaded["snap_version"]

    soft_delete_client(src, client_id=2)

    delta_df = ct_delta_clients(spark, snap)
    delta_rows = {r["client_id"]: r["SYS_CHANGE_OPERATION"] for r in delta_df.collect()}
    # Soft delete is an UPDATE in CT — the deleted column changes to 'Y'
    assert delta_rows.get(2) == "U"

    write_delta_to_staging(delta_df)
    apply_clients_delta_from_staging(tgt)

    cursor = tgt.cursor()
    cursor.execute("SELECT deleted FROM dbo.clients WHERE src_client_id = 2")
    assert cursor.fetchone()[0] == "Y"


def test_delta_hard_delete_becomes_soft_on_target(spark, src, tgt, bulk_loaded):
    snap = bulk_loaded["snap_version"]

    hard_delete_client(src, client_id=3)

    delta_df = ct_delta_clients(spark, snap)
    delta_rows = {r["client_id"]: r["SYS_CHANGE_OPERATION"] for r in delta_df.collect()}
    # Hard delete tracked as 'D'; data columns are NULL (LEFT JOIN returns no row)
    assert delta_rows.get(3) == "D"

    write_delta_to_staging(delta_df)
    apply_clients_delta_from_staging(tgt)

    # Target row must still exist (soft deleted) to preserve FK integrity for orders/schedules
    cursor = tgt.cursor()
    cursor.execute("SELECT deleted FROM dbo.clients WHERE src_client_id = 3")
    row = cursor.fetchone()
    assert row is not None, "Row was hard deleted on target — downstream FK integrity broken"
    assert row[0] == "Y"


def test_no_further_changes_after_sync(spark, src):
    """After all mutations are synced, a fresh CT read at current version should be empty."""
    current_version = get_current_ct_version(src)
    delta_df = ct_delta_clients(spark, current_version)
    assert delta_df.count() == 0
