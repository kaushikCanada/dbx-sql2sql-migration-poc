"""
Test 2 — Delta Load

Flow:
  Source changes made (insert / update / soft delete / hard delete)
  → Spark JDBC reads CHANGETABLE delta from source
  → Spark JDBC writes delta to clients_delta_staging on target
  → pyodbc calls apply_clients_delta_from_staging SP
  → SP applies I/U/D operations to target clients table
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
    fac_pk_map, client_pk_map, snap = bulk_loaded

    # Insert a new client on source after the bulk load snapshot
    new_src_id = insert_client(src, fac_id=1, first="Delta", last="Insert")

    # Read CT delta since snapshot via Spark JDBC
    delta_df = ct_delta_clients(spark, snap)

    # Verify the new row appears as an insert in the delta
    delta_rows = {r["client_id"]: r["SYS_CHANGE_OPERATION"] for r in delta_df.collect()}
    assert delta_rows.get(new_src_id) == "I"

    # Write delta to staging, SP applies it to target
    write_delta_to_staging(delta_df)
    apply_clients_delta_from_staging(tgt)

    # New client should now exist on target with correct name
    cursor = tgt.cursor()
    cursor.execute(
        "SELECT first_name FROM dbo.clients WHERE src_client_id = ?", new_src_id
    )
    row = cursor.fetchone()
    assert row is not None
    assert row[0] == "Delta"


def test_delta_update(spark, src, tgt, bulk_loaded):
    fac_pk_map, client_pk_map, snap = bulk_loaded

    # Update Alice (src client_id=1) on source
    update_client(src, client_id=1, first="UpdatedFirst", last="UpdatedLast")

    delta_df = ct_delta_clients(spark, snap)

    delta_rows = {r["client_id"]: r["SYS_CHANGE_OPERATION"] for r in delta_df.collect()}
    assert delta_rows.get(1) == "U"

    write_delta_to_staging(delta_df)
    apply_clients_delta_from_staging(tgt)

    cursor = tgt.cursor()
    cursor.execute(
        "SELECT first_name, last_name FROM dbo.clients WHERE src_client_id = 1"
    )
    row = cursor.fetchone()
    assert row[0] == "UpdatedFirst"
    assert row[1] == "UpdatedLast"


def test_delta_soft_delete(spark, src, tgt, bulk_loaded):
    fac_pk_map, client_pk_map, snap = bulk_loaded

    # Soft delete Bob (src client_id=2) on source
    soft_delete_client(src, client_id=2)

    delta_df = ct_delta_clients(spark, snap)

    delta_rows = {r["client_id"]: r["SYS_CHANGE_OPERATION"] for r in delta_df.collect()}
    # Soft delete is an UPDATE in CT — deleted column changes to 'Y'
    assert delta_rows.get(2) == "U"

    write_delta_to_staging(delta_df)
    apply_clients_delta_from_staging(tgt)

    cursor = tgt.cursor()
    cursor.execute("SELECT deleted FROM dbo.clients WHERE src_client_id = 2")
    assert cursor.fetchone()[0] == "Y"


def test_delta_hard_delete_becomes_soft_on_target(spark, src, tgt, bulk_loaded):
    fac_pk_map, client_pk_map, snap = bulk_loaded

    # Hard delete Carol (src client_id=3) on source — row gone completely
    hard_delete_client(src, client_id=3)

    delta_df = ct_delta_clients(spark, snap)

    delta_rows = {r["client_id"]: r["SYS_CHANGE_OPERATION"] for r in delta_df.collect()}
    # Hard delete tracked as 'D', data columns NULL
    assert delta_rows.get(3) == "D"

    write_delta_to_staging(delta_df)
    apply_clients_delta_from_staging(tgt)

    # Target row must still exist (soft deleted) to preserve FK integrity
    cursor = tgt.cursor()
    cursor.execute("SELECT deleted FROM dbo.clients WHERE src_client_id = 3")
    row = cursor.fetchone()
    assert row is not None, "Row was hard deleted on target — FK integrity broken"
    assert row[0] == "Y"


def test_no_further_changes_after_sync(spark, src):
    # After all delta operations are applied, a fresh CT read should be empty
    current_version = get_current_ct_version(src)
    delta_df = ct_delta_clients(spark, current_version)
    assert delta_df.count() == 0
