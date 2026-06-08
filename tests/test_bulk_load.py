"""
Test 1 — Bulk Load

Flow:
  Source (Spark JDBC read)
  → Python builds pk_map via reserve_pk_block
  → Spark JDBC write to facility_staging / clients_staging
  → pyodbc calls load_facility_from_staging / load_clients_from_staging SP
  → SP inserts into target with IDENTITY_INSERT ON
"""
import pytest


def test_facilities_loaded_to_target(bulk_loaded, tgt):
    fac_pk_map, _, _ = bulk_loaded

    # Source had 2 facilities — both should be in target
    assert len(fac_pk_map) == 2

    cursor = tgt.cursor()

    # Each source fac_id should map to a real row in target
    for src_id, tgt_id in fac_pk_map.items():
        cursor.execute(
            "SELECT fac_id, src_fac_id FROM dbo.facility WHERE fac_id = ?", tgt_id
        )
        row = cursor.fetchone()
        assert row is not None, f"target fac_id {tgt_id} not found"
        assert row[1] == src_id, f"src_fac_id mismatch: expected {src_id}, got {row[1]}"


def test_clients_loaded_to_target(bulk_loaded, tgt):
    _, client_pk_map, _ = bulk_loaded

    # Source had 5 clients — all should be in target
    assert len(client_pk_map) == 5

    cursor = tgt.cursor()

    for src_id, tgt_id in client_pk_map.items():
        cursor.execute(
            "SELECT client_id, src_client_id FROM dbo.clients WHERE client_id = ?", tgt_id
        )
        row = cursor.fetchone()
        assert row is not None, f"target client_id {tgt_id} not found"
        assert row[1] == src_id, f"src_client_id mismatch: expected {src_id}, got {row[1]}"


def test_fk_integrity_after_bulk_load(tgt):
    # No client on target should reference a non-existent facility
    cursor = tgt.cursor()
    cursor.execute(
        "SELECT COUNT(*) FROM dbo.clients c "
        "WHERE NOT EXISTS (SELECT 1 FROM dbo.facility f WHERE f.fac_id = c.fac_id)"
    )
    assert cursor.fetchone()[0] == 0


def test_fac_id_remapped_correctly(bulk_loaded, tgt):
    # Clients loaded into target must use TARGET fac_ids, not source fac_ids
    fac_pk_map, client_pk_map, _ = bulk_loaded
    tgt_fac_ids = set(fac_pk_map.values())

    cursor = tgt.cursor()
    cursor.execute("SELECT DISTINCT fac_id FROM dbo.clients")
    client_fac_ids = {row[0] for row in cursor.fetchall()}

    assert client_fac_ids.issubset(tgt_fac_ids), (
        f"Clients reference fac_ids not in target facility table: "
        f"{client_fac_ids - tgt_fac_ids}"
    )
