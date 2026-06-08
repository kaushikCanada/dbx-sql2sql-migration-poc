"""
Test 1 — Bulk Load

Flow:
  Mirror Delta (Spark read)
  → Python builds pk_map via reserve_pk_block
  → Spark JDBC write to *_staging tables
  → pyodbc calls load_*_from_staging SPs
  → SPs insert into target with IDENTITY_INSERT ON

Validates all 7 tables, the full 5-level FK dependency chain, and that every
FK column on the target carries a target (not source) ID.
"""
import pytest


# ── Row-count + traceability per table ────────────────────────────────────────

def test_facility_loaded_to_target(bulk_loaded, tgt):
    assert len(bulk_loaded["facility"]) == 2
    cursor = tgt.cursor()
    for src_id, tgt_id in bulk_loaded["facility"].items():
        cursor.execute("SELECT src_fac_id FROM dbo.facility WHERE fac_id = ?", tgt_id)
        row = cursor.fetchone()
        assert row is not None, f"target fac_id {tgt_id} not found"
        assert row[0] == src_id


def test_mpi_loaded_to_target(bulk_loaded, tgt):
    assert len(bulk_loaded["mpi"]) == 3
    cursor = tgt.cursor()
    for src_id, tgt_id in bulk_loaded["mpi"].items():
        cursor.execute("SELECT src_mpi_id FROM dbo.mpi WHERE mpi_id = ?", tgt_id)
        row = cursor.fetchone()
        assert row is not None, f"target mpi_id {tgt_id} not found"
        assert row[0] == src_id


def test_clients_loaded_to_target(bulk_loaded, tgt):
    assert len(bulk_loaded["clients"]) == 3
    cursor = tgt.cursor()
    for src_id, tgt_id in bulk_loaded["clients"].items():
        cursor.execute("SELECT src_client_id FROM dbo.clients WHERE client_id = ?", tgt_id)
        row = cursor.fetchone()
        assert row is not None, f"target client_id {tgt_id} not found"
        assert row[0] == src_id


def test_pho_phys_order_loaded_to_target(bulk_loaded, tgt):
    assert len(bulk_loaded["pho_phys_order"]) == 2
    cursor = tgt.cursor()
    for src_id, tgt_id in bulk_loaded["pho_phys_order"].items():
        cursor.execute(
            "SELECT src_phys_order_id FROM dbo.pho_phys_order WHERE phys_order_id = ?", tgt_id
        )
        row = cursor.fetchone()
        assert row is not None
        assert row[0] == src_id


def test_pho_order_schedule_loaded_to_target(bulk_loaded, tgt):
    assert len(bulk_loaded["pho_order_schedule"]) == 2
    cursor = tgt.cursor()
    for src_id, tgt_id in bulk_loaded["pho_order_schedule"].items():
        cursor.execute(
            "SELECT src_order_schedule_id FROM dbo.pho_order_schedule WHERE order_schedule_id = ?",
            tgt_id,
        )
        row = cursor.fetchone()
        assert row is not None
        assert row[0] == src_id


def test_pho_schedule_loaded_to_target(bulk_loaded, tgt):
    assert len(bulk_loaded["pho_schedule"]) == 2
    cursor = tgt.cursor()
    for src_id, tgt_id in bulk_loaded["pho_schedule"].items():
        cursor.execute(
            "SELECT src_schedule_id FROM dbo.pho_schedule WHERE schedule_id = ?", tgt_id
        )
        row = cursor.fetchone()
        assert row is not None
        assert row[0] == src_id


def test_pho_schedule_details_loaded_to_target(bulk_loaded, tgt):
    assert len(bulk_loaded["pho_schedule_details"]) == 4
    cursor = tgt.cursor()
    for src_id, tgt_id in bulk_loaded["pho_schedule_details"].items():
        cursor.execute(
            "SELECT src_pho_schedule_detail_id FROM dbo.pho_schedule_details "
            "WHERE pho_schedule_detail_id = ?",
            tgt_id,
        )
        row = cursor.fetchone()
        assert row is not None
        assert row[0] == src_id


# ── FK integrity: full 5-level chain ─────────────────────────────────────────

def test_fk_clients_references_valid_facility(tgt):
    cursor = tgt.cursor()
    cursor.execute(
        "SELECT COUNT(*) FROM dbo.clients c "
        "WHERE NOT EXISTS (SELECT 1 FROM dbo.facility f WHERE f.fac_id = c.fac_id)"
    )
    assert cursor.fetchone()[0] == 0


def test_fk_phys_order_references_valid_client(tgt):
    cursor = tgt.cursor()
    cursor.execute(
        "SELECT COUNT(*) FROM dbo.pho_phys_order o "
        "WHERE NOT EXISTS (SELECT 1 FROM dbo.clients c WHERE c.client_id = o.client_id)"
    )
    assert cursor.fetchone()[0] == 0


def test_fk_order_schedule_references_valid_phys_order(tgt):
    cursor = tgt.cursor()
    cursor.execute(
        "SELECT COUNT(*) FROM dbo.pho_order_schedule os "
        "WHERE NOT EXISTS (SELECT 1 FROM dbo.pho_phys_order o WHERE o.phys_order_id = os.phys_order_id)"
    )
    assert cursor.fetchone()[0] == 0


def test_fk_schedule_references_valid_order_schedule(tgt):
    cursor = tgt.cursor()
    cursor.execute(
        "SELECT COUNT(*) FROM dbo.pho_schedule s "
        "WHERE s.order_schedule_id IS NOT NULL "
        "AND NOT EXISTS ("
        "  SELECT 1 FROM dbo.pho_order_schedule os WHERE os.order_schedule_id = s.order_schedule_id"
        ")"
    )
    assert cursor.fetchone()[0] == 0


def test_fk_schedule_details_references_valid_schedule(tgt):
    cursor = tgt.cursor()
    cursor.execute(
        "SELECT COUNT(*) FROM dbo.pho_schedule_details sd "
        "WHERE NOT EXISTS (SELECT 1 FROM dbo.pho_schedule s WHERE s.schedule_id = sd.pho_schedule_id)"
    )
    assert cursor.fetchone()[0] == 0


# ── FK remapping correctness ──────────────────────────────────────────────────

def test_fac_id_remapped_in_clients(bulk_loaded, tgt):
    tgt_fac_ids = set(bulk_loaded["facility"].values())
    cursor = tgt.cursor()
    cursor.execute("SELECT DISTINCT fac_id FROM dbo.clients")
    client_fac_ids = {row[0] for row in cursor.fetchall()}
    assert client_fac_ids.issubset(tgt_fac_ids), (
        f"Clients reference fac_ids not in target facility: {client_fac_ids - tgt_fac_ids}"
    )


def test_client_id_remapped_in_phys_order(bulk_loaded, tgt):
    tgt_client_ids = set(bulk_loaded["clients"].values())
    cursor = tgt.cursor()
    cursor.execute("SELECT DISTINCT client_id FROM dbo.pho_phys_order")
    order_client_ids = {row[0] for row in cursor.fetchall()}
    assert order_client_ids.issubset(tgt_client_ids)


def test_phys_order_id_remapped_in_order_schedule(bulk_loaded, tgt):
    tgt_po_ids = set(bulk_loaded["pho_phys_order"].values())
    cursor = tgt.cursor()
    cursor.execute("SELECT DISTINCT phys_order_id FROM dbo.pho_order_schedule")
    os_po_ids = {row[0] for row in cursor.fetchall()}
    assert os_po_ids.issubset(tgt_po_ids)


def test_order_schedule_id_remapped_in_schedule(bulk_loaded, tgt):
    tgt_os_ids = set(bulk_loaded["pho_order_schedule"].values())
    cursor = tgt.cursor()
    cursor.execute(
        "SELECT DISTINCT order_schedule_id FROM dbo.pho_schedule WHERE order_schedule_id IS NOT NULL"
    )
    sched_os_ids = {row[0] for row in cursor.fetchall()}
    assert sched_os_ids.issubset(tgt_os_ids)


def test_schedule_id_remapped_in_details(bulk_loaded, tgt):
    tgt_sched_ids = set(bulk_loaded["pho_schedule"].values())
    cursor = tgt.cursor()
    cursor.execute("SELECT DISTINCT pho_schedule_id FROM dbo.pho_schedule_details")
    detail_sched_ids = {row[0] for row in cursor.fetchall()}
    assert detail_sched_ids.issubset(tgt_sched_ids)
