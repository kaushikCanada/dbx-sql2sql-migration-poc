import os
import pytest
from src.mirror_ops import (
    read_mirror, read_mirror_state, get_watermark_ct_version,
    MIRROR_BASE, WAVE_CONFIG, ALL_TABLES,
)


class TestMirror:
    def test_run_summary_has_all_tables(self, mirror):
        """run_mirror returns one success entry per table (7 total)."""
        assert len(mirror) == 7
        statuses = {r["table"]: r["status"] for r in mirror}
        for table in ALL_TABLES:
            assert statuses[table] == "success", f"Table {table} did not succeed"

    def test_results_ordered_by_wave(self, mirror):
        """Tables appear in WAVE_CONFIG wave order — wave 2 before 3 before 5 before 6."""
        tables_in_order = [r["table"] for r in mirror]
        assert tables_in_order == ALL_TABLES

    def test_wave_numbers_in_results(self, mirror):
        wave_by_table = {r["table"]: r["wave"] for r in mirror}
        assert wave_by_table["facility"] == 2
        assert wave_by_table["mpi"] == 3
        assert wave_by_table["clients"] == 3
        assert wave_by_table["pho_phys_order"] == 5
        assert wave_by_table["pho_order_schedule"] == 5
        assert wave_by_table["pho_schedule"] == 6
        assert wave_by_table["pho_schedule_details"] == 6

    def test_facility_row_count(self, spark, mirror):
        """Mirror captures all 2 seed facilities."""
        assert read_mirror(spark, "facility").count() == 2

    def test_mpi_row_count(self, spark, mirror):
        """Mirror captures all 3 seed MPI records."""
        assert read_mirror(spark, "mpi").count() == 3

    def test_clients_row_count(self, spark, mirror):
        """Mirror captures all 3 seed clients."""
        assert read_mirror(spark, "clients").count() == 3

    def test_pho_phys_order_row_count(self, spark, mirror):
        """Mirror captures all 2 seed physician orders."""
        assert read_mirror(spark, "pho_phys_order").count() == 2

    def test_pho_order_schedule_row_count(self, spark, mirror):
        """Mirror captures all 2 seed order schedules."""
        assert read_mirror(spark, "pho_order_schedule").count() == 2

    def test_pho_schedule_row_count(self, spark, mirror):
        """Mirror captures all 2 seed schedules."""
        assert read_mirror(spark, "pho_schedule").count() == 2

    def test_pho_schedule_details_row_count(self, spark, mirror):
        """Mirror captures all 4 seed administration records."""
        assert read_mirror(spark, "pho_schedule_details").count() == 4

    def test_clients_scoped_to_mirrored_facilities(self, spark, mirror):
        """Every client in the mirror belongs to a facility also in the mirror."""
        fac_ids = {r["fac_id"] for r in read_mirror(spark, "facility").collect()}
        client_fac_ids = {r["fac_id"] for r in read_mirror(spark, "clients").collect()}
        assert client_fac_ids.issubset(fac_ids)

    def test_delta_log_exists_on_disk(self, mirror):
        """Delta write produced a valid _delta_log directory for each table."""
        for table in ALL_TABLES:
            delta_log = os.path.join(MIRROR_BASE, table, "_delta_log")
            assert os.path.isdir(delta_log), f"No _delta_log found at {delta_log}"

    def test_result_includes_row_count_and_timing(self, mirror):
        """Each result entry carries rows and elapsed_sec."""
        for r in mirror:
            assert r["rows"] > 0
            assert r["elapsed_sec"] >= 0

    def test_read_mirror_raises_if_table_missing(self, spark, mirror):
        """read_mirror raises FileNotFoundError for an unknown table."""
        with pytest.raises(FileNotFoundError):
            read_mirror(spark, "nonexistent_table")

    def test_state_ct_version_is_non_negative(self, spark, mirror):
        """Watermark ct_version is persisted in _state and readable after mirror run."""
        ct = get_watermark_ct_version(spark)
        assert isinstance(ct, int) and ct >= 0

    def test_state_all_rows_share_run_id_and_ct_version(self, spark, mirror):
        """All _state rows have the same run_id and ct_version — one atomic snapshot."""
        rows = read_mirror_state(spark).collect()
        assert len({r["run_id"] for r in rows}) == 1
        assert len({r["ct_version"] for r in rows}) == 1

    def test_state_has_entry_for_every_table(self, spark, mirror):
        """_state table has exactly one row per mirrored table."""
        rows = read_mirror_state(spark).collect()
        tables_in_state = {r["table"] for r in rows}
        assert tables_in_state == set(ALL_TABLES)
