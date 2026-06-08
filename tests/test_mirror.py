import os
import pytest
from src.mirror_ops import read_mirror, read_mirror_state, get_watermark_ct_version, MIRROR_BASE


class TestMirror:
    def test_run_summary_has_both_tables(self, mirror):
        """run_mirror returns one success entry per table."""
        assert len(mirror) == 2
        statuses = {r["table"]: r["status"] for r in mirror}
        assert statuses["facility"] == "success"
        assert statuses["clients"] == "success"

    def test_facility_row_count(self, spark, mirror):
        """Mirror captures all 2 seed facilities."""
        assert read_mirror(spark, "facility").count() == 2

    def test_clients_row_count(self, spark, mirror):
        """Mirror captures all 5 seed clients."""
        assert read_mirror(spark, "clients").count() == 5

    def test_clients_scoped_to_mirrored_facilities(self, spark, mirror):
        """Every client in the mirror belongs to a facility also in the mirror."""
        fac_ids = {r["fac_id"] for r in read_mirror(spark, "facility").collect()}
        client_fac_ids = {r["fac_id"] for r in read_mirror(spark, "clients").collect()}
        assert client_fac_ids.issubset(fac_ids)

    def test_delta_log_exists_on_disk(self, mirror):
        """Delta write produced a valid _delta_log directory for each table."""
        for table in ("facility", "clients"):
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
