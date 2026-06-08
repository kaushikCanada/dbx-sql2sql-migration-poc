# dbx-sql2sql-migration-poc

A proof-of-concept for migrating data from a source SQL Server to a target SQL Server using PySpark and Delta Lake. Built to validate the core patterns before wiring them into a full Databricks production pipeline.

---

## What This Proves

Three patterns that a production migration pipeline depends on:

| Pattern | What it validates |
|---|---|
| **Mirror** | Snapshot source tables into Delta Lake before migration starts — ensures a consistent point-in-time read and avoids hammering the live source DB during the load |
| **Bulk load** | Reserve identity PK blocks on the target, write pre-mapped rows through staging tables, and atomically load via stored procedures with `IDENTITY_INSERT ON` |
| **CT delta** | Use SQL Server Change Tracking to catch up on changes that happened on the source *after* the mirror snapshot — the incremental catch-up phase before cutover |

The CT version at mirror time is persisted in a `_state` Delta table (watermark), so the delta phase can resume independently without relying on an in-memory variable.

---

## Architecture

```
SOURCE SQL SERVER                    DELTA (local)                TARGET SQL SERVER
─────────────────                    ─────────────                ─────────────────

  dbo.facility    ──── JDBC read ──▶  _mirror/facility   ──┐
  dbo.clients     ──── JDBC read ──▶  _mirror/clients    ──┤
                                      _mirror/_state      ──┤  PK reservation
                                        (ct_version)        ├─ staging write ──▶ dbo.facility
                                                            └─ SP load       ──▶ dbo.clients

  CHANGETABLE()   ──── JDBC read ──────────────────────────── delta staging ──▶ apply SP
  (live, after snapshot)                                                    ──▶ U / soft-D / I
```

**Flow:**
1. `run_mirror()` — snapshots source to Delta, writes `ct_version` watermark to `_state`
2. Bulk load — reads from `_mirror` Delta, reserves PKs, writes staging, calls load SPs
3. CT delta — reads `CHANGETABLE` from live source since `ct_version`, applies I/U/D to target
4. Repeat step 3 until lag is near zero, then cut over

---

## Project Structure

```
migration-poc/
├── Dockerfile                 # Python 3.10 + Spark 4.0.0 + ODBC Driver 18
├── docker-compose.yml         # source_db, target_db (SQL Server 2022), app container
├── init_db.sh                 # waits for SQL Server readiness then runs init.sql
├── requirements.txt           # pyodbc, pytest
│
├── src/
│   ├── spark_session.py       # SparkSession builder with Delta + MSSQL JDBC packages
│   ├── connections.py         # pyodbc connection factories (source + target)
│   ├── source_ops.py          # bulk JDBC reads, CT delta reads, test mutation helpers
│   ├── target_ops.py          # PK reservation, staging writes, SP calls
│   └── mirror_ops.py          # run_mirror(), read_mirror(), _state watermark
│
├── db/
│   ├── source/init.sql        # sourcedb schema, Change Tracking enabled, seed data
│   └── target/init.sql        # targetdb schema, staging tables, 4 stored procedures
│
└── tests/
    ├── conftest.py            # session-scoped fixtures: spark, src, tgt, mirror, bulk_loaded
    ├── test_mirror.py         # mirror snapshot, Delta files on disk, _state watermark
    ├── test_bulk_load.py      # row counts, PK mapping, FK integrity after bulk load
    ├── test_delta_load.py     # CT insert / update / soft-delete / hard-delete → soft
    └── test_pk.py             # PK reservation correctness and concurrency safety
```

---

## How to Run

**Prerequisites:** Docker Desktop

### 1. Start the containers

```bash
cd migration-poc
docker-compose up -d --build
```

This starts three containers:
- `source_db` — SQL Server 2022 on port 1433
- `target_db` — SQL Server 2022 on port 1434
- `migration_app` — Python + Spark app

### 2. Initialize the databases

```bash
docker cp init_db.sh source_db:/init_db.sh
docker cp db/source/init.sql source_db:/init.sql
docker cp init_db.sh target_db:/init_db.sh
docker cp db/target/init.sql target_db:/init.sql

docker exec source_db bash /init_db.sh localhost "SourcePass123!" /init.sql
docker exec target_db bash /init_db.sh localhost "TargetPass123!" /init.sql
```

### 3. Run all tests

```bash
docker exec migration_app pytest tests/ -v
```

Expected output:
```
tests/test_bulk_load.py::test_facilities_loaded_to_target PASSED
tests/test_bulk_load.py::test_clients_loaded_to_target PASSED
tests/test_bulk_load.py::test_fk_integrity_after_bulk_load PASSED
tests/test_bulk_load.py::test_fac_id_remapped_correctly PASSED
tests/test_delta_load.py::test_delta_insert PASSED
tests/test_delta_load.py::test_delta_update PASSED
tests/test_delta_load.py::test_delta_soft_delete PASSED
tests/test_delta_load.py::test_delta_hard_delete_becomes_soft_on_target PASSED
tests/test_delta_load.py::test_no_further_changes_after_sync PASSED
tests/test_mirror.py::TestMirror::test_run_summary_has_both_tables PASSED
tests/test_mirror.py::TestMirror::test_facility_row_count PASSED
tests/test_mirror.py::TestMirror::test_clients_row_count PASSED
tests/test_mirror.py::TestMirror::test_clients_scoped_to_mirrored_facilities PASSED
tests/test_mirror.py::TestMirror::test_delta_log_exists_on_disk PASSED
tests/test_mirror.py::TestMirror::test_result_includes_row_count_and_timing PASSED
tests/test_mirror.py::TestMirror::test_read_mirror_raises_if_table_missing PASSED
tests/test_mirror.py::TestMirror::test_state_ct_version_is_non_negative PASSED
tests/test_mirror.py::TestMirror::test_state_all_rows_share_run_id_and_ct_version PASSED
tests/test_pk.py::test_reserve_returns_positive_int PASSED
tests/test_pk.py::test_sequential_reservations_dont_overlap PASSED
tests/test_pk.py::test_zero_block_size_raises PASSED
tests/test_pk.py::test_concurrent_reservations_dont_overlap PASSED

22 passed in ~33s
```

### 4. Inspect the databases (optional)

Connect via any SQL client (e.g. TablePlus):

| | Source | Target |
|---|---|---|
| Host | 127.0.0.1 | 127.0.0.1 |
| Port | 1433 | 1434 |
| User | sa | sa |
| Password | SourcePass123! | TargetPass123! |
| Database | sourcedb | targetdb |

> Enable **Trust Server Certificate** in your client's advanced settings.

### 5. Tear down

```bash
docker-compose down -v
```

The `-v` flag removes the database volumes so the next run starts with a clean state.

---

## Key Design Decisions

**Why staging tables?**
Spark cannot call stored procedures or use `IDENTITY_INSERT` directly. Staging tables act as a constraint-free landing zone — Spark writes pre-mapped rows freely, then a stored procedure moves them into the real table atomically.

**Why PK reservation?**
The target DB is live while migration runs. Without reserving a block of identity values, migrated rows and live inserts could get the same PK. The `reserve_pk_block` SP locks the table, reads the current identity high-water mark, reseeds it to `current + block_size`, and returns the first reserved ID.

**Why soft-delete hard deletes?**
When a row is hard-deleted on source, child rows on the target may still reference it. Hard-deleting on target would break FK constraints. The delta SP converts all `D` CT operations to `deleted = 'Y'` instead.

**Why a `_state` Delta watermark?**
The CT version captured at mirror time is written to `_mirror/_state` as a Delta table. The delta phase reads this instead of relying on an in-memory variable — meaning the pipeline can restart after a failure and resume from the correct CT version without re-running the mirror.

---

## Production Pipeline (Databricks)

This POC maps to a 4-job Databricks Workflow:

```
Job 1: Mirror       →   Job 2: Bulk Load   →   Job 3: Delta Loop   →   Job 4: Cutover
source → _mirror        _mirror → target        CT catch-up             final sync + switch
(runs once)             (runs once)             (runs on schedule)      (manual trigger)
```

Jobs communicate through Delta tables (`_mirror`, `_state`) — not through return values — so each job can be restarted independently.

---

## Tech Stack

| Component | Version |
|---|---|
| Python | 3.10 |
| Apache Spark | 4.0.0 |
| Delta Lake | 4.0.0 |
| SQL Server | 2022 |
| MSSQL JDBC | 12.6.1 |
| ODBC Driver | 18 |
| pyodbc | 5.1.0 |
| pytest | 8.2.2 |
| Docker Compose | v2 |
