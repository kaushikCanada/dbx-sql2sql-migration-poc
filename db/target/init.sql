CREATE DATABASE targetdb;    -- create the target database that migrated data lands in
GO

USE targetdb;               -- all subsequent statements run inside targetdb
GO

-- =============================================================================
-- MAIN TABLES
-- These are the live tables the application will read from after migration.
-- They mirror the source schema but each adds a src_* traceability column
-- so we can always trace a target row back to its original source ID.
-- The delta SP also uses src_* to find the right row to update/delete.
-- =============================================================================

CREATE TABLE dbo.facility (
    fac_id     INT IDENTITY(1,1) PRIMARY KEY,  -- auto-increment PK; real value set by PK reservation, not auto-generated
    name       VARCHAR(100) NOT NULL,
    prov       CHAR(2)      NOT NULL,
    deleted    CHAR(1)      NOT NULL DEFAULT 'N',
    src_fac_id INT          NULL     -- original fac_id from source DB; NULL until migrated row is inserted
);
GO

CREATE TABLE dbo.clients (
    client_id     INT IDENTITY(1,1) PRIMARY KEY,  -- auto-increment PK; real value set by PK reservation
    fac_id        INT          NOT NULL REFERENCES dbo.facility(fac_id),  -- FK to target facility (translated from source fac_id)
    first_name    VARCHAR(100) NOT NULL,
    last_name     VARCHAR(100) NOT NULL,
    deleted       CHAR(1)      NOT NULL DEFAULT 'N',
    src_client_id INT          NULL  -- original client_id from source; used by delta SP to find this row on updates/deletes
);
GO

-- =============================================================================
-- STAGING TABLES
-- Temporary landing zone that Spark JDBC writes into before the load SPs run.
--
-- WHY STAGING EXISTS:
--   Spark cannot call stored procedures or do IDENTITY_INSERT directly.
--   So the flow is: Spark writes raw pre-mapped rows here with no constraints,
--   then a SP moves them into the real table atomically with full PK/FK control.
--
-- KEY DESIGN CHOICE: no IDENTITY columns, no FK constraints on staging tables.
--   Spark writes the already-translated PKs and FKs directly as plain integers.
--   The SPs trust what staging contains — no re-translation needed inside SQL.
-- =============================================================================

-- Holds facility rows waiting to be loaded into dbo.facility.
-- fac_id here is already the reserved target PK — not the original source ID.
CREATE TABLE dbo.facility_staging (
    fac_id     INT          NOT NULL,   -- reserved target PK assigned by Python before this write
    name       VARCHAR(100) NOT NULL,
    prov       CHAR(2)      NOT NULL,
    deleted    CHAR(1)      NOT NULL DEFAULT 'N',
    src_fac_id INT          NOT NULL    -- original source fac_id; written into facility.src_fac_id for traceability
);
GO

-- Holds client rows waiting to be loaded into dbo.clients.
CREATE TABLE dbo.clients_staging (
    client_id     INT          NOT NULL,  -- reserved target PK assigned by Python
    fac_id        INT          NOT NULL,  -- already translated to target fac_id by Python before this write
    first_name    VARCHAR(100) NOT NULL,
    last_name     VARCHAR(100) NOT NULL,
    deleted       CHAR(1)      NOT NULL DEFAULT 'N',
    src_client_id INT          NOT NULL   -- original source client_id for traceability
);
GO

-- Holds CT delta rows for incremental catch-up runs (Job 3).
-- One row per changed client since the last CT version checkpoint.
-- operation mirrors SQL Server's SYS_CHANGE_OPERATION:
--   'I' = row was inserted on source after the mirror snapshot
--   'U' = row was updated on source after the mirror snapshot
--   'D' = row was hard-deleted on source after the mirror snapshot
-- Data columns are NULL for 'D' because the row no longer exists on source —
-- only the src_client_id is available from CHANGETABLE for deleted rows.
CREATE TABLE dbo.clients_delta_staging (
    src_client_id INT          NOT NULL,  -- source client_id; used to match the right target row
    operation     CHAR(1)      NOT NULL,  -- 'I', 'U', or 'D'
    fac_id        INT          NULL,      -- source fac_id; NULL for D rows since data is gone
    first_name    VARCHAR(100) NULL,      -- NULL for D rows
    last_name     VARCHAR(100) NULL,      -- NULL for D rows
    deleted       CHAR(1)      NULL       -- NULL for D rows
);
GO

-- =============================================================================
-- SP: reserve_pk_block
--
-- PURPOSE:
--   Before Spark inserts migrated rows it needs to know what target PKs to
--   assign. This SP reserves a contiguous block of identity values so that:
--     1. Migrated rows get predictable non-colliding PKs.
--     2. Any live inserts on the target after this call get IDs above the block.
--
-- HOW IT WORKS (step by step):
--   1. Acquire an exclusive app lock on the table name so two concurrent callers
--      can't read the same IDENT_CURRENT and reserve overlapping blocks.
--   2. Read IDENT_CURRENT — the highest identity value ever inserted.
--   3. Set @first_id = current + 1 — the start of the reserved block.
--   4. Reseed the identity counter to current + block_size so the next
--      auto-generated INSERT on the table lands above our reserved range.
--   5. Return @first_id so Python knows what ID to assign to its first row.
--
-- EXAMPLE:
--   IDENT_CURRENT = 10, block_size = 5
--   → @first_id = 11  (Python assigns 11, 12, 13, 14, 15 to migrated rows)
--   → identity reseeded to 15
--   → next live INSERT auto-generates 16  ← no collision
-- =============================================================================

CREATE PROCEDURE dbo.reserve_pk_block
    @table_name  NVARCHAR(128),  -- name of the table to reserve IDs on (e.g. 'clients')
    @block_size  INT,            -- how many IDs to reserve (= number of rows about to be inserted)
    @first_id    INT OUTPUT      -- returns the first ID in the reserved block
AS
BEGIN
    SET NOCOUNT ON;    -- suppress "rows affected" messages
    SET XACT_ABORT ON; -- automatically roll back the transaction on any error

    DECLARE @current_max INT;    -- current identity high-water mark
    DECLARE @sql         NVARCHAR(500);
    DECLARE @reseed_val  INT;    -- value to reseed identity to after reservation
    DECLARE @lock_result INT;    -- return code from sp_getapplock; negative = lock failed

    BEGIN TRANSACTION;

    -- Acquire an exclusive application-level lock scoped to this transaction.
    -- If another session is reserving on the same table at the same time, this
    -- blocks until they commit — guaranteeing sequential non-overlapping blocks.
    EXEC @lock_result = sp_getapplock
        @Resource    = @table_name,   -- lock name is the table name string
        @LockMode    = 'Exclusive',   -- nobody else can hold any lock on this resource simultaneously
        @LockOwner   = 'Transaction', -- lock is released automatically when the transaction ends
        @LockTimeout = 10000;         -- give up and error after 10 seconds rather than wait forever

    IF @lock_result < 0  -- negative return means lock could not be acquired
    BEGIN
        ROLLBACK;
        RAISERROR('Could not acquire lock on %s', 16, 1, @table_name);
        RETURN;
    END

    -- Read the current identity high-water mark using dynamic SQL because
    -- IDENT_CURRENT() requires a string literal table name, not a variable.
    -- ISNULL(..., 0) handles the case where the table has never had a row inserted.
    SET @sql = N'SELECT @m = ISNULL(IDENT_CURRENT(''' + @table_name + N'''), 0)';
    EXEC sp_executesql @sql, N'@m INT OUTPUT', @m = @current_max OUTPUT;

    SET @first_id   = @current_max + 1;          -- first ID Python should use
    SET @reseed_val = @current_max + @block_size; -- identity jumps to end of reserved range

    -- Reseed the identity counter so the next auto-generated insert on this
    -- table will get an ID of @reseed_val + 1, safely above our reserved block.
    SET @sql = N'DBCC CHECKIDENT (''' + @table_name + N''', RESEED, ' + CAST(@reseed_val AS NVARCHAR) + N') WITH NO_INFOMSGS';
    EXEC sp_executesql @sql;

    COMMIT; -- releases the app lock
END;
GO

-- =============================================================================
-- SP: load_facility_from_staging
--
-- PURPOSE:
--   Atomically moves pre-mapped facility rows from staging into the live table.
--   Called by Python (via pyodbc) after Spark has written to facility_staging.
--
-- WHY IDENTITY_INSERT ON:
--   By default SQL Server ignores explicit values on IDENTITY columns and
--   generates its own. IDENTITY_INSERT ON overrides this so we can force in
--   the exact reserved PKs that Python pre-calculated.
--
-- TRUNCATE AT END:
--   Empties staging so it's clean for the next run. TRUNCATE is faster than
--   DELETE because it doesn't log individual row deletions.
-- =============================================================================

CREATE PROCEDURE dbo.load_facility_from_staging
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;  -- roll back everything if any statement fails

    BEGIN TRANSACTION;

    SET IDENTITY_INSERT dbo.facility ON;  -- allow explicit PK values to be inserted

    INSERT INTO dbo.facility (fac_id, name, prov, deleted, src_fac_id)
    SELECT fac_id, name, prov, deleted, src_fac_id  -- staging already has all values pre-translated
    FROM dbo.facility_staging;

    SET IDENTITY_INSERT dbo.facility OFF; -- restore normal auto-increment behaviour

    TRUNCATE TABLE dbo.facility_staging;  -- clean up staging for next run

    COMMIT;
END;
GO

-- =============================================================================
-- SP: load_clients_from_staging
--
-- PURPOSE:
--   Same pattern as load_facility_from_staging but for clients.
--
-- FK NOTE:
--   clients_staging.fac_id already holds the TARGET fac_id (not source).
--   Python translated it before writing to staging using the fac_pk_map,
--   so no JOIN is needed here — the value is ready to insert directly.
-- =============================================================================

CREATE PROCEDURE dbo.load_clients_from_staging
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRANSACTION;

    SET IDENTITY_INSERT dbo.clients ON;  -- allow the reserved PKs to be inserted explicitly

    INSERT INTO dbo.clients (client_id, fac_id, first_name, last_name, deleted, src_client_id)
    SELECT client_id, fac_id, first_name, last_name, deleted, src_client_id  -- fac_id already translated to target
    FROM dbo.clients_staging;

    SET IDENTITY_INSERT dbo.clients OFF; -- restore normal auto-increment

    TRUNCATE TABLE dbo.clients_staging;  -- clean up

    COMMIT;
END;
GO

-- =============================================================================
-- SP: apply_clients_delta_from_staging
--
-- PURPOSE:
--   Applies CT delta changes from clients_delta_staging to the live clients
--   table. Called after each CT delta run (Job 3 in the pipeline).
--
-- OPERATION TYPES:
--   D (hard delete on source)
--     → soft delete on target (set deleted = 'Y')
--     → we never hard-delete on target because child rows elsewhere may still
--       reference this client; a hard delete would break FK constraints.
--
--   U (update on source)
--     → update the matching target row found via src_client_id
--     → src_client_id is the permanent stable link between source and target rows
--
--   I (new insert on source after the mirror snapshot)
--     → reserve one PK block for ALL new rows in this batch (one SP call = efficient)
--     → translate fac_id: staging holds the SOURCE fac_id; JOIN facility on
--       src_fac_id to find the corresponding TARGET fac_id before inserting
--
-- WHY D AND U RUN BEFORE I:
--   A row could be deleted and re-inserted in the same delta window.
--   Running D first clears the old row; I then re-adds it cleanly.
--
-- TRUNCATE AT END:
--   Empties staging so the next delta run starts fresh.
-- =============================================================================

CREATE PROCEDURE dbo.apply_clients_delta_from_staging
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;  -- roll back the entire transaction if any statement fails

    BEGIN TRANSACTION;

    -- -------------------------------------------------------------------------
    -- Step 1: Apply hard deletes from source as soft deletes on target.
    -- Match rows using src_client_id — the permanent source→target link.
    -- -------------------------------------------------------------------------
    UPDATE c SET c.deleted = 'Y'                    -- mark as deleted without removing the row
    FROM dbo.clients c
    INNER JOIN dbo.clients_delta_staging s
        ON c.src_client_id = s.src_client_id        -- find the target row that corresponds to the source row
    WHERE s.operation = 'D';                        -- only process hard-delete events

    -- -------------------------------------------------------------------------
    -- Step 2: Apply updates — propagate name and deleted flag changes.
    -- -------------------------------------------------------------------------
    UPDATE c SET
        c.first_name = s.first_name,                -- overwrite with latest value from source
        c.last_name  = s.last_name,
        c.deleted    = s.deleted                    -- propagates soft deletes from source too
    FROM dbo.clients c
    INNER JOIN dbo.clients_delta_staging s
        ON c.src_client_id = s.src_client_id        -- find the target row by its source ID
    WHERE s.operation = 'U';                        -- only process update events

    -- -------------------------------------------------------------------------
    -- Step 3: Insert new rows that appeared on source after the mirror snapshot.
    -- -------------------------------------------------------------------------
    DECLARE @insert_count INT = (
        SELECT COUNT(*) FROM dbo.clients_delta_staging WHERE operation = 'I'
    );  -- count new rows so we can reserve exactly the right number of PKs

    IF @insert_count > 0  -- skip reservation and insert entirely if nothing to insert
    BEGIN
        DECLARE @first_id INT;

        -- Reserve a contiguous block of PKs for all inserts in this batch.
        -- One reservation call for the whole batch is more efficient than
        -- calling reserve_pk_block once per row.
        EXEC dbo.reserve_pk_block
            @table_name = 'clients',
            @block_size = @insert_count,    -- reserve exactly as many IDs as there are new rows
            @first_id   = @first_id OUTPUT; -- @first_id is the start of the reserved range

        SET IDENTITY_INSERT dbo.clients ON; -- allow explicit PK values

        -- ROW_NUMBER() assigns sequential offsets (0, 1, 2...) within the reserved block.
        -- e.g. if @first_id = 101 and there are 3 rows: IDs 101, 102, 103 are assigned.
        -- JOIN on facility.src_fac_id translates source fac_id → target fac_id
        -- (staging holds the source fac_id; we need the target fac_id for the FK).
        INSERT INTO dbo.clients (client_id, fac_id, first_name, last_name, deleted, src_client_id)
        SELECT
            @first_id + ROW_NUMBER() OVER (ORDER BY s.src_client_id) - 1,  -- assigned target PK within reserved block
            f.fac_id,           -- target fac_id looked up via facility.src_fac_id
            s.first_name,
            s.last_name,
            s.deleted,
            s.src_client_id     -- preserve source ID for future delta matching
        FROM dbo.clients_delta_staging s
        INNER JOIN dbo.facility f
            ON f.src_fac_id = s.fac_id  -- translate source fac_id to target fac_id
        WHERE s.operation = 'I';        -- only process insert events

        SET IDENTITY_INSERT dbo.clients OFF; -- restore normal auto-increment
    END

    TRUNCATE TABLE dbo.clients_delta_staging;  -- empty staging; next delta run starts clean

    COMMIT;
END;
GO
