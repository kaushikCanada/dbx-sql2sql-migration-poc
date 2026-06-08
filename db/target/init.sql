-- Target DB init: all PKs are IDENTITY (reserve_pk_block + IDENTITY_INSERT pattern).
-- Each table has a src_* traceability column pointing back to its source PK.
-- Staging tables have no FK/IDENTITY constraints — Spark writes freely.
-- SPs move staging → target atomically with IDENTITY_INSERT ON.

CREATE DATABASE targetdb;
GO

USE targetdb;
GO

-- ============================================================
-- MAIN TABLES  (wave order: 2 → 3 → 5 → 6)
-- ============================================================

-- Wave 2
CREATE TABLE dbo.facility (
    fac_id     INT          IDENTITY(1,1) NOT NULL,
    src_fac_id INT          NOT NULL,
    name       VARCHAR(100) NOT NULL,
    prov       VARCHAR(2)   NOT NULL,
    deleted    VARCHAR(1)   NOT NULL DEFAULT 'N',
    CONSTRAINT PK_facility PRIMARY KEY (fac_id)
);
GO

-- Wave 3
CREATE TABLE dbo.mpi (
    mpi_id        INT         IDENTITY(1,1) NOT NULL,
    src_mpi_id    INT         NOT NULL,
    first_name    VARCHAR(50) NULL,
    last_name     VARCHAR(50) NULL,
    date_of_birth DATETIME    NULL,
    sex           CHAR(1)     NULL,
    deleted       VARCHAR(1)  NOT NULL DEFAULT 'N',
    created_by    VARCHAR(60) NOT NULL DEFAULT 'system',
    created_date  DATETIME    NOT NULL DEFAULT GETDATE(),
    CONSTRAINT PK_mpi PRIMARY KEY (mpi_id)
);
GO

-- Wave 3
CREATE TABLE dbo.clients (
    client_id      INT         IDENTITY(1,1) NOT NULL,
    src_client_id  INT         NOT NULL,
    fac_id         INT         NOT NULL,
    mpi_id         INT         NULL,
    deleted        VARCHAR(1)  NOT NULL DEFAULT 'N',
    admission_date DATETIME    NULL,
    discharge_date DATETIME    NULL,
    created_by     VARCHAR(60) NOT NULL DEFAULT 'system',
    created_date   DATETIME    NOT NULL DEFAULT GETDATE(),
    CONSTRAINT PK_clients          PRIMARY KEY (client_id),
    CONSTRAINT FK_clients_facility FOREIGN KEY (fac_id) REFERENCES dbo.facility(fac_id),
    CONSTRAINT FK_clients_mpi      FOREIGN KEY (mpi_id) REFERENCES dbo.mpi(mpi_id)
);
GO

-- Wave 5
CREATE TABLE dbo.pho_phys_order (
    phys_order_id     INT           IDENTITY(1,1) NOT NULL,
    src_phys_order_id INT           NOT NULL,
    client_id         INT           NOT NULL,
    fac_id            INT           NOT NULL,
    drug_name         VARCHAR(500)  NULL,
    strength          VARCHAR(30)   NULL,
    directions        VARCHAR(1000) NULL,
    order_date        DATETIME      NOT NULL,
    active_flag       CHAR(1)       NOT NULL DEFAULT 'Y',
    deleted           VARCHAR(1)    NOT NULL DEFAULT 'N',
    created_by        VARCHAR(60)   NOT NULL DEFAULT 'system',
    created_date      DATETIME      NOT NULL DEFAULT GETDATE(),
    CONSTRAINT PK_pho_phys_order     PRIMARY KEY (phys_order_id),
    CONSTRAINT FK_phys_order_clients FOREIGN KEY (client_id) REFERENCES dbo.clients(client_id)
);
GO

-- Wave 5
CREATE TABLE dbo.pho_order_schedule (
    order_schedule_id     INT           IDENTITY(1,1) NOT NULL,
    src_order_schedule_id INT           NOT NULL,
    phys_order_id         INT           NOT NULL,
    fac_id                INT           NOT NULL,
    deleted               VARCHAR(1)    NOT NULL DEFAULT 'N',
    dose_value            VARCHAR(31)   NULL,
    directions            VARCHAR(1000) NULL,
    mon                   CHAR(1)       NULL,
    tues                  CHAR(1)       NULL,
    wed                   CHAR(1)       NULL,
    thurs                 CHAR(1)       NULL,
    fri                   CHAR(1)       NULL,
    sat                   CHAR(1)       NULL,
    sun                   CHAR(1)       NULL,
    created_by            VARCHAR(60)   NOT NULL DEFAULT 'system',
    created_date          DATETIME      NOT NULL DEFAULT GETDATE(),
    CONSTRAINT PK_pho_order_schedule        PRIMARY KEY (order_schedule_id),
    CONSTRAINT FK_order_schedule_phys_order FOREIGN KEY (phys_order_id) REFERENCES dbo.pho_phys_order(phys_order_id)
);
GO

-- Wave 6
CREATE TABLE dbo.pho_schedule (
    schedule_id           INT         IDENTITY(1,1) NOT NULL,
    src_schedule_id       INT         NOT NULL,
    order_schedule_id     INT         NULL,
    phys_order_id         INT         NULL,
    fac_id                INT         NOT NULL,
    deleted               CHAR(1)     NOT NULL DEFAULT 'N',
    description           VARCHAR(35) NULL,
    start_time            VARCHAR(4)  NULL,
    dose                  VARCHAR(31) NULL,
    created_by            VARCHAR(60) NOT NULL DEFAULT 'system',
    created_date          DATETIME    NOT NULL DEFAULT GETDATE(),
    CONSTRAINT PK_pho_schedule                PRIMARY KEY (schedule_id),
    CONSTRAINT FK_pho_schedule_order_schedule FOREIGN KEY (order_schedule_id) REFERENCES dbo.pho_order_schedule(order_schedule_id)
);
GO

-- Wave 6
CREATE TABLE dbo.pho_schedule_details (
    pho_schedule_detail_id     BIGINT      IDENTITY(1,1) NOT NULL,
    src_pho_schedule_detail_id BIGINT      NOT NULL,
    pho_schedule_id            INT         NOT NULL,
    schedule_date              DATETIME    NOT NULL,
    dose                       VARCHAR(31) NULL,
    deleted                    VARCHAR(1)  NOT NULL DEFAULT 'N',
    perform_by                 VARCHAR(60) NULL,
    perform_date               DATETIME    NULL,
    perform_initials           VARCHAR(4)  NULL,
    created_by                 VARCHAR(60) NOT NULL DEFAULT 'system',
    created_date               DATETIME    NOT NULL DEFAULT GETDATE(),
    CONSTRAINT PK_pho_schedule_details      PRIMARY KEY (pho_schedule_detail_id),
    CONSTRAINT FK_schedule_details_schedule FOREIGN KEY (pho_schedule_id) REFERENCES dbo.pho_schedule(schedule_id)
);
GO

-- ============================================================
-- STAGING TABLES  (no FK or IDENTITY constraints — Spark-friendly)
-- ============================================================

CREATE TABLE dbo.facility_staging (
    fac_id     INT          NOT NULL,
    src_fac_id INT          NOT NULL,
    name       VARCHAR(100) NOT NULL,
    prov       VARCHAR(2)   NOT NULL,
    deleted    VARCHAR(1)   NOT NULL
);
GO

CREATE TABLE dbo.mpi_staging (
    mpi_id        INT         NOT NULL,
    src_mpi_id    INT         NOT NULL,
    first_name    VARCHAR(50) NULL,
    last_name     VARCHAR(50) NULL,
    date_of_birth DATETIME    NULL,
    sex           CHAR(1)     NULL,
    deleted       VARCHAR(1)  NOT NULL,
    created_by    VARCHAR(60) NOT NULL,
    created_date  DATETIME    NOT NULL
);
GO

CREATE TABLE dbo.clients_staging (
    client_id      INT         NOT NULL,
    src_client_id  INT         NOT NULL,
    fac_id         INT         NOT NULL,
    mpi_id         INT         NULL,
    deleted        VARCHAR(1)  NOT NULL,
    admission_date DATETIME    NULL,
    discharge_date DATETIME    NULL,
    created_by     VARCHAR(60) NOT NULL,
    created_date   DATETIME    NOT NULL
);
GO

CREATE TABLE dbo.pho_phys_order_staging (
    phys_order_id     INT           NOT NULL,
    src_phys_order_id INT           NOT NULL,
    client_id         INT           NOT NULL,
    fac_id            INT           NOT NULL,
    drug_name         VARCHAR(500)  NULL,
    strength          VARCHAR(30)   NULL,
    directions        VARCHAR(1000) NULL,
    order_date        DATETIME      NOT NULL,
    active_flag       CHAR(1)       NOT NULL,
    deleted           VARCHAR(1)    NOT NULL,
    created_by        VARCHAR(60)   NOT NULL,
    created_date      DATETIME      NOT NULL
);
GO

CREATE TABLE dbo.pho_order_schedule_staging (
    order_schedule_id     INT           NOT NULL,
    src_order_schedule_id INT           NOT NULL,
    phys_order_id         INT           NOT NULL,
    fac_id                INT           NOT NULL,
    deleted               VARCHAR(1)    NOT NULL,
    dose_value            VARCHAR(31)   NULL,
    directions            VARCHAR(1000) NULL,
    mon                   CHAR(1)       NULL,
    tues                  CHAR(1)       NULL,
    wed                   CHAR(1)       NULL,
    thurs                 CHAR(1)       NULL,
    fri                   CHAR(1)       NULL,
    sat                   CHAR(1)       NULL,
    sun                   CHAR(1)       NULL,
    created_by            VARCHAR(60)   NOT NULL,
    created_date          DATETIME      NOT NULL
);
GO

CREATE TABLE dbo.pho_schedule_staging (
    schedule_id           INT         NOT NULL,
    src_schedule_id       INT         NOT NULL,
    order_schedule_id     INT         NULL,
    phys_order_id         INT         NULL,
    fac_id                INT         NOT NULL,
    deleted               CHAR(1)     NOT NULL,
    description           VARCHAR(35) NULL,
    start_time            VARCHAR(4)  NULL,
    dose                  VARCHAR(31) NULL,
    created_by            VARCHAR(60) NOT NULL,
    created_date          DATETIME    NOT NULL
);
GO

CREATE TABLE dbo.pho_schedule_details_staging (
    pho_schedule_detail_id     BIGINT      NOT NULL,
    src_pho_schedule_detail_id BIGINT      NOT NULL,
    pho_schedule_id            INT         NOT NULL,
    schedule_date              DATETIME    NOT NULL,
    dose                       VARCHAR(31) NULL,
    deleted                    VARCHAR(1)  NOT NULL,
    perform_by                 VARCHAR(60) NULL,
    perform_date               DATETIME    NULL,
    perform_initials           VARCHAR(4)  NULL,
    created_by                 VARCHAR(60) NOT NULL,
    created_date               DATETIME    NOT NULL
);
GO

-- Delta staging: source IDs kept as-is; SP resolves to target IDs via src_* lookups.
CREATE TABLE dbo.clients_delta_staging (
    src_client_id  INT        NOT NULL,
    operation      CHAR(1)    NOT NULL,   -- I, U, D
    src_fac_id     INT        NULL,
    src_mpi_id     INT        NULL,
    deleted        VARCHAR(1) NULL,
    admission_date DATETIME   NULL,
    discharge_date DATETIME   NULL
);
GO

-- ============================================================
-- STORED PROCEDURES
-- ============================================================

-- reserve_pk_block: acquires exclusive app lock, reserves a contiguous IDENTITY
-- block, returns the first id in the block. Works for all IDENTITY tables on target.
CREATE OR ALTER PROCEDURE dbo.reserve_pk_block
    @table_name NVARCHAR(128),
    @block_size INT,
    @first_id   BIGINT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    IF @block_size <= 0
        THROW 50001, 'block_size must be > 0', 1;

    DECLARE @lock NVARCHAR(255) = N'pk_reserve_' + @table_name;
    EXEC sp_getapplock @Resource = @lock, @LockMode = 'Exclusive', @LockOwner = 'Session';

    DECLARE @cur      BIGINT = CAST(ISNULL(IDENT_CURRENT(@table_name), 0) AS BIGINT);
    DECLARE @new_seed BIGINT = @cur + @block_size;
    SET @first_id = @cur + 1;
    DBCC CHECKIDENT (@table_name, RESEED, @new_seed) WITH NO_INFOMSGS;

    EXEC sp_releaseapplock @Resource = @lock, @LockOwner = 'Session';
END;
GO

-- ── Wave 2 ────────────────────────────────────────────────────────────────────

CREATE OR ALTER PROCEDURE dbo.load_facility_from_staging AS
BEGIN
    SET NOCOUNT ON;
    SET IDENTITY_INSERT dbo.facility ON;
    INSERT INTO dbo.facility (fac_id, src_fac_id, name, prov, deleted)
    SELECT fac_id, src_fac_id, name, prov, deleted FROM dbo.facility_staging;
    SET IDENTITY_INSERT dbo.facility OFF;
    TRUNCATE TABLE dbo.facility_staging;
END;
GO

-- ── Wave 3 ────────────────────────────────────────────────────────────────────

CREATE OR ALTER PROCEDURE dbo.load_mpi_from_staging AS
BEGIN
    SET NOCOUNT ON;
    SET IDENTITY_INSERT dbo.mpi ON;
    INSERT INTO dbo.mpi (mpi_id, src_mpi_id, first_name, last_name, date_of_birth,
                         sex, deleted, created_by, created_date)
    SELECT mpi_id, src_mpi_id, first_name, last_name, date_of_birth,
           sex, deleted, created_by, created_date
    FROM dbo.mpi_staging;
    SET IDENTITY_INSERT dbo.mpi OFF;
    TRUNCATE TABLE dbo.mpi_staging;
END;
GO

CREATE OR ALTER PROCEDURE dbo.load_clients_from_staging AS
BEGIN
    SET NOCOUNT ON;
    SET IDENTITY_INSERT dbo.clients ON;
    INSERT INTO dbo.clients (client_id, src_client_id, fac_id, mpi_id,
                             deleted, admission_date, discharge_date, created_by, created_date)
    SELECT client_id, src_client_id, fac_id, mpi_id,
           deleted, admission_date, discharge_date, created_by, created_date
    FROM dbo.clients_staging;
    SET IDENTITY_INSERT dbo.clients OFF;
    TRUNCATE TABLE dbo.clients_staging;
END;
GO

-- ── Wave 5 ────────────────────────────────────────────────────────────────────

CREATE OR ALTER PROCEDURE dbo.load_pho_phys_order_from_staging AS
BEGIN
    SET NOCOUNT ON;
    SET IDENTITY_INSERT dbo.pho_phys_order ON;
    INSERT INTO dbo.pho_phys_order (phys_order_id, src_phys_order_id, client_id, fac_id,
                                    drug_name, strength, directions, order_date,
                                    active_flag, deleted, created_by, created_date)
    SELECT phys_order_id, src_phys_order_id, client_id, fac_id,
           drug_name, strength, directions, order_date,
           active_flag, deleted, created_by, created_date
    FROM dbo.pho_phys_order_staging;
    SET IDENTITY_INSERT dbo.pho_phys_order OFF;
    TRUNCATE TABLE dbo.pho_phys_order_staging;
END;
GO

CREATE OR ALTER PROCEDURE dbo.load_pho_order_schedule_from_staging AS
BEGIN
    SET NOCOUNT ON;
    SET IDENTITY_INSERT dbo.pho_order_schedule ON;
    INSERT INTO dbo.pho_order_schedule (order_schedule_id, src_order_schedule_id, phys_order_id,
                                        fac_id, deleted, dose_value, directions,
                                        mon, tues, wed, thurs, fri, sat, sun,
                                        created_by, created_date)
    SELECT order_schedule_id, src_order_schedule_id, phys_order_id,
           fac_id, deleted, dose_value, directions,
           mon, tues, wed, thurs, fri, sat, sun,
           created_by, created_date
    FROM dbo.pho_order_schedule_staging;
    SET IDENTITY_INSERT dbo.pho_order_schedule OFF;
    TRUNCATE TABLE dbo.pho_order_schedule_staging;
END;
GO

-- ── Wave 6 ────────────────────────────────────────────────────────────────────

CREATE OR ALTER PROCEDURE dbo.load_pho_schedule_from_staging AS
BEGIN
    SET NOCOUNT ON;
    SET IDENTITY_INSERT dbo.pho_schedule ON;
    INSERT INTO dbo.pho_schedule (schedule_id, src_schedule_id, order_schedule_id, phys_order_id,
                                  fac_id, deleted, description, start_time, dose,
                                  created_by, created_date)
    SELECT schedule_id, src_schedule_id, order_schedule_id, phys_order_id,
           fac_id, deleted, description, start_time, dose,
           created_by, created_date
    FROM dbo.pho_schedule_staging;
    SET IDENTITY_INSERT dbo.pho_schedule OFF;
    TRUNCATE TABLE dbo.pho_schedule_staging;
END;
GO

CREATE OR ALTER PROCEDURE dbo.load_pho_schedule_details_from_staging AS
BEGIN
    SET NOCOUNT ON;
    SET IDENTITY_INSERT dbo.pho_schedule_details ON;
    INSERT INTO dbo.pho_schedule_details (pho_schedule_detail_id, src_pho_schedule_detail_id,
                                          pho_schedule_id, schedule_date, dose, deleted,
                                          perform_by, perform_date, perform_initials,
                                          created_by, created_date)
    SELECT pho_schedule_detail_id, src_pho_schedule_detail_id,
           pho_schedule_id, schedule_date, dose, deleted,
           perform_by, perform_date, perform_initials,
           created_by, created_date
    FROM dbo.pho_schedule_details_staging;
    SET IDENTITY_INSERT dbo.pho_schedule_details OFF;
    TRUNCATE TABLE dbo.pho_schedule_details_staging;
END;
GO

-- ── CT Delta ──────────────────────────────────────────────────────────────────
-- Applies I/U/D changes from clients_delta_staging to target clients table.
-- staging stores source IDs (src_fac_id, src_mpi_id); SP resolves to target IDs
-- via facility.src_fac_id and mpi.src_mpi_id — same pattern as production notebook.

CREATE OR ALTER PROCEDURE dbo.apply_clients_delta_from_staging AS
BEGIN
    SET NOCOUNT ON;

    -- Step 1: Hard deletes on source → soft deletes on target (preserve FK integrity).
    UPDATE c
    SET    c.deleted = 'Y'
    FROM   dbo.clients c
    JOIN   dbo.clients_delta_staging s ON s.src_client_id = c.src_client_id
    WHERE  s.operation = 'D';

    -- Step 2: Updates — propagate column changes, remapping src IDs to target IDs.
    UPDATE c
    SET    c.fac_id         = ISNULL(f.fac_id,        c.fac_id),
           c.mpi_id         = ISNULL(m.mpi_id,        c.mpi_id),
           c.deleted        = ISNULL(s.deleted,        c.deleted),
           c.admission_date = ISNULL(s.admission_date, c.admission_date),
           c.discharge_date = s.discharge_date
    FROM   dbo.clients c
    JOIN   dbo.clients_delta_staging s  ON s.src_client_id = c.src_client_id
    LEFT JOIN dbo.facility f            ON f.src_fac_id    = s.src_fac_id
    LEFT JOIN dbo.mpi      m            ON m.src_mpi_id    = s.src_mpi_id
    WHERE  s.operation = 'U';

    -- Step 3: Inserts — reserve a PK block, then insert with target IDs.
    DECLARE @new_rows INT = (
        SELECT COUNT(*) FROM dbo.clients_delta_staging WHERE operation = 'I'
    );
    IF @new_rows > 0
    BEGIN
        DECLARE @first_id BIGINT;
        EXEC dbo.reserve_pk_block 'clients', @new_rows, @first_id OUTPUT;

        SET IDENTITY_INSERT dbo.clients ON;
        INSERT INTO dbo.clients (client_id, src_client_id, fac_id, mpi_id,
                                 deleted, admission_date, discharge_date)
        SELECT CAST(@first_id AS INT) + ROW_NUMBER() OVER (ORDER BY s.src_client_id) - 1,
               s.src_client_id,
               f.fac_id,
               m.mpi_id,
               ISNULL(s.deleted, 'N'),
               s.admission_date,
               s.discharge_date
        FROM   dbo.clients_delta_staging s
        JOIN   dbo.facility f  ON f.src_fac_id = s.src_fac_id
        LEFT JOIN dbo.mpi   m  ON m.src_mpi_id = s.src_mpi_id
        WHERE  s.operation = 'I';
        SET IDENTITY_INSERT dbo.clients OFF;
    END;

    TRUNCATE TABLE dbo.clients_delta_staging;
END;
GO
