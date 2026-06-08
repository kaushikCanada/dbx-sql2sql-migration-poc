-- Source DB init: MUS_fve_bip26204 schema (simplified for POC)
-- Tables match real column structure; PKs are app-managed (non-IDENTITY)
-- except pho_order_schedule, pho_schedule, pho_schedule_details which use IDENTITY.

CREATE DATABASE sourcedb;
GO

USE sourcedb;
GO

ALTER DATABASE sourcedb SET CHANGE_TRACKING = ON
    (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON);
GO

-- ============================================================
-- Wave 2: FACILITY  (root reference table)
-- ============================================================
CREATE TABLE dbo.facility (
    fac_id  INT          NOT NULL,
    name    VARCHAR(100) NOT NULL,
    prov    VARCHAR(2)   NOT NULL,
    deleted VARCHAR(1)   NOT NULL DEFAULT 'N',
    CONSTRAINT PK_facility PRIMARY KEY (fac_id)
);
ALTER TABLE dbo.facility ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = OFF);
GO

-- ============================================================
-- Wave 3: MPI  (Master Patient Index — demographics)
-- Non-IDENTITY: PK is app-managed in the real system.
-- ============================================================
CREATE TABLE dbo.mpi (
    mpi_id        INT          NOT NULL,
    first_name    VARCHAR(50)  NULL,
    last_name     VARCHAR(50)  NULL,
    date_of_birth DATETIME     NULL,
    sex           CHAR(1)      NULL,
    deleted       VARCHAR(1)   NOT NULL DEFAULT 'N',
    created_by    VARCHAR(60)  NOT NULL DEFAULT 'system',
    created_date  DATETIME     NOT NULL DEFAULT GETDATE(),
    CONSTRAINT PK_mpi PRIMARY KEY (mpi_id)
);
ALTER TABLE dbo.mpi ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = OFF);
GO

-- ============================================================
-- Wave 3: CLIENTS  (depends on facility + mpi)
-- Non-IDENTITY: PK is app-managed in the real system.
-- ============================================================
CREATE TABLE dbo.clients (
    client_id      INT          NOT NULL,
    fac_id         INT          NOT NULL,
    mpi_id         INT          NULL,
    deleted        VARCHAR(1)   NOT NULL DEFAULT 'N',
    admission_date DATETIME     NULL,
    discharge_date DATETIME     NULL,
    created_by     VARCHAR(60)  NOT NULL DEFAULT 'system',
    created_date   DATETIME     NOT NULL DEFAULT GETDATE(),
    CONSTRAINT PK_clients          PRIMARY KEY (client_id),
    CONSTRAINT FK_clients_facility FOREIGN KEY (fac_id)  REFERENCES dbo.facility(fac_id),
    CONSTRAINT FK_clients_mpi      FOREIGN KEY (mpi_id)  REFERENCES dbo.mpi(mpi_id)
);
ALTER TABLE dbo.clients ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = OFF);
GO

-- ============================================================
-- Wave 5: PHO_PHYS_ORDER  (depends on clients)
-- Non-IDENTITY: PK is app-managed in the real system.
-- ============================================================
CREATE TABLE dbo.pho_phys_order (
    phys_order_id INT           NOT NULL,
    client_id     INT           NOT NULL,
    fac_id        INT           NOT NULL,
    drug_name     VARCHAR(500)  NULL,
    strength      VARCHAR(30)   NULL,
    directions    VARCHAR(1000) NULL,
    order_date    DATETIME      NOT NULL DEFAULT GETDATE(),
    active_flag   CHAR(1)       NOT NULL DEFAULT 'Y',
    deleted       VARCHAR(1)    NOT NULL DEFAULT 'N',
    created_by    VARCHAR(60)   NOT NULL DEFAULT 'system',
    created_date  DATETIME      NOT NULL DEFAULT GETDATE(),
    CONSTRAINT PK_pho_phys_order     PRIMARY KEY (phys_order_id),
    CONSTRAINT FK_phys_order_clients FOREIGN KEY (client_id) REFERENCES dbo.clients(client_id)
);
ALTER TABLE dbo.pho_phys_order ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = OFF);
GO

-- ============================================================
-- Wave 5: PHO_ORDER_SCHEDULE  (depends on pho_phys_order)
-- IDENTITY: PK is DB-managed in the real system.
-- ============================================================
CREATE TABLE dbo.pho_order_schedule (
    order_schedule_id INT           IDENTITY(1,1) NOT NULL,
    phys_order_id     INT           NOT NULL,
    fac_id            INT           NOT NULL,
    deleted           VARCHAR(1)    NOT NULL DEFAULT 'N',
    dose_value        VARCHAR(31)   NULL,
    directions        VARCHAR(1000) NULL,
    mon               CHAR(1)       NULL,
    tues              CHAR(1)       NULL,
    wed               CHAR(1)       NULL,
    thurs             CHAR(1)       NULL,
    fri               CHAR(1)       NULL,
    sat               CHAR(1)       NULL,
    sun               CHAR(1)       NULL,
    created_by        VARCHAR(60)   NOT NULL DEFAULT 'system',
    created_date      DATETIME      NOT NULL DEFAULT GETDATE(),
    CONSTRAINT PK_pho_order_schedule        PRIMARY KEY (order_schedule_id),
    CONSTRAINT FK_order_schedule_phys_order FOREIGN KEY (phys_order_id) REFERENCES dbo.pho_phys_order(phys_order_id)
);
ALTER TABLE dbo.pho_order_schedule ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = OFF);
GO

-- ============================================================
-- Wave 6: PHO_SCHEDULE  (depends on pho_order_schedule)
-- IDENTITY: PK is DB-managed in the real system.
-- ============================================================
CREATE TABLE dbo.pho_schedule (
    schedule_id       INT         IDENTITY(1,1) NOT NULL,
    order_schedule_id INT         NULL,
    phys_order_id     INT         NULL,
    fac_id            INT         NOT NULL,
    deleted           CHAR(1)     NOT NULL DEFAULT 'N',
    description       VARCHAR(35) NULL,
    start_time        VARCHAR(4)  NULL,
    dose              VARCHAR(31) NULL,
    created_by        VARCHAR(60) NOT NULL DEFAULT 'system',
    created_date      DATETIME    NOT NULL DEFAULT GETDATE(),
    CONSTRAINT PK_pho_schedule                 PRIMARY KEY (schedule_id),
    CONSTRAINT FK_pho_schedule_order_schedule  FOREIGN KEY (order_schedule_id) REFERENCES dbo.pho_order_schedule(order_schedule_id)
);
ALTER TABLE dbo.pho_schedule ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = OFF);
GO

-- ============================================================
-- Wave 6: PHO_SCHEDULE_DETAILS  (depends on pho_schedule)
-- IDENTITY BIGINT: PK is DB-managed in the real system.
-- ============================================================
CREATE TABLE dbo.pho_schedule_details (
    pho_schedule_detail_id BIGINT      IDENTITY(1,1) NOT NULL,
    pho_schedule_id        INT         NOT NULL,
    schedule_date          DATETIME    NOT NULL,
    dose                   VARCHAR(31) NULL,
    deleted                VARCHAR(1)  NOT NULL DEFAULT 'N',
    perform_by             VARCHAR(60) NULL,
    perform_date           DATETIME    NULL,
    perform_initials       VARCHAR(4)  NULL,
    created_by             VARCHAR(60) NOT NULL DEFAULT 'system',
    created_date           DATETIME    NOT NULL DEFAULT GETDATE(),
    CONSTRAINT PK_pho_schedule_details      PRIMARY KEY (pho_schedule_detail_id),
    CONSTRAINT FK_schedule_details_schedule FOREIGN KEY (pho_schedule_id) REFERENCES dbo.pho_schedule(schedule_id)
);
ALTER TABLE dbo.pho_schedule_details ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = OFF);
GO

-- ============================================================
-- SEED DATA
-- ============================================================

-- Wave 2: 2 facilities
INSERT INTO dbo.facility (fac_id, name, prov) VALUES
(1, 'Sunrise Long Term Care', 'ON'),
(2, 'Lakeview Nursing Home',  'ON');

-- Wave 3: 3 MPI (patient demographics)
INSERT INTO dbo.mpi (mpi_id, first_name, last_name, date_of_birth, sex) VALUES
(1, 'Alice',    'Nguyen',  '1945-03-15', 'F'),
(2, 'Robert',   'Okafor',  '1938-07-22', 'M'),
(3, 'Margaret', 'Chen',    '1950-11-30', 'F');

-- Wave 3: 3 clients (2 in fac 1, 1 in fac 2)
INSERT INTO dbo.clients (client_id, fac_id, mpi_id, admission_date) VALUES
(1, 1, 1, '2023-01-10'),
(2, 1, 2, '2023-02-20'),
(3, 2, 3, '2023-03-05');

-- Wave 5: 2 physician orders (both for fac 1 clients)
INSERT INTO dbo.pho_phys_order (phys_order_id, client_id, fac_id, drug_name, strength, directions, order_date) VALUES
(101, 1, 1, 'Metformin',  '500mg', 'Twice daily with meals', '2023-01-15'),
(102, 2, 1, 'Lisinopril', '10mg',  'Once daily morning',     '2023-02-25');

-- Wave 5: 2 order schedules (one per physician order)
INSERT INTO dbo.pho_order_schedule (phys_order_id, fac_id, dose_value, directions, mon, tues, wed, thurs, fri, sat, sun) VALUES
(101, 1, '500mg', 'Twice daily with meals', 'Y','Y','Y','Y','Y','Y','Y'),
(102, 1, '10mg',  'Once daily morning',     'Y','Y','Y','Y','Y','N','N');

-- Wave 6: 2 schedules (one per order schedule)
INSERT INTO dbo.pho_schedule (order_schedule_id, phys_order_id, fac_id, description, start_time, dose) VALUES
(1, 101, 1, 'AM Metformin',  '0800', '500mg'),
(2, 102, 1, 'AM Lisinopril', '0900', '10mg');

-- Wave 6: 4 administration records (2 per schedule)
INSERT INTO dbo.pho_schedule_details (pho_schedule_id, schedule_date, dose, perform_by, perform_date, perform_initials) VALUES
(1, '2023-01-16 08:00:00', '500mg', 'jsmith', '2023-01-16 08:05:00', 'JS'),
(1, '2023-01-17 08:00:00', '500mg', 'mkumar', '2023-01-17 08:03:00', 'MK'),
(2, '2023-02-26 09:00:00', '10mg',  'jsmith', '2023-02-26 09:02:00', 'JS'),
(2, '2023-02-27 09:00:00', '10mg',  'jpatel', '2023-02-27 09:05:00', 'JP');
GO
