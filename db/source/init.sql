-- Source DB init: schema, Change Tracking, seed data

CREATE DATABASE sourcedb;
GO

USE sourcedb;
GO

ALTER DATABASE sourcedb SET CHANGE_TRACKING = ON
    (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON);
GO

CREATE TABLE dbo.facility (
    fac_id  INT IDENTITY(1,1) PRIMARY KEY,
    name    VARCHAR(100)      NOT NULL,
    prov    CHAR(2)           NOT NULL,
    deleted CHAR(1)           NOT NULL DEFAULT 'N'
);
GO

ALTER TABLE dbo.facility ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = OFF);
GO

CREATE TABLE dbo.clients (
    client_id  INT IDENTITY(1,1) PRIMARY KEY,
    fac_id     INT          NOT NULL REFERENCES dbo.facility(fac_id),
    first_name VARCHAR(100) NOT NULL,
    last_name  VARCHAR(100) NOT NULL,
    deleted    CHAR(1)      NOT NULL DEFAULT 'N'
);
GO

ALTER TABLE dbo.clients ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = OFF);
GO

-- Seed: 2 facilities
INSERT INTO dbo.facility (name, prov) VALUES
    ('Sunrise Health Centre', 'ON'),
    ('Maple Leaf Clinic',     'BC');
GO

-- Seed: 5 clients across the two facilities
INSERT INTO dbo.clients (fac_id, first_name, last_name) VALUES
    (1, 'Alice',   'Smith'),
    (1, 'Bob',     'Jones'),
    (1, 'Carol',   'White'),
    (2, 'David',   'Brown'),
    (2, 'Eve',     'Davis');
GO
