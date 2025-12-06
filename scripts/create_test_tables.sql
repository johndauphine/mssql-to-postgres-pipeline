-- Test tables for universal partitioning
-- Creates tables with different PK types to test NTILE and ROW_NUMBER partitioning

USE master;
GO

-- Create test database if not exists
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'PartitionTest')
BEGIN
    CREATE DATABASE PartitionTest;
END
GO

USE PartitionTest;
GO

-- Drop existing test tables
DROP TABLE IF EXISTS dbo.GuidOrders;
DROP TABLE IF EXISTS dbo.StringProducts;
DROP TABLE IF EXISTS dbo.CompositeOrderDetails;
DROP TABLE IF EXISTS dbo.SparseIntTable;
GO

------------------------------------------------------------
-- 1. GUID Primary Key Table (~2M rows)
------------------------------------------------------------
CREATE TABLE dbo.GuidOrders (
    OrderId UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
    CustomerId INT NOT NULL,
    OrderDate DATETIME2 DEFAULT GETDATE(),
    TotalAmount DECIMAL(18,2),
    Status VARCHAR(20) DEFAULT 'Pending'
);
GO

-- Insert 2M rows with GUIDs
PRINT 'Inserting GuidOrders (2M rows)...';
SET NOCOUNT ON;

DECLARE @i INT = 0;
DECLARE @batch INT = 50000;

WHILE @i < 2000000
BEGIN
    INSERT INTO dbo.GuidOrders (OrderId, CustomerId, OrderDate, TotalAmount, Status)
    SELECT
        NEWID(),
        ABS(CHECKSUM(NEWID())) % 100000,
        DATEADD(DAY, -ABS(CHECKSUM(NEWID())) % 365, GETDATE()),
        CAST(ABS(CHECKSUM(NEWID())) % 10000 AS DECIMAL(18,2)) / 100,
        CASE ABS(CHECKSUM(NEWID())) % 4
            WHEN 0 THEN 'Pending'
            WHEN 1 THEN 'Shipped'
            WHEN 2 THEN 'Delivered'
            ELSE 'Cancelled'
        END
    FROM (SELECT TOP (@batch) 1 as n FROM sys.all_objects a CROSS JOIN sys.all_objects b) x;

    SET @i = @i + @batch;
    IF @i % 500000 = 0 PRINT CONCAT('  ', @i, ' rows inserted...');
END

PRINT CONCAT('GuidOrders: ', (SELECT COUNT(*) FROM dbo.GuidOrders), ' rows');
GO

------------------------------------------------------------
-- 2. String Primary Key Table (~1.5M rows)
------------------------------------------------------------
CREATE TABLE dbo.StringProducts (
    ProductCode VARCHAR(20) PRIMARY KEY,
    ProductName NVARCHAR(200) NOT NULL,
    Category VARCHAR(50),
    Price DECIMAL(18,2),
    StockQuantity INT
);
GO

-- Insert 1.5M rows with string PKs
PRINT 'Inserting StringProducts (1.5M rows)...';
SET NOCOUNT ON;

DECLARE @j INT = 0;
DECLARE @batch2 INT = 50000;

WHILE @j < 1500000
BEGIN
    INSERT INTO dbo.StringProducts (ProductCode, ProductName, Category, Price, StockQuantity)
    SELECT
        CONCAT('PRD-', FORMAT(@j + ROW_NUMBER() OVER (ORDER BY (SELECT NULL)), '0000000')),
        CONCAT('Product ', @j + ROW_NUMBER() OVER (ORDER BY (SELECT NULL))),
        CASE ABS(CHECKSUM(NEWID())) % 5
            WHEN 0 THEN 'Electronics'
            WHEN 1 THEN 'Clothing'
            WHEN 2 THEN 'Food'
            WHEN 3 THEN 'Books'
            ELSE 'Other'
        END,
        CAST(ABS(CHECKSUM(NEWID())) % 100000 AS DECIMAL(18,2)) / 100,
        ABS(CHECKSUM(NEWID())) % 1000
    FROM (SELECT TOP (@batch2) 1 as n FROM sys.all_objects a CROSS JOIN sys.all_objects b) x;

    SET @j = @j + @batch2;
    IF @j % 500000 = 0 PRINT CONCAT('  ', @j, ' rows inserted...');
END

PRINT CONCAT('StringProducts: ', (SELECT COUNT(*) FROM dbo.StringProducts), ' rows');
GO

------------------------------------------------------------
-- 3. Composite Primary Key Table (~2M rows)
------------------------------------------------------------
CREATE TABLE dbo.CompositeOrderDetails (
    OrderId INT NOT NULL,
    LineNumber INT NOT NULL,
    ProductId INT NOT NULL,
    Quantity INT NOT NULL,
    UnitPrice DECIMAL(18,2),
    Discount DECIMAL(5,2) DEFAULT 0,
    PRIMARY KEY (OrderId, LineNumber)
);
GO

-- Insert 2M rows with composite PK
PRINT 'Inserting CompositeOrderDetails (2M rows)...';
SET NOCOUNT ON;

DECLARE @k INT = 0;
DECLARE @batch3 INT = 50000;
DECLARE @orderNum INT = 1;

WHILE @k < 2000000
BEGIN
    INSERT INTO dbo.CompositeOrderDetails (OrderId, LineNumber, ProductId, Quantity, UnitPrice, Discount)
    SELECT
        @orderNum + (ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1) / 5,  -- ~5 lines per order
        ((ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1) % 5) + 1,
        ABS(CHECKSUM(NEWID())) % 10000,
        ABS(CHECKSUM(NEWID())) % 20 + 1,
        CAST(ABS(CHECKSUM(NEWID())) % 50000 AS DECIMAL(18,2)) / 100,
        CAST(ABS(CHECKSUM(NEWID())) % 30 AS DECIMAL(5,2))
    FROM (SELECT TOP (@batch3) 1 as n FROM sys.all_objects a CROSS JOIN sys.all_objects b) x;

    SET @k = @k + @batch3;
    SET @orderNum = @orderNum + @batch3 / 5;
    IF @k % 500000 = 0 PRINT CONCAT('  ', @k, ' rows inserted...');
END

PRINT CONCAT('CompositeOrderDetails: ', (SELECT COUNT(*) FROM dbo.CompositeOrderDetails), ' rows');
GO

------------------------------------------------------------
-- 4. Sparse Integer PK Table (~1.5M rows with gaps)
-- This tests NTILE handles gaps correctly
------------------------------------------------------------
CREATE TABLE dbo.SparseIntTable (
    Id INT PRIMARY KEY,
    Data VARCHAR(100),
    CreatedAt DATETIME2 DEFAULT GETDATE()
);
GO

-- Insert 1.5M rows with sparse IDs (lots of gaps)
PRINT 'Inserting SparseIntTable (1.5M rows with gaps)...';
SET NOCOUNT ON;

-- Create IDs with large gaps: 1-100, 10001-10100, 20001-20100, etc.
DECLARE @m INT = 0;
DECLARE @batch4 INT = 100;
DECLARE @baseId INT = 1;

WHILE @m < 1500000
BEGIN
    INSERT INTO dbo.SparseIntTable (Id, Data, CreatedAt)
    SELECT
        @baseId + ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1,
        CONCAT('Data row ', @baseId + ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1),
        DATEADD(SECOND, -ABS(CHECKSUM(NEWID())) % 86400, GETDATE())
    FROM (SELECT TOP (@batch4) 1 as n FROM sys.all_objects) x;

    SET @m = @m + @batch4;
    SET @baseId = @baseId + 10000;  -- Jump 10000 between batches = lots of gaps
    IF @m % 500000 = 0 PRINT CONCAT('  ', @m, ' rows inserted...');
END

PRINT CONCAT('SparseIntTable: ', (SELECT COUNT(*) FROM dbo.SparseIntTable), ' rows');
GO

------------------------------------------------------------
-- Summary
------------------------------------------------------------
PRINT '';
PRINT '=== Test Tables Created ===';
SELECT
    t.name AS TableName,
    p.rows AS RowCount,
    STRING_AGG(c.name + ' (' + ty.name + ')', ', ') WITHIN GROUP (ORDER BY ic.key_ordinal) AS PrimaryKeyColumns
FROM sys.tables t
INNER JOIN sys.partitions p ON t.object_id = p.object_id AND p.index_id IN (0,1)
LEFT JOIN sys.indexes i ON t.object_id = i.object_id AND i.is_primary_key = 1
LEFT JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
LEFT JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
LEFT JOIN sys.types ty ON c.user_type_id = ty.user_type_id
WHERE t.schema_id = SCHEMA_ID('dbo')
GROUP BY t.name, p.rows
ORDER BY t.name;
GO
