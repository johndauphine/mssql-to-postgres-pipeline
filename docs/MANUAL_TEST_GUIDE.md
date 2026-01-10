# Manual Testing Guide: Date-Based Incremental Loading

**Quick reference for manual validation of the date-based incremental loading feature**

---

## Prerequisites

1. **Docker Compose Running**
   ```bash
   docker-compose up -d
   docker-compose ps  # Verify all services are "Up"
   ```

2. **Access to Airflow UI**
   - URL: http://localhost:8080
   - Username: `airflow`
   - Password: `airflow`

3. **Database Access**
   - MSSQL: `localhost:1433` (sa / YourStrong@Passw0rd)
   - PostgreSQL: `localhost:5433` (postgres / PostgresPassword123)

---

## Test Scenario 1: First Sync (Full Load)

### Step 1: Prepare Test Data

```sql
-- Connect to MSSQL (localhost:1433)
-- Create test table
CREATE TABLE dbo.TestDateSync (
    id INT PRIMARY KEY,
    name NVARCHAR(100),
    value INT,
    updated_at DATETIME2,
    created_at DATETIME2 DEFAULT GETDATE()
);

-- Insert 100 test rows
DECLARE @i INT = 0;
WHILE @i < 100
BEGIN
    INSERT INTO dbo.TestDateSync (id, name, value, updated_at)
    VALUES (
        @i,
        'User_' + CAST(@i AS NVARCHAR(10)),
        @i * 10,
        DATEADD(DAY, -(@i % 10), GETDATE())
    );
    SET @i = @i + 1;
END;

-- Insert 10 rows with NULL updated_at
WHILE @i < 110
BEGIN
    INSERT INTO dbo.TestDateSync (id, name, value, updated_at)
    VALUES (@i, 'NullUser_' + CAST(@i AS NVARCHAR(10)), @i * 10, NULL);
    SET @i = @i + 1;
END;

-- Verify data
SELECT COUNT(*) as total_rows FROM dbo.TestDateSync;
SELECT COUNT(*) as null_rows FROM dbo.TestDateSync WHERE updated_at IS NULL;
-- Expected: 110 total, 10 NULL
```

### Step 2: Configure DAG

Edit `.env` file:
```bash
DATE_UPDATED_FIELD=updated_at
INCLUDE_TABLES=dbo.TestDateSync
```

Restart services:
```bash
docker-compose restart airflow-scheduler airflow-webserver
```

### Step 3: Create Target Table

```sql
-- Connect to PostgreSQL (localhost:5433)
-- Create target schema
CREATE SCHEMA IF NOT EXISTS mssql__stackoverfl__dbo;

-- Create target table (matching structure)
CREATE TABLE mssql__stackoverfl__dbo.testdatesync (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100),
    value INTEGER,
    updated_at TIMESTAMP,
    created_at TIMESTAMP
);
```

### Step 4: Run DAG

**Option A: Via Airflow UI**
1. Navigate to http://localhost:8080
2. Find DAG: `mssql_to_postgres_incremental`
3. Click "Trigger DAG"
4. Wait for completion (green)

**Option B: Via CLI**
```bash
docker exec airflow-scheduler airflow dags trigger mssql_to_postgres_incremental
```

### Step 5: Validate Results

```sql
-- PostgreSQL: Check target table
SELECT COUNT(*) as total_rows FROM mssql__stackoverfl__dbo.testdatesync;
-- Expected: 110

SELECT COUNT(*) as null_rows FROM mssql__stackoverfl__dbo.testdatesync WHERE updated_at IS NULL;
-- Expected: 10

-- Check state table
SELECT
    table_name,
    sync_status,
    last_sync_timestamp,
    rows_inserted,
    rows_updated,
    sync_duration_seconds
FROM _migration._migration_state
WHERE table_name = 'TestDateSync'
ORDER BY last_sync_end DESC
LIMIT 1;
-- Expected: sync_status='completed', rows_inserted=110, last_sync_timestamp IS NOT NULL
```

### Step 6: Check Logs

```bash
# View scheduler logs
docker-compose logs airflow-scheduler --tail=100 | grep -i "date-based"

# Expected log entries:
# "Date column 'updated_at' found (type: datetime2)"
# "Date-based incremental: no previous sync timestamp, doing full sync"
# "Updated sync timestamp to ... for next date-based sync"
```

**✅ Pass Criteria:**
- All 110 rows in target table
- 10 NULL rows transferred
- State record shows `completed` status
- `last_sync_timestamp` set to sync start time
- Logs show date column detected and full sync performed

---

## Test Scenario 2: Incremental Sync (New Rows Only)

### Step 1: Record Baseline

```sql
-- PostgreSQL: Record the last_sync_timestamp
SELECT last_sync_timestamp FROM _migration._migration_state WHERE table_name = 'TestDateSync';
-- Save this value as T1
```

### Step 2: Add New Data to MSSQL

```sql
-- MSSQL: Insert 20 new rows with recent updated_at
DECLARE @i INT = 110;
WHILE @i < 130
BEGIN
    INSERT INTO dbo.TestDateSync (id, name, value, updated_at)
    VALUES (@i, 'NewUser_' + CAST(@i AS NVARCHAR(10)), @i * 10, GETDATE());
    SET @i = @i + 1;
END;

-- Update 10 existing rows (change updated_at to now)
UPDATE TOP (10) dbo.TestDateSync
SET updated_at = GETDATE(), value = value + 100
WHERE id < 100;

-- Insert 5 rows with old dates (should be skipped)
DECLARE @old_date DATETIME2 = DATEADD(DAY, -30, GETDATE());
DECLARE @j INT = 130;
WHILE @j < 135
BEGIN
    INSERT INTO dbo.TestDateSync (id, name, value, updated_at)
    VALUES (@j, 'OldUser_' + CAST(@j AS NVARCHAR(10)), @j * 10, @old_date);
    SET @j = @j + 1;
END;

-- Insert 3 rows with NULL (should be included)
WHILE @j < 138
BEGIN
    INSERT INTO dbo.TestDateSync (id, name, value, updated_at)
    VALUES (@j, 'NewNullUser_' + CAST(@j AS NVARCHAR(10)), @j * 10, NULL);
    SET @j = @j + 1;
END;

-- Verify source count
SELECT COUNT(*) FROM dbo.TestDateSync;
-- Expected: 138 (110 original + 28 new)
```

### Step 3: Run Second Sync

```bash
docker exec airflow-scheduler airflow dags trigger mssql_to_postgres_incremental
```

Wait for completion.

### Step 4: Validate Incremental Behavior

```sql
-- PostgreSQL: Check row counts
SELECT COUNT(*) FROM mssql__stackoverfl__dbo.testdatesync;
-- Expected: 133 (110 + 20 new + 3 NULL, NOT +5 old)

-- Check state record
SELECT
    sync_status,
    last_sync_timestamp,
    rows_inserted,
    rows_updated
FROM _migration._migration_state
WHERE table_name = 'TestDateSync'
ORDER BY last_sync_end DESC
LIMIT 1;
-- Expected: rows_inserted=23 (20 new + 3 NULL), rows_updated=10
```

### Step 5: Verify Old Rows Skipped

```sql
-- MSSQL: Check old rows exist in source
SELECT COUNT(*) FROM dbo.TestDateSync WHERE id BETWEEN 130 AND 134;
-- Expected: 5

-- PostgreSQL: Verify they were NOT transferred
SELECT COUNT(*) FROM mssql__stackoverfl__dbo.testdatesync WHERE id BETWEEN 130 AND 134;
-- Expected: 0 (skipped because updated_at < last_sync_timestamp)
```

### Step 6: Check Logs for Date Filtering

```bash
docker-compose logs airflow-scheduler --tail=100 | grep -i "date-based"

# Expected:
# "Date-based incremental: syncing rows where updated_at > {T1}"
# "Updated sync timestamp to ... for next date-based sync"
```

**✅ Pass Criteria:**
- 33 rows transferred (20 new + 10 updated + 3 NULL)
- 5 old rows skipped (not in target)
- State shows rows_inserted=23, rows_updated=10
- `last_sync_timestamp` updated to new sync time
- Logs show date filter applied

---

## Test Scenario 3: NULL Date Handling

### Step 1: Prepare NULL Test Data

```sql
-- MSSQL: Insert 15 rows with NULL updated_at
DECLARE @i INT = 138;
WHILE @i < 153
BEGIN
    INSERT INTO dbo.TestDateSync (id, name, value, updated_at)
    VALUES (@i, 'NullOnlyUser_' + CAST(@i AS NVARCHAR(10)), @i * 10, NULL);
    SET @i = @i + 1;
END;

-- Verify NULL count in source
SELECT COUNT(*) FROM dbo.TestDateSync WHERE updated_at IS NULL;
-- Expected: 28 (10 original + 3 from scenario 2 + 15 new)
```

### Step 2: Run Sync

```bash
docker exec airflow-scheduler airflow dags trigger mssql_to_postgres_incremental
```

### Step 3: Validate NULL Handling

```sql
-- PostgreSQL: Check NULL rows transferred
SELECT COUNT(*) FROM mssql__stackoverfl__dbo.testdatesync WHERE updated_at IS NULL;
-- Expected: 28 (all NULL rows from source)

-- Check total count
SELECT COUNT(*) FROM mssql__stackoverfl__dbo.testdatesync;
-- Expected: 148 (133 + 15 new NULL)

-- Check state
SELECT rows_inserted, rows_updated, rows_unchanged
FROM _migration._migration_state
WHERE table_name = 'TestDateSync'
ORDER BY last_sync_end DESC
LIMIT 1;
-- Expected: rows_inserted=15 (new NULL), rows_unchanged=13 (original NULL re-synced)
```

**✅ Pass Criteria:**
- All NULL rows transferred (even if unchanged)
- New NULL rows inserted
- Original NULL rows marked as unchanged
- No errors in sync

---

## Test Scenario 4: Table Without Date Column (Fallback)

### Step 1: Create Table Without Date Column

```sql
-- MSSQL: Create table without updated_at
CREATE TABLE dbo.TestNoDateColumn (
    id INT PRIMARY KEY,
    name NVARCHAR(100),
    value INT
);

-- Insert data
DECLARE @i INT = 0;
WHILE @i < 50
BEGIN
    INSERT INTO dbo.TestNoDateColumn (id, name, value)
    VALUES (@i, 'User_' + CAST(@i AS NVARCHAR(10)), @i * 10);
    SET @i = @i + 1;
END;
```

### Step 2: Create Target Table

```sql
-- PostgreSQL
CREATE TABLE mssql__stackoverfl__dbo.testnodatecolumn (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100),
    value INTEGER
);

-- Insert initial data to simulate previous sync
INSERT INTO mssql__stackoverfl__dbo.testnodatecolumn (id, name, value)
SELECT id, name, value FROM generate_series(0, 29) id;
```

### Step 3: Update Include Tables

Edit `.env`:
```bash
INCLUDE_TABLES=dbo.TestDateSync,dbo.TestNoDateColumn
```

Restart:
```bash
docker-compose restart airflow-scheduler airflow-webserver
```

### Step 4: Run Sync

```bash
docker exec airflow-scheduler airflow dags trigger mssql_to_postgres_incremental
```

### Step 5: Validate Fallback Behavior

```bash
# Check logs for warning
docker-compose logs airflow-scheduler --tail=200 | grep -i "testnodatecolumn"

# Expected warning:
# "date column 'updated_at' not found in dbo.TestNoDateColumn, falling back to full sync"
```

```sql
-- PostgreSQL: Verify full sync occurred
SELECT COUNT(*) FROM mssql__stackoverfl__dbo.testnodatecolumn;
-- Expected: 50 (all rows synced despite missing date column)
```

**✅ Pass Criteria:**
- Warning logged about missing date column
- Full sync performed (all rows transferred)
- Sync completes successfully
- No errors

---

## Test Scenario 5: Failed Sync (Timestamp Not Updated)

### Step 1: Trigger a Failure

```sql
-- PostgreSQL: Drop target table to force failure
DROP TABLE mssql__stackoverfl__dbo.testdatesync;
```

### Step 2: Attempt Sync

```bash
docker exec airflow-scheduler airflow dags trigger mssql_to_postgres_incremental
```

Wait for failure (red in UI).

### Step 3: Verify State Not Updated

```sql
-- PostgreSQL: Check state record
SELECT
    sync_status,
    last_sync_timestamp,
    last_sync_end
FROM _migration._migration_state
WHERE table_name = 'TestDateSync'
ORDER BY id DESC
LIMIT 2;
-- Expected:
-- Latest record: sync_status='failed', last_sync_timestamp unchanged from previous sync
-- Previous record: sync_status='completed'
```

### Step 4: Recreate Table and Retry

```sql
-- PostgreSQL: Recreate target table
CREATE TABLE mssql__stackoverfl__dbo.testdatesync (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100),
    value INTEGER,
    updated_at TIMESTAMP,
    created_at TIMESTAMP
);
```

```bash
# Retry sync
docker exec airflow-scheduler airflow dags trigger mssql_to_postgres_incremental
```

### Step 5: Verify Recovery

```sql
-- PostgreSQL: Check sync completed
SELECT sync_status FROM _migration._migration_state
WHERE table_name = 'TestDateSync'
ORDER BY last_sync_end DESC LIMIT 1;
-- Expected: 'completed'
```

**✅ Pass Criteria:**
- Failed sync does not update `last_sync_timestamp`
- Retry uses old timestamp (re-syncs same data)
- Recovery sync completes successfully

---

## Security Test: SQL Injection Attempt

### Step 1: Attempt Malicious Column Name

Edit `.env`:
```bash
DATE_UPDATED_FIELD=updated_at'; DROP TABLE TestDateSync; --
```

Restart:
```bash
docker-compose restart airflow-scheduler airflow-webserver
```

### Step 2: Run Sync

```bash
docker exec airflow-scheduler airflow dags trigger mssql_to_postgres_incremental
```

### Step 3: Verify Security

```sql
-- MSSQL: Verify table still exists
SELECT COUNT(*) FROM dbo.TestDateSync;
-- Expected: Table still exists, count > 0

-- Check that malicious column name was rejected
-- (sync should fall back to full sync due to invalid column)
```

```bash
# Check logs for warning
docker-compose logs airflow-scheduler --tail=200 | grep -i "date column"

# Expected: Warning about column not found, fallback to full sync
# NO DROP TABLE executed
```

**✅ Pass Criteria:**
- Table NOT dropped (SQL injection failed)
- Parameterized query prevented injection
- Sync falls back to full sync mode
- No security breach

---

## Performance Check: Large Table

### Step 1: Create Large Test Table (Optional)

```sql
-- MSSQL: Create table with 100K rows
CREATE TABLE dbo.TestLargeTable (
    id INT PRIMARY KEY,
    name NVARCHAR(100),
    value INT,
    updated_at DATETIME2
);

-- Insert 100K rows (this may take a minute)
DECLARE @i INT = 0;
WHILE @i < 100000
BEGIN
    INSERT INTO dbo.TestLargeTable (id, name, value, updated_at)
    VALUES (
        @i,
        'User_' + CAST(@i AS NVARCHAR(10)),
        @i * 10,
        DATEADD(MINUTE, -(@i % 10000), GETDATE())
    );
    SET @i = @i + 1;

    IF @i % 10000 = 0
        PRINT 'Inserted ' + CAST(@i AS NVARCHAR(10)) + ' rows...';
END;

-- Create index on updated_at for performance
CREATE INDEX idx_updated_at ON dbo.TestLargeTable(updated_at);
```

### Step 2: First Sync (Baseline)

- Create target table
- Run sync
- Record time from logs or UI

### Step 3: Incremental Sync (1% Change)

```sql
-- MSSQL: Update 1% of rows
UPDATE TOP (1000) dbo.TestLargeTable
SET updated_at = GETDATE(), value = value + 1
WHERE id < 1000;
```

- Run second sync
- Record time
- Compare to first sync

**✅ Pass Criteria:**
- Second sync significantly faster (10x+ speedup expected)
- Only ~1000 rows transferred (verify in state table)
- No errors with large dataset

---

## Cleanup

```sql
-- MSSQL: Drop test tables
DROP TABLE IF EXISTS dbo.TestDateSync;
DROP TABLE IF EXISTS dbo.TestNoDateColumn;
DROP TABLE IF EXISTS dbo.TestLargeTable;

-- PostgreSQL: Drop test tables and state
DROP TABLE IF EXISTS mssql__stackoverfl__dbo.testdatesync;
DROP TABLE IF EXISTS mssql__stackoverfl__dbo.testnodatecolumn;
DROP TABLE IF EXISTS mssql__stackoverfl__dbo.testlargetable;

DELETE FROM _migration._migration_state WHERE table_name LIKE 'Test%';
```

Edit `.env` back to original settings:
```bash
DATE_UPDATED_FIELD=
INCLUDE_TABLES=<original value>
```

Restart:
```bash
docker-compose restart airflow-scheduler airflow-webserver
```

---

## Troubleshooting

### Issue: "Column not found" but column exists

**Cause:** Case-sensitive column name mismatch

**Solution:** Check exact case in MSSQL:
```sql
SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'YourTable' AND COLUMN_NAME LIKE '%updated%';
```

Use exact case in `DATE_UPDATED_FIELD`.

---

### Issue: Date filter not working (all rows transferred)

**Check:**
1. `DATE_UPDATED_FIELD` env var set correctly
2. DAG parameter not overriding to empty string
3. Column is valid temporal type (not varchar)
4. Last sync completed successfully (check state table)

---

### Issue: NULL rows not transferred

**Cause:** WHERE clause missing `OR IS NULL`

**Verify:** Check data_transfer.py line ~2866:
```python
date_where_clause = f"([{safe_date_col}] > ? OR [{safe_date_col}] IS NULL)"
```

Should include NULL handling.

---

### Issue: Performance not improved

**Check:**
1. Index exists on date column in MSSQL
2. Date filter actually being applied (check logs)
3. Percentage of changed rows (< 10% for noticeable improvement)
4. Batch size appropriate for table width

---

## Sign-Off

**Tester:** _________________
**Date:** _________________
**Environment:** _________________

**Test Results:**

| Scenario | Status | Notes |
|----------|--------|-------|
| 1. First Sync | ☐ Pass ☐ Fail | |
| 2. Incremental Sync | ☐ Pass ☐ Fail | |
| 3. NULL Handling | ☐ Pass ☐ Fail | |
| 4. Fallback (No Date Column) | ☐ Pass ☐ Fail | |
| 5. Failed Sync Recovery | ☐ Pass ☐ Fail | |
| 6. SQL Injection Prevention | ☐ Pass ☐ Fail | |
| 7. Performance (Optional) | ☐ Pass ☐ Fail ☐ Skipped | |

**Overall Result:** ☐ Pass ☐ Fail

**Comments:**

_______________________________________

_______________________________________

_______________________________________
