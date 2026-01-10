# Test Plan: Date-Based Incremental Loading Feature

**Feature:** PR #45 - Date-Based Incremental Loading
**Status:** Unit Tests Complete, Integration Tests Needed
**Date:** 2026-01-10

## 1. Executive Summary

This test plan covers comprehensive testing for the date-based incremental loading feature that adds `DATE_UPDATED_FIELD` environment variable support to enable efficient incremental synchronization based on a timestamp column.

### Feature Behavior Summary

- **When configured:** Only rows where `date_column > last_sync_timestamp` OR `date_column IS NULL` are transferred
- **First sync:** Performs full load and records `sync_start_time` as `last_sync_timestamp`
- **Subsequent syncs:** Filters by `last_sync_timestamp` from previous successful sync
- **Missing column:** Tables without the date column fall back to full sync with warning
- **Failed syncs:** Do not update `last_sync_timestamp` (retry will re-sync from last successful timestamp)

---

## 2. Test Coverage Overview

### 2.1 Current Coverage (Unit Tests)

The following are already covered by `/home/johnd/repos/mssql-to-postgres-pipeline/tests/plugins/test_date_based_incremental.py`:

#### `get_date_column_info()` Tests
- ✅ Returns column info for valid datetime types (datetime, datetime2, datetimeoffset)
- ✅ Returns None for missing column
- ✅ Returns None for non-temporal types (varchar, int)
- ✅ Uses parameterized queries (SQL injection prevention)
- ✅ Returns None on exception

#### `IncrementalStateManager` Tests
- ✅ `get_last_sync_timestamp()` returns timestamp for completed sync
- ✅ Returns None for missing state record
- ✅ Returns None for failed sync (status != 'completed')
- ✅ Returns None for NULL timestamp (first sync)
- ✅ `update_sync_timestamp()` persists timestamp correctly

#### WHERE Clause Tests
- ✅ Date filter format validation
- ✅ Parameter ordering (where_params before pk pagination param)
- ✅ Column name escaping (bracket doubling)
- ✅ Valid temporal types validation

### 2.2 Gaps Requiring Additional Testing

1. **Integration Tests** - Testing with actual MSSQL and PostgreSQL databases
2. **End-to-End Scenarios** - Full DAG execution with various table states
3. **Edge Cases** - Timezone handling, NULL dates, concurrent syncs
4. **Security Validation** - SQL injection attempts, privilege escalation
5. **Performance Testing** - Large datasets, partition handling with date filters
6. **Failure Recovery** - Mid-sync failures, state recovery

---

## 3. Integration Test Suite

### 3.1 Environment Setup

**Prerequisites:**
- Docker Compose running (`docker-compose up -d`)
- MSSQL container with test database
- PostgreSQL target container
- Test tables with known data patterns

**Test Data Setup:**
```sql
-- MSSQL Test Table
CREATE TABLE dbo.TestIncrementalDates (
    id INT PRIMARY KEY,
    name NVARCHAR(100),
    value INT,
    updated_at DATETIME2,
    created_at DATETIME2
);

-- Insert baseline data (100 rows, dates from 2024-01-01 to 2024-01-10)
-- Insert rows with NULL updated_at
-- Insert rows with various datetime types
```

### 3.2 Integration Test Cases

#### Test 3.2.1: First Sync - Full Load
**Objective:** Verify first sync loads all data and records timestamp

**Setup:**
- Clean target table
- Clean state table
- Source has 1000 rows with `updated_at` between 2024-01-01 and 2024-01-10
- 10 rows have NULL `updated_at`

**Execution:**
```bash
docker exec airflow-scheduler pytest /opt/airflow/tests/integration/test_date_incremental_first_sync.py -v
```

**Expected Results:**
1. All 1000 rows transferred (including 10 NULL rows)
2. `_migration._migration_state` record created with:
   - `sync_status = 'completed'`
   - `last_sync_timestamp` set to sync start time (before query execution)
   - `rows_inserted = 1000`
3. Target table count matches source
4. Logs show: "Date-based incremental: no previous sync timestamp, doing full sync"

**SQL Validation:**
```sql
-- Verify state record
SELECT table_name, sync_status, last_sync_timestamp, rows_inserted
FROM _migration._migration_state
WHERE table_name = 'TestIncrementalDates';

-- Verify row count
SELECT COUNT(*) FROM mssql__testdb__dbo.TestIncrementalDates;

-- Verify NULL rows transferred
SELECT COUNT(*) FROM mssql__testdb__dbo.TestIncrementalDates WHERE updated_at IS NULL;
```

---

#### Test 3.2.2: Second Sync - Incremental with New Rows
**Objective:** Verify subsequent sync only transfers new/modified rows

**Setup:**
- First sync completed (timestamp = T1)
- Insert 50 new rows with `updated_at > T1`
- Update 20 existing rows, set `updated_at > T1`
- Insert 5 rows with `updated_at < T1` (should be skipped)
- Insert 3 rows with NULL `updated_at` (should be included)

**Execution:**
```bash
# Run second sync
docker exec airflow-scheduler airflow dags trigger mssql_to_postgres_incremental \
  --conf '{"date_updated_field": "updated_at"}'
```

**Expected Results:**
1. 78 rows transferred (50 new + 20 updated + 5 old skipped + 3 NULL)
2. Wait - correction: 73 rows transferred (50 new + 20 updated + 3 NULL)
3. 5 rows with old timestamps skipped
4. State updated with new `last_sync_timestamp = T2`
5. `rows_inserted = 53`, `rows_updated = 20`
6. Logs show: "Date-based incremental: syncing rows where updated_at > {T1}"

**SQL Validation:**
```sql
-- Verify new state timestamp
SELECT last_sync_timestamp FROM _migration._migration_state
WHERE table_name = 'TestIncrementalDates';

-- Verify row counts
SELECT COUNT(*) FROM mssql__testdb__dbo.TestIncrementalDates;

-- Verify old rows were skipped (not in staging)
-- This requires query log analysis or debug logging
```

---

#### Test 3.2.3: NULL Date Handling
**Objective:** Verify rows with NULL dates are always included

**Setup:**
- Completed sync at T1
- Insert 10 rows with NULL `updated_at`
- Do NOT modify any other rows

**Expected Results:**
1. 10 rows transferred (all NULL)
2. `rows_inserted = 10`, `rows_updated = 0`
3. WHERE clause includes: `(updated_at > ? OR updated_at IS NULL)`
4. NULL rows successfully inserted

**Security Note:** Verify NULL values are properly handled in parameterized queries (no SQL injection via NULL)

---

#### Test 3.2.4: Table Without Date Column - Fallback
**Objective:** Verify graceful fallback when date column doesn't exist

**Setup:**
- Table `TestNoDateColumn` has no `updated_at` column
- `DATE_UPDATED_FIELD=updated_at` configured

**Expected Results:**
1. Warning logged: "date column 'updated_at' not found, falling back to full sync"
2. Full sync performed (all rows transferred)
3. No date filter applied
4. `has_date_column = False` in table_info
5. Sync completes successfully

---

#### Test 3.2.5: Invalid Date Column Type - Fallback
**Objective:** Verify fallback when column exists but is not temporal type

**Setup:**
- Table has column `updated_at` of type VARCHAR
- `DATE_UPDATED_FIELD=updated_at` configured

**Expected Results:**
1. Warning logged: "Column 'updated_at' is type 'varchar', not a valid temporal type"
2. Full sync performed (no date filter)
3. Sync completes successfully

---

#### Test 3.2.6: Multiple Temporal Types Support
**Objective:** Verify all SQL Server temporal types work correctly

**Test Tables:**
- `TestDatetime` (updated_at DATETIME)
- `TestDatetime2` (updated_at DATETIME2)
- `TestDatetimeoffset` (updated_at DATETIMEOFFSET)
- `TestDate` (updated_at DATE)
- `TestSmalldatetime` (updated_at SMALLDATETIME)

**Expected Results:**
1. All types detected as valid temporal types
2. Date filters work correctly for each type
3. Timezone handling correct (especially for DATETIMEOFFSET)

---

#### Test 3.2.7: Failed Sync - Timestamp Not Updated
**Objective:** Verify failed sync does not advance timestamp

**Setup:**
- Successful sync at T1
- Insert rows with `updated_at > T1`
- Trigger sync but force failure (drop target table mid-sync, simulate network error)

**Expected Results:**
1. Sync fails
2. State record has `sync_status = 'failed'`
3. `last_sync_timestamp` remains T1 (not updated to T2)
4. Next retry will re-attempt all rows with `updated_at > T1`

---

#### Test 3.2.8: Partitioned Table with Date Filter
**Objective:** Verify partitioning works correctly with date-based filtering

**Setup:**
- Large table (>1M rows) triggers partitioning
- Has valid `updated_at` column
- Previous sync at T1
- Insert 10K rows with `updated_at > T1` spread across multiple partitions

**Expected Results:**
1. Table partitioned into N partitions
2. Each partition applies date filter correctly
3. Only partitions with matching rows do work
4. All 10K rows transferred across partitions
5. Partition state tracking correct

---

## 4. End-to-End Scenarios

### 4.1 E2E Test: Full DAG Execution

**Scenario:** Run complete incremental DAG with date filtering

**Steps:**
1. Start with clean state
2. Configure `DATE_UPDATED_FIELD=updated_at`
3. Set `include_tables=['dbo.Users', 'dbo.Posts']`
4. Trigger DAG
5. Wait for completion
6. Verify all tables synced correctly

**Validation:**
- All tables with date column use date filtering
- Tables without date column fall back to full sync
- State table records correct for all tables
- No errors in logs

---

### 4.2 E2E Test: Multi-Sync Workflow

**Scenario:** Simulate real-world incremental sync workflow

**Steps:**
1. **Day 1 (T1):** Full sync, 100K rows
2. **Day 2 (T2):** Incremental, +5K rows modified
3. **Day 3 (T3):** Incremental, +2K rows modified
4. **Day 4 (T4):** Incremental, no changes
5. **Day 5 (T5):** Incremental, +10K rows modified

**Validation:**
- Each sync transfers only changed rows
- Timestamps advance correctly
- No duplicate inserts
- Row counts match source
- Performance improves (fewer rows transferred)

---

### 4.3 E2E Test: Resume After Failure

**Scenario:** Sync fails mid-transfer, verify resume behavior

**Steps:**
1. Start incremental sync with 50K rows to transfer
2. Kill DAG mid-sync (after 20K rows in staging)
3. Restart DAG

**Expected:**
- Resume point detection works
- Remaining rows transferred
- Final state correct
- Timestamp updated only after successful completion

---

## 5. Edge Cases and Corner Cases

### 5.1 Timezone Handling

**Test Cases:**

#### Test 5.1.1: UTC Timezone Storage
**Verify:** `last_sync_timestamp` stored as UTC in PostgreSQL

**Validation:**
```sql
SELECT last_sync_timestamp,
       EXTRACT(TIMEZONE FROM last_sync_timestamp) as tz_offset
FROM _migration._migration_state;
-- Should show timezone = UTC
```

#### Test 5.1.2: MSSQL DATETIMEOFFSET Comparison
**Setup:** Source has DATETIMEOFFSET column with various timezones

**Expected:** Comparison works correctly accounting for timezone differences

#### Test 5.1.3: Daylight Saving Time Transitions
**Setup:** Sync during DST transition

**Expected:** Timestamp comparison handles DST correctly (no rows missed or duplicated)

---

### 5.2 Concurrent Syncs

#### Test 5.2.1: Overlapping Syncs
**Setup:** Two DAG runs execute simultaneously (should not happen with `max_active_runs=1`, but test anyway)

**Expected:**
- State locking prevents corruption
- One sync waits for other to complete
- No data loss or duplication

---

### 5.3 Data Type Edge Cases

#### Test 5.3.1: Microsecond Precision
**Setup:** `updated_at` has microsecond precision

**Expected:** No precision loss, correct filtering

#### Test 5.3.2: Far Future Dates
**Setup:** Rows with `updated_at = '9999-12-31'`

**Expected:** Handled correctly, no overflow errors

#### Test 5.3.3: Min Date Values
**Setup:** Rows with `updated_at = '1753-01-01'` (SQL Server min datetime)

**Expected:** Handled correctly

---

### 5.4 NULL Handling Edge Cases

#### Test 5.4.1: All Rows Have NULL Dates
**Setup:** Table where all rows have `updated_at IS NULL`

**Expected:**
- All rows transferred every sync
- Warning logged about poor incremental efficiency
- No errors

#### Test 5.4.2: NULL to Non-NULL Update
**Setup:** Update row from `updated_at = NULL` to `updated_at = T2`

**Expected:**
- Row transferred in both syncs (NULL in first, dated in second)
- No duplication (upsert handles it)

---

## 6. Security Testing

### 6.1 SQL Injection Prevention

#### Test 6.1.1: Malicious Date Column Name
**Attack Vector:** `DATE_UPDATED_FIELD="updated_at'; DROP TABLE Users; --"`

**Expected:**
- Parameterized query prevents injection
- Column name validation fails
- Query returns 0 results (column not found)
- No table dropped
- Error logged

**Verification:**
```python
# Verify query uses parameters, not string concatenation
assert "?" in query
assert date_column not in query  # Not directly concatenated
```

#### Test 6.1.2: Bracket Injection in Column Name
**Attack Vector:** `DATE_UPDATED_FIELD="updated_at]]; DROP TABLE Users; --"`

**Expected:**
- Bracket escaping (`]` → `]]`) prevents injection
- Query uses `[updated_at]]]]; DROP TABLE Users; --]`
- No table dropped

#### Test 6.1.3: Unicode/Special Characters in Column Name
**Attack Vector:** `DATE_UPDATED_FIELD="updated_at\x00DROP"`

**Expected:**
- Column name sanitized
- Query fails safely
- No SQL execution

---

### 6.2 Privilege Escalation

#### Test 6.2.1: Schema Access Control
**Verify:** `_migration` schema has restricted access

**Validation:**
```sql
-- As non-owner user, verify cannot access _migration schema
SET ROLE readonly_user;
SELECT * FROM _migration._migration_state;
-- Should fail with permission denied
```

#### Test 6.2.2: State Table Write Protection
**Verify:** Only migration user can write to state table

---

### 6.3 Data Leakage

#### Test 6.3.1: Error Messages Don't Leak Sensitive Data
**Setup:** Trigger error with sensitive column data

**Expected:**
- Error messages truncated appropriately
- No full SQL statements in logs (could contain sensitive data)
- PII not logged

---

## 7. Performance Testing

### 7.1 Large Dataset Performance

#### Test 7.1.1: 10M Row Table - First Sync
**Objective:** Verify performance of first sync on large table

**Setup:**
- Table with 10M rows
- Valid `updated_at` column

**Metrics:**
- Total transfer time
- Rows per second
- Memory usage
- CPU usage

**Baseline:** Should complete within reasonable time (e.g., < 30 minutes for 10M rows)

#### Test 7.1.2: 10M Row Table - Incremental Sync (1% Change)
**Objective:** Verify date filter improves performance

**Setup:**
- 10M row table, previous sync at T1
- 100K rows (1%) have `updated_at > T1`

**Expected:**
- Transfer time ~1/100th of full sync
- Only 100K rows read from source (verify with query logs)
- Significant performance improvement

**Verification:**
```sql
-- MSSQL Query Store or Extended Events to verify filtered reads
SELECT query_text, execution_count, total_rows
FROM sys.dm_exec_query_stats
WHERE query_text LIKE '%updated_at%';
```

---

### 7.2 Partition Performance with Date Filter

#### Test 7.2.1: Partitioned Table - Even Distribution
**Setup:**
- 5M row table partitioned into 6 partitions
- Changed rows evenly distributed across partitions
- Date filter enabled

**Expected:**
- All partitions execute in parallel
- Date filter applied in each partition
- Total time ~1/6th of single-partition time

#### Test 7.2.2: Partitioned Table - Skewed Distribution
**Setup:**
- 5M row table partitioned into 6 partitions
- All changed rows in one partition

**Expected:**
- 5 partitions complete quickly (no matching rows)
- 1 partition does all the work
- Overall time reasonable

---

### 7.3 Index Impact

#### Test 7.3.1: Index on Date Column
**Setup:** Create index on `updated_at` column in MSSQL

**Expected:**
- Query optimizer uses index for date filter
- Significant performance improvement
- Verify with execution plan

**Validation:**
```sql
SET SHOWPLAN_ALL ON;
SELECT * FROM dbo.TestTable WHERE updated_at > '2024-01-01';
-- Verify Index Seek on updated_at index
```

#### Test 7.3.2: No Index on Date Column
**Setup:** No index on `updated_at`

**Expected:**
- Query still works (table scan)
- Performance degraded vs indexed
- Still faster than full sync if changes < 10% of table

---

## 8. Failure Recovery and Resilience

### 8.1 Database Failures

#### Test 8.1.1: MSSQL Connection Lost During Sync
**Simulation:** Kill MSSQL connection mid-transfer

**Expected:**
- Error logged
- Sync marked as failed
- `last_sync_timestamp` NOT updated
- Retry succeeds

#### Test 8.1.2: PostgreSQL Connection Lost During Upsert
**Simulation:** Kill PostgreSQL connection during upsert from staging

**Expected:**
- Transaction rolled back
- Staging table cleaned up
- State remains consistent
- Retry succeeds

---

### 8.2 State Corruption Recovery

#### Test 8.2.1: Invalid Timestamp in State Table
**Setup:** Manually set `last_sync_timestamp` to invalid value (e.g., NULL, far future)

**Expected:**
- DAG detects invalid state
- Falls back to full sync or fails safely
- Operator alerted

#### Test 8.2.2: Orphaned Running State
**Setup:** State record stuck in 'running' from crashed DAG

**Expected:**
- Next sync detects stale state
- Resets to 'failed' or 'pending'
- Continues successfully

---

### 8.3 Data Consistency Validation

#### Test 8.3.1: Row Count Reconciliation
**After each sync:**
```sql
-- Verify source and target counts match
SELECT
    (SELECT COUNT(*) FROM mssql_source.dbo.Users) as source_count,
    (SELECT COUNT(*) FROM postgres_target.mssql__testdb__dbo.users) as target_count;
```

#### Test 8.3.2: Data Hash Comparison
**Periodic validation:**
```sql
-- Compare hashes of sample rows to ensure data integrity
SELECT id, MD5(CONCAT(name, value, updated_at)) as row_hash
FROM mssql_source.dbo.Users
WHERE id IN (1, 100, 1000)
ORDER BY id;

-- Compare with target
SELECT id, MD5(CONCAT(name, value, updated_at)) as row_hash
FROM postgres_target.mssql__testdb__dbo.users
WHERE id IN (1, 100, 1000)
ORDER BY id;
```

---

## 9. Configuration and Usability Testing

### 9.1 Environment Variable Configuration

#### Test 9.1.1: `DATE_UPDATED_FIELD` Set
**Config:** `DATE_UPDATED_FIELD=updated_at`

**Expected:** Feature enabled, date filtering applied

#### Test 9.1.2: `DATE_UPDATED_FIELD` Empty
**Config:** `DATE_UPDATED_FIELD=""`

**Expected:** Feature disabled, full sync behavior

#### Test 9.1.3: `DATE_UPDATED_FIELD` Unset
**Config:** Variable not set

**Expected:** Feature disabled (defaults to empty string)

---

### 9.2 DAG Parameter Override

#### Test 9.2.1: DAG Param Overrides Env Var
**Setup:**
- `DATE_UPDATED_FIELD=updated_at` in env
- Trigger DAG with `{"date_updated_field": "modified_date"}`

**Expected:** DAG param takes precedence, uses `modified_date`

---

### 9.3 Logging and Observability

#### Test 9.3.1: Log Messages Clarity
**Verify logs contain:**
- "Date-based incremental: syncing rows where {column} > {timestamp}"
- "Date column '{name}' found (type: {type})"
- "No previous sync timestamp, doing full sync"
- "Updated sync timestamp to {time} for next date-based sync"

#### Test 9.3.2: Metrics Collection
**Verify state table records:**
- `sync_duration_seconds`
- `rows_inserted`, `rows_updated`, `rows_unchanged`
- `last_sync_timestamp`

---

## 10. Automated Test Implementation

### 10.1 Integration Test File Structure

```
tests/
├── integration/
│   ├── test_date_incremental_first_sync.py
│   ├── test_date_incremental_subsequent_sync.py
│   ├── test_date_incremental_null_handling.py
│   ├── test_date_incremental_fallback.py
│   ├── test_date_incremental_partitioned.py
│   └── test_date_incremental_failure_recovery.py
├── security/
│   ├── test_sql_injection_date_column.py
│   └── test_state_table_permissions.py
└── performance/
    ├── test_large_dataset_performance.py
    └── test_partition_performance.py
```

### 10.2 Test Fixtures

**Common fixtures needed:**

```python
@pytest.fixture
def mssql_test_table():
    """Create and populate test table in MSSQL."""
    # Setup
    conn = connect_mssql()
    create_test_table(conn)
    insert_test_data(conn)
    yield 'TestIncrementalDates'
    # Teardown
    drop_test_table(conn)

@pytest.fixture
def clean_state():
    """Clean migration state table."""
    conn = connect_postgres()
    conn.execute("DELETE FROM _migration._migration_state WHERE table_name LIKE 'Test%'")
    conn.commit()
```

---

## 11. Manual Test Scenarios

### 11.1 Smoke Test Checklist

**Tester:** _______________
**Date:** _______________
**Environment:** _______________

- [ ] Fresh install, first sync works
- [ ] Second sync only transfers new rows
- [ ] Logs show correct date filtering
- [ ] State table updated correctly
- [ ] NULL dates handled
- [ ] Fallback works for tables without date column
- [ ] Failed sync does not update timestamp
- [ ] Partitioned tables work with date filter

### 11.2 Production Readiness Checklist

- [ ] Performance acceptable on largest table (measure: _______)
- [ ] Security review passed (SQL injection tests)
- [ ] Error handling graceful (no crashes)
- [ ] Logging adequate for troubleshooting
- [ ] Documentation complete
- [ ] Monitoring/alerting configured
- [ ] Rollback plan tested

---

## 12. Test Data Scenarios

### 12.1 Test Data Set 1: Balanced Distribution

```sql
-- 1000 rows
-- 100 rows per day for 10 days
-- 10% NULL dates
-- Even distribution across PK range
```

### 12.2 Test Data Set 2: Skewed Distribution

```sql
-- 1000 rows
-- 900 rows on day 1
-- 10 rows per day for days 2-10
-- Tests performance when most changes recent
```

### 12.3 Test Data Set 3: Edge Cases

```sql
-- Include:
-- - Far future dates (9999-12-31)
-- - Far past dates (1753-01-01)
-- - Microsecond precision
-- - All timezones (for DATETIMEOFFSET)
-- - Duplicate timestamps
-- - NULL values
```

---

## 13. Regression Testing

After any code changes to date-based incremental feature:

1. **Run full unit test suite:**
   ```bash
   docker exec airflow-scheduler pytest /opt/airflow/tests/plugins/test_date_based_incremental.py -v
   ```

2. **Run integration tests:**
   ```bash
   docker exec airflow-scheduler pytest /opt/airflow/tests/integration/test_date_incremental_*.py -v
   ```

3. **Run smoke test:**
   - Trigger DAG with known dataset
   - Verify row counts match expected
   - Check logs for errors

4. **Performance regression:**
   - Compare sync time to baseline
   - Alert if > 20% degradation

---

## 14. Test Metrics and Success Criteria

### 14.1 Coverage Metrics

- **Unit Test Coverage:** ≥ 90% of date-based code paths
- **Integration Test Coverage:** All major scenarios (minimum 8 tests)
- **Security Test Coverage:** All SQL injection vectors tested

### 14.2 Performance Criteria

- **First sync (1M rows):** < 5 minutes
- **Incremental sync (1% change):** < 30 seconds
- **Incremental sync (10% change):** < 2 minutes
- **Partitioned sync (5M rows, 6 partitions):** < 8 minutes

### 14.3 Reliability Criteria

- **Zero data loss:** All source rows present in target
- **Zero data corruption:** Hashes match for sample rows
- **State consistency:** No orphaned or invalid state records
- **Idempotency:** Re-running sync produces same result

---

## 15. Test Execution Schedule

### Phase 1: Unit Tests (Completed)
- ✅ All unit tests passing
- ✅ 17 test cases implemented

### Phase 2: Integration Tests (Week 1)
- [ ] Set up test database infrastructure
- [ ] Implement first sync test
- [ ] Implement incremental sync test
- [ ] Implement NULL handling test
- [ ] Implement fallback test

### Phase 3: Edge Cases and Security (Week 2)
- [ ] Timezone tests
- [ ] SQL injection tests
- [ ] Concurrent sync tests
- [ ] Failure recovery tests

### Phase 4: Performance Testing (Week 3)
- [ ] Large dataset tests (10M rows)
- [ ] Partition performance tests
- [ ] Index impact analysis
- [ ] Benchmark results documentation

### Phase 5: Manual Testing and UAT (Week 4)
- [ ] Smoke testing in dev environment
- [ ] End-to-end workflow testing
- [ ] Production readiness review
- [ ] Documentation review

---

## 16. Known Limitations and Future Enhancements

### Current Limitations

1. **Single column filtering:** Only supports one date column per table (not multiple)
2. **Temporal types only:** Does not support integer epoch timestamps
3. **No custom filter logic:** Cannot combine date filter with other WHERE conditions
4. **Case-sensitive column names:** Must match exact case from schema

### Future Enhancement Ideas

1. Support for composite date filters (e.g., `created_at OR updated_at`)
2. Support for integer epoch timestamps
3. Custom WHERE clause injection for advanced filtering
4. Automatic index suggestion for date columns
5. Date column auto-discovery (use most recent datetime column)

---

## 17. Appendix A: SQL Queries for Manual Validation

### A.1 Verify Date Filter Correct

```sql
-- MSSQL: Count rows that should be transferred
DECLARE @LastSyncTimestamp DATETIME2 = '2024-01-15 10:30:00';

SELECT COUNT(*) as rows_to_transfer
FROM dbo.Users
WHERE updated_at > @LastSyncTimestamp OR updated_at IS NULL;
```

### A.2 Verify State Table Correct

```sql
-- PostgreSQL: Check state record
SELECT
    table_name,
    sync_status,
    last_sync_timestamp,
    rows_inserted,
    rows_updated,
    sync_duration_seconds
FROM _migration._migration_state
WHERE table_name = 'Users'
ORDER BY last_sync_end DESC
LIMIT 1;
```

### A.3 Verify Data Consistency

```sql
-- Compare source and target counts
-- MSSQL
SELECT COUNT(*) as source_count FROM dbo.Users;

-- PostgreSQL
SELECT COUNT(*) as target_count FROM mssql__testdb__dbo.users;
```

---

## 18. Appendix B: Test Environment Configuration

### B.1 Docker Compose Test Services

```yaml
# Add to docker-compose.yml for testing
services:
  test-mssql:
    image: mcr.microsoft.com/mssql/server:2022-latest
    environment:
      - ACCEPT_EULA=Y
      - SA_PASSWORD=TestPassword123!
      - MSSQL_DB=TestDatabase
    ports:
      - "1434:1433"

  test-postgres:
    image: postgres:15
    environment:
      - POSTGRES_PASSWORD=TestPassword123!
      - POSTGRES_DB=testdb
    ports:
      - "5434:5432"
```

### B.2 Test Connection Configuration

```python
# conftest.py
@pytest.fixture(scope="session")
def test_mssql_conn():
    return "mssql://sa:TestPassword123!@localhost:1434/TestDatabase"

@pytest.fixture(scope="session")
def test_postgres_conn():
    return "postgresql://postgres:TestPassword123!@localhost:5434/testdb"
```

---

## 19. Sign-Off

### Test Plan Approval

| Role | Name | Signature | Date |
|------|------|-----------|------|
| Developer | __________ | __________ | ______ |
| QA Lead | __________ | __________ | ______ |
| Tech Lead | __________ | __________ | ______ |

### Test Execution Sign-Off

| Phase | Status | Tester | Date | Notes |
|-------|--------|--------|------|-------|
| Unit Tests | ✅ Pass | | | 17/17 tests passing |
| Integration Tests | ⏳ Pending | | | |
| Security Tests | ⏳ Pending | | | |
| Performance Tests | ⏳ Pending | | | |
| UAT | ⏳ Pending | | | |

---

**Document Version:** 1.0
**Last Updated:** 2026-01-10
**Next Review:** After integration test completion
