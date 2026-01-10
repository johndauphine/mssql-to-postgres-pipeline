# Date-Based Incremental Loading Test Plan - Summary

**Date:** 2026-01-10
**Feature:** PR #45 - Date-Based Incremental Loading
**Status:** Unit Tests Complete, Integration Tests Pending

---

## Quick Links

- **Detailed Test Plan:** [TEST_PLAN_DATE_BASED_INCREMENTAL.md](./TEST_PLAN_DATE_BASED_INCREMENTAL.md)
- **Integration Test Templates:** [INTEGRATION_TEST_TEMPLATES.md](./INTEGRATION_TEST_TEMPLATES.md)
- **Unit Tests:** `/home/johnd/repos/mssql-to-postgres-pipeline/tests/plugins/test_date_based_incremental.py`

---

## Overview

The date-based incremental loading feature enables efficient synchronization by filtering rows based on a timestamp column. This test plan ensures the feature is secure, correct, and performant.

### Key Behavior

```
First Sync (T1):
  - Full load: All rows transferred
  - Record sync_start_time as last_sync_timestamp

Second Sync (T2):
  - Filter: WHERE (date_column > T1 OR date_column IS NULL)
  - Only changed rows transferred
  - Update last_sync_timestamp to T2
```

---

## Test Coverage Status

### ✅ Complete (Unit Tests)

1. **Date Column Validation** - 8 tests
   - Valid temporal types (datetime, datetime2, datetimeoffset, date, smalldatetime)
   - Invalid types (varchar, int)
   - Missing columns
   - Parameterized queries (SQL injection prevention)

2. **State Management** - 5 tests
   - `get_last_sync_timestamp()` behavior
   - `update_sync_timestamp()` persistence
   - Status-based filtering (only 'completed' syncs)

3. **WHERE Clause Generation** - 4 tests
   - Date filter format
   - NULL handling (OR IS NULL)
   - Parameter ordering
   - Column name escaping

**Total: 17 unit tests passing**

---

### ⏳ Pending (Integration Tests)

1. **Core Functionality** (Priority 1)
   - First sync: Full load behavior
   - Second sync: Incremental filtering
   - NULL date handling
   - Fallback for missing date column
   - Fallback for invalid date column type

2. **Edge Cases** (Priority 2)
   - Timezone handling (UTC storage, DATETIMEOFFSET)
   - Concurrent syncs
   - Failed sync recovery (timestamp not updated)
   - Partitioned tables with date filter
   - All NULL dates in table

3. **Security** (Priority 1 - Critical)
   - SQL injection attempts (malicious column names)
   - Schema access control validation
   - Error message data leakage

4. **Performance** (Priority 2)
   - Large dataset (10M rows, 1% change)
   - Partitioned table performance
   - Index impact analysis

---

## Implementation Priority

### Phase 1: Critical Path (Week 1)

**Goal:** Validate core functionality and security

1. **Integration Test Infrastructure**
   - Create `BaseDateIncrementalTest` class
   - Set up test database fixtures
   - Implement helper methods

2. **Core Functionality Tests** (3 tests)
   - `test_first_sync_full_load_with_date_column`
   - `test_second_sync_only_transfers_new_rows`
   - `test_null_dates_always_included`

3. **Security Tests** (2 tests)
   - `test_malicious_date_column_name_parameterized_query`
   - `test_bracket_injection_in_column_name`

**Success Criteria:**
- All 5 integration tests passing
- No SQL injection vulnerabilities found
- Data integrity verified (row counts match)

---

### Phase 2: Edge Cases (Week 2)

**Goal:** Handle corner cases and failure scenarios

1. **Fallback Tests** (2 tests)
   - `test_table_without_date_column_fallback`
   - `test_invalid_date_column_type_fallback`

2. **Timezone Tests** (2 tests)
   - `test_utc_timezone_storage`
   - `test_datetimeoffset_comparison`

3. **Failure Recovery** (1 test)
   - `test_failed_sync_timestamp_not_updated`

**Success Criteria:**
- Graceful degradation when date column missing/invalid
- Timezone handling correct
- Failed syncs don't advance timestamp

---

### Phase 3: Performance (Week 3)

**Goal:** Validate performance improvements

1. **Benchmark Tests**
   - Large table incremental sync (measure speedup)
   - Partitioned table with date filter
   - Index impact analysis

2. **Documentation**
   - Record performance metrics
   - Update README with performance characteristics

**Success Criteria:**
- 10x+ speedup for 1% change scenarios
- Partitioning works correctly with date filters
- Metrics documented

---

## Running Tests

### Unit Tests Only
```bash
docker exec airflow-scheduler pytest /opt/airflow/tests/plugins/test_date_based_incremental.py -v
```

### Integration Tests (Once Implemented)
```bash
# All integration tests
docker exec airflow-scheduler pytest /opt/airflow/tests/integration/test_date_incremental_*.py -v

# Specific test
docker exec airflow-scheduler pytest /opt/airflow/tests/integration/test_date_incremental_first_sync.py::TestFirstSyncDateIncremental::test_first_sync_full_load_with_date_column -v
```

### Security Tests
```bash
docker exec airflow-scheduler pytest /opt/airflow/tests/security/test_sql_injection_date_column.py -v
```

### Performance Tests (Slow, Run Separately)
```bash
docker exec airflow-scheduler pytest /opt/airflow/tests/performance/ -v -m performance
```

---

## Test Data Requirements

### Minimal Test Dataset (For Integration Tests)

**Table:** `TestIncrementalDates`
- **Row count:** 1,000 - 10,000 rows
- **Columns:** id (PK), name, value, updated_at, created_at
- **Date distribution:**
  - 90% with valid dates spread across 30 days
  - 10% with NULL dates
  - Include edge cases (far past, far future)

### Large Test Dataset (For Performance Tests)

**Table:** `TestLargeTable`
- **Row count:** 1,000,000+ rows
- **Indexed:** updated_at column should have index
- **Purpose:** Measure performance improvement from date filtering

---

## Security Validation Checklist

Before merging to production, verify:

- [ ] All date column names validated before use
- [ ] Parameterized queries used (no string concatenation)
- [ ] Bracket escaping prevents SQL injection via identifiers
- [ ] No sensitive data in error messages or logs
- [ ] `_migration` schema has restricted access
- [ ] State table write access controlled
- [ ] SQL injection tests pass (malicious column names fail safely)

---

## Known Test Gaps

1. **Multi-schema Testing**
   - Test with multiple source schemas simultaneously
   - Verify target schema derivation correct

2. **DAG Parameter Override**
   - Test that DAG param overrides env var
   - Test empty string vs unset behavior

3. **Concurrent Modifications**
   - Test rows modified during sync (between start and upsert)
   - Verify timestamp capture is correct

4. **State Corruption Recovery**
   - Test orphaned 'running' state cleanup
   - Test invalid timestamp values

5. **All Temporal Types**
   - Comprehensive test of all 5 temporal types
   - Verify datetime2 precision handling

---

## Next Steps

### For Developer

1. **Review Test Plan**
   - Read detailed test plan: `TEST_PLAN_DATE_BASED_INCREMENTAL.md`
   - Understand test scenarios and expected results

2. **Implement Infrastructure**
   - Create `tests/integration/base_date_incremental_test.py`
   - Copy base test class from `INTEGRATION_TEST_TEMPLATES.md`
   - Set up test database fixtures

3. **Implement Priority 1 Tests**
   - First sync test
   - Incremental sync test
   - SQL injection tests

4. **Execute and Validate**
   - Run tests, verify they pass
   - Document any issues found
   - Update test plan with results

### For QA/Reviewer

1. **Manual Testing**
   - Follow smoke test checklist in test plan
   - Execute end-to-end scenarios
   - Verify logs and state table entries

2. **Security Review**
   - Attempt SQL injection manually
   - Review query construction in code
   - Verify parameterized queries used everywhere

3. **Performance Validation**
   - Measure sync time on largest table
   - Compare full sync vs incremental sync
   - Document performance characteristics

---

## Success Criteria for Production Release

### Functional Requirements
- ✅ Unit tests: 100% passing (17/17)
- ⏳ Integration tests: ≥ 8 core tests passing
- ⏳ Edge case tests: ≥ 5 tests passing
- ⏳ End-to-end workflow tested

### Security Requirements
- ⏳ SQL injection tests: All passing
- ⏳ Security review: Approved
- ⏳ Access control: Validated

### Performance Requirements
- ⏳ Large dataset tested (≥ 1M rows)
- ⏳ Performance improvement documented (≥ 10x for 1% change)
- ⏳ Partitioning works with date filter

### Documentation Requirements
- ✅ Test plan documented
- ✅ Integration test templates created
- ⏳ Performance metrics recorded
- ⏳ README updated with feature usage

---

## Appendix: Test File Structure

```
tests/
├── plugins/
│   └── test_date_based_incremental.py          ✅ Unit tests (17 tests)
├── integration/
│   ├── base_date_incremental_test.py           ⏳ Base test class
│   ├── test_date_incremental_first_sync.py     ⏳ First sync tests
│   ├── test_date_incremental_subsequent_sync.py ⏳ Incremental tests
│   ├── test_date_incremental_null_handling.py  ⏳ NULL handling tests
│   ├── test_date_incremental_fallback.py       ⏳ Fallback tests
│   ├── test_date_incremental_partitioned.py    ⏳ Partition tests
│   └── test_date_incremental_failure_recovery.py ⏳ Failure tests
├── security/
│   └── test_sql_injection_date_column.py       ⏳ Security tests
├── performance/
│   ├── test_large_dataset_performance.py       ⏳ Performance tests
│   └── test_partition_performance.py           ⏳ Partition perf tests
└── fixtures/
    └── test_data_generator.py                  ⏳ Test data utilities
```

---

## Contact and Support

For questions about this test plan:
- **Developer:** Check implementation in PR #45
- **Test Templates:** See `INTEGRATION_TEST_TEMPLATES.md`
- **Detailed Scenarios:** See `TEST_PLAN_DATE_BASED_INCREMENTAL.md`

---

**Last Updated:** 2026-01-10
**Status:** Ready for integration test implementation
