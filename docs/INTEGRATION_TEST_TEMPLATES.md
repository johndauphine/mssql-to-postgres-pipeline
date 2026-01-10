# Integration Test Templates for Date-Based Incremental Loading

This document provides code templates and implementation guidance for creating integration tests based on the test plan.

---

## 1. Test Infrastructure Setup

### 1.1 Base Test Class

```python
"""
Base integration test class for date-based incremental loading tests.

Location: tests/integration/base_date_incremental_test.py
"""

import pytest
import pyodbc
import psycopg2
from datetime import datetime, timezone, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook
from mssql_pg_migration.odbc_helper import OdbcConnectionHelper


class BaseDateIncrementalTest:
    """Base class for date-based incremental integration tests."""

    @pytest.fixture(scope="class")
    def mssql_conn_id(self):
        """SQL Server connection ID for testing."""
        return "mssql_source"

    @pytest.fixture(scope="class")
    def postgres_conn_id(self):
        """PostgreSQL connection ID for testing."""
        return "postgres_target"

    @pytest.fixture(scope="class")
    def test_schema(self):
        """Test schema name in MSSQL."""
        return "dbo"

    @pytest.fixture(scope="class")
    def test_table_name(self):
        """Test table name."""
        return "TestIncrementalDates"

    @pytest.fixture(scope="class")
    def target_schema(self):
        """Target schema in PostgreSQL."""
        return "mssql__testdb__dbo"

    @pytest.fixture
    def mssql_connection(self, mssql_conn_id):
        """Get MSSQL connection."""
        helper = OdbcConnectionHelper(mssql_conn_id)
        with helper.get_conn() as conn:
            yield conn

    @pytest.fixture
    def postgres_connection(self, postgres_conn_id):
        """Get PostgreSQL connection."""
        hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        conn = hook.get_conn()
        yield conn
        conn.close()

    @pytest.fixture(autouse=True)
    def setup_test_table(self, mssql_connection, postgres_connection, test_schema, test_table_name, target_schema):
        """Create test table before each test, clean up after."""
        # Setup: Create MSSQL test table
        self._create_mssql_test_table(mssql_connection, test_schema, test_table_name)
        # Setup: Create PostgreSQL target table
        self._create_postgres_test_table(postgres_connection, target_schema, test_table_name)

        yield

        # Teardown: Clean up tables
        self._drop_mssql_test_table(mssql_connection, test_schema, test_table_name)
        self._drop_postgres_test_table(postgres_connection, target_schema, test_table_name)
        self._clean_state_table(postgres_connection, test_table_name)

    def _create_mssql_test_table(self, conn, schema, table_name):
        """Create test table in MSSQL."""
        cursor = conn.cursor()
        cursor.execute(f"""
            IF OBJECT_ID('[{schema}].[{table_name}]', 'U') IS NOT NULL
                DROP TABLE [{schema}].[{table_name}]
        """)
        cursor.execute(f"""
            CREATE TABLE [{schema}].[{table_name}] (
                id INT PRIMARY KEY,
                name NVARCHAR(100),
                value INT,
                updated_at DATETIME2,
                created_at DATETIME2 DEFAULT GETDATE()
            )
        """)
        conn.commit()
        cursor.close()

    def _create_postgres_test_table(self, conn, schema, table_name):
        """Create test table in PostgreSQL."""
        cursor = conn.cursor()
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        cursor.execute(f"""
            DROP TABLE IF EXISTS {schema}.{table_name}
        """)
        cursor.execute(f"""
            CREATE TABLE {schema}.{table_name} (
                id INTEGER PRIMARY KEY,
                name VARCHAR(100),
                value INTEGER,
                updated_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
        cursor.close()

    def _drop_mssql_test_table(self, conn, schema, table_name):
        """Drop test table from MSSQL."""
        cursor = conn.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS [{schema}].[{table_name}]")
        conn.commit()
        cursor.close()

    def _drop_postgres_test_table(self, conn, schema, table_name):
        """Drop test table from PostgreSQL."""
        cursor = conn.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS {schema}.{table_name}")
        conn.commit()
        cursor.close()

    def _clean_state_table(self, conn, table_name):
        """Clean up state table entries for test table."""
        cursor = conn.cursor()
        cursor.execute(
            "DELETE FROM _migration._migration_state WHERE table_name = %s",
            (table_name,)
        )
        conn.commit()
        cursor.close()

    def insert_test_data_mssql(self, conn, schema, table_name, rows):
        """
        Insert test data into MSSQL table.

        Args:
            conn: MSSQL connection
            schema: Schema name
            table_name: Table name
            rows: List of tuples (id, name, value, updated_at)
        """
        cursor = conn.cursor()
        for row in rows:
            cursor.execute(
                f"INSERT INTO [{schema}].[{table_name}] (id, name, value, updated_at) VALUES (?, ?, ?, ?)",
                row
            )
        conn.commit()
        cursor.close()

    def count_rows_mssql(self, conn, schema, table_name, where_clause=None):
        """Count rows in MSSQL table."""
        cursor = conn.cursor()
        query = f"SELECT COUNT(*) FROM [{schema}].[{table_name}]"
        if where_clause:
            query += f" WHERE {where_clause}"
        cursor.execute(query)
        count = cursor.fetchone()[0]
        cursor.close()
        return count

    def count_rows_postgres(self, conn, schema, table_name, where_clause=None):
        """Count rows in PostgreSQL table."""
        cursor = conn.cursor()
        query = f"SELECT COUNT(*) FROM {schema}.{table_name}"
        if where_clause:
            query += f" WHERE {where_clause}"
        cursor.execute(query)
        count = cursor.fetchone()[0]
        cursor.close()
        return count

    def get_state_record(self, conn, table_name):
        """Get state record for table."""
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT sync_status, last_sync_timestamp, rows_inserted, rows_updated,
                   rows_unchanged, sync_duration_seconds
            FROM _migration._migration_state
            WHERE table_name = %s
            ORDER BY last_sync_end DESC
            LIMIT 1
            """,
            (table_name,)
        )
        result = cursor.fetchone()
        cursor.close()
        return result
```

---

## 2. Test Case Templates

### 2.1 First Sync Test

```python
"""
Integration test: First sync with date-based incremental loading.

Location: tests/integration/test_date_incremental_first_sync.py
"""

import pytest
from datetime import datetime, timezone, timedelta
from .base_date_incremental_test import BaseDateIncrementalTest
from mssql_pg_migration.data_transfer import transfer_incremental_staging
from mssql_pg_migration.incremental_state import IncrementalStateManager


class TestFirstSyncDateIncremental(BaseDateIncrementalTest):
    """Test first sync behavior with date-based incremental loading."""

    def test_first_sync_full_load_with_date_column(
        self,
        mssql_connection,
        postgres_connection,
        mssql_conn_id,
        postgres_conn_id,
        test_schema,
        test_table_name,
        target_schema,
    ):
        """
        Test that first sync does full load and records timestamp.

        Scenario:
        - Source has 1000 rows with various updated_at values
        - 10 rows have NULL updated_at
        - No previous sync (clean state)

        Expected:
        - All 1000 rows transferred
        - State record created with last_sync_timestamp
        - rows_inserted = 1000, rows_updated = 0
        """
        # Arrange: Insert test data
        base_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        test_rows = []

        # 990 rows with dates
        for i in range(990):
            updated_at = base_date + timedelta(days=i % 10, hours=i % 24)
            test_rows.append((i, f"User_{i}", i * 10, updated_at))

        # 10 rows with NULL dates
        for i in range(990, 1000):
            test_rows.append((i, f"User_{i}", i * 10, None))

        self.insert_test_data_mssql(mssql_connection, test_schema, test_table_name, test_rows)

        # Verify source count
        source_count = self.count_rows_mssql(mssql_connection, test_schema, test_table_name)
        assert source_count == 1000, f"Expected 1000 source rows, got {source_count}"

        # Build table_info
        table_info = {
            "table_name": test_table_name,
            "source_schema": test_schema,
            "target_schema": target_schema,
            "columns": ["id", "name", "value", "updated_at", "created_at"],
            "pk_columns": ["id"],
        }

        # Capture sync start time (before sync)
        sync_start_time = datetime.now(timezone.utc)

        # Initialize state manager
        state_manager = IncrementalStateManager(postgres_conn_id)
        state_manager.ensure_state_table_exists()

        # Start sync (no previous timestamp, so full load)
        sync_id = state_manager.start_sync(
            table_name=test_table_name,
            source_schema=test_schema,
            target_schema=target_schema,
            source_row_count=1000,
        )

        # Act: Execute transfer
        result = transfer_incremental_staging(
            mssql_conn_id=mssql_conn_id,
            postgres_conn_id=postgres_conn_id,
            table_info=table_info,
            chunk_size=200,
            date_column="updated_at",
            last_sync_timestamp=None,  # First sync, no previous timestamp
        )

        # Assert: Verify transfer success
        assert result["success"] is True, f"Transfer failed: {result.get('errors')}"
        assert result["rows_inserted"] == 1000, f"Expected 1000 inserts, got {result['rows_inserted']}"
        assert result["rows_updated"] == 0, f"Expected 0 updates on first sync, got {result['rows_updated']}"

        # Verify target count
        target_count = self.count_rows_postgres(postgres_connection, target_schema, test_table_name)
        assert target_count == 1000, f"Expected 1000 target rows, got {target_count}"

        # Verify NULL rows transferred
        null_count = self.count_rows_postgres(
            postgres_connection, target_schema, test_table_name, "updated_at IS NULL"
        )
        assert null_count == 10, f"Expected 10 NULL rows, got {null_count}"

        # Complete sync and update timestamp
        state_manager.complete_sync(
            sync_id=sync_id,
            rows_inserted=result["rows_inserted"],
            rows_updated=result["rows_updated"],
            rows_unchanged=result.get("rows_unchanged", 0),
        )
        state_manager.update_sync_timestamp(sync_id, sync_start_time)

        # Assert: Verify state record
        state = self.get_state_record(postgres_connection, test_table_name)
        assert state is not None, "State record not found"

        sync_status, last_sync_timestamp, rows_inserted, rows_updated, rows_unchanged, duration = state

        assert sync_status == "completed", f"Expected status 'completed', got '{sync_status}'"
        assert last_sync_timestamp is not None, "last_sync_timestamp should be set"
        assert rows_inserted == 1000, f"State shows {rows_inserted} inserted, expected 1000"
        assert rows_updated == 0, f"State shows {rows_updated} updated, expected 0"

        # Verify timestamp is close to sync_start_time (within 1 second)
        time_diff = abs((last_sync_timestamp - sync_start_time).total_seconds())
        assert time_diff < 1, f"Timestamp difference too large: {time_diff} seconds"
```

---

### 2.2 Incremental Sync Test

```python
"""
Integration test: Incremental sync with date filtering.

Location: tests/integration/test_date_incremental_subsequent_sync.py
"""

import pytest
from datetime import datetime, timezone, timedelta
from .base_date_incremental_test import BaseDateIncrementalTest
from mssql_pg_migration.data_transfer import transfer_incremental_staging
from mssql_pg_migration.incremental_state import IncrementalStateManager


class TestIncrementalSyncDateFiltering(BaseDateIncrementalTest):
    """Test incremental sync behavior with date filtering."""

    def test_second_sync_only_transfers_new_rows(
        self,
        mssql_connection,
        postgres_connection,
        mssql_conn_id,
        postgres_conn_id,
        test_schema,
        test_table_name,
        target_schema,
    ):
        """
        Test that second sync only transfers rows modified after last sync.

        Scenario:
        1. First sync at T1: 1000 rows transferred
        2. Between T1 and T2:
           - Insert 50 new rows with updated_at > T1
           - Update 20 existing rows, set updated_at > T1
           - Insert 5 rows with updated_at < T1 (should be skipped)
           - Insert 3 rows with updated_at = NULL (should be included)
        3. Second sync at T2

        Expected:
        - Only 73 rows transferred (50 new + 20 updated + 3 NULL)
        - 5 old rows skipped
        - State updated with new timestamp
        """
        # Arrange: First sync
        T1 = datetime(2024, 1, 10, 12, 0, 0, tzinfo=timezone.utc)

        # Initial 1000 rows (all with dates < T1)
        initial_rows = []
        for i in range(1000):
            updated_at = T1 - timedelta(days=10 - (i % 10))
            initial_rows.append((i, f"User_{i}", i * 10, updated_at))

        self.insert_test_data_mssql(mssql_connection, test_schema, test_table_name, initial_rows)

        # Perform first sync
        table_info = {
            "table_name": test_table_name,
            "source_schema": test_schema,
            "target_schema": target_schema,
            "columns": ["id", "name", "value", "updated_at", "created_at"],
            "pk_columns": ["id"],
        }

        state_manager = IncrementalStateManager(postgres_conn_id)
        state_manager.ensure_state_table_exists()

        sync_id_1 = state_manager.start_sync(
            table_name=test_table_name,
            source_schema=test_schema,
            target_schema=target_schema,
            source_row_count=1000,
        )

        result_1 = transfer_incremental_staging(
            mssql_conn_id=mssql_conn_id,
            postgres_conn_id=postgres_conn_id,
            table_info=table_info,
            chunk_size=200,
            date_column="updated_at",
            last_sync_timestamp=None,  # First sync
        )

        assert result_1["success"] is True

        state_manager.complete_sync(
            sync_id=sync_id_1,
            rows_inserted=result_1["rows_inserted"],
            rows_updated=result_1["rows_updated"],
            rows_unchanged=result_1.get("rows_unchanged", 0),
        )
        state_manager.update_sync_timestamp(sync_id_1, T1)

        # Verify first sync state
        assert self.count_rows_postgres(postgres_connection, target_schema, test_table_name) == 1000

        # Arrange: Prepare for second sync
        T2 = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)

        # Insert 50 new rows with updated_at > T1
        new_rows = []
        for i in range(1000, 1050):
            updated_at = T1 + timedelta(hours=i % 24 + 1)  # > T1
            new_rows.append((i, f"NewUser_{i}", i * 10, updated_at))

        self.insert_test_data_mssql(mssql_connection, test_schema, test_table_name, new_rows)

        # Update 20 existing rows (set updated_at > T1)
        cursor = mssql_connection.cursor()
        update_time = T1 + timedelta(hours=5)
        for i in range(20):
            cursor.execute(
                f"UPDATE [{test_schema}].[{test_table_name}] SET updated_at = ?, value = value + 100 WHERE id = ?",
                (update_time, i)
            )
        mssql_connection.commit()
        cursor.close()

        # Insert 5 rows with updated_at < T1 (should be skipped)
        old_rows = []
        for i in range(1050, 1055):
            updated_at = T1 - timedelta(days=1)  # < T1
            old_rows.append((i, f"OldUser_{i}", i * 10, updated_at))

        self.insert_test_data_mssql(mssql_connection, test_schema, test_table_name, old_rows)

        # Insert 3 rows with NULL updated_at (should be included)
        null_rows = []
        for i in range(1055, 1058):
            null_rows.append((i, f"NullUser_{i}", i * 10, None))

        self.insert_test_data_mssql(mssql_connection, test_schema, test_table_name, null_rows)

        # Verify source count
        source_count = self.count_rows_mssql(mssql_connection, test_schema, test_table_name)
        assert source_count == 1058, f"Expected 1058 source rows, got {source_count}"

        # Act: Second sync with date filtering
        last_sync_timestamp = state_manager.get_last_sync_timestamp(
            table_name=test_table_name,
            source_schema=test_schema,
            target_schema=target_schema,
        )

        assert last_sync_timestamp is not None, "Should have last_sync_timestamp from first sync"
        assert last_sync_timestamp == T1, f"Expected T1={T1}, got {last_sync_timestamp}"

        sync_id_2 = state_manager.start_sync(
            table_name=test_table_name,
            source_schema=test_schema,
            target_schema=target_schema,
            source_row_count=1058,
        )

        result_2 = transfer_incremental_staging(
            mssql_conn_id=mssql_conn_id,
            postgres_conn_id=postgres_conn_id,
            table_info=table_info,
            chunk_size=200,
            date_column="updated_at",
            last_sync_timestamp=last_sync_timestamp,  # Filter by T1
        )

        # Assert: Verify second sync results
        assert result_2["success"] is True, f"Second sync failed: {result_2.get('errors')}"

        # Expected: 50 new + 20 updated + 3 NULL = 73 rows transferred
        total_transferred = result_2["rows_inserted"] + result_2["rows_updated"]
        assert total_transferred == 73, f"Expected 73 rows transferred, got {total_transferred}"

        # Breakdown: 53 new (50 new + 3 NULL), 20 updated
        assert result_2["rows_inserted"] == 53, f"Expected 53 inserts, got {result_2['rows_inserted']}"
        assert result_2["rows_updated"] == 20, f"Expected 20 updates, got {result_2['rows_updated']}"

        # Complete second sync
        state_manager.complete_sync(
            sync_id=sync_id_2,
            rows_inserted=result_2["rows_inserted"],
            rows_updated=result_2["rows_updated"],
            rows_unchanged=result_2.get("rows_unchanged", 0),
        )
        state_manager.update_sync_timestamp(sync_id_2, T2)

        # Verify final target count (1000 + 53 new = 1053, but 5 old rows not transferred)
        # Wait - 5 old rows ARE in source but NOT transferred to target
        # Final target count should be 1000 (initial) + 50 (new) + 3 (NULL) = 1053
        target_count = self.count_rows_postgres(postgres_connection, target_schema, test_table_name)
        assert target_count == 1053, f"Expected 1053 target rows, got {target_count}"

        # Verify state record updated
        state = self.get_state_record(postgres_connection, test_table_name)
        sync_status, new_timestamp, rows_inserted, rows_updated, rows_unchanged, duration = state

        assert sync_status == "completed"
        assert new_timestamp == T2, f"Expected T2={T2}, got {new_timestamp}"
```

---

### 2.3 NULL Handling Test

```python
"""
Integration test: NULL date handling.

Location: tests/integration/test_date_incremental_null_handling.py
"""

import pytest
from datetime import datetime, timezone, timedelta
from .base_date_incremental_test import BaseDateIncrementalTest
from mssql_pg_migration.data_transfer import transfer_incremental_staging
from mssql_pg_migration.incremental_state import IncrementalStateManager


class TestNullDateHandling(BaseDateIncrementalTest):
    """Test NULL date handling in incremental sync."""

    def test_null_dates_always_included(
        self,
        mssql_connection,
        postgres_connection,
        mssql_conn_id,
        postgres_conn_id,
        test_schema,
        test_table_name,
        target_schema,
    ):
        """
        Test that rows with NULL updated_at are always included in sync.

        Scenario:
        1. First sync at T1: 100 rows (90 with dates, 10 NULL)
        2. After T1: Insert 10 more rows with NULL updated_at
        3. Second sync at T2

        Expected:
        - All 10 new NULL rows transferred
        - Original 10 NULL rows also re-synced (upsert as unchanged)
        """
        # Arrange: First sync
        T1 = datetime(2024, 1, 10, 12, 0, 0, tzinfo=timezone.utc)

        initial_rows = []
        # 90 rows with dates
        for i in range(90):
            updated_at = T1 - timedelta(days=i % 10)
            initial_rows.append((i, f"User_{i}", i * 10, updated_at))

        # 10 rows with NULL
        for i in range(90, 100):
            initial_rows.append((i, f"NullUser_{i}", i * 10, None))

        self.insert_test_data_mssql(mssql_connection, test_schema, test_table_name, initial_rows)

        # First sync
        table_info = {
            "table_name": test_table_name,
            "source_schema": test_schema,
            "target_schema": target_schema,
            "columns": ["id", "name", "value", "updated_at", "created_at"],
            "pk_columns": ["id"],
        }

        state_manager = IncrementalStateManager(postgres_conn_id)
        state_manager.ensure_state_table_exists()

        sync_id_1 = state_manager.start_sync(
            table_name=test_table_name,
            source_schema=test_schema,
            target_schema=target_schema,
            source_row_count=100,
        )

        result_1 = transfer_incremental_staging(
            mssql_conn_id=mssql_conn_id,
            postgres_conn_id=postgres_conn_id,
            table_info=table_info,
            chunk_size=50,
            date_column="updated_at",
            last_sync_timestamp=None,
        )

        assert result_1["success"] is True
        assert result_1["rows_inserted"] == 100

        state_manager.complete_sync(
            sync_id=sync_id_1,
            rows_inserted=result_1["rows_inserted"],
            rows_updated=0,
            rows_unchanged=0,
        )
        state_manager.update_sync_timestamp(sync_id_1, T1)

        # Verify initial NULL count
        null_count_1 = self.count_rows_postgres(
            postgres_connection, target_schema, test_table_name, "updated_at IS NULL"
        )
        assert null_count_1 == 10

        # Arrange: Add 10 more NULL rows
        new_null_rows = []
        for i in range(100, 110):
            new_null_rows.append((i, f"NewNullUser_{i}", i * 10, None))

        self.insert_test_data_mssql(mssql_connection, test_schema, test_table_name, new_null_rows)

        # Act: Second sync
        last_sync_timestamp = state_manager.get_last_sync_timestamp(
            table_name=test_table_name,
            source_schema=test_schema,
            target_schema=target_schema,
        )

        sync_id_2 = state_manager.start_sync(
            table_name=test_table_name,
            source_schema=test_schema,
            target_schema=target_schema,
            source_row_count=110,
        )

        result_2 = transfer_incremental_staging(
            mssql_conn_id=mssql_conn_id,
            postgres_conn_id=postgres_conn_id,
            table_info=table_info,
            chunk_size=50,
            date_column="updated_at",
            last_sync_timestamp=last_sync_timestamp,
        )

        # Assert: Verify NULL handling
        assert result_2["success"] is True

        # Expected: 20 total NULL rows in staging (10 original + 10 new)
        # 10 are inserts (new), 10 are unchanged (original re-synced)
        total_transferred = result_2["rows_inserted"] + result_2["rows_unchanged"]
        assert total_transferred == 20, f"Expected 20 NULL rows transferred, got {total_transferred}"
        assert result_2["rows_inserted"] == 10, f"Expected 10 new NULL inserts, got {result_2['rows_inserted']}"

        # Verify final NULL count in target
        null_count_2 = self.count_rows_postgres(
            postgres_connection, target_schema, test_table_name, "updated_at IS NULL"
        )
        assert null_count_2 == 20, f"Expected 20 NULL rows in target, got {null_count_2}"

        # Verify total count
        total_count = self.count_rows_postgres(postgres_connection, target_schema, test_table_name)
        assert total_count == 110, f"Expected 110 total rows, got {total_count}"
```

---

## 3. Security Test Templates

### 3.1 SQL Injection Test

```python
"""
Security test: SQL injection prevention in date column handling.

Location: tests/security/test_sql_injection_date_column.py
"""

import pytest
from mssql_pg_migration.data_transfer import get_date_column_info


class TestSQLInjectionPrevention:
    """Test SQL injection prevention in date-based incremental features."""

    @pytest.fixture
    def mssql_conn_id(self):
        return "mssql_source"

    def test_malicious_date_column_name_parameterized_query(self, mssql_conn_id):
        """
        Test that malicious column name cannot inject SQL.

        Attack vector: Column name with SQL injection attempt.
        Expected: Parameterized query prevents injection, returns None.
        """
        # Arrange: Malicious column name
        malicious_column = "updated_at'; DROP TABLE Users; --"

        # Act: Attempt to get column info
        result = get_date_column_info(
            mssql_conn_id=mssql_conn_id,
            source_schema="dbo",
            table_name="Users",
            date_column_name=malicious_column,
        )

        # Assert: Should return None (column not found)
        assert result is None, "Malicious column name should not be found"

        # Verify tables still exist (no SQL injection occurred)
        # This would require checking table existence, but conceptually:
        # If DROP TABLE executed, this test would fail due to missing Users table
        # in subsequent tests

    def test_bracket_injection_in_column_name(self, mssql_conn_id):
        """
        Test that bracket injection is prevented by escaping.

        Attack vector: Column name with closing bracket and SQL.
        Expected: Bracket doubling prevents injection.
        """
        # Arrange
        malicious_column = "updated_at]]; DROP TABLE Users; --"

        # Act
        result = get_date_column_info(
            mssql_conn_id=mssql_conn_id,
            source_schema="dbo",
            table_name="Users",
            date_column_name=malicious_column,
        )

        # Assert: Should return None
        assert result is None

        # If bracket escaping works, the query will look for column:
        # [updated_at]]]]; DROP TABLE Users; --]
        # which doesn't exist, so returns None safely

    def test_unicode_null_byte_injection(self, mssql_conn_id):
        """
        Test that null byte injection is handled safely.

        Attack vector: Column name with null byte to truncate query.
        """
        # Arrange
        malicious_column = "updated_at\x00DROP TABLE Users"

        # Act
        result = get_date_column_info(
            mssql_conn_id=mssql_conn_id,
            source_schema="dbo",
            table_name="Users",
            date_column_name=malicious_column,
        )

        # Assert: Should be handled safely
        assert result is None
```

---

## 4. Performance Test Templates

### 4.1 Large Dataset Performance

```python
"""
Performance test: Large dataset sync with date filtering.

Location: tests/performance/test_large_dataset_performance.py
"""

import pytest
import time
from datetime import datetime, timezone, timedelta
from tests.integration.base_date_incremental_test import BaseDateIncrementalTest
from mssql_pg_migration.data_transfer import transfer_incremental_staging
from mssql_pg_migration.incremental_state import IncrementalStateManager


class TestLargeDatasetPerformance(BaseDateIncrementalTest):
    """Performance tests for date-based incremental with large datasets."""

    @pytest.mark.slow
    @pytest.mark.performance
    def test_large_table_incremental_performance(
        self,
        mssql_connection,
        postgres_connection,
        mssql_conn_id,
        postgres_conn_id,
        test_schema,
        target_schema,
    ):
        """
        Test incremental sync performance on large table (1M rows).

        Scenario:
        - 1M row table
        - 10K rows (1%) modified since last sync
        - Measure time with and without date filtering

        Expected:
        - Date-filtered sync significantly faster than full sync
        - Metrics logged for analysis
        """
        # Note: This test requires a large test table
        # Consider using a dedicated performance test database

        table_name = "TestLargeTable"

        # Arrange: Create large table (this may take time in setup)
        # ... (implementation omitted for brevity)

        # Baseline: Full sync time
        start = time.time()
        # ... perform full sync
        full_sync_time = time.time() - start

        # Test: Incremental sync with 1% change
        start = time.time()
        # ... perform incremental sync
        incremental_sync_time = time.time() - start

        # Assert: Performance improvement
        speedup = full_sync_time / incremental_sync_time
        print(f"Full sync: {full_sync_time:.2f}s")
        print(f"Incremental sync: {incremental_sync_time:.2f}s")
        print(f"Speedup: {speedup:.2f}x")

        # Expect at least 10x speedup for 1% change
        assert speedup > 10, f"Expected >10x speedup, got {speedup:.2f}x"
```

---

## 5. Test Execution Script

```bash
#!/bin/bash
# run_integration_tests.sh

set -e

echo "Running Date-Based Incremental Integration Tests"
echo "================================================"

# Ensure Docker Compose is running
docker-compose ps | grep -q "Up" || {
    echo "ERROR: Docker Compose services not running"
    echo "Run: docker-compose up -d"
    exit 1
}

# Run unit tests first
echo ""
echo "Step 1: Running Unit Tests..."
docker exec airflow-scheduler pytest /opt/airflow/tests/plugins/test_date_based_incremental.py -v

# Run integration tests
echo ""
echo "Step 2: Running Integration Tests..."
docker exec airflow-scheduler pytest /opt/airflow/tests/integration/test_date_incremental_*.py -v

# Run security tests
echo ""
echo "Step 3: Running Security Tests..."
docker exec airflow-scheduler pytest /opt/airflow/tests/security/test_sql_injection_date_column.py -v

# Run performance tests (optional, marked as slow)
echo ""
echo "Step 4: Running Performance Tests (optional)..."
docker exec airflow-scheduler pytest /opt/airflow/tests/performance/ -v -m performance --slow

echo ""
echo "All tests completed successfully!"
```

---

## 6. CI/CD Integration

### 6.1 GitHub Actions Workflow

```yaml
# .github/workflows/test-date-incremental.yml

name: Date-Based Incremental Tests

on:
  pull_request:
    paths:
      - 'plugins/mssql_pg_migration/incremental_state.py'
      - 'plugins/mssql_pg_migration/data_transfer.py'
      - 'dags/mssql_to_postgres_incremental.py'
      - 'tests/**'
  push:
    branches:
      - main

jobs:
  test:
    runs-on: ubuntu-latest

    services:
      mssql:
        image: mcr.microsoft.com/mssql/server:2022-latest
        env:
          ACCEPT_EULA: Y
          SA_PASSWORD: TestPassword123!
        ports:
          - 1433:1433

      postgres:
        image: postgres:15
        env:
          POSTGRES_PASSWORD: TestPassword123!
        ports:
          - 5432:5432

    steps:
      - uses: actions/checkout@v3

      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: |
          pip install -r requirements.txt
          pip install pytest pytest-cov

      - name: Run unit tests
        run: |
          pytest tests/plugins/test_date_based_incremental.py -v --cov=plugins/mssql_pg_migration

      - name: Run integration tests
        run: |
          pytest tests/integration/test_date_incremental_*.py -v
        env:
          MSSQL_CONN: "mssql://sa:TestPassword123!@localhost:1433/master"
          POSTGRES_CONN: "postgresql://postgres:TestPassword123!@localhost:5432/postgres"

      - name: Upload coverage
        uses: codecov/codecov-action@v3
```

---

## 7. Test Data Generators

### 7.1 Realistic Test Data Generator

```python
"""
Test data generator for date-based incremental tests.

Location: tests/fixtures/test_data_generator.py
"""

from datetime import datetime, timezone, timedelta
from typing import List, Tuple, Optional
import random


class DateIncrementalTestDataGenerator:
    """Generate realistic test data for date-based incremental testing."""

    @staticmethod
    def generate_rows_with_dates(
        count: int,
        start_id: int = 0,
        date_range_days: int = 30,
        base_date: Optional[datetime] = None,
        null_percentage: float = 0.1,
    ) -> List[Tuple[int, str, int, Optional[datetime]]]:
        """
        Generate test rows with varied updated_at dates.

        Args:
            count: Number of rows to generate
            start_id: Starting ID value
            date_range_days: Spread dates across this many days
            base_date: Base date to generate around (default: now - 30 days)
            null_percentage: Percentage of rows with NULL dates (0.0 to 1.0)

        Returns:
            List of tuples: (id, name, value, updated_at)
        """
        if base_date is None:
            base_date = datetime.now(timezone.utc) - timedelta(days=30)

        rows = []
        null_count = int(count * null_percentage)

        for i in range(count):
            row_id = start_id + i
            name = f"User_{row_id}"
            value = row_id * 10

            # Decide if this row should have NULL date
            if i < null_count:
                updated_at = None
            else:
                # Random date within range
                days_offset = random.uniform(0, date_range_days)
                hours_offset = random.uniform(0, 24)
                updated_at = base_date + timedelta(days=days_offset, hours=hours_offset)

            rows.append((row_id, name, value, updated_at))

        return rows

    @staticmethod
    def generate_incremental_changes(
        existing_ids: List[int],
        new_count: int,
        update_count: int,
        update_timestamp: datetime,
        next_id: int,
    ) -> Tuple[List[Tuple[int, str, int, datetime]], List[Tuple[int, datetime, int]]]:
        """
        Generate incremental changes for testing second sync.

        Args:
            existing_ids: List of existing row IDs
            new_count: Number of new rows to insert
            update_count: Number of existing rows to update
            update_timestamp: Timestamp for new/updated rows
            next_id: Next available ID for new rows

        Returns:
            Tuple of (new_rows, updated_rows)
            new_rows: List of (id, name, value, updated_at)
            updated_rows: List of (id, updated_at, value_delta)
        """
        # Generate new rows
        new_rows = []
        for i in range(new_count):
            row_id = next_id + i
            name = f"NewUser_{row_id}"
            value = row_id * 10
            new_rows.append((row_id, name, value, update_timestamp))

        # Generate updates to existing rows
        updated_rows = []
        if update_count > 0:
            sample_ids = random.sample(existing_ids, min(update_count, len(existing_ids)))
            for row_id in sample_ids:
                value_delta = 100  # Increment value by 100
                updated_rows.append((row_id, update_timestamp, value_delta))

        return new_rows, updated_rows
```

---

This template document provides a foundation for implementing comprehensive integration tests. Each template can be adapted to specific test scenarios and environment configurations.
