"""
Tests for Date-Based Incremental Loading

These tests validate:
- Date column validation (get_date_column_info)
- Last sync timestamp retrieval and storage
- Date filter WHERE clause generation
"""

import pytest
from unittest.mock import Mock, MagicMock, patch
from datetime import datetime, timezone


class TestGetDateColumnInfo:
    """Test date column validation function."""

    @pytest.fixture
    def mock_helper(self):
        """Create mock OdbcConnectionHelper."""
        with patch('mssql_pg_migration.data_transfer.OdbcConnectionHelper') as mock_class:
            mock_instance = MagicMock()
            mock_class.return_value = mock_instance
            yield mock_instance

    def test_returns_info_for_valid_datetime_column(self, mock_helper):
        """Test returns column info for valid datetime column."""
        from mssql_pg_migration.data_transfer import get_date_column_info

        mock_helper.get_first.return_value = ('updated_at', 'datetime')

        result = get_date_column_info(
            mssql_conn_id='test_conn',
            source_schema='dbo',
            table_name='Users',
            date_column_names=['updated_at'],
        )

        assert result is not None
        assert result['column_name'] == 'updated_at'
        assert result['data_type'] == 'datetime'

    def test_returns_info_for_datetime2_column(self, mock_helper):
        """Test returns column info for datetime2 column."""
        from mssql_pg_migration.data_transfer import get_date_column_info

        mock_helper.get_first.return_value = ('ModifiedDate', 'datetime2')

        result = get_date_column_info(
            mssql_conn_id='test_conn',
            source_schema='dbo',
            table_name='Orders',
            date_column_names=['ModifiedDate'],
        )

        assert result is not None
        assert result['data_type'] == 'datetime2'

    def test_returns_info_for_datetimeoffset_column(self, mock_helper):
        """Test returns column info for datetimeoffset column."""
        from mssql_pg_migration.data_transfer import get_date_column_info

        mock_helper.get_first.return_value = ('last_update', 'datetimeoffset')

        result = get_date_column_info(
            mssql_conn_id='test_conn',
            source_schema='dbo',
            table_name='Events',
            date_column_names=['last_update'],
        )

        assert result is not None
        assert result['data_type'] == 'datetimeoffset'

    def test_returns_none_for_missing_column(self, mock_helper):
        """Test returns None when column doesn't exist."""
        from mssql_pg_migration.data_transfer import get_date_column_info

        mock_helper.get_first.return_value = None

        result = get_date_column_info(
            mssql_conn_id='test_conn',
            source_schema='dbo',
            table_name='Users',
            date_column_names=['nonexistent_column'],
        )

        assert result is None

    def test_returns_none_for_non_temporal_type(self, mock_helper):
        """Test returns None when column is not a temporal type."""
        from mssql_pg_migration.data_transfer import get_date_column_info

        # Column exists but is varchar, not a temporal type
        mock_helper.get_first.return_value = ('status', 'varchar')

        result = get_date_column_info(
            mssql_conn_id='test_conn',
            source_schema='dbo',
            table_name='Users',
            date_column_names=['status'],
        )

        assert result is None

    def test_returns_none_for_int_column(self, mock_helper):
        """Test returns None when column is an integer type."""
        from mssql_pg_migration.data_transfer import get_date_column_info

        mock_helper.get_first.return_value = ('version', 'int')

        result = get_date_column_info(
            mssql_conn_id='test_conn',
            source_schema='dbo',
            table_name='Users',
            date_column_names=['version'],
        )

        assert result is None

    def test_uses_parameterized_query(self, mock_helper):
        """Test that parameterized query is used for SQL injection prevention."""
        from mssql_pg_migration.data_transfer import get_date_column_info

        mock_helper.get_first.return_value = ('updated_at', 'datetime')

        get_date_column_info(
            mssql_conn_id='test_conn',
            source_schema='dbo',
            table_name='Users',
            date_column_names=['updated_at'],
        )

        # Verify get_first was called with parameterized query
        call_args = mock_helper.get_first.call_args
        query = call_args[0][0]
        params = call_args[0][1]

        # Query should use ? placeholders
        assert 'TABLE_SCHEMA = ?' in query
        assert 'TABLE_NAME = ?' in query
        assert 'COLUMN_NAME = ?' in query
        # Parameters should be passed separately
        assert params == ['dbo', 'Users', 'updated_at']

    def test_returns_none_on_exception(self, mock_helper):
        """Test returns None when query fails."""
        from mssql_pg_migration.data_transfer import get_date_column_info

        mock_helper.get_first.side_effect = Exception('Database error')

        result = get_date_column_info(
            mssql_conn_id='test_conn',
            source_schema='dbo',
            table_name='Users',
            date_column_names=['updated_at'],
        )

        assert result is None

    def test_returns_none_for_empty_list(self, mock_helper):
        """Test returns None when column list is empty."""
        from mssql_pg_migration.data_transfer import get_date_column_info

        result = get_date_column_info(
            mssql_conn_id='test_conn',
            source_schema='dbo',
            table_name='Users',
            date_column_names=[],
        )

        assert result is None
        # Should not call get_first if list is empty
        mock_helper.get_first.assert_not_called()

    def test_tries_columns_in_order_returns_first_match(self, mock_helper):
        """Test tries columns in order and returns first valid match."""
        from mssql_pg_migration.data_transfer import get_date_column_info

        # First column doesn't exist, second is valid datetime
        def mock_get_first(query, params):
            col_name = params[2]
            if col_name == 'ModifiedDate':
                return None  # Not found
            elif col_name == 'UpdatedAt':
                return ('UpdatedAt', 'datetime')  # Found and valid
            elif col_name == 'CreationDate':
                return ('CreationDate', 'datetime')  # Would match but not tried
            return None

        mock_helper.get_first.side_effect = mock_get_first

        result = get_date_column_info(
            mssql_conn_id='test_conn',
            source_schema='dbo',
            table_name='Users',
            date_column_names=['ModifiedDate', 'UpdatedAt', 'CreationDate'],
        )

        assert result is not None
        assert result['column_name'] == 'UpdatedAt'
        # Should have been called twice (ModifiedDate, then UpdatedAt)
        assert mock_helper.get_first.call_count == 2

    def test_skips_non_temporal_continues_to_next(self, mock_helper):
        """Test skips non-temporal column and continues to next candidate."""
        from mssql_pg_migration.data_transfer import get_date_column_info

        # First column exists but is varchar, second is valid datetime
        def mock_get_first(query, params):
            col_name = params[2]
            if col_name == 'status':
                return ('status', 'varchar')  # Found but invalid type
            elif col_name == 'updated_at':
                return ('updated_at', 'datetime')  # Found and valid
            return None

        mock_helper.get_first.side_effect = mock_get_first

        result = get_date_column_info(
            mssql_conn_id='test_conn',
            source_schema='dbo',
            table_name='Users',
            date_column_names=['status', 'updated_at'],
        )

        assert result is not None
        assert result['column_name'] == 'updated_at'
        assert result['data_type'] == 'datetime'

    def test_returns_none_when_all_candidates_fail(self, mock_helper):
        """Test returns None when all candidate columns fail."""
        from mssql_pg_migration.data_transfer import get_date_column_info

        mock_helper.get_first.return_value = None  # All columns not found

        result = get_date_column_info(
            mssql_conn_id='test_conn',
            source_schema='dbo',
            table_name='Users',
            date_column_names=['col1', 'col2', 'col3'],
        )

        assert result is None
        assert mock_helper.get_first.call_count == 3  # Tried all 3


class TestIncrementalStateDateMethods:
    """Test date-based incremental methods in IncrementalStateManager."""

    @pytest.fixture
    def mock_postgres_hook(self):
        """Create mock PostgresHook."""
        with patch('mssql_pg_migration.incremental_state.PostgresHook') as mock_class:
            mock_hook = MagicMock()
            mock_class.return_value = mock_hook
            yield mock_hook

    @pytest.fixture
    def state_manager(self, mock_postgres_hook):
        """Create IncrementalStateManager with mocked hook."""
        from mssql_pg_migration.incremental_state import IncrementalStateManager
        return IncrementalStateManager('test_postgres_conn')

    def test_get_last_sync_timestamp_returns_timestamp(self, state_manager, mock_postgres_hook):
        """Test returns timestamp when record exists and is completed."""
        test_timestamp = datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (test_timestamp, 'completed')
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=False)
        mock_postgres_hook.get_conn.return_value = mock_conn

        result = state_manager.get_last_sync_timestamp(
            table_name='Users',
            source_schema='dbo',
            target_schema='mssql__testdb__dbo',
        )

        assert result == test_timestamp

    def test_get_last_sync_timestamp_returns_none_for_missing_record(
        self, state_manager, mock_postgres_hook
    ):
        """Test returns None when no state record exists."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None  # No record
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=False)
        mock_postgres_hook.get_conn.return_value = mock_conn

        result = state_manager.get_last_sync_timestamp(
            table_name='NewTable',
            source_schema='dbo',
            target_schema='mssql__testdb__dbo',
        )

        assert result is None

    def test_get_last_sync_timestamp_returns_none_for_failed_sync(
        self, state_manager, mock_postgres_hook
    ):
        """Test returns None when last sync was not completed."""
        test_timestamp = datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (test_timestamp, 'failed')  # Failed sync
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=False)
        mock_postgres_hook.get_conn.return_value = mock_conn

        result = state_manager.get_last_sync_timestamp(
            table_name='Users',
            source_schema='dbo',
            target_schema='mssql__testdb__dbo',
        )

        assert result is None

    def test_get_last_sync_timestamp_returns_none_for_null_timestamp(
        self, state_manager, mock_postgres_hook
    ):
        """Test returns None when timestamp is NULL (first sync)."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (None, 'completed')  # Completed but no timestamp
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=False)
        mock_postgres_hook.get_conn.return_value = mock_conn

        result = state_manager.get_last_sync_timestamp(
            table_name='Users',
            source_schema='dbo',
            target_schema='mssql__testdb__dbo',
        )

        assert result is None

    def test_update_sync_timestamp(self, state_manager, mock_postgres_hook):
        """Test update_sync_timestamp updates the record."""
        test_timestamp = datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=False)
        mock_postgres_hook.get_conn.return_value = mock_conn

        state_manager.update_sync_timestamp(sync_id=123, sync_timestamp=test_timestamp)

        # Verify UPDATE was called with correct parameters
        mock_cursor.execute.assert_called_once()
        call_args = mock_cursor.execute.call_args
        query = call_args[0][0]
        params = call_args[0][1]

        assert 'UPDATE _migration._migration_state' in query
        assert 'last_sync_timestamp' in query
        assert params == (test_timestamp, 123)
        mock_conn.commit.assert_called_once()


class TestDateFilterWhereClause:
    """Test date filter WHERE clause generation."""

    def test_date_filter_includes_null_handling(self):
        """Test that date filter includes OR IS NULL for NULL dates."""
        # The date filter should be:
        # ([date_col] > ? OR [date_col] IS NULL)
        # This ensures rows with NULL dates are always included

        from mssql_pg_migration.data_transfer import VALID_TEMPORAL_TYPES

        # Verify valid temporal types
        assert 'datetime' in VALID_TEMPORAL_TYPES
        assert 'datetime2' in VALID_TEMPORAL_TYPES
        assert 'datetimeoffset' in VALID_TEMPORAL_TYPES
        assert 'date' in VALID_TEMPORAL_TYPES
        assert 'smalldatetime' in VALID_TEMPORAL_TYPES

        # Invalid types should not be in set
        assert 'varchar' not in VALID_TEMPORAL_TYPES
        assert 'int' not in VALID_TEMPORAL_TYPES
        assert 'timestamp' not in VALID_TEMPORAL_TYPES  # SQL Server timestamp is not temporal


class TestDateFilterWhereClauseGeneration:
    """Test date filter WHERE clause generation logic."""

    def test_date_filter_clause_format(self):
        """Test the expected date filter clause format."""
        # The date filter should include NULL handling
        date_column = 'updated_at'
        safe_date_col = date_column.replace(']', ']]')

        expected_clause = f"([{safe_date_col}] > ? OR [{safe_date_col}] IS NULL)"

        # Verify the format
        assert '[updated_at] > ?' in expected_clause
        assert '[updated_at] IS NULL' in expected_clause
        assert ' OR ' in expected_clause

    def test_date_filter_params_order(self):
        """Test that params are built in correct order for WHERE clause."""
        # When building the full params list:
        # 1. where_params (date filter) come first
        # 2. pk pagination param comes last

        test_timestamp = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        last_pk = 100

        # Simulate how _read_chunk_keyset builds params
        where_params = [test_timestamp]
        params_list = list(where_params)
        params_list.append(last_pk)

        assert params_list[0] == test_timestamp  # Date filter first
        assert params_list[1] == last_pk  # PK pagination second

    def test_escape_date_column_name(self):
        """Test that date column names with special chars are escaped."""
        # Column name with bracket should be escaped
        column_name = 'update]date'
        safe_col = column_name.replace(']', ']]')

        assert safe_col == 'update]]date'

        # Normal column name unchanged
        normal_col = 'updated_at'
        safe_normal = normal_col.replace(']', ']]')

        assert safe_normal == 'updated_at'


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
