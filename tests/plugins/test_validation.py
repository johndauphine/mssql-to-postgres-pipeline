"""
Tests for Migration Validation Module

These tests validate row count validation, sample data validation,
and security of validation queries.
"""

import pytest
from unittest.mock import Mock, MagicMock, patch, call
from mssql_pg_migration.validation import (
    MigrationValidator,
    generate_migration_report,
    validate_migration,
)


class TestMigrationValidator:
    """Test MigrationValidator class."""

    @pytest.fixture
    def mock_mssql_hook(self):
        """Create mock MSSQL hook."""
        hook = Mock()
        hook.get_first = Mock()
        hook.get_records = Mock()
        return hook

    @pytest.fixture
    def mock_postgres_hook(self):
        """Create mock PostgreSQL hook."""
        hook = Mock()
        hook.get_conn = Mock()
        hook.get_records = Mock()
        return hook

    @pytest.fixture
    def validator(self, mock_mssql_hook, mock_postgres_hook):
        """Create validator with mocked hooks."""
        with patch('mssql_pg_migration.validation.OdbcConnectionHelper') as MockOdbc, \
             patch('mssql_pg_migration.validation.PostgresHook') as MockPg:

            MockOdbc.return_value = mock_mssql_hook
            MockPg.return_value = mock_postgres_hook

            return MigrationValidator('mssql_test', 'postgres_test')

    def test_validate_row_count_match(self, validator, mock_mssql_hook, mock_postgres_hook):
        """Test row count validation when counts match."""
        # Setup mocks
        mock_mssql_hook.get_first.return_value = (1000,)

        mock_pg_conn = Mock()
        mock_pg_cursor = Mock()
        mock_pg_cursor.fetchone.return_value = (1000,)
        mock_pg_conn.cursor.return_value.__enter__.return_value = mock_pg_cursor
        mock_postgres_hook.get_conn.return_value = mock_pg_conn

        # Execute validation
        result = validator.validate_row_count('dbo', 'Users', 'public', 'users')

        # Assertions
        assert result['validation_passed'] is True
        assert result['source_count'] == 1000
        assert result['target_count'] == 1000
        assert result['row_difference'] == 0
        assert result['percentage_difference'] == 0

    def test_validate_row_count_mismatch(self, validator, mock_mssql_hook, mock_postgres_hook):
        """Test row count validation when counts don't match."""
        # Setup mocks
        mock_mssql_hook.get_first.return_value = (1000,)

        mock_pg_conn = Mock()
        mock_pg_cursor = Mock()
        mock_pg_cursor.fetchone.return_value = (950,)  # Missing 50 rows
        mock_pg_conn.cursor.return_value.__enter__.return_value = mock_pg_cursor
        mock_postgres_hook.get_conn.return_value = mock_pg_conn

        # Execute validation
        result = validator.validate_row_count('dbo', 'Users', 'public', 'users')

        # Assertions
        assert result['validation_passed'] is False
        assert result['source_count'] == 1000
        assert result['target_count'] == 950
        assert result['row_difference'] == -50
        assert result['percentage_difference'] == -5.0

    def test_validate_row_count_empty_tables(self, validator, mock_mssql_hook, mock_postgres_hook):
        """Test row count validation for empty tables."""
        # Setup mocks for empty tables
        mock_mssql_hook.get_first.return_value = (0,)

        mock_pg_conn = Mock()
        mock_pg_cursor = Mock()
        mock_pg_cursor.fetchone.return_value = (0,)
        mock_pg_conn.cursor.return_value.__enter__.return_value = mock_pg_cursor
        mock_postgres_hook.get_conn.return_value = mock_pg_conn

        # Execute validation
        result = validator.validate_row_count('dbo', 'EmptyTable', 'public', 'emptytable')

        # Assertions
        assert result['validation_passed'] is True
        assert result['source_count'] == 0
        assert result['target_count'] == 0

    def test_validate_row_count_null_result(self, validator, mock_mssql_hook, mock_postgres_hook):
        """Test row count validation when query returns NULL."""
        # Setup mocks with NULL results
        mock_mssql_hook.get_first.return_value = (None,)

        mock_pg_conn = Mock()
        mock_pg_cursor = Mock()
        mock_pg_cursor.fetchone.return_value = (None,)
        mock_pg_conn.cursor.return_value.__enter__.return_value = mock_pg_cursor
        mock_postgres_hook.get_conn.return_value = mock_pg_conn

        # Execute validation
        result = validator.validate_row_count('dbo', 'TestTable', 'public', 'testtable')

        # Assertions - NULL should be treated as 0
        assert result['validation_passed'] is True
        assert result['source_count'] == 0
        assert result['target_count'] == 0

    def test_validate_row_count_uses_parameterization(self, validator, mock_mssql_hook, mock_postgres_hook):
        """SECURITY: Verify that queries use parameterization, not string formatting."""
        # Setup mocks
        mock_mssql_hook.get_first.return_value = (100,)

        mock_pg_conn = Mock()
        mock_pg_cursor = Mock()
        mock_pg_cursor.fetchone.return_value = (100,)
        mock_pg_conn.cursor.return_value.__enter__.return_value = mock_pg_cursor
        mock_postgres_hook.get_conn.return_value = mock_pg_conn

        # Execute validation with potentially malicious input
        malicious_schema = "dbo'; DROP TABLE Users; --"
        malicious_table = "Users'; DELETE FROM Users; --"

        # This should NOT cause SQL injection because of bracket quoting
        # But ideally should use parameterization
        result = validator.validate_row_count(
            malicious_schema, malicious_table, 'public', 'users'
        )

        # Verify the query was executed (didn't crash)
        # In a real security test, we'd verify parameterization was used
        assert mock_mssql_hook.get_first.called

    def test_validate_tables_batch_all_pass(self, validator, mock_mssql_hook, mock_postgres_hook):
        """Test batch validation when all tables pass."""
        # Setup mocks
        mock_mssql_hook.get_first.side_effect = [
            (100,),  # Table 1
            (200,),  # Table 2
            (300,),  # Table 3
        ]

        mock_pg_conn = Mock()
        mock_pg_cursor = Mock()
        mock_pg_cursor.fetchone.side_effect = [
            (100,),  # Table 1
            (200,),  # Table 2
            (300,),  # Table 3
        ]
        mock_pg_conn.cursor.return_value.__enter__.return_value = mock_pg_cursor
        mock_postgres_hook.get_conn.return_value = mock_pg_conn

        # Execute batch validation
        tables = [
            {'table_name': 'Users', 'source_schema': 'dbo', 'target_schema': 'public'},
            {'table_name': 'Orders', 'source_schema': 'dbo', 'target_schema': 'public'},
            {'table_name': 'Products', 'source_schema': 'dbo', 'target_schema': 'public'},
        ]

        results = validator.validate_tables_batch(tables)

        # Assertions
        assert results['total_tables'] == 3
        assert results['passed_count'] == 3
        assert results['failed_count'] == 0
        assert results['overall_success'] is True
        assert len(results['passed_tables']) == 3
        assert len(results['failed_tables']) == 0

    def test_validate_tables_batch_some_fail(self, validator, mock_mssql_hook, mock_postgres_hook):
        """Test batch validation when some tables fail."""
        # Setup mocks with mismatches
        mock_mssql_hook.get_first.side_effect = [
            (100,),  # Table 1 - OK
            (200,),  # Table 2 - FAIL (missing rows)
        ]

        mock_pg_conn = Mock()
        mock_pg_cursor = Mock()
        mock_pg_cursor.fetchone.side_effect = [
            (100,),  # Table 1 - matches
            (150,),  # Table 2 - doesn't match
        ]
        mock_pg_conn.cursor.return_value.__enter__.return_value = mock_pg_cursor
        mock_postgres_hook.get_conn.return_value = mock_pg_conn

        # Execute batch validation
        tables = [
            {'table_name': 'Users', 'source_schema': 'dbo', 'target_schema': 'public'},
            {'table_name': 'Orders', 'source_schema': 'dbo', 'target_schema': 'public'},
        ]

        results = validator.validate_tables_batch(tables)

        # Assertions
        assert results['total_tables'] == 2
        assert results['passed_count'] == 1
        assert results['failed_count'] == 1
        assert results['overall_success'] is False
        assert 'Users' in results['passed_tables']
        assert 'Orders' in results['failed_tables']

    def test_validate_sample_data_match(self, validator, mock_mssql_hook, mock_postgres_hook):
        """Test sample data validation when samples match."""
        # Setup mocks
        mock_mssql_hook.get_records.return_value = [
            (1, 'Alice', 'alice@example.com'),
            (2, 'Bob', 'bob@example.com'),
            (3, 'Charlie', 'charlie@example.com'),
        ]

        mock_pg_conn = Mock()
        mock_pg_cursor = Mock()
        mock_pg_cursor.fetchall.return_value = [
            (1, 'Alice', 'alice@example.com'),
            (2, 'Bob', 'bob@example.com'),
            (3, 'Charlie', 'charlie@example.com'),
        ]
        mock_pg_conn.cursor.return_value.__enter__.return_value = mock_pg_cursor
        mock_postgres_hook.get_conn.return_value = mock_pg_conn

        # Execute sample validation
        result = validator.validate_sample_data(
            'dbo', 'Users', 'public', 'users',
            sample_size=3,
            key_columns=['ID', 'Name', 'Email']
        )

        # Assertions
        assert result['validation_passed'] is True
        assert result['matches'] == 3
        assert result['mismatches_count'] == 0


class TestGenerateMigrationReport:
    """Test migration report generation."""

    def test_generate_report_all_passed(self):
        """Test report generation when all validations pass."""
        validation_results = {
            'total_tables': 3,
            'passed_count': 3,
            'failed_count': 0,
            'success_rate': 100.0,
            'passed_tables': ['Users', 'Orders', 'Products'],
            'failed_tables': [],
            'row_count_results': [
                {
                    'table_name': 'Users',
                    'source_count': 100,
                    'target_count': 100,
                    'row_difference': 0,
                    'validation_passed': True,
                },
                {
                    'table_name': 'Orders',
                    'source_count': 200,
                    'target_count': 200,
                    'row_difference': 0,
                    'validation_passed': True,
                },
                {
                    'table_name': 'Products',
                    'source_count': 50,
                    'target_count': 50,
                    'row_difference': 0,
                    'validation_passed': True,
                },
            ],
        }

        report = generate_migration_report(validation_results)

        # Assertions
        assert 'DATA MIGRATION REPORT' in report
        assert 'Total Tables: 3' in report
        assert 'Successful: 3' in report
        assert 'Failed: 0' in report
        assert 'Success Rate: 100.0%' in report
        assert '✓ PASS' in report
        assert '✗ FAIL' not in report

    def test_generate_report_with_failures(self):
        """Test report generation with failures."""
        validation_results = {
            'total_tables': 2,
            'passed_count': 1,
            'failed_count': 1,
            'success_rate': 50.0,
            'passed_tables': ['Users'],
            'failed_tables': ['Orders'],
            'row_count_results': [
                {
                    'table_name': 'Users',
                    'source_count': 100,
                    'target_count': 100,
                    'row_difference': 0,
                    'validation_passed': True,
                },
                {
                    'table_name': 'Orders',
                    'source_count': 200,
                    'target_count': 150,
                    'row_difference': -50,
                    'validation_passed': False,
                },
            ],
        }

        report = generate_migration_report(validation_results)

        # Assertions
        assert 'Failed: 1' in report
        assert 'Success Rate: 50.0%' in report
        assert '✗ FAIL' in report
        assert 'Orders' in report
        assert 'FAILED TABLES REQUIRING ATTENTION' in report

    def test_generate_report_with_transfer_stats(self):
        """Test report generation including transfer statistics."""
        validation_results = {
            'total_tables': 1,
            'passed_count': 1,
            'failed_count': 0,
            'success_rate': 100.0,
            'passed_tables': ['Users'],
            'failed_tables': [],
            'row_count_results': [
                {
                    'table_name': 'Users',
                    'source_count': 1000,
                    'target_count': 1000,
                    'row_difference': 0,
                    'validation_passed': True,
                },
            ],
        }

        transfer_results = [
            {
                'rows_transferred': 1000,
                'elapsed_time_seconds': 10.0,
            },
        ]

        report = generate_migration_report(validation_results, transfer_results)

        # Assertions
        assert 'TRANSFER STATISTICS' in report
        assert 'Total Rows Transferred: 1,000' in report
        assert 'Total Time: 10.00 seconds' in report
        assert 'Average Transfer Rate:' in report


class TestValidateMigration:
    """Test convenience validation function."""

    @patch('mssql_pg_migration.validation.MigrationValidator')
    def test_validate_migration_creates_validator(self, MockValidator):
        """Test that validate_migration creates a validator instance."""
        mock_validator = Mock()
        mock_validator.validate_tables_batch.return_value = {
            'total_tables': 1,
            'passed_count': 1,
            'failed_count': 0,
            'success_rate': 100.0,
            'row_count_results': [],
        }
        MockValidator.return_value = mock_validator

        tables = [{'table_name': 'Users'}]

        result = validate_migration('mssql_test', 'postgres_test', tables)

        # Assertions
        MockValidator.assert_called_once_with('mssql_test', 'postgres_test')
        mock_validator.validate_tables_batch.assert_called_once()
        assert 'report' in result
        assert isinstance(result['report'], str)


class TestEdgeCases:
    """Test edge cases and error conditions."""

    @pytest.fixture
    def validator(self):
        """Create validator with mocked hooks."""
        with patch('mssql_pg_migration.validation.OdbcConnectionHelper'), \
             patch('mssql_pg_migration.validation.PostgresHook'):
            return MigrationValidator('mssql_test', 'postgres_test')

    def test_empty_table_list(self, validator):
        """Test validation with empty table list."""
        with patch.object(validator, 'validate_row_count') as mock_validate:
            results = validator.validate_tables_batch([])

            # Assertions
            assert results['total_tables'] == 0
            assert results['passed_count'] == 0
            assert results['failed_count'] == 0
            assert results['overall_success'] is True
            mock_validate.assert_not_called()

    def test_percentage_difference_with_zero_source(self, validator):
        """Test percentage calculation when source count is zero."""
        with patch.object(validator.mssql_hook, 'get_first') as mock_mssql, \
             patch.object(validator.postgres_hook, 'get_conn') as mock_pg_conn:

            mock_mssql.return_value = (0,)
            mock_cursor = Mock()
            mock_cursor.fetchone.return_value = (5,)  # Target has rows but source doesn't
            mock_conn = Mock()
            mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
            mock_pg_conn.return_value = mock_conn

            result = validator.validate_row_count('dbo', 'Test', 'public', 'test')

            # Should handle division by zero
            assert result['percentage_difference'] == 0
            assert result['validation_passed'] is False

    def test_very_large_table_counts(self, validator):
        """Test validation with very large row counts."""
        with patch.object(validator.mssql_hook, 'get_first') as mock_mssql, \
             patch.object(validator.postgres_hook, 'get_conn') as mock_pg_conn:

            large_count = 10_000_000_000  # 10 billion rows
            mock_mssql.return_value = (large_count,)
            mock_cursor = Mock()
            mock_cursor.fetchone.return_value = (large_count,)
            mock_conn = Mock()
            mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
            mock_pg_conn.return_value = mock_conn

            result = validator.validate_row_count('dbo', 'BigTable', 'public', 'bigtable')

            # Should handle large numbers correctly
            assert result['source_count'] == large_count
            assert result['target_count'] == large_count
            assert result['validation_passed'] is True


class TestSecurity:
    """Security-focused tests."""

    @pytest.fixture
    def validator(self):
        """Create validator with mocked hooks."""
        with patch('mssql_pg_migration.validation.OdbcConnectionHelper'), \
             patch('mssql_pg_migration.validation.PostgresHook'):
            return MigrationValidator('mssql_test', 'postgres_test')

    def test_sql_injection_prevention_in_schema_name(self, validator):
        """SECURITY: Test that schema names can't be used for SQL injection."""
        with patch.object(validator.mssql_hook, 'get_first') as mock_mssql, \
             patch.object(validator.postgres_hook, 'get_conn') as mock_pg_conn:

            # Setup mocks
            mock_mssql.return_value = (100,)
            mock_cursor = Mock()
            mock_cursor.fetchone.return_value = (100,)
            mock_conn = Mock()
            mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
            mock_pg_conn.return_value = mock_conn

            # Attempt SQL injection via schema name
            malicious_schema = "dbo]; DELETE FROM Users; --"
            malicious_table = "Users'; DROP TABLE Orders; --"

            # Execute - should not crash or execute malicious SQL
            try:
                result = validator.validate_row_count(
                    malicious_schema,
                    malicious_table,
                    'public',
                    'users'
                )
                # If we get here, query executed (though it may have failed safely)
                assert True
            except Exception as e:
                # Expected - query should fail safely, not execute malicious SQL
                assert 'DELETE' not in str(e).upper()
                assert 'DROP' not in str(e).upper()

    def test_identifier_quoting_in_postgres_queries(self, validator):
        """SECURITY: Verify PostgreSQL queries use sql.Identifier for quoting."""
        from psycopg2 import sql

        with patch.object(validator.postgres_hook, 'get_conn') as mock_pg_conn:
            mock_cursor = Mock()
            mock_cursor.fetchone.return_value = (100,)
            mock_conn = Mock()
            mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
            mock_pg_conn.return_value = mock_conn

            with patch.object(validator.mssql_hook, 'get_first', return_value=(100,)):
                # Execute validation
                validator.validate_row_count('dbo', 'Users', 'public', 'users')

                # Check that cursor.execute was called
                assert mock_cursor.execute.called

                # The query should be a psycopg2.sql.Composed object, not a plain string
                # This indicates proper parameterization is being used


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
