"""
Tests for SQL Server to PostgreSQL Migration DAG

This module contains comprehensive tests for the migration DAG and its utilities,
ensuring proper functionality, data type mapping, and DAG validation.
"""

import os
import sys
import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

# Add parent directories to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from airflow.models import DagBag
from mssql_pg_migration import type_mapping, validation


class TestDAGValidation:
    """Test DAG structure and Airflow requirements."""

    @pytest.fixture
    def dag_bag(self):
        """Create a DagBag for testing."""
        return DagBag(dag_folder="dags", include_examples=False)

    def test_dag_loaded(self, dag_bag):
        """Test that the migration DAG is loaded without import errors."""
        dag = dag_bag.get_dag("mssql_to_postgres_migration")
        assert dag is not None, "mssql_to_postgres_migration DAG not found"
        assert len(dag_bag.import_errors) == 0, f"Import errors: {dag_bag.import_errors}"

    def test_dag_has_required_tags(self, dag_bag):
        """Test that the DAG has required tags."""
        dag = dag_bag.get_dag("mssql_to_postgres_migration")
        assert len(dag.tags) > 0, "DAG must have at least one tag"
        assert "migration" in dag.tags, "DAG must have 'migration' tag"
        assert "etl" in dag.tags, "DAG must have 'etl' tag"

    def test_dag_has_retry_policy(self, dag_bag):
        """Test that the DAG has proper retry configuration."""
        dag = dag_bag.get_dag("mssql_to_postgres_migration")
        assert dag.default_args.get("retries", 0) >= 2, "DAG must have at least 2 retries"
        assert "retry_delay" in dag.default_args, "DAG must have retry_delay configured"

    def test_dag_parameters(self, dag_bag):
        """Test that the DAG has all required parameters."""
        dag = dag_bag.get_dag("mssql_to_postgres_migration")
        params = dag.params

        required_params = [
            "source_conn_id",
            "target_conn_id",
            "source_schema",
            "target_schema",
            "chunk_size",
            "exclude_tables",
        ]

        for param in required_params:
            assert param in params, f"Missing required parameter: {param}"

    def test_dag_tasks(self, dag_bag):
        """Test that the DAG has all expected tasks."""
        dag = dag_bag.get_dag("mssql_to_postgres_migration")
        task_ids = [task.task_id for task in dag.tasks]

        expected_tasks = [
            "extract_source_schema",
            "create_target_schema",
            "create_target_tables",
            "trigger_validation_dag",
            "generate_migration_summary",
        ]

        for task_id in expected_tasks:
            assert task_id in task_ids, f"Missing expected task: {task_id}"

    def test_dag_schedule(self, dag_bag):
        """Test that the DAG has appropriate schedule settings."""
        dag = dag_bag.get_dag("mssql_to_postgres_migration")
        assert dag.schedule is None, "Migration DAG should be manually triggered"
        assert dag.catchup is False, "Migration DAG should not catch up"
        assert dag.max_active_runs == 1, "Only one migration should run at a time"


class TestTypeMapping:
    """Test SQL Server to PostgreSQL type mapping."""

    def test_numeric_type_mapping(self):
        """Test mapping of numeric data types."""
        assert type_mapping.map_type("bit") == "BOOLEAN"
        assert type_mapping.map_type("tinyint") == "SMALLINT"
        assert type_mapping.map_type("smallint") == "SMALLINT"
        assert type_mapping.map_type("int") == "INTEGER"
        assert type_mapping.map_type("bigint") == "BIGINT"
        assert type_mapping.map_type("float") == "DOUBLE PRECISION"
        assert type_mapping.map_type("real") == "REAL"
        assert type_mapping.map_type("money") == "NUMERIC(19,4)"
        assert type_mapping.map_type("smallmoney") == "NUMERIC(10,4)"

    def test_decimal_type_mapping(self):
        """Test mapping of decimal/numeric types with precision and scale."""
        assert type_mapping.map_type("decimal", precision=10, scale=2) == "DECIMAL(10,2)"
        assert type_mapping.map_type("numeric", precision=18, scale=4) == "NUMERIC(18,4)"
        assert type_mapping.map_type("decimal", precision=5) == "DECIMAL(5,0)"

    def test_string_type_mapping(self):
        """Test mapping of string data types."""
        assert type_mapping.map_type("char", max_length=10) == "CHAR(10)"
        assert type_mapping.map_type("varchar", max_length=255) == "VARCHAR(255)"
        assert type_mapping.map_type("text") == "TEXT"
        assert type_mapping.map_type("nchar", max_length=20) == "CHAR(10)"  # Unicode adjustment
        assert type_mapping.map_type("nvarchar", max_length=100) == "VARCHAR(50)"  # Unicode adjustment
        assert type_mapping.map_type("ntext") == "TEXT"

    def test_max_type_mapping(self):
        """Test mapping of MAX types."""
        assert type_mapping.map_type("varchar", is_max=True) == "TEXT"
        assert type_mapping.map_type("nvarchar", is_max=True) == "TEXT"
        assert type_mapping.map_type("varbinary", is_max=True) == "BYTEA"

    def test_datetime_type_mapping(self):
        """Test mapping of date/time data types."""
        assert type_mapping.map_type("date") == "DATE"
        assert type_mapping.map_type("datetime") == "TIMESTAMP(3)"
        assert type_mapping.map_type("datetime2", precision=7) == "TIMESTAMP(7)"
        assert type_mapping.map_type("smalldatetime") == "TIMESTAMP(0)"
        assert type_mapping.map_type("time", precision=3) == "TIME(3)"
        assert type_mapping.map_type("datetimeoffset", precision=7) == "TIMESTAMP(7) WITH TIME ZONE"

    def test_binary_type_mapping(self):
        """Test mapping of binary data types."""
        assert type_mapping.map_type("binary") == "BYTEA"
        assert type_mapping.map_type("varbinary") == "BYTEA"
        assert type_mapping.map_type("image") == "BYTEA"

    def test_special_type_mapping(self):
        """Test mapping of special data types."""
        assert type_mapping.map_type("uniqueidentifier") == "UUID"
        assert type_mapping.map_type("xml") == "XML"
        assert type_mapping.map_type("hierarchyid") == "VARCHAR(4000)"
        assert type_mapping.map_type("geography") == "GEOGRAPHY"
        assert type_mapping.map_type("geometry") == "GEOMETRY"

    def test_unknown_type_mapping(self):
        """Test handling of unknown data types."""
        assert type_mapping.map_type("unknown_type") == "TEXT"
        assert type_mapping.map_type("custom_type") == "TEXT"

    def test_column_mapping(self):
        """Test complete column mapping."""
        column_info = {
            "column_name": "user_id",
            "data_type": "int",
            "is_nullable": False,
            "is_identity": True,
        }

        mapped = type_mapping.map_column(column_info)
        assert mapped["column_name"] == "user_id"
        assert mapped["data_type"] == "SERIAL"
        assert mapped["is_nullable"] is False
        assert mapped["is_identity"] is True

    def test_default_value_mapping(self):
        """Test mapping of default values."""
        assert type_mapping.map_default_value("getdate()") == "CURRENT_TIMESTAMP"
        assert type_mapping.map_default_value("(getdate())") == "CURRENT_TIMESTAMP"
        assert type_mapping.map_default_value("newid()") == "gen_random_uuid()"
        assert type_mapping.map_default_value("(0)") == "0"
        assert type_mapping.map_default_value("('active')") == "'active'"


class TestValidation:
    """Test validation utilities."""

    @patch("mssql_pg_migration.validation.OdbcConnectionHelper")
    @patch("mssql_pg_migration.validation.PostgresHook")
    def test_row_count_validation_success(self, mock_pg_hook, mock_odbc_helper):
        """Test successful row count validation."""
        # Mock SQL Server connection via OdbcConnectionHelper
        mock_odbc = MagicMock()
        mock_odbc.get_first.return_value = (1000,)
        mock_odbc_helper.return_value = mock_odbc

        # Mock PostgreSQL connection
        mock_pg = MagicMock()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_pg.get_conn.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_cursor.fetchone.return_value = (1000,)
        mock_pg_hook.return_value = mock_pg

        validator = validation.MigrationValidator("mssql_conn", "pg_conn")
        result = validator.validate_row_count("dbo", "users", "public", "users")

        assert result["validation_passed"] is True
        assert result["source_count"] == 1000
        assert result["target_count"] == 1000
        assert result["row_difference"] == 0

    @patch("mssql_pg_migration.validation.OdbcConnectionHelper")
    @patch("mssql_pg_migration.validation.PostgresHook")
    def test_row_count_validation_failure(self, mock_pg_hook, mock_odbc_helper):
        """Test failed row count validation."""
        # Mock SQL Server connection via OdbcConnectionHelper
        mock_odbc = MagicMock()
        mock_odbc.get_first.return_value = (1000,)
        mock_odbc_helper.return_value = mock_odbc

        # Mock PostgreSQL connection
        mock_pg = MagicMock()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_pg.get_conn.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_cursor.fetchone.return_value = (950,)
        mock_pg_hook.return_value = mock_pg

        validator = validation.MigrationValidator("mssql_conn", "pg_conn")
        result = validator.validate_row_count("dbo", "users", "public", "users")

        assert result["validation_passed"] is False
        assert result["source_count"] == 1000
        assert result["target_count"] == 950
        assert result["row_difference"] == -50
        assert result["percentage_difference"] == -5.0

    def test_migration_report_generation(self):
        """Test migration report generation."""
        validation_results = {
            "total_tables": 5,
            "passed_count": 4,
            "failed_count": 1,
            "success_rate": 80.0,
            "overall_success": False,
            "passed_tables": ["users", "products", "orders", "categories"],
            "failed_tables": ["audit_log"],
            "row_count_results": [
                {
                    "table_name": "users",
                    "source_count": 1000,
                    "target_count": 1000,
                    "validation_passed": True,
                    "row_difference": 0,
                },
                {
                    "table_name": "audit_log",
                    "source_count": 5000,
                    "target_count": 4950,
                    "validation_passed": False,
                    "row_difference": -50,
                },
            ],
        }

        report = validation.generate_migration_report(validation_results)

        assert "DATA MIGRATION REPORT" in report
        assert "Total Tables: 5" in report
        assert "Successful: 4" in report
        assert "Failed: 1" in report
        assert "Success Rate: 80.0%" in report
        assert "audit_log" in report
        assert "✓ PASS" in report
        assert "✗ FAIL" in report


class TestSchemaExtractor:
    """Test schema extraction functionality."""

    def test_pattern_matching(self):
        """Test table name pattern matching."""
        from mssql_pg_migration.schema_extractor import SchemaExtractor

        # Create extractor without actual connection
        extractor = SchemaExtractor.__new__(SchemaExtractor)

        assert extractor._matches_pattern("audit_log", "audit_*") is True
        assert extractor._matches_pattern("users", "audit_*") is False
        assert extractor._matches_pattern("temp_table", "temp_*") is True
        assert extractor._matches_pattern("user_history", "*_history") is True
        assert extractor._matches_pattern("products", "*_history") is False


class TestDDLGenerator:
    """Test DDL generation functionality."""

    def test_identifier_quoting(self):
        """Test PostgreSQL identifier quoting."""
        from mssql_pg_migration.ddl_generator import DDLGenerator

        generator = DDLGenerator.__new__(DDLGenerator)

        # Reserved words should be quoted
        assert generator._quote_identifier("user") == '"user"'
        assert generator._quote_identifier("order") == '"order"'
        assert generator._quote_identifier("table") == '"table"'

        # Normal identifiers should be quoted too (safe by default)
        assert generator._quote_identifier("users") == '"users"'
        assert generator._quote_identifier("customer_id") == '"customer_id"'

        # Special characters should trigger quoting
        assert generator._quote_identifier("user-id") == '"user-id"'
        assert generator._quote_identifier("1_table") == '"1_table"'

    def test_drop_table_generation(self):
        """Test DROP TABLE statement generation."""
        from mssql_pg_migration.ddl_generator import DDLGenerator

        generator = DDLGenerator.__new__(DDLGenerator)

        ddl = generator.generate_drop_table("users", "public", cascade=True)
        assert ddl == 'DROP TABLE IF EXISTS "public"."users" CASCADE'

        ddl = generator.generate_drop_table("products", "store", cascade=False)
        assert ddl == 'DROP TABLE IF EXISTS "store"."products"'

    def test_truncate_table_generation(self):
        """Test TRUNCATE TABLE statement generation."""
        from mssql_pg_migration.ddl_generator import DDLGenerator

        generator = DDLGenerator.__new__(DDLGenerator)

        ddl = generator.generate_truncate_table("users", "public", cascade=True)
        assert ddl == 'TRUNCATE TABLE "public"."users" CASCADE'


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v"])