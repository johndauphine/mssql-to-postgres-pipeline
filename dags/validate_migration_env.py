"""
Migration Validation DAG

Validates SQL Server to PostgreSQL migration by comparing row counts.
Uses only Airflow connections - no environment variables required.

Table selection (priority order):
1. Config file: config/{database}_include_tables.txt (database from connection)
2. Explicit param: include_tables when triggering DAG

Airflow connections:
- source_conn_id: SQL Server connection (default: mssql_source)
- target_conn_id: PostgreSQL connection (default: postgres_target)
"""

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pendulum import datetime
from datetime import timedelta
from typing import Tuple
import logging

from mssql_pg_migration.odbc_helper import OdbcConnectionHelper
from mssql_pg_migration.table_config import (
    expand_include_tables_param,
    validate_include_tables,
    parse_include_tables,
    derive_target_schema,
    get_alias_for_hostname,
    load_include_tables_from_config,
)
from mssql_pg_migration.type_mapping import sanitize_identifier
from psycopg2 import sql

logger = logging.getLogger(__name__)


def get_connection_info(conn_id: str) -> Tuple[str, str]:
    """
    Get database name and instance alias from Airflow connection.

    Retrieves the connection once and extracts both values to avoid
    duplicate BaseHook.get_connection() calls.

    Args:
        conn_id: Airflow connection ID

    Returns:
        Tuple of (database_name, instance_alias)
    """
    conn = BaseHook.get_connection(conn_id)

    database = conn.schema
    if not database:
        raise ValueError(
            f"Cannot extract database name from connection '{conn_id}'. "
            f"Ensure the connection has a database/schema configured."
        )

    host = conn.host or ''
    instance_alias = get_alias_for_hostname(host)

    return database, instance_alias


@dag(
    dag_id="validate_migration_env",
    start_date=datetime(2025, 1, 1),
    schedule=None,  # Run manually or triggered by migration DAG
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    doc_md=__doc__,
    default_args={
        "owner": "data-team",
        "retries": 2,
    },
    params={
        "source_conn_id": Param(default="mssql_source", type="string"),
        "target_conn_id": Param(default="postgres_target", type="string"),
        "include_tables": Param(
            default=[],
            description="Tables to include in 'schema.table' format (e.g., ['dbo.Users', 'dbo.Posts']). "
                        "If empty, loads from config/{database}_include_tables.txt."
        ),
    },
    tags=["validation", "migration"],
)
def validate_migration_env():
    """
    Validate migration by comparing row counts between source and target.
    """

    @task
    def validate_tables(**context) -> str:
        """
        Validate all tables by comparing row counts.
        """
        params = context["params"]
        source_conn_id = params.get("source_conn_id", "mssql_source")
        target_conn_id = params.get("target_conn_id", "postgres_target")

        # Priority: config file > param/env var
        # Try loading from database-specific config file first
        include_tables = load_include_tables_from_config(source_conn_id)

        # Fall back to explicit param
        if not include_tables:
            include_tables_raw = params.get("include_tables", [])
            include_tables = expand_include_tables_param(include_tables_raw)

        # Raise clear error if no tables configured
        if not include_tables:
            raise ValueError(
                "No tables configured for validation. Either:\n"
                "1. Create config/{database}_include_tables.txt with table names, or\n"
                "2. Provide 'include_tables' param when triggering the DAG"
            )

        # Validate include_tables format
        validate_include_tables(include_tables)

        # Parse into {schema: [tables]} dict
        schema_tables = parse_include_tables(include_tables)

        logger.info(f"Validating {len(include_tables)} tables from schemas: {list(schema_tables.keys())}")

        # Get source database and instance name from connection (single lookup)
        source_db, instance_name = get_connection_info(source_conn_id)

        # Create connection helpers using Airflow connections
        mssql_helper = OdbcConnectionHelper(source_conn_id)
        pg_hook = PostgresHook(postgres_conn_id=target_conn_id)

        logger.info(f"Using connections: source={source_conn_id}, target={target_conn_id}")
        logger.info(f"Source database: {source_db}, instance: {instance_name}")

        # Validate tables
        passed = 0
        failed = 0
        missing = 0
        total_requested = len(include_tables)

        mssql_conn = None
        pg_conn = None

        try:
            # Get connections
            mssql_conn = mssql_helper.get_conn()
            mssql_cursor = mssql_conn.cursor()
            pg_conn = pg_hook.get_conn()
            pg_cursor = pg_conn.cursor()

            for source_schema, tables in schema_tables.items():
                target_schema = derive_target_schema(source_db, source_schema, instance_name)
                logger.info(f"Validating {len(tables)} tables: {source_schema} -> {target_schema}")

                for table_name in sorted(tables):
                    # Get source count using actual COUNT(*) for accuracy
                    # Note: Using dynamic SQL with QUOTENAME for safe identifier handling
                    try:
                        source_query = """
                            DECLARE @sql NVARCHAR(500);
                            SET @sql = N'SELECT COUNT(*) FROM ' + QUOTENAME(?) + N'.' + QUOTENAME(?);
                            EXEC sp_executesql @sql;
                        """
                        mssql_cursor.execute(source_query, (source_schema, table_name))
                        result = mssql_cursor.fetchone()
                        source_count = result[0] if result else 0
                    except Exception as e:
                        logger.warning(f"Failed to count source table {source_schema}.{table_name}: {e}")
                        missing += 1
                        continue

                    # Query target (use sanitized table name for PostgreSQL)
                    target_table_name = sanitize_identifier(table_name)
                    try:
                        query = sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
                            sql.Identifier(target_schema),
                            sql.Identifier(target_table_name),
                        )
                        pg_cursor.execute(query)
                        target_count = pg_cursor.fetchone()[0]
                    except Exception as e:
                        logger.warning(f"Failed to count target table {target_schema}.{target_table_name}: {e}")
                        missing += 1
                        continue

                    # Both counts retrieved successfully - compare them
                    if source_count == target_count:
                        status = "PASS"
                        passed += 1
                    else:
                        status = "FAIL"
                        failed += 1

                    diff = target_count - source_count
                    logger.info(
                        f"{status} {source_schema}.{table_name:25} -> {target_table_name:25} | "
                        f"Source: {source_count:>10,} | "
                        f"Target: {target_count:>10,} | "
                        f"Diff: {diff:>+10,}"
                    )

        finally:
            # Clean up connections
            if mssql_conn:
                mssql_helper.release_conn(mssql_conn)
            if pg_conn:
                pg_conn.close()

        # Summary - calculate success rate based on validated tables only
        validated = passed + failed
        success_rate = (passed / validated * 100) if validated > 0 else 0

        summary = (
            f"\n{'='*60}\n"
            f"VALIDATION SUMMARY\n"
            f"{'='*60}\n"
            f"Tables Requested: {total_requested}\n"
            f"Tables Validated: {validated}\n"
            f"Passed: {passed}\n"
            f"Failed: {failed}\n"
            f"Missing/Errors: {missing}\n"
            f"Success Rate: {success_rate:.1f}% (of validated)\n"
            f"{'='*60}"
        )
        logger.info(summary)

        if missing > 0:
            return f"Incomplete: {missing}/{total_requested} tables could not be validated"
        elif failed > 0:
            return f"Failed: {failed}/{validated} tables have row count mismatches"
        else:
            return f"Success: All {validated} tables match"

    # Execute
    validate_tables()

# Instantiate
validate_migration_env()
