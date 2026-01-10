"""
Migration Validation DAG using Environment Variables

This DAG validates SQL Server to PostgreSQL migration by comparing row counts.
Tables must be explicitly specified in 'schema.table' format in include_tables.
Target PostgreSQL schema is derived as: {sourcedb}__{sourceschema} (lowercase)

Uses environment variables for connection details to avoid hardcoding and
to work around Airflow 3 SDK connection resolution issues.

Set these environment variables before running:
- MSSQL_HOST, MSSQL_PORT, MSSQL_DATABASE, MSSQL_USERNAME, MSSQL_PASSWORD
- POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DATABASE, POSTGRES_USERNAME, POSTGRES_PASSWORD

Or use the default test values if not set.
"""

from airflow.decorators import dag, task
from airflow.models.param import Param
from pendulum import datetime
from datetime import timedelta
import logging
import os
import pyodbc
import psycopg2
from psycopg2 import sql

from mssql_pg_migration.table_config import (
    expand_include_tables_param,
    validate_include_tables,
    parse_include_tables,
    derive_target_schema,
    get_default_include_tables,
    get_alias_for_hostname,
)
from mssql_pg_migration.type_mapping import sanitize_identifier

logger = logging.getLogger(__name__)


@dag(
    start_date=datetime(2025, 1, 1),
    schedule=None,  # Run manually
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    doc_md=__doc__,
    default_args={
        "owner": "data-team",
        "retries": 2,
    },
    params={
        # Note: source_conn_id/target_conn_id are accepted for API compatibility when
        # triggered by other DAGs, but this DAG uses environment variables for actual
        # connections due to Airflow 3.0 SDK connection resolution issues.
        "source_conn_id": Param(default="mssql_source", type="string"),
        "target_conn_id": Param(default="postgres_target", type="string"),
        "include_tables": Param(
            default=get_default_include_tables(),
            description="Tables to include in 'schema.table' format (e.g., ['dbo.Users', 'dbo.Posts']). "
                        "Defaults from INCLUDE_TABLES env var."
        ),
    },
    tags=["validation", "migration", "env-based"],
)
def validate_migration_env():
    """
    Validate migration using environment variables for connections.
    """

    @task
    def validate_tables(**context) -> str:
        """
        Validate all tables using environment variable connections.
        """
        params = context["params"]

        # Parse and expand include_tables parameter
        include_tables_raw = params.get("include_tables", [])
        include_tables = expand_include_tables_param(include_tables_raw)

        # Fall back to environment variable if empty
        if not include_tables:
            include_tables = get_default_include_tables()

        # Validate include_tables
        validate_include_tables(include_tables)

        # Parse into {schema: [tables]} dict
        schema_tables = parse_include_tables(include_tables)

        logger.info(f"Validating {len(include_tables)} tables from schemas: {list(schema_tables.keys())}")

        # Get connection details from environment with defaults for testing
        mssql_host = os.environ.get('MSSQL_HOST', 'mssql-server')
        mssql_port = int(os.environ.get('MSSQL_PORT', '1433'))
        mssql_database = os.environ.get('MSSQL_DATABASE', 'StackOverflow2010')
        mssql_user = os.environ.get('MSSQL_USERNAME', 'sa')
        mssql_password = os.environ.get('MSSQL_PASSWORD', 'YourStrong@Passw0rd')

        # Build ODBC connection string
        server = f"{mssql_host},{mssql_port}" if mssql_port != 1433 else mssql_host
        mssql_conn_str = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={server};"
            f"DATABASE={mssql_database};"
            f"UID={mssql_user};"
            f"PWD={mssql_password};"
            f"TrustServerCertificate=yes;"
        )

        postgres_config = {
            'host': os.environ.get('POSTGRES_HOST', 'postgres-target'),
            'port': int(os.environ.get('POSTGRES_PORT', '5432')),
            'database': os.environ.get('POSTGRES_DATABASE', 'stackoverflow'),
            'user': os.environ.get('POSTGRES_USERNAME', 'postgres'),
            'password': os.environ.get('POSTGRES_PASSWORD', 'PostgresPassword123'),
        }

        # Derive target schema from source instance and database name
        # Using env vars directly since this DAG doesn't use Airflow connections
        source_db = mssql_database
        instance_name = get_alias_for_hostname(mssql_host)

        # Connect to databases
        logger.info("Connecting to databases...")

        try:
            mssql_conn = pyodbc.connect(mssql_conn_str)
            mssql_cursor = mssql_conn.cursor()
            logger.info(f"Connected to SQL Server: {mssql_host}/{mssql_database}")
        except Exception as e:
            logger.error(f"SQL Server connection failed: {e}")
            return f"SQL Server connection failed: {e}"

        try:
            postgres_conn = psycopg2.connect(**postgres_config)
            postgres_cursor = postgres_conn.cursor()
            logger.info(f"Connected to PostgreSQL: {postgres_config['host']}")
        except Exception as e:
            logger.error(f"PostgreSQL connection failed: {e}")
            mssql_conn.close()
            return f"PostgreSQL connection failed: {e}"

        # Validate tables
        results = []
        passed = 0
        failed = 0
        missing = 0
        total = 0

        for source_schema, tables in schema_tables.items():
            target_schema = derive_target_schema(source_db, source_schema, instance_name)
            logger.info(f"Validating {len(tables)} tables: {source_schema} -> {target_schema}")

            for table_name in sorted(tables):
                total += 1

                # Get source count (use COLLATE for case-insensitive matching)
                try:
                    source_query = """
                        SELECT COALESCE(SUM(p.rows), 0)
                        FROM sys.tables t
                        INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                        LEFT JOIN sys.partitions p ON p.object_id = t.object_id
                            AND p.index_id IN (0, 1)
                        WHERE s.name COLLATE SQL_Latin1_General_CI_AS = ?
                          AND t.name COLLATE SQL_Latin1_General_CI_AS = ?
                        GROUP BY t.name
                    """
                    mssql_cursor.execute(source_query, (source_schema, table_name))
                    result = mssql_cursor.fetchone()
                    source_count = result[0] if result else 0
                except Exception as e:
                    logger.warning(f"Failed to count source table {source_schema}.{table_name}: {e}")
                    source_count = None
                    missing += 1
                    continue

                # Query target (use sanitized table name for PostgreSQL)
                target_table_name = sanitize_identifier(table_name)
                try:
                    query = sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
                        sql.Identifier(target_schema),
                        sql.Identifier(target_table_name),
                    )
                    postgres_cursor.execute(query)
                    target_count = postgres_cursor.fetchone()[0]
                except Exception as e:
                    logger.warning(f"Failed to count target table {target_schema}.{target_table_name}: {e}")
                    target_count = None
                    missing += 1
                    continue

                if source_count == target_count:
                    status = "PASS"
                    passed += 1
                else:
                    status = "FAIL"
                    failed += 1

                diff = target_count - source_count if target_count is not None else 0
                logger.info(
                    f"{status} {source_schema}.{table_name:25} -> {target_table_name:25} | "
                    f"Source: {source_count:>10,} | "
                    f"Target: {target_count:>10,} | "
                    f"Diff: {diff:>+10,}"
                )

        # Clean up
        mssql_conn.close()
        postgres_conn.close()

        # Summary
        success_rate = (passed / total * 100) if total > 0 else 0

        summary = (
            f"\n{'='*60}\n"
            f"VALIDATION SUMMARY\n"
            f"{'='*60}\n"
            f"Tables Checked: {total}\n"
            f"Passed: {passed}\n"
            f"Failed: {failed}\n"
            f"Missing: {missing}\n"
            f"Success Rate: {success_rate:.1f}%\n"
            f"{'='*60}"
        )
        logger.info(summary)

        if missing > 0:
            return f"Incomplete: {missing} tables missing"
        elif failed > 0:
            return f"Failed: {failed} tables have mismatches"
        else:
            return f"Success: All {total} tables match"

    # Execute
    validate_tables()

# Instantiate
validate_migration_env()
