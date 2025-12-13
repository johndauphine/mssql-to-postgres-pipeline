"""
Migration Validation DAG using Environment Variables

This DAG validates SQL Server to PostgreSQL migration by comparing row counts.
It uses environment variables for connection details to avoid hardcoding and
to work around Airflow 3 SDK connection resolution issues.

Set these environment variables before running:
- MSSQL_HOST, MSSQL_PORT, MSSQL_DATABASE, MSSQL_USERNAME, MSSQL_PASSWORD
- POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DATABASE, POSTGRES_USERNAME, POSTGRES_PASSWORD

Or use the default test values if not set.
"""

from airflow.sdk import dag, task
from airflow.models.param import Param
from pendulum import datetime
from datetime import timedelta
import logging
import os
import pymssql
import psycopg2
from psycopg2 import sql

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
        "source_schema": Param(default="dbo", type="string"),
        "target_schema": Param(default="public", type="string"),
        "exclude_tables": Param(
            default=["sysdiagrams"],
            type="array",
            description="Tables to exclude from validation"
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

        # Get connection details from environment with defaults for testing
        mssql_config = {
            'server': os.environ.get('MSSQL_HOST', 'mssql-server'),
            'port': int(os.environ.get('MSSQL_PORT', '1433')),
            'database': os.environ.get('MSSQL_DATABASE', 'StackOverflow2010'),
            'user': os.environ.get('MSSQL_USERNAME', 'sa'),
            'password': os.environ.get('MSSQL_PASSWORD', 'YourStrong@Passw0rd'),
        }

        postgres_config = {
            'host': os.environ.get('POSTGRES_HOST', 'postgres-target'),
            'port': int(os.environ.get('POSTGRES_PORT', '5432')),
            'database': os.environ.get('POSTGRES_DATABASE', 'stackoverflow'),
            'user': os.environ.get('POSTGRES_USERNAME', 'postgres'),
            'password': os.environ.get('POSTGRES_PASSWORD', 'PostgresPassword123'),
        }

        source_schema = params["source_schema"]
        target_schema = params["target_schema"]
        exclude_tables = params.get("exclude_tables", [])

        # Connect to databases
        logger.info("Connecting to databases...")

        try:
            mssql_conn = pymssql.connect(**mssql_config)
            mssql_cursor = mssql_conn.cursor()
            logger.info(f"✓ Connected to SQL Server: {mssql_config['server']}")
        except Exception as e:
            logger.error(f"SQL Server connection failed: {e}")
            return f"SQL Server connection failed: {e}"

        try:
            postgres_conn = psycopg2.connect(**postgres_config)
            postgres_cursor = postgres_conn.cursor()
            logger.info(f"✓ Connected to PostgreSQL: {postgres_config['host']}")
        except Exception as e:
            logger.error(f"PostgreSQL connection failed: {e}")
            mssql_conn.close()
            return f"PostgreSQL connection failed: {e}"

        # Discover tables from source
        logger.info(f"Discovering tables in {source_schema}...")

        discovery_query = """
            SELECT
                t.TABLE_NAME,
                COALESCE(SUM(p.rows), 0) AS row_count
            FROM INFORMATION_SCHEMA.TABLES t
            LEFT JOIN sys.tables st ON st.name = t.TABLE_NAME
            LEFT JOIN sys.schemas ss ON ss.schema_id = st.schema_id
                AND ss.name = t.TABLE_SCHEMA
            LEFT JOIN sys.partitions p ON p.object_id = st.object_id
                AND p.index_id IN (0, 1)
            WHERE t.TABLE_SCHEMA = %s
              AND t.TABLE_TYPE = 'BASE TABLE'
            GROUP BY t.TABLE_NAME
            ORDER BY t.TABLE_NAME
        """

        try:
            mssql_cursor.execute(discovery_query, (source_schema,))
            source_results = mssql_cursor.fetchall()
            source_counts = {
                row[0]: row[1]
                for row in source_results
                if row[0] not in exclude_tables
            }
        except Exception as e:
            logger.error(f"Failed to query source: {e}")
            mssql_conn.close()
            postgres_conn.close()
            return f"Failed to query source: {e}"

        # Get target counts
        logger.info(f"Validating {len(source_counts)} tables...")

        results = []
        passed = 0
        failed = 0
        missing = 0

        for table_name in sorted(source_counts.keys()):
            source_count = source_counts[table_name]

            # Query target
            try:
                # Preserve identifier case to match how tables were created; quote safely
                query = sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
                    sql.Identifier(target_schema),
                    sql.Identifier(table_name),
                )
                postgres_cursor.execute(query)
                target_count = postgres_cursor.fetchone()[0]
            except Exception as e:
                logger.warning(f"Failed to count target table {target_schema}.{table_name}: {e}")
                target_count = None
                missing += 1

            if target_count is not None:
                if source_count == target_count:
                    status = "✓"
                    passed += 1
                else:
                    status = "✗"
                    failed += 1

                logger.info(
                    f"{status} {table_name:25} | "
                    f"Source: {source_count:>10,} | "
                    f"Target: {target_count:>10,} | "
                    f"Diff: {target_count - source_count:>+10,}"
                )
            else:
                logger.warning(f"? {table_name:25} | MISSING IN TARGET")

        # Clean up
        mssql_conn.close()
        postgres_conn.close()

        # Summary
        total = len(source_counts)
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
