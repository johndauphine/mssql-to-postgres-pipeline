"""
Migration Validation DAG

This DAG validates SQL Server to PostgreSQL migration by comparing row counts.
Tables must be explicitly specified in 'schema.table' format in include_tables.
Target PostgreSQL schema is derived as: {instance}__{sourcedb}__{sourceschema} (lowercase)

Uses the same Airflow connections as the migration DAGs:
- source_conn_id: Airflow connection ID for SQL Server (default: mssql_source)
- target_conn_id: Airflow connection ID for PostgreSQL (default: postgres_target)
"""

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pendulum import datetime
from datetime import timedelta
import logging

from mssql_pg_migration.odbc_helper import OdbcConnectionHelper
from mssql_pg_migration.table_config import (
    expand_include_tables_param,
    validate_include_tables,
    parse_include_tables,
    derive_target_schema,
    get_default_include_tables,
    get_source_database,
    get_instance_name,
)
from mssql_pg_migration.type_mapping import sanitize_identifier
from psycopg2 import sql

logger = logging.getLogger(__name__)


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
            default=get_default_include_tables(),
            description="Tables to include in 'schema.table' format (e.g., ['dbo.Users', 'dbo.Posts']). "
                        "Defaults from INCLUDE_TABLES env var."
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

        # Get source database and instance name from connection
        source_db = get_source_database(source_conn_id)
        instance_name = get_instance_name(source_conn_id)

        # Create connection helpers using Airflow connections
        mssql_helper = OdbcConnectionHelper(source_conn_id)
        pg_hook = PostgresHook(postgres_conn_id=target_conn_id)

        logger.info(f"Using connections: source={source_conn_id}, target={target_conn_id}")
        logger.info(f"Source database: {source_db}, instance: {instance_name}")

        # Validate tables
        results = []
        passed = 0
        failed = 0
        missing = 0
        total = 0

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
                    total += 1

                    # Get source count
                    try:
                        source_query = """
                            SELECT COALESCE(SUM(p.rows), 0)
                            FROM sys.tables t
                            INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                            LEFT JOIN sys.partitions p ON p.object_id = t.object_id
                                AND p.index_id IN (0, 1)
                            WHERE s.name = ? AND t.name = ?
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
                        pg_cursor.execute(query)
                        target_count = pg_cursor.fetchone()[0]
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

        finally:
            # Clean up connections
            if mssql_conn:
                mssql_helper.release_conn(mssql_conn)
            if pg_conn:
                pg_conn.close()

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
