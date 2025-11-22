"""
Standalone Migration Validation DAG (V2 - Direct Connection)

This version uses direct database connections to work around Airflow 3 SDK
connection resolution issues. It validates SQL Server to PostgreSQL migration
by comparing row counts between source and target databases.

Key Features:
- Direct database connections (bypasses Airflow connection issues)
- Single task architecture (no inter-task data passing)
- Dynamic table discovery from INFORMATION_SCHEMA
- Comprehensive reporting with detailed logs

Run this DAG manually after migration to verify data integrity.
"""

from airflow.sdk import dag, task
from airflow.models.param import Param
from pendulum import datetime
from datetime import timedelta
from typing import Dict, List
import logging
import pymssql
import pg8000

logger = logging.getLogger(__name__)


@dag(
    start_date=datetime(2025, 1, 1),
    schedule=None,  # Run manually after migration
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    doc_md=__doc__,
    default_args={
        "owner": "data-team",
        "retries": 0,  # No retries for debugging
    },
    params={
        # Connection IDs (will get details from Airflow connections)
        "source_conn_id": Param(
            default="mssql_source",
            type="string",
            description="SQL Server connection ID"
        ),
        "target_conn_id": Param(
            default="postgres_target",
            type="string",
            description="PostgreSQL connection ID"
        ),
        "source_schema": Param(default="dbo", type="string"),
        "target_schema": Param(default="public", type="string"),
        "exclude_tables": Param(
            default=["sysdiagrams"],
            type="array",
            description="Tables to exclude from validation"
        ),
    },
    tags=["validation", "migration", "standalone-v2", "direct-connection"],
)
def validate_migration_standalone_v2():
    """
    Standalone DAG to validate migration row counts using direct connections.
    """

    @task
    def validate_all_tables_direct(**context) -> str:
        """
        Validate all tables using direct database connections.

        Returns:
            Status string summarizing validation results
        """
        params = context["params"]

        # ===== GET CONNECTION DETAILS FROM AIRFLOW =====
        from airflow.hooks.base import BaseHook

        # Get connection details from Airflow
        try:
            mssql_conn_obj = BaseHook.get_connection(params["source_conn_id"])
            postgres_conn_obj = BaseHook.get_connection(params["target_conn_id"])
        except Exception as e:
            logger.error(f"Failed to get Airflow connections: {e}")
            return f"Failed to get Airflow connections: {e}"

        # ===== CONNECT TO DATABASES DIRECTLY =====
        logger.info("Connecting to databases using Airflow connection details...")

        try:
            # Connect to SQL Server
            mssql_conn = pymssql.connect(
                server=mssql_conn_obj.host,
                port=mssql_conn_obj.port or 1433,
                user=mssql_conn_obj.login,
                password=mssql_conn_obj.password,
                database=mssql_conn_obj.schema
            )
            mssql_cursor = mssql_conn.cursor()
            logger.info(f"✓ Connected to SQL Server: {mssql_conn_obj.host}:{mssql_conn_obj.port}/{mssql_conn_obj.schema}")

        except Exception as e:
            logger.error(f"Failed to connect to SQL Server: {e}")
            return f"Failed to connect to SQL Server: {e}"

        try:
            # Connect to PostgreSQL using pg8000
            postgres_conn = pg8000.connect(
                host=postgres_conn_obj.host,
                port=postgres_conn_obj.port or 5432,
                user=postgres_conn_obj.login,
                password=postgres_conn_obj.password,
                database=postgres_conn_obj.schema
            )
            postgres_cursor = postgres_conn.cursor()
            logger.info(f"✓ Connected to PostgreSQL: {postgres_conn_obj.host}:{postgres_conn_obj.port}/{postgres_conn_obj.schema}")

        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            mssql_conn.close()
            return f"Failed to connect to PostgreSQL: {e}"

        source_schema = params["source_schema"]
        target_schema = params["target_schema"]
        exclude_tables = params.get("exclude_tables", [])

        # ===== DISCOVER TABLES FROM SOURCE =====
        logger.info(f"Discovering tables in source schema: {source_schema}")

        source_query = """
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
            mssql_cursor.execute(source_query, (source_schema,))
            source_results = mssql_cursor.fetchall()
            source_counts = {
                row[0]: row[1]
                for row in source_results
                if row[0] not in exclude_tables
            }
            logger.info(f"Found {len(source_counts)} tables in source schema {source_schema}")

        except Exception as e:
            logger.error(f"Failed to query source tables: {e}")
            mssql_conn.close()
            postgres_conn.close()
            return f"Failed to query source tables: {e}"

        # ===== GET TARGET TABLE COUNTS =====
        logger.info(f"Validating tables in target schema: {target_schema}")

        target_counts = {}
        for table_name in source_counts.keys():
            target_table = table_name.lower()
            target_query = f'SELECT COUNT(*) FROM {target_schema}."{target_table}"'

            try:
                postgres_cursor.execute(target_query)
                result = postgres_cursor.fetchone()
                target_counts[table_name] = result[0] if result else 0
            except Exception as e:
                logger.warning(f"Table {target_schema}.{target_table} not accessible: {e}")
                target_counts[table_name] = None

        # ===== COMPARE AND GENERATE REPORT =====
        results = []
        passed = 0
        failed = 0
        missing = 0
        total_source_rows = 0
        total_target_rows = 0

        for table_name in sorted(source_counts.keys()):
            source_count = source_counts[table_name]
            target_count = target_counts.get(table_name)

            total_source_rows += source_count

            if target_count is None:
                status = "MISSING"
                missing += 1
                diff = None
            else:
                total_target_rows += target_count
                diff = target_count - source_count

                if source_count == target_count:
                    status = "PASS"
                    passed += 1
                else:
                    status = "FAIL"
                    failed += 1

            results.append({
                'table': table_name,
                'source': source_count,
                'target': target_count,
                'diff': diff,
                'status': status,
            })

        # ===== FORMAT AND OUTPUT REPORT =====
        report_lines = [
            "",
            "=" * 80,
            "          MIGRATION VALIDATION REPORT (V2)",
            "=" * 80,
            f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}",
            f"Source: {params['source_conn_id']}.{source_schema}",
            f"Target: {params['target_conn_id']}.{target_schema}",
            "-" * 80,
            f"Tables Validated: {len(source_counts)}",
            f"Passed: {passed}",
            f"Failed: {failed}",
            f"Missing: {missing}",
            f"Success Rate: {passed/len(source_counts)*100:.1f}%" if source_counts else "N/A",
            "-" * 80,
            "TABLE DETAILS:",
            "-" * 80,
        ]

        for r in results:
            status_icon = {
                "PASS": "[✓]",
                "FAIL": "[✗]",
                "MISSING": "[?]"
            }.get(r['status'], "[-]")

            target_str = str(r['target']) if r['target'] is not None else "MISSING"
            diff_str = f"{r['diff']:+,}" if r['diff'] is not None else "N/A"

            report_lines.append(
                f"{status_icon} {r['table']:<25} | Source: {r['source']:>12,} | "
                f"Target: {target_str:>12} | Diff: {diff_str:>10}"
            )

        report_lines.extend([
            "-" * 80,
            f"Total Source Rows: {total_source_rows:,}",
            f"Total Target Rows: {total_target_rows:,}",
            "=" * 80,
        ])

        # Output report
        full_report = "\n".join(report_lines)
        logger.info(full_report)

        # Clean up connections
        mssql_conn.close()
        postgres_conn.close()

        # Return status
        if missing > 0:
            return f"Validation incomplete: {missing} tables missing"
        elif failed > 0:
            return f"Validation failed: {failed} tables have mismatches"
        else:
            return f"Validation successful: All {len(source_counts)} tables match"

    # Execute
    validate_all_tables_direct()

# Instantiate
validate_migration_standalone_v2()