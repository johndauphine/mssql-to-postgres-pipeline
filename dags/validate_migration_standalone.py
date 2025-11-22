"""
Standalone Migration Validation DAG

This DAG validates SQL Server to PostgreSQL migration by comparing row counts
between source and target databases. It operates independently without relying
on XCom data from other tasks, avoiding Airflow 3 XCom resolution bugs.

Key Features:
- Single task architecture (no inter-task data passing)
- Dynamic table discovery from INFORMATION_SCHEMA
- Direct database queries for real-time validation
- Comprehensive reporting with detailed logs

Run this DAG manually after migration to verify data integrity.
"""

from airflow.sdk import dag, task
from airflow.models.param import Param
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pendulum import datetime
from datetime import timedelta
from typing import Dict, List, Tuple
import logging

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
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
        "retry_exponential_backoff": False,
    },
    params={
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
        "source_schema": Param(
            default="dbo",
            type="string",
            description="Source schema in SQL Server"
        ),
        "target_schema": Param(
            default="public",
            type="string",
            description="Target schema in PostgreSQL"
        ),
        "exclude_tables": Param(
            default=["sysdiagrams"],
            type="array",
            description="Tables to exclude from validation"
        ),
        "fail_on_mismatch": Param(
            default=False,
            type="boolean",
            description="Whether to fail the task if row counts don't match"
        ),
    },
    tags=["validation", "migration", "standalone", "mssql", "postgres"],
)
def validate_migration_standalone():
    """
    Standalone DAG to validate migration row counts.
    Does not use XCom for inter-task communication.
    """

    @task
    def validate_all_tables(**context) -> str:
        """
        Single task that validates all tables by comparing row counts
        between SQL Server source and PostgreSQL target.

        Returns:
            Status string summarizing validation results
        """
        params = context["params"]

        # Initialize database hooks
        logger.info(f"Connecting to source: {params['source_conn_id']}")
        mssql_hook = MsSqlHook(mssql_conn_id=params["source_conn_id"])

        logger.info(f"Connecting to target: {params['target_conn_id']}")
        postgres_hook = PostgresHook(postgres_conn_id=params["target_conn_id"])

        source_schema = params["source_schema"]
        target_schema = params["target_schema"]
        exclude_tables = params.get("exclude_tables", [])
        fail_on_mismatch = params.get("fail_on_mismatch", False)

        # ===== DISCOVER TABLES FROM SOURCE =====
        logger.info(f"Discovering tables in source schema: {source_schema}")

        # Get all user tables with row counts from SQL Server
        # Using sys.partitions for accurate row counts
        source_query = """
            SELECT
                t.TABLE_NAME,
                COALESCE(SUM(p.rows), 0) AS row_count
            FROM INFORMATION_SCHEMA.TABLES t
            LEFT JOIN sys.tables st ON st.name = t.TABLE_NAME
            LEFT JOIN sys.schemas ss ON ss.schema_id = st.schema_id
                AND ss.name = t.TABLE_SCHEMA
            LEFT JOIN sys.partitions p ON p.object_id = st.object_id
                AND p.index_id IN (0, 1)  -- Heap or clustered index
            WHERE t.TABLE_SCHEMA = %s
              AND t.TABLE_TYPE = 'BASE TABLE'
            GROUP BY t.TABLE_NAME
            ORDER BY t.TABLE_NAME
        """

        try:
            source_results = mssql_hook.get_records(source_query, parameters=[source_schema])
            source_counts = {
                row[0]: row[1]
                for row in source_results
                if row[0] not in exclude_tables
            }
            logger.info(f"Found {len(source_counts)} tables in source schema {source_schema}")

            if not source_counts:
                logger.warning(f"No tables found in source schema {source_schema}")
                return "No tables found in source schema"

        except Exception as e:
            logger.error(f"Failed to query source tables: {str(e)}")
            raise

        # ===== GET TARGET TABLE COUNTS =====
        logger.info(f"Validating tables in target schema: {target_schema}")

        target_counts = {}
        for table_name in source_counts.keys():
            # PostgreSQL typically uses lowercase table names
            target_table = table_name.lower()

            # Use actual count for accuracy (pg_stat_user_tables can be stale)
            target_query = f'SELECT COUNT(*) FROM {target_schema}."{target_table}"'

            try:
                count_result = postgres_hook.get_first(target_query)
                target_counts[table_name] = count_result[0] if count_result else 0
            except Exception as e:
                # Table might not exist in target
                logger.warning(f"Table {target_schema}.{target_table} not accessible: {str(e)}")
                target_counts[table_name] = None

        # ===== COMPARE AND GENERATE REPORT =====
        results = []
        passed = 0
        failed = 0
        missing = 0
        total_source_rows = 0
        total_target_rows = 0

        # Build detailed results
        for table_name in sorted(source_counts.keys()):
            source_count = source_counts[table_name]
            target_count = target_counts.get(table_name)

            total_source_rows += source_count

            if target_count is None:
                status = "MISSING"
                missing += 1
                diff = None
                percentage = None
            else:
                total_target_rows += target_count
                diff = target_count - source_count
                percentage = (diff / source_count * 100) if source_count > 0 else 0

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
                'percentage': percentage,
                'status': status,
            })

        # ===== FORMAT AND OUTPUT REPORT =====
        report_lines = [
            "",
            "=" * 80,
            "          MIGRATION VALIDATION REPORT",
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
            f"{'Status':<8} {'Table':<25} {'Source':>12} {'Target':>12} {'Diff':>12} {'%':>8}",
            "-" * 80,
        ]

        # Add table details
        for r in results:
            status_icon = {
                "PASS": "[✓]",
                "FAIL": "[✗]",
                "MISSING": "[?]"
            }.get(r['status'], "[-]")

            target_str = str(r['target']) if r['target'] is not None else "MISSING"
            diff_str = f"{r['diff']:+,}" if r['diff'] is not None else "N/A"
            pct_str = f"{r['percentage']:+.1f}%" if r['percentage'] is not None else "N/A"

            report_lines.append(
                f"{status_icon:<8} {r['table']:<25} {r['source']:>12,} {target_str:>12} {diff_str:>12} {pct_str:>8}"
            )

        # Add summary
        report_lines.extend([
            "-" * 80,
            "SUMMARY:",
            f"  Total Source Rows: {total_source_rows:,}",
            f"  Total Target Rows: {total_target_rows:,}",
            f"  Total Difference: {total_target_rows - total_source_rows:+,}",
            "=" * 80,
        ])

        # Failed tables detail (if any)
        if failed > 0:
            report_lines.extend([
                "",
                "FAILED TABLES REQUIRING ATTENTION:",
                "-" * 40,
            ])
            for r in results:
                if r['status'] == 'FAIL':
                    report_lines.append(
                        f"  • {r['table']}: {r['source']:,} → {r['target']:,} "
                        f"(diff: {r['diff']:+,}, {r['percentage']:+.1f}%)"
                    )

        # Missing tables detail (if any)
        if missing > 0:
            report_lines.extend([
                "",
                "MISSING TABLES IN TARGET:",
                "-" * 40,
            ])
            for r in results:
                if r['status'] == 'MISSING':
                    report_lines.append(f"  • {r['table']} ({r['source']:,} rows)")

        report_lines.append("")

        # Output full report to logs
        full_report = "\n".join(report_lines)
        logger.info(full_report)

        # Build return status
        if missing > 0:
            status_msg = f"Validation incomplete: {missing} tables missing in target"
            if fail_on_mismatch:
                raise ValueError(status_msg)
        elif failed > 0:
            status_msg = f"Validation failed: {failed}/{len(source_counts)} tables have row count mismatches"
            if fail_on_mismatch:
                raise ValueError(status_msg)
        else:
            status_msg = f"Validation successful: All {len(source_counts)} tables match perfectly"

        logger.info(f"Final status: {status_msg}")
        return status_msg

    # Execute the validation task
    validation_status = validate_all_tables()

# Instantiate the DAG
validate_migration_standalone()