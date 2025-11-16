"""
Data Migration Validation Module

This module provides validation functions to verify data migration success,
including row count comparisons and data integrity checks.
"""

from typing import Dict, Any, List, Optional, Tuple
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class MigrationValidator:
    """Validate data migration from SQL Server to PostgreSQL."""

    def __init__(self, mssql_conn_id: str, postgres_conn_id: str):
        """
        Initialize the migration validator.

        Args:
            mssql_conn_id: Airflow connection ID for SQL Server
            postgres_conn_id: Airflow connection ID for PostgreSQL
        """
        self.mssql_hook = MsSqlHook(mssql_conn_id=mssql_conn_id)
        self.postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)

    def validate_row_count(
        self,
        source_schema: str,
        source_table: str,
        target_schema: str,
        target_table: str
    ) -> Dict[str, Any]:
        """
        Compare row counts between source and target tables.

        Args:
            source_schema: Source schema name in SQL Server
            source_table: Source table name in SQL Server
            target_schema: Target schema name in PostgreSQL
            target_table: Target table name in PostgreSQL

        Returns:
            Validation result dictionary
        """
        # Get source row count
        source_query = f"SELECT COUNT(*) FROM [{source_schema}].[{source_table}]"
        source_count = self.mssql_hook.get_first(source_query)[0] or 0

        # Get target row count
        target_query = f'SELECT COUNT(*) FROM {target_schema}."{target_table}"'
        target_count = self.postgres_hook.get_first(target_query)[0] or 0

        # Calculate difference
        row_difference = target_count - source_count
        percentage_difference = (row_difference / source_count * 100) if source_count > 0 else 0

        validation_result = {
            'table_name': f"{source_schema}.{source_table}",
            'source_count': source_count,
            'target_count': target_count,
            'row_difference': row_difference,
            'percentage_difference': percentage_difference,
            'validation_passed': source_count == target_count,
            'validation_time': datetime.now().isoformat(),
        }

        if validation_result['validation_passed']:
            logger.info(f"✓ Row count validation passed for {source_table}: {source_count:,} rows")
        else:
            logger.warning(
                f"✗ Row count mismatch for {source_table}: "
                f"Source={source_count:,}, Target={target_count:,}, "
                f"Difference={row_difference:+,} ({percentage_difference:+.2f}%)"
            )

        return validation_result

    def validate_sample_data(
        self,
        source_schema: str,
        source_table: str,
        target_schema: str,
        target_table: str,
        sample_size: int = 100,
        key_columns: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Validate sample data between source and target tables.

        Args:
            source_schema: Source schema name in SQL Server
            source_table: Source table name in SQL Server
            target_schema: Target schema name in PostgreSQL
            target_table: Target table name in PostgreSQL
            sample_size: Number of rows to sample
            key_columns: Key columns to use for comparison (if None, uses all columns)

        Returns:
            Sample validation result dictionary
        """
        logger.info(f"Validating sample data for {source_table} (sample size: {sample_size})")

        # Get column names if not specified
        if not key_columns:
            columns_query = """
            SELECT c.name
            FROM sys.columns c
            INNER JOIN sys.tables t ON c.object_id = t.object_id
            INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = %s AND t.name = %s
            ORDER BY c.column_id
            """
            columns = self.mssql_hook.get_records(columns_query, parameters=[source_schema, source_table])
            key_columns = [col[0] for col in columns[:5]]  # Use first 5 columns for comparison

        # Build column lists
        source_columns = ', '.join([f'[{col}]' for col in key_columns])
        target_columns = ', '.join([f'"{col}"' for col in key_columns])

        # Get sample from source
        source_query = f"""
        SELECT TOP {sample_size} {source_columns}
        FROM [{source_schema}].[{source_table}]
        ORDER BY (SELECT NULL)
        """
        source_sample = self.mssql_hook.get_records(source_query)

        # Get sample from target
        target_query = f"""
        SELECT {target_columns}
        FROM {target_schema}."{target_table}"
        LIMIT {sample_size}
        """
        target_sample = self.postgres_hook.get_records(target_query)

        # Compare samples
        matches = 0
        mismatches = []

        for i, source_row in enumerate(source_sample[:min(len(source_sample), len(target_sample))]):
            if i < len(target_sample):
                target_row = target_sample[i]
                # Convert to strings for comparison (handles type differences)
                source_str = str(source_row)
                target_str = str(target_row)

                if source_str == target_str:
                    matches += 1
                else:
                    if len(mismatches) < 5:  # Only keep first 5 mismatches
                        mismatches.append({
                            'row_index': i,
                            'source': source_str[:100],  # Truncate for readability
                            'target': target_str[:100],
                        })

        validation_result = {
            'table_name': f"{source_schema}.{source_table}",
            'sample_size': sample_size,
            'rows_compared': min(len(source_sample), len(target_sample)),
            'matches': matches,
            'mismatches_count': len(source_sample) - matches if len(source_sample) == len(target_sample) else -1,
            'sample_mismatches': mismatches,
            'validation_passed': matches == min(len(source_sample), len(target_sample)),
            'columns_compared': key_columns,
        }

        if validation_result['validation_passed']:
            logger.info(f"✓ Sample data validation passed for {source_table}")
        else:
            logger.warning(f"✗ Sample data validation found differences for {source_table}")

        return validation_result

    def validate_tables_batch(
        self,
        tables: List[Dict[str, Any]],
        validate_samples: bool = False
    ) -> Dict[str, Any]:
        """
        Validate multiple tables in batch.

        Args:
            tables: List of table information dictionaries
            validate_samples: Whether to also validate sample data

        Returns:
            Batch validation results
        """
        results = {
            'total_tables': len(tables),
            'passed_tables': [],
            'failed_tables': [],
            'row_count_results': [],
            'sample_results': [],
            'overall_success': True,
            'validation_time': datetime.now().isoformat(),
        }

        for table_info in tables:
            source_schema = table_info.get('source_schema', table_info.get('schema_name', 'dbo'))
            source_table = table_info['table_name']
            target_schema = table_info.get('target_schema', 'public')
            target_table = table_info.get('target_table', source_table)

            # Validate row count
            row_count_result = self.validate_row_count(
                source_schema,
                source_table,
                target_schema,
                target_table
            )
            results['row_count_results'].append(row_count_result)

            if row_count_result['validation_passed']:
                results['passed_tables'].append(source_table)
            else:
                results['failed_tables'].append(source_table)
                results['overall_success'] = False

            # Validate sample data if requested
            if validate_samples:
                sample_result = self.validate_sample_data(
                    source_schema,
                    source_table,
                    target_schema,
                    target_table
                )
                results['sample_results'].append(sample_result)

        results['passed_count'] = len(results['passed_tables'])
        results['failed_count'] = len(results['failed_tables'])
        results['success_rate'] = (
            results['passed_count'] / results['total_tables'] * 100
            if results['total_tables'] > 0 else 0
        )

        return results


def generate_migration_report(
    validation_results: Dict[str, Any],
    transfer_results: Optional[List[Dict[str, Any]]] = None
) -> str:
    """
    Generate a human-readable migration report.

    Args:
        validation_results: Validation results from validate_tables_batch
        transfer_results: Optional transfer results from data_transfer module

    Returns:
        Formatted report string
    """
    report_lines = [
        "=" * 80,
        "DATA MIGRATION REPORT",
        "=" * 80,
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
    ]

    # Overall Summary
    report_lines.extend([
        "SUMMARY",
        "-" * 40,
        f"Total Tables: {validation_results.get('total_tables', 0)}",
        f"Successful: {validation_results.get('passed_count', 0)}",
        f"Failed: {validation_results.get('failed_count', 0)}",
        f"Success Rate: {validation_results.get('success_rate', 0):.1f}%",
        "",
    ])

    # Transfer Statistics (if available)
    if transfer_results:
        total_rows = sum(r.get('rows_transferred', 0) for r in transfer_results)
        total_time = sum(r.get('elapsed_time_seconds', 0) for r in transfer_results)
        avg_rate = total_rows / total_time if total_time > 0 else 0

        report_lines.extend([
            "TRANSFER STATISTICS",
            "-" * 40,
            f"Total Rows Transferred: {total_rows:,}",
            f"Total Time: {total_time:.2f} seconds",
            f"Average Transfer Rate: {avg_rate:,.0f} rows/second",
            "",
        ])

    # Table Details
    report_lines.extend([
        "TABLE DETAILS",
        "-" * 40,
    ])

    for result in validation_results.get('row_count_results', []):
        status = "✓ PASS" if result['validation_passed'] else "✗ FAIL"
        table_name = result['table_name']
        source_count = result['source_count']
        target_count = result['target_count']
        diff = result['row_difference']

        if result['validation_passed']:
            report_lines.append(
                f"{status} | {table_name:<30} | {source_count:>10,} rows"
            )
        else:
            report_lines.append(
                f"{status} | {table_name:<30} | Source: {source_count:>10,} | "
                f"Target: {target_count:>10,} | Diff: {diff:>+10,}"
            )

    # Failed Tables Summary (if any)
    if validation_results.get('failed_tables'):
        report_lines.extend([
            "",
            "FAILED TABLES REQUIRING ATTENTION",
            "-" * 40,
        ])
        for table in validation_results['failed_tables']:
            report_lines.append(f"  • {table}")

    # Sample Validation Results (if available)
    if validation_results.get('sample_results'):
        sample_failures = [
            r for r in validation_results['sample_results']
            if not r['validation_passed']
        ]
        if sample_failures:
            report_lines.extend([
                "",
                "SAMPLE DATA VALIDATION ISSUES",
                "-" * 40,
            ])
            for result in sample_failures:
                report_lines.append(
                    f"  • {result['table_name']}: {result['mismatches_count']} mismatches "
                    f"in {result['rows_compared']} rows sampled"
                )

    # Footer
    report_lines.extend([
        "",
        "=" * 80,
        "END OF REPORT",
        "=" * 80,
    ])

    return "\\n".join(report_lines)


def validate_migration(
    mssql_conn_id: str,
    postgres_conn_id: str,
    tables: List[Dict[str, Any]],
    validate_samples: bool = False,
    transfer_results: Optional[List[Dict[str, Any]]] = None
) -> Dict[str, Any]:
    """
    Convenience function to validate a complete migration.

    Args:
        mssql_conn_id: SQL Server connection ID
        postgres_conn_id: PostgreSQL connection ID
        tables: List of table information
        validate_samples: Whether to validate sample data
        transfer_results: Optional transfer results for reporting

    Returns:
        Complete validation results with report
    """
    validator = MigrationValidator(mssql_conn_id, postgres_conn_id)

    # Validate all tables
    validation_results = validator.validate_tables_batch(tables, validate_samples)

    # Generate report
    report = generate_migration_report(validation_results, transfer_results)

    # Log the report
    logger.info("\\n" + report)

    # Add report to results
    validation_results['report'] = report

    return validation_results