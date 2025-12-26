"""
Diff Detector Module

This module detects new and changed rows between source (SQL Server) and target (PostgreSQL)
tables for incremental loading.

Uses a batched anti-join strategy:
1. Stream source PKs in batches (keyset pagination)
2. Query target for matching PKs
3. New rows = source PKs not in target
4. Changed rows = matching PKs with different row hash (optional)
"""

from typing import Dict, Any, Optional, List, Tuple, Set
from mssql_pg_migration.odbc_helper import OdbcConnectionHelper
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2 import sql
import logging
import os

logger = logging.getLogger(__name__)


def _is_strict_consistency_mode() -> bool:
    """Check if strict consistency mode is enabled (disables NOLOCK)."""
    val = os.environ.get('STRICT_CONSISTENCY', '').lower()
    return val in ('true', '1', 'yes', 'on')


class DiffDetector:
    """
    Detects new and changed rows between source and target tables.

    Uses batched comparison to handle tables of any size without
    loading all PKs into memory at once.
    """

    def __init__(
        self,
        mssql_conn_id: str,
        postgres_conn_id: str,
        batch_size: int = 100000,
    ):
        """
        Initialize the diff detector.

        Args:
            mssql_conn_id: Airflow connection ID for SQL Server
            postgres_conn_id: Airflow connection ID for PostgreSQL
            batch_size: Number of PKs to process per batch
        """
        self.mssql_hook = OdbcConnectionHelper(odbc_conn_id=mssql_conn_id)
        self.postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        self.batch_size = batch_size

    def detect_changes(
        self,
        source_schema: str,
        source_table: str,
        target_schema: str,
        target_table: str,
        pk_columns: List[str],
        compare_columns: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Detect new and optionally changed rows between source and target.

        Strategy:
        1. Stream source PKs in batches using keyset pagination
        2. For each batch, query target to find which PKs exist
        3. New rows = source PKs not found in target
        4. If compare_columns specified, also detect changed rows via hash comparison

        Args:
            source_schema: Source schema in SQL Server
            source_table: Source table name
            target_schema: Target schema in PostgreSQL
            target_table: Target table name
            pk_columns: Primary key column name(s)
            compare_columns: Columns to compare for change detection (None = skip change detection)

        Returns:
            Dict with:
            - new_pks: List of PK tuples for new rows
            - changed_pks: List of PK tuples for changed rows (empty if compare_columns is None)
            - new_count: Number of new rows
            - changed_count: Number of changed rows
            - source_count: Total source row count
            - target_count: Total target row count (matching PKs)
            - unchanged_count: Rows that exist in both with same data
        """
        logger.info(
            f"Detecting changes: {source_schema}.{source_table} -> {target_schema}.{target_table}"
        )
        logger.info(f"PK columns: {pk_columns}, Compare columns: {compare_columns or 'None (skip change detection)'}")

        # Get total source row count
        source_count = self._get_source_count(source_schema, source_table)
        logger.info(f"Source row count: {source_count:,}")

        if source_count == 0:
            return {
                'new_pks': [],
                'changed_pks': [],
                'new_count': 0,
                'changed_count': 0,
                'unchanged_count': 0,
                'source_count': 0,
                'target_count': 0,
            }

        # Process in batches
        new_pks: List[Tuple] = []
        changed_pks: List[Tuple] = []
        unchanged_count = 0
        last_pk_value = None
        batches_processed = 0

        # Determine if we have a single-column PK (can use keyset) or composite
        is_single_pk = len(pk_columns) == 1
        pk_column = pk_columns[0] if is_single_pk else None

        while True:
            # Get next batch of source PKs
            if is_single_pk:
                batch_pks = self._get_source_pks_keyset(
                    source_schema, source_table, pk_column,
                    last_pk_value, self.batch_size
                )
            else:
                # For composite PKs, use offset-based pagination
                offset = batches_processed * self.batch_size
                batch_pks = self._get_source_pks_offset(
                    source_schema, source_table, pk_columns,
                    offset, self.batch_size
                )

            if not batch_pks:
                break

            batches_processed += 1

            # Find which PKs exist in target
            existing_pks = self._find_existing_pks(
                target_schema, target_table, pk_columns, batch_pks
            )

            # New PKs = batch - existing
            batch_new = [pk for pk in batch_pks if pk not in existing_pks]
            new_pks.extend(batch_new)

            # If comparing columns, check for changes in existing rows
            if compare_columns and existing_pks:
                batch_changed = self._find_changed_rows(
                    source_schema, source_table,
                    target_schema, target_table,
                    pk_columns, compare_columns,
                    list(existing_pks)
                )
                changed_pks.extend(batch_changed)
                unchanged_count += len(existing_pks) - len(batch_changed)
            else:
                unchanged_count += len(existing_pks)

            # Update last PK for keyset pagination
            if is_single_pk and batch_pks:
                last_pk_value = batch_pks[-1][0]

            # Log progress
            if batches_processed % 10 == 0:
                logger.info(
                    f"Processed {batches_processed} batches: "
                    f"{len(new_pks):,} new, {len(changed_pks):,} changed"
                )

            # Stop if batch was smaller than requested (end of data)
            if len(batch_pks) < self.batch_size:
                break

        # Get final target count for statistics
        target_count = self._get_target_count(target_schema, target_table)

        result = {
            'new_pks': new_pks,
            'changed_pks': changed_pks,
            'new_count': len(new_pks),
            'changed_count': len(changed_pks),
            'unchanged_count': unchanged_count,
            'source_count': source_count,
            'target_count': target_count,
        }

        logger.info(
            f"Diff detection complete: {result['new_count']:,} new, "
            f"{result['changed_count']:,} changed, {result['unchanged_count']:,} unchanged"
        )

        return result

    def _get_source_count(self, schema: str, table: str) -> int:
        """Get total row count from source table."""
        query = f"SELECT COUNT(*) FROM [{schema}].[{table}]"
        result = self.mssql_hook.get_first(query)
        return result[0] if result else 0

    def _get_target_count(self, schema: str, table: str) -> int:
        """Get total row count from target table."""
        conn = None
        try:
            conn = self.postgres_hook.get_conn()
            with conn.cursor() as cursor:
                cursor.execute(
                    sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
                        sql.Identifier(schema),
                        sql.Identifier(table)
                    )
                )
                result = cursor.fetchone()
                return result[0] if result else 0
        finally:
            if conn:
                conn.close()

    def _get_source_pks_keyset(
        self,
        schema: str,
        table: str,
        pk_column: str,
        last_pk_value: Optional[Any],
        limit: int,
    ) -> List[Tuple]:
        """
        Get source PKs using keyset pagination (efficient for single-column PKs).

        Args:
            schema: Source schema
            table: Source table
            pk_column: Primary key column name
            last_pk_value: Last PK value from previous batch (None for first batch)
            limit: Maximum rows to return

        Returns:
            List of PK tuples
        """
        table_hint = "" if _is_strict_consistency_mode() else " WITH (NOLOCK)"

        if last_pk_value is not None:
            query = f"""
                SELECT TOP {limit} [{pk_column}]
                FROM [{schema}].[{table}]{table_hint}
                WHERE [{pk_column}] > ?
                ORDER BY [{pk_column}]
            """
            result = self.mssql_hook.get_records(query, parameters=[last_pk_value])
        else:
            query = f"""
                SELECT TOP {limit} [{pk_column}]
                FROM [{schema}].[{table}]{table_hint}
                ORDER BY [{pk_column}]
            """
            result = self.mssql_hook.get_records(query)

        return [tuple(row) for row in result] if result else []

    def _get_source_pks_offset(
        self,
        schema: str,
        table: str,
        pk_columns: List[str],
        offset: int,
        limit: int,
    ) -> List[Tuple]:
        """
        Get source PKs using offset pagination (for composite PKs).

        Less efficient than keyset but works for any PK structure.

        Args:
            schema: Source schema
            table: Source table
            pk_columns: Primary key column names
            offset: Number of rows to skip
            limit: Maximum rows to return

        Returns:
            List of PK tuples
        """
        table_hint = "" if _is_strict_consistency_mode() else " WITH (NOLOCK)"
        pk_cols = ', '.join([f'[{col}]' for col in pk_columns])
        order_by = ', '.join([f'[{col}]' for col in pk_columns])

        query = f"""
            SELECT {pk_cols}
            FROM [{schema}].[{table}]{table_hint}
            ORDER BY {order_by}
            OFFSET {offset} ROWS
            FETCH NEXT {limit} ROWS ONLY
        """
        result = self.mssql_hook.get_records(query)
        return [tuple(row) for row in result] if result else []

    def _find_existing_pks(
        self,
        schema: str,
        table: str,
        pk_columns: List[str],
        source_pks: List[Tuple],
    ) -> Set[Tuple]:
        """
        Find which source PKs exist in the target table.

        Args:
            schema: Target schema
            table: Target table
            pk_columns: PK column names
            source_pks: List of PK tuples to check

        Returns:
            Set of PK tuples that exist in target
        """
        if not source_pks:
            return set()

        conn = None
        try:
            conn = self.postgres_hook.get_conn()
            with conn.cursor() as cursor:
                # Build query with VALUES clause for efficiency
                pk_cols_sql = sql.SQL(', ').join([sql.Identifier(c) for c in pk_columns])

                # Create placeholders for VALUES
                num_cols = len(pk_columns)
                row_placeholder = sql.SQL('({})').format(
                    sql.SQL(', ').join([sql.Placeholder()] * num_cols)
                )

                # Build the query
                query = sql.SQL("""
                    SELECT {pk_cols}
                    FROM {schema}.{table}
                    WHERE ({pk_cols}) IN (VALUES {values})
                """).format(
                    pk_cols=pk_cols_sql,
                    schema=sql.Identifier(schema),
                    table=sql.Identifier(table),
                    values=sql.SQL(', ').join([row_placeholder] * len(source_pks))
                )

                # Flatten the PK tuples for parameter binding
                params = []
                for pk in source_pks:
                    params.extend(pk)

                cursor.execute(query, params)
                results = cursor.fetchall()

                return {tuple(row) for row in results}
        except Exception as e:
            logger.error(f"Error finding existing PKs: {e}")
            return set()
        finally:
            if conn:
                conn.close()

    def _find_changed_rows(
        self,
        source_schema: str,
        source_table: str,
        target_schema: str,
        target_table: str,
        pk_columns: List[str],
        compare_columns: List[str],
        existing_pks: List[Tuple],
    ) -> List[Tuple]:
        """
        Find rows that exist in both source and target but have different data.

        Uses hash comparison for efficiency.

        Args:
            source_schema: Source schema
            source_table: Source table
            target_schema: Target schema
            target_table: Target table
            pk_columns: PK column names
            compare_columns: Columns to compare for changes
            existing_pks: PKs that exist in both tables

        Returns:
            List of PK tuples where source data differs from target
        """
        if not existing_pks or not compare_columns:
            return []

        # Get source hashes
        source_hashes = self._get_source_row_hashes(
            source_schema, source_table, pk_columns, compare_columns, existing_pks
        )

        # Get target hashes
        target_hashes = self._get_target_row_hashes(
            target_schema, target_table, pk_columns, compare_columns, existing_pks
        )

        # Find PKs with different hashes
        changed = []
        for pk in existing_pks:
            source_hash = source_hashes.get(pk)
            target_hash = target_hashes.get(pk)
            if source_hash != target_hash:
                changed.append(pk)

        return changed

    def _get_source_row_hashes(
        self,
        schema: str,
        table: str,
        pk_columns: List[str],
        compare_columns: List[str],
        pks: List[Tuple],
    ) -> Dict[Tuple, str]:
        """
        Get row hashes from source for comparison.

        Uses SQL Server HASHBYTES for consistent hashing.
        """
        if not pks:
            return {}

        table_hint = "" if _is_strict_consistency_mode() else " WITH (NOLOCK)"
        pk_cols = ', '.join([f'[{col}]' for col in pk_columns])

        # Build hash expression: HASHBYTES('SHA2_256', CONCAT(...))
        concat_cols = ' + '.join([
            f"ISNULL(CAST([{col}] AS NVARCHAR(MAX)), '')"
            for col in compare_columns
        ])
        hash_expr = f"CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', {concat_cols}), 2)"

        # Build WHERE clause for PKs
        if len(pk_columns) == 1:
            pk_values = ', '.join([self._format_pk_value(pk[0]) for pk in pks])
            where_clause = f"[{pk_columns[0]}] IN ({pk_values})"
        else:
            # Composite PK - use OR of AND conditions
            conditions = []
            for pk in pks:
                pk_condition = ' AND '.join([
                    f"[{col}] = {self._format_pk_value(val)}"
                    for col, val in zip(pk_columns, pk)
                ])
                conditions.append(f"({pk_condition})")
            where_clause = ' OR '.join(conditions)

        query = f"""
            SELECT {pk_cols}, {hash_expr} as row_hash
            FROM [{schema}].[{table}]{table_hint}
            WHERE {where_clause}
        """

        result = self.mssql_hook.get_records(query)

        # Build dict mapping PK tuple -> hash
        num_pk_cols = len(pk_columns)
        return {
            tuple(row[:num_pk_cols]): row[num_pk_cols]
            for row in result
        } if result else {}

    def _get_target_row_hashes(
        self,
        schema: str,
        table: str,
        pk_columns: List[str],
        compare_columns: List[str],
        pks: List[Tuple],
    ) -> Dict[Tuple, str]:
        """
        Get row hashes from target for comparison.

        Uses PostgreSQL MD5 for consistent hashing.
        """
        if not pks:
            return {}

        conn = None
        try:
            conn = self.postgres_hook.get_conn()
            with conn.cursor() as cursor:
                # Build hash expression: MD5(col1 || col2 || ...)
                concat_cols = " || ".join([
                    f"COALESCE({sql.Identifier(col).as_string(conn)}::TEXT, '')"
                    for col in compare_columns
                ])

                pk_cols_sql = sql.SQL(', ').join([sql.Identifier(c) for c in pk_columns])

                # Build VALUES clause for PK filter
                num_cols = len(pk_columns)
                row_placeholder = sql.SQL('({})').format(
                    sql.SQL(', ').join([sql.Placeholder()] * num_cols)
                )

                query = sql.SQL("""
                    SELECT {pk_cols}, MD5({hash_expr}) as row_hash
                    FROM {schema}.{table}
                    WHERE ({pk_cols}) IN (VALUES {values})
                """).format(
                    pk_cols=pk_cols_sql,
                    hash_expr=sql.SQL(concat_cols),
                    schema=sql.Identifier(schema),
                    table=sql.Identifier(table),
                    values=sql.SQL(', ').join([row_placeholder] * len(pks))
                )

                # Flatten params
                params = []
                for pk in pks:
                    params.extend(pk)

                cursor.execute(query, params)
                results = cursor.fetchall()

                # Build dict mapping PK tuple -> hash
                return {
                    tuple(row[:num_cols]): row[num_cols]
                    for row in results
                } if results else {}
        except Exception as e:
            logger.error(f"Error getting target row hashes: {e}")
            return {}
        finally:
            if conn:
                conn.close()

    def _format_pk_value(self, value: Any) -> str:
        """Format a PK value for SQL IN clause."""
        if value is None:
            return "NULL"
        elif isinstance(value, str):
            # Escape single quotes
            escaped = value.replace("'", "''")
            return f"'{escaped}'"
        elif isinstance(value, (int, float)):
            return str(value)
        else:
            # For other types (datetime, uuid, etc.), convert to string
            escaped = str(value).replace("'", "''")
            return f"'{escaped}'"


def detect_table_changes(
    mssql_conn_id: str,
    postgres_conn_id: str,
    table_info: Dict[str, Any],
    compare_columns: Optional[List[str]] = None,
    batch_size: int = 100000,
) -> Dict[str, Any]:
    """
    Convenience function to detect changes for a single table.

    Args:
        mssql_conn_id: SQL Server connection ID
        postgres_conn_id: PostgreSQL connection ID
        table_info: Table information dict with source/target schema and table names
        compare_columns: Columns to compare for change detection (None = detect new only)
        batch_size: PKs to process per batch

    Returns:
        Diff detection result dict
    """
    detector = DiffDetector(mssql_conn_id, postgres_conn_id, batch_size)

    source_schema = table_info.get('source_schema', 'dbo')
    source_table = table_info['table_name']
    target_schema = table_info.get('target_schema', 'public')
    target_table = table_info.get('target_table', source_table)
    pk_columns = table_info.get('pk_columns', [])

    # If pk_columns is a dict (from schema extractor), extract column names
    if isinstance(pk_columns, dict):
        pk_columns = [col['name'] for col in pk_columns.get('columns', [])]

    if not pk_columns:
        raise ValueError(f"No primary key columns found for table {source_table}")

    return detector.detect_changes(
        source_schema=source_schema,
        source_table=source_table,
        target_schema=target_schema,
        target_table=target_table,
        pk_columns=pk_columns,
        compare_columns=compare_columns,
    )
