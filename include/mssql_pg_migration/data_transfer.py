"""
Data Transfer Module

This module handles the actual data migration from SQL Server to PostgreSQL,
including chunked reading, bulk loading, and progress tracking.

Uses direct pymssql connections for keyset pagination to avoid issues with
Airflow MSSQL hook's get_pandas_df method on large datasets.
"""

from typing import Dict, Any, Optional, List, Tuple, Iterable
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, date, time as dt_time
from decimal import Decimal
from io import StringIO, TextIOBase
import contextlib
import logging
import threading
import time
import csv
import math
import pymssql
from psycopg2 import pool as pg_pool

logger = logging.getLogger(__name__)


class DataTransfer:
    """Handle data transfer from SQL Server to PostgreSQL."""

    _postgres_pools: Dict[str, pg_pool.ThreadedConnectionPool] = {}
    _pool_lock = threading.Lock()

    def __init__(self, mssql_conn_id: str, postgres_conn_id: str):
        """
        Initialize the data transfer handler.

        Args:
            mssql_conn_id: Airflow connection ID for SQL Server
            postgres_conn_id: Airflow connection ID for PostgreSQL
        """
        self.mssql_hook = MsSqlHook(mssql_conn_id=mssql_conn_id)
        self.postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        self._postgres_conn_id = postgres_conn_id

        # Get direct MSSQL connection parameters for keyset pagination
        # This avoids issues with Airflow hook's get_pandas_df on large datasets
        mssql_conn = self.mssql_hook.get_connection(mssql_conn_id)
        self._mssql_config = {
            'server': mssql_conn.host,
            'port': mssql_conn.port or 1433,
            'database': mssql_conn.schema,
            'user': mssql_conn.login,
            'password': mssql_conn.password,
        }

        # Initialize shared PostgreSQL connection pool for this connection ID
        if postgres_conn_id not in DataTransfer._postgres_pools:
            with DataTransfer._pool_lock:
                if postgres_conn_id not in DataTransfer._postgres_pools:
                    pg_conn = self.postgres_hook.get_connection(postgres_conn_id)
                    DataTransfer._postgres_pools[postgres_conn_id] = pg_pool.ThreadedConnectionPool(
                        minconn=1,
                        maxconn=8,
                        host=pg_conn.host,
                        port=pg_conn.port or 5432,
                        database=pg_conn.schema or pg_conn.login,
                        user=pg_conn.login,
                        password=pg_conn.password,
                    )

    def _acquire_postgres_connection(self):
        pool = DataTransfer._postgres_pools.get(self._postgres_conn_id)
        if pool:
            return pool.getconn()
        return self.postgres_hook.get_conn()

    def _release_postgres_connection(self, conn) -> None:
        if conn is None:
            return
        pool = DataTransfer._postgres_pools.get(self._postgres_conn_id)
        if pool:
            pool.putconn(conn)
        else:
            conn.close()

    @contextlib.contextmanager
    def _postgres_connection(self):
        conn = self._acquire_postgres_connection()
        try:
            yield conn
        finally:
            if conn and getattr(conn, "autocommit", False) is False:
                try:
                    conn.rollback()
                except Exception as e:
                    logger.exception("Exception occurred during PostgreSQL connection rollback")
            self._release_postgres_connection(conn)

    @contextlib.contextmanager
    def _mssql_connection(self):
        conn = pymssql.connect(**self._mssql_config)
        try:
            yield conn
        finally:
            conn.close()

    def transfer_table(
        self,
        source_schema: str,
        source_table: str,
        target_schema: str,
        target_table: str,
        chunk_size: int = 10000,
        truncate_target: bool = True,
        columns: Optional[List[str]] = None,
        where_clause: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Transfer data from SQL Server table to PostgreSQL table.

        Args:
            source_schema: Source schema name in SQL Server
            source_table: Source table name in SQL Server
            target_schema: Target schema name in PostgreSQL
            target_table: Target table name in PostgreSQL
            chunk_size: Number of rows to transfer per batch
            truncate_target: Whether to truncate target table before transfer
            columns: Specific columns to transfer (None for all columns)
            where_clause: Optional WHERE clause for filtering source data

        Returns:
            Transfer result dictionary with statistics
        """
        start_time = time.time()
        logger.info(f"Starting transfer: {source_schema}.{source_table} -> {target_schema}.{target_table}")

        # Get source row count
        source_row_count = self._get_row_count(source_schema, source_table, is_source=True, where_clause=where_clause)
        logger.info(f"Source table has {source_row_count:,} rows{' (filtered)' if where_clause else ''}")

        # Truncate target if requested
        if truncate_target:
            self._truncate_table(target_schema, target_table)
            logger.info(f"Truncated target table {target_schema}.{target_table}")

        # Get column list if not specified
        if not columns:
            columns = self._get_table_columns(source_schema, source_table)
            logger.info(f"Transferring {len(columns)} columns")

        # Right-size chunk size for given table
        optimal_chunk_size = self._calculate_optimal_chunk_size(source_row_count, chunk_size)
        if optimal_chunk_size != chunk_size:
            logger.info(
                "Adjusted chunk size from %s to %s rows based on table volume",
                chunk_size,
                optimal_chunk_size,
            )
            chunk_size = optimal_chunk_size

        rows_transferred = 0
        chunks_processed = 0
        errors = []

        pk_column = self._get_primary_key_column(source_schema, source_table, columns)
        logger.info(f"Using '{pk_column}' for keyset pagination")
        pk_index = columns.index(pk_column) if pk_column in columns else 0

        try:
            with self._mssql_connection() as mssql_conn, self._postgres_connection() as postgres_conn:
                last_key_value = None
                while rows_transferred < source_row_count:
                    chunk_start = time.time()

                    rows, last_key_value = self._read_chunk_keyset(
                        mssql_conn,
                        source_schema,
                        source_table,
                        columns,
                        pk_column,
                        last_key_value,
                        chunk_size,
                        pk_index,
                        where_clause,
                    )

                    if not rows:
                        break

                    rows_written = self._write_chunk(
                        rows,
                        target_schema,
                        target_table,
                        columns,
                        postgres_conn
                    )

                    rows_transferred += rows_written
                    chunks_processed += 1

                    chunk_time = time.time() - chunk_start
                    rows_per_second = rows_written / chunk_time if chunk_time > 0 else 0

                    logger.info(
                        f"Chunk {chunks_processed}: Transferred {rows_written:,} rows "
                        f"({rows_transferred:,}/{source_row_count:,} total) "
                        f"at {rows_per_second:,.0f} rows/sec"
                    )

                    postgres_conn.commit()

        except Exception as e:
            error_msg = f"Error transferring data: {str(e)}"
            logger.error(error_msg)
            errors.append(error_msg)

        # Get final row count in target
        target_row_count = self._get_row_count(target_schema, target_table, is_source=False)

        elapsed_time = time.time() - start_time
        avg_rows_per_second = rows_transferred / elapsed_time if elapsed_time > 0 else 0

        result = {
            'source_table': f"{source_schema}.{source_table}",
            'target_table': f"{target_schema}.{target_table}",
            'source_row_count': source_row_count,
            'target_row_count': target_row_count,
            'rows_transferred': rows_transferred,
            'chunks_processed': chunks_processed,
            'chunk_size': chunk_size,
            'elapsed_time_seconds': elapsed_time,
            'avg_rows_per_second': avg_rows_per_second,
            'success': len(errors) == 0 and target_row_count == source_row_count,
            'errors': errors,
            'timestamp': datetime.now().isoformat(),
        }

        if result['success']:
            logger.info(
                f"Successfully transferred {rows_transferred:,} rows in {elapsed_time:.2f} seconds "
                f"({avg_rows_per_second:,.0f} rows/sec average)"
            )
        else:
            logger.warning(
                f"Transfer completed with issues. Source: {source_row_count:,}, "
                f"Target: {target_row_count:,}, Transferred: {rows_transferred:,}"
            )

        return result

    def _get_row_count(self, schema_name: str, table_name: str, is_source: bool = True, where_clause: Optional[str] = None) -> int:
        """
        Get row count from a table.

        Args:
            schema_name: Schema name
            table_name: Table name
            is_source: Whether this is the source (SQL Server) or target (PostgreSQL)
            where_clause: Optional WHERE clause for filtering (only for source)

        Returns:
            Row count
        """
        if is_source:
            # SQL Server
            query = f"SELECT COUNT(*) FROM [{schema_name}].[{table_name}]"
            if where_clause:
                query += f" WHERE {where_clause}"
            count = self.mssql_hook.get_first(query)[0]
        else:
            # PostgreSQL - use unquoted names (PostgreSQL lowercases them)
            query = f'SELECT COUNT(*) FROM {schema_name}.{table_name}'
            count = self.postgres_hook.get_first(query)[0]

        return count or 0

    def _truncate_table(self, schema_name: str, table_name: str) -> None:
        """
        Truncate a PostgreSQL table.

        Args:
            schema_name: Schema name
            table_name: Table name
        """
        query = f'TRUNCATE TABLE {schema_name}.{table_name} CASCADE'
        self.postgres_hook.run(query)

    def _get_table_columns(self, schema_name: str, table_name: str) -> List[str]:
        """
        Get column names from SQL Server table.

        Args:
            schema_name: Schema name
            table_name: Table name

        Returns:
            List of column names
        """
        query = """
        SELECT c.name
        FROM sys.columns c
        INNER JOIN sys.tables t ON c.object_id = t.object_id
        INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name = %s AND t.name = %s
        ORDER BY c.column_id
        """
        columns = self.mssql_hook.get_records(query, parameters=[schema_name, table_name])
        return [col[0] for col in columns]

    def _get_primary_key_column(
        self,
        schema_name: str,
        table_name: str,
        columns: List[str]
    ) -> str:
        """
        Get the primary key column for keyset pagination.
        Prefers 'Id' column if available, otherwise uses the first column.

        Args:
            schema_name: Schema name
            table_name: Table name
            columns: List of available columns

        Returns:
            Column name to use for keyset pagination
        """
        # Prefer 'Id' column if it exists (case-insensitive)
        for col in columns:
            if col.lower() == 'id':
                return col

        # Try to get actual primary key from database
        query = """
        SELECT c.name
        FROM sys.index_columns ic
        INNER JOIN sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id
        INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        INNER JOIN sys.tables t ON i.object_id = t.object_id
        INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name = %s AND t.name = %s AND i.is_primary_key = 1
        ORDER BY ic.key_ordinal
        """
        try:
            pk_cols = self.mssql_hook.get_records(query, parameters=[schema_name, table_name])
            if pk_cols:
                return pk_cols[0][0]
        except Exception:
            pass

        # Fallback to first column
        return columns[0] if columns else 'Id'

    def _calculate_optimal_chunk_size(self, row_count: int, requested_chunk: int) -> int:
        """Determine an appropriate chunk size based on table volume."""
        if row_count <= 0:
            return requested_chunk

        if row_count < 100_000:
            target = min(requested_chunk, 10_000)
        elif row_count < 1_000_000:
            target = max(requested_chunk, 20_000)
        elif row_count < 5_000_000:
            target = max(requested_chunk, 50_000)
        else:
            target = max(requested_chunk, 100_000)

        return min(max(target, 5_000), 200_000)

    def _read_chunk_keyset(
        self,
        conn,
        schema_name: str,
        table_name: str,
        columns: List[str],
        pk_column: str,
        last_key_value: Optional[Any],
        limit: int,
        pk_index: int,
        where_clause: Optional[str] = None,
    ) -> Tuple[List[Tuple[Any, ...]], Optional[Any]]:
        """Read rows using keyset pagination with deterministic ordering."""

        quoted_columns = ', '.join([f'[{col}]' for col in columns])
        base_query = f"""
        SELECT TOP {limit} {quoted_columns}
        FROM [{schema_name}].[{table_name}] WITH (NOLOCK)
        """
        order_by = f"ORDER BY [{pk_column}]"

        # Build WHERE clause combining filter and pagination
        where_conditions = []
        if where_clause:
            where_conditions.append(f"({where_clause})")
        if last_key_value is not None:
            where_conditions.append(f"[{pk_column}] > %s")

        if where_conditions:
            where_part = "WHERE " + " AND ".join(where_conditions)
            query = f"{base_query}\n{where_part}\n{order_by}"
            params = (last_key_value,) if last_key_value is not None else None
        else:
            query = f"{base_query}\n{order_by}"
            params = None

        try:
            with conn.cursor() as cursor:
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                rows = cursor.fetchall()

                if not rows:
                    return [], last_key_value

                next_key = rows[-1][pk_index]
                return rows, next_key
        except Exception as e:
            logger.error(f"Error reading chunk after key {last_key_value}: {str(e)}")
            raise

    def _write_chunk(
        self,
        rows: List[Tuple[Any, ...]],
        schema_name: str,
        table_name: str,
        columns: List[str],
        postgres_conn
    ) -> int:
        """
        Stream rows to PostgreSQL using COPY.

        Args:
            rows: Sequence of rows to write
            schema_name: Target schema name
            table_name: Target table name
            columns: List of column names
            postgres_conn: Active PostgreSQL connection

        Returns:
            Number of rows written
        """
        if not rows:
            return 0

        column_list = ', '.join(columns)
        copy_sql = (
            f"COPY {schema_name}.{table_name} ({column_list}) "
            "FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', QUOTE '\"', NULL '')"
        )

        stream = _CSVRowStream(rows, self._normalize_value)
        with postgres_conn.cursor() as cursor:
            cursor.copy_expert(copy_sql, stream)

        return len(rows)

    def _normalize_value(self, value: Any) -> Any:
        """Normalize Python values for COPY consumption."""
        if value is None:
            return ''

        if isinstance(value, datetime):
            return value.isoformat(sep=' ')
        if isinstance(value, date):
            return value.isoformat()
        if isinstance(value, dt_time):
            return value.isoformat()
        if isinstance(value, Decimal):
            return str(value)
        if isinstance(value, bool):
            return 't' if value else 'f'
        if isinstance(value, (bytes, bytearray, memoryview)):
            try:
                return bytes(value).decode('utf-8', 'ignore')
            except Exception:
                return ''
        if isinstance(value, float) and not math.isfinite(value):
            return ''

        return value


def transfer_table_data(
    mssql_conn_id: str,
    postgres_conn_id: str,
    table_info: Dict[str, Any],
    chunk_size: int = 10000,
    truncate: bool = True,
    where_clause: Optional[str] = None
) -> Dict[str, Any]:
    """
    Convenience function to transfer a single table.

    Args:
        mssql_conn_id: SQL Server connection ID
        postgres_conn_id: PostgreSQL connection ID
        table_info: Table information dictionary with schema and table names
        chunk_size: Rows per chunk
        truncate: Whether to truncate target before transfer
        where_clause: Optional WHERE clause for filtering source data

    Returns:
        Transfer result dictionary
    """
    transfer = DataTransfer(mssql_conn_id, postgres_conn_id)

    source_schema = table_info.get('source_schema', table_info.get('schema_name', 'dbo'))
    source_table = table_info['table_name']
    target_schema = table_info.get('target_schema', 'public')
    target_table = table_info.get('target_table', source_table)

    return transfer.transfer_table(
        source_schema=source_schema,
        source_table=source_table,
        target_schema=target_schema,
        target_table=target_table,
        chunk_size=chunk_size,
        truncate_target=truncate,
        columns=table_info.get('columns'),
        where_clause=where_clause
    )


def parallel_transfer_tables(
    mssql_conn_id: str,
    postgres_conn_id: str,
    tables: List[Dict[str, Any]],
    chunk_size: int = 10000,
    truncate: bool = True
) -> List[Dict[str, Any]]:
    """
    Transfer multiple tables (designed for use with Airflow's expand operator).

    Args:
        mssql_conn_id: SQL Server connection ID
        postgres_conn_id: PostgreSQL connection ID
        tables: List of table information dictionaries
        chunk_size: Rows per chunk
        truncate: Whether to truncate targets before transfer

    Returns:
        List of transfer result dictionaries
    """
    results = []
    for table_info in tables:
        result = transfer_table_data(
            mssql_conn_id,
            postgres_conn_id,
            table_info,
            chunk_size,
            truncate
        )
        results.append(result)

    return results


class _CSVRowStream(TextIOBase):
    """Lazy text stream that feeds COPY FROM without large buffers."""

    def __init__(self, rows: Iterable[Tuple[Any, ...]], normalizer):
        self._iterator = iter(rows)
        self._normalizer = normalizer
        self._buffer = ''
        self._exhausted = False

    def readable(self) -> bool:
        return True

    def read(self, size: int = -1) -> str:
        while (size < 0 or len(self._buffer) < size) and not self._exhausted:
            try:
                row = next(self._iterator)
            except StopIteration:
                self._exhausted = True
                break
            self._buffer += self._format_row(row)

        if size < 0:
            data = self._buffer
            self._buffer = ''
            return data

        data = self._buffer[:size]
        self._buffer = self._buffer[size:]
        return data

    def _format_row(self, row: Tuple[Any, ...]) -> str:
        buffer = StringIO()
        writer = csv.writer(
            buffer,
            delimiter='\t',
            quoting=csv.QUOTE_MINIMAL,
            lineterminator='\n',
        )
        writer.writerow([self._normalizer(value) for value in row])
        return buffer.getvalue()