"""
Data Transfer Module

This module handles the actual data migration from SQL Server to PostgreSQL,
including chunked reading, bulk loading, and progress tracking.

Uses direct pyodbc connections for keyset pagination to avoid issues with
Airflow MSSQL hook's get_pandas_df method on large datasets.
"""

from typing import Dict, Any, Optional, List, Tuple, Iterable, Iterator
from mssql_pg_migration.odbc_helper import OdbcConnectionHelper
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, date, time as dt_time
from decimal import Decimal
from io import StringIO, TextIOBase
from concurrent.futures import ThreadPoolExecutor, Future
import contextlib
import logging
import os
import queue
import threading
import time
import csv
import math
import pyodbc
from psycopg2 import pool as pg_pool
from psycopg2 import sql

logger = logging.getLogger(__name__)


def _get_parallel_reader_config() -> Tuple[int, int]:
    """
    Get parallel reader configuration from environment variables.

    Returns:
        Tuple of (num_readers, queue_size)
    """
    num_readers = int(os.environ.get('PARALLEL_READERS', '1'))
    queue_size = int(os.environ.get('READER_QUEUE_SIZE', '5'))
    return max(1, num_readers), max(1, queue_size)


class ParallelReader:
    """
    Manages multiple SQL Server reader threads that feed a bounded queue.

    Each reader thread has its own MSSQL connection (pyodbc connections are not
    thread-safe) and reads a disjoint row range. Chunks are placed in a bounded
    queue that the writer consumes.

    Uses backpressure: if queue is full, readers block until writer consumes.
    """

    # Sentinel value to signal end of data
    _DONE = object()

    def __init__(
        self,
        mssql_config: Dict[str, str],
        num_readers: int,
        queue_size: int,
    ):
        """
        Initialize the parallel reader manager.

        Args:
            mssql_config: MSSQL connection configuration dict
            num_readers: Number of parallel reader threads
            queue_size: Maximum chunks buffered in queue (backpressure)
        """
        self.mssql_config = mssql_config
        self.num_readers = num_readers
        self.chunk_queue: queue.Queue = queue.Queue(maxsize=queue_size)
        self.executor: Optional[ThreadPoolExecutor] = None
        self.futures: List[Future] = []
        self.cancel_event = threading.Event()
        self.error: Optional[Exception] = None
        self._error_lock = threading.Lock()
        self._readers_done = 0
        self._done_lock = threading.Lock()

    def _create_mssql_connection(self) -> pyodbc.Connection:
        """Create a new MSSQL connection for a reader thread."""
        conn_str = ';'.join([f"{k}={v}" for k, v in self.mssql_config.items() if v])
        return pyodbc.connect(conn_str)

    def _set_error(self, error: Exception) -> None:
        """Thread-safe error setter."""
        with self._error_lock:
            if self.error is None:
                self.error = error
                self.cancel_event.set()

    def _reader_done(self) -> None:
        """Signal that a reader has finished."""
        with self._done_lock:
            self._readers_done += 1
            if self._readers_done >= self.num_readers:
                # All readers done, signal end to consumer
                self.chunk_queue.put(self._DONE)

    def _reader_thread(
        self,
        reader_id: int,
        schema_name: str,
        table_name: str,
        columns: List[str],
        order_by_columns: List[str],
        start_row: int,
        end_row: int,
        chunk_size: int,
        where_clause: Optional[str],
        read_func,
    ) -> None:
        """
        Single reader thread - reads assigned row range and queues chunks.

        Args:
            reader_id: Reader identifier for logging
            schema_name: Source schema
            table_name: Source table
            columns: Columns to select
            order_by_columns: ORDER BY columns
            start_row: First row to read (1-indexed)
            end_row: Last row to read
            chunk_size: Rows per chunk
            where_clause: Optional filter
            read_func: Function to read a chunk (from DataTransfer)
        """
        conn = None
        try:
            conn = self._create_mssql_connection()
            current_start = start_row

            while current_start <= end_row and not self.cancel_event.is_set():
                current_end = min(current_start + chunk_size - 1, end_row)

                # Read chunk using the provided read function
                rows = read_func(
                    conn,
                    schema_name,
                    table_name,
                    columns,
                    order_by_columns,
                    current_start,
                    current_end,
                    where_clause,
                )

                if not rows:
                    break

                if self.cancel_event.is_set():
                    break

                # Put chunk in queue (blocks if full - backpressure)
                try:
                    self.chunk_queue.put(
                        (reader_id, current_start, current_end, rows),
                        timeout=30.0
                    )
                except queue.Full:
                    if self.cancel_event.is_set():
                        break
                    # Retry once more
                    self.chunk_queue.put(
                        (reader_id, current_start, current_end, rows),
                        timeout=60.0
                    )

                current_start = current_end + 1

            logger.debug(f"Reader {reader_id} finished (rows {start_row}-{end_row})")

        except Exception as e:
            logger.error(f"Reader {reader_id} error: {e}")
            self._set_error(e)
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass
            self._reader_done()

    def _keyset_reader_thread(
        self,
        reader_id: int,
        schema_name: str,
        table_name: str,
        columns: List[str],
        pk_column: str,
        pk_index: int,
        min_pk: Any,
        max_pk: Any,
        chunk_size: int,
        base_where_clause: Optional[str],
        read_func,
    ) -> None:
        """
        Keyset reader thread - reads assigned PK range using keyset pagination.

        Args:
            reader_id: Reader identifier for logging
            schema_name: Source schema
            table_name: Source table
            columns: Columns to select
            pk_column: Primary key column name
            pk_index: Index of PK column in columns list
            min_pk: Minimum PK value for this reader (inclusive)
            max_pk: Maximum PK value for this reader (inclusive)
            chunk_size: Rows per chunk
            base_where_clause: Optional base filter
            read_func: Function to read a keyset chunk
        """
        conn = None
        chunks_read = 0
        total_rows = 0
        try:
            conn = self._create_mssql_connection()
            last_key_value = None
            is_first_chunk = True

            while not self.cancel_event.is_set():
                # Build WHERE clause for this reader's PK range
                range_conditions = []
                if base_where_clause:
                    range_conditions.append(f"({base_where_clause})")

                # For first chunk, start from min_pk
                # For subsequent chunks, use keyset pagination (pk > last_key)
                if is_first_chunk:
                    range_conditions.append(f"[{pk_column}] >= ?")
                    start_param = min_pk
                    is_first_chunk = False
                else:
                    range_conditions.append(f"[{pk_column}] > ?")
                    start_param = last_key_value

                # Always limit to max_pk
                range_conditions.append(f"[{pk_column}] <= ?")

                where_clause = " AND ".join(range_conditions)

                # Read chunk using keyset pagination
                rows, new_last_key = read_func(
                    conn,
                    schema_name,
                    table_name,
                    columns,
                    pk_column,
                    start_param,
                    chunk_size,
                    pk_index,
                    where_clause,
                    max_pk,
                )

                if not rows:
                    break

                if self.cancel_event.is_set():
                    break

                chunks_read += 1
                total_rows += len(rows)
                last_key_value = new_last_key

                # Put chunk in queue (blocks if full - backpressure)
                try:
                    self.chunk_queue.put(
                        (reader_id, chunks_read, total_rows, rows),
                        timeout=30.0
                    )
                except queue.Full:
                    if self.cancel_event.is_set():
                        break
                    self.chunk_queue.put(
                        (reader_id, chunks_read, total_rows, rows),
                        timeout=60.0
                    )

                # Stop if we've reached the end of our range
                if new_last_key is None or new_last_key >= max_pk:
                    break

            logger.debug(
                f"Keyset reader {reader_id} finished "
                f"(pk {min_pk}-{max_pk}, {total_rows:,} rows in {chunks_read} chunks)"
            )

        except Exception as e:
            logger.error(f"Keyset reader {reader_id} error: {e}")
            self._set_error(e)
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass
            self._reader_done()

    def start_readers(
        self,
        schema_name: str,
        table_name: str,
        columns: List[str],
        order_by_columns: List[str],
        total_start_row: int,
        total_end_row: int,
        chunk_size: int,
        where_clause: Optional[str],
        read_func,
    ) -> None:
        """
        Launch reader threads for the specified row range.

        Divides the row range equally among readers.

        Args:
            schema_name: Source schema
            table_name: Source table
            columns: Columns to select
            order_by_columns: ORDER BY columns
            total_start_row: First row of entire range
            total_end_row: Last row of entire range
            chunk_size: Rows per chunk
            where_clause: Optional filter
            read_func: Function to read a chunk
        """
        total_rows = total_end_row - total_start_row + 1
        rows_per_reader = total_rows // self.num_readers

        self.executor = ThreadPoolExecutor(max_workers=self.num_readers)
        self.futures = []

        for i in range(self.num_readers):
            r_start = total_start_row + (i * rows_per_reader)
            if i == self.num_readers - 1:
                # Last reader gets remaining rows
                r_end = total_end_row
            else:
                r_end = r_start + rows_per_reader - 1

            logger.info(
                f"Starting reader {i+1}/{self.num_readers} "
                f"for rows {r_start:,}-{r_end:,}"
            )

            future = self.executor.submit(
                self._reader_thread,
                i + 1,
                schema_name,
                table_name,
                columns,
                order_by_columns,
                r_start,
                r_end,
                chunk_size,
                where_clause,
                read_func,
            )
            self.futures.append(future)

    def start_keyset_readers(
        self,
        schema_name: str,
        table_name: str,
        columns: List[str],
        pk_column: str,
        pk_index: int,
        pk_boundaries: List[Tuple[Any, Any]],
        chunk_size: int,
        base_where_clause: Optional[str],
        read_func,
    ) -> None:
        """
        Launch keyset reader threads with pre-computed PK boundaries.

        Each reader handles a specific PK range using keyset pagination.

        Args:
            schema_name: Source schema
            table_name: Source table
            columns: Columns to select
            pk_column: Primary key column name
            pk_index: Index of PK column in columns list
            pk_boundaries: List of (min_pk, max_pk) tuples for each reader
            chunk_size: Rows per chunk
            base_where_clause: Optional base filter
            read_func: Function to read a keyset chunk
        """
        self.executor = ThreadPoolExecutor(max_workers=self.num_readers)
        self.futures = []

        for i, (min_pk, max_pk) in enumerate(pk_boundaries):
            logger.info(
                f"Starting keyset reader {i+1}/{len(pk_boundaries)} "
                f"for pk range {min_pk}-{max_pk}"
            )

            future = self.executor.submit(
                self._keyset_reader_thread,
                i + 1,
                schema_name,
                table_name,
                columns,
                pk_column,
                pk_index,
                min_pk,
                max_pk,
                chunk_size,
                base_where_clause,
                read_func,
            )
            self.futures.append(future)

    def get_chunks(self) -> Iterator[Tuple[int, int, int, List[Tuple[Any, ...]]]]:
        """
        Generator that yields chunks from queue until all readers are done.

        Yields:
            Tuples of (reader_id, start_row, end_row, rows)

        Raises:
            Exception: If any reader encountered an error
        """
        while True:
            try:
                item = self.chunk_queue.get(timeout=5.0)
            except queue.Empty:
                # Check if we should stop
                if self.cancel_event.is_set():
                    break
                continue

            if item is self._DONE:
                break

            yield item

        # Check for errors
        if self.error:
            raise self.error

    def shutdown(self) -> None:
        """Clean up resources."""
        self.cancel_event.set()
        if self.executor:
            self.executor.shutdown(wait=True)
            self.executor = None


def _is_strict_consistency_mode() -> bool:
    """
    P0.4: Check if strict consistency mode is enabled.

    When STRICT_CONSISTENCY=true, NOLOCK hints are disabled for correctness-first runs.
    NOLOCK can cause missing rows, duplicates, and inconsistent reads under concurrent writes.

    Returns:
        True if strict consistency mode is enabled
    """
    import os
    val = os.environ.get('STRICT_CONSISTENCY', '').lower()
    return val in ('true', '1', 'yes', 'on')


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
        self.mssql_hook = OdbcConnectionHelper(odbc_conn_id=mssql_conn_id)
        self.postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        self._postgres_conn_id = postgres_conn_id

        # Get direct MSSQL connection parameters for keyset pagination
        # This avoids issues with Airflow hook's get_pandas_df on large datasets
        mssql_conn = self.mssql_hook.get_connection(mssql_conn_id)
        # Build ODBC connection parameters
        port = mssql_conn.port or 1433
        server = f"{mssql_conn.host},{port}" if port != 1433 else mssql_conn.host

        self._mssql_config = {
            'DRIVER': '{ODBC Driver 18 for SQL Server}',
            'SERVER': server,
            'DATABASE': mssql_conn.schema,
            'UID': mssql_conn.login if mssql_conn.login else '',
            'PWD': mssql_conn.password if mssql_conn.password else '',
            'TrustServerCertificate': 'yes',
            'Trusted_Connection': 'yes' if not mssql_conn.login else 'no',
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
        # Build ODBC connection string from config dict
        conn_str = ';'.join([f"{k}={v}" for k, v in self._mssql_config.items() if v])
        conn = pyodbc.connect(conn_str)
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
        chunk_size: int = 200000,
        truncate_target: bool = True,
        columns: Optional[List[str]] = None,
        where_clause: Optional[str] = None,
        use_row_number: bool = False,
        order_by_columns: Optional[List[str]] = None,
        start_row: Optional[int] = None,
        end_row: Optional[int] = None
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
            use_row_number: Use ROW_NUMBER pagination instead of keyset (for composite PKs)
            order_by_columns: Columns for ORDER BY when using ROW_NUMBER mode
            start_row: Starting row number (1-indexed) for ROW_NUMBER mode partition
            end_row: Ending row number for ROW_NUMBER mode partition

        Returns:
            Transfer result dictionary with statistics
        """
        start_time = time.time()
        logger.info(f"Starting transfer: {source_schema}.{source_table} -> {target_schema}.{target_table}")

        # P0.4: Log strict consistency mode status
        if _is_strict_consistency_mode():
            logger.info("P0.4 STRICT_CONSISTENCY mode ENABLED - NOLOCK hints disabled for data integrity")

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

        # Determine pagination mode
        # Auto-detect composite PKs and switch to ROW_NUMBER mode
        pk_columns = self._get_primary_key_columns(source_schema, source_table, columns)
        is_composite_pk = len(pk_columns) > 1

        if is_composite_pk and not use_row_number:
            # Composite PK detected - must use ROW_NUMBER pagination
            use_row_number = True
            order_by_columns = pk_columns
            logger.info(f"Detected composite PK ({', '.join(pk_columns)}) - switching to ROW_NUMBER pagination")

        if use_row_number:
            if not order_by_columns:
                order_by_columns = pk_columns if pk_columns else [columns[0]]
            logger.info(f"Using ROW_NUMBER pagination (ORDER BY {', '.join(order_by_columns)})")
            if start_row and end_row:
                logger.info(f"Processing rows {start_row:,} to {end_row:,}")
        else:
            pk_column = pk_columns[0] if pk_columns else columns[0]
            logger.info(f"Using '{pk_column}' for keyset pagination")
            pk_index = columns.index(pk_column) if pk_column in columns else 0

        try:
            with self._mssql_connection() as mssql_conn, self._postgres_connection() as postgres_conn:
                # Disable statement timeout for the entire transfer operation
                with postgres_conn.cursor() as cursor:
                    cursor.execute("SET statement_timeout = 0")

                # For partition transfers (not first partition), clean up any existing data
                # This makes partition retries idempotent - prevents duplicate rows
                # Detect both ROW_NUMBER partitions and keyset partitions
                is_row_number_partition = use_row_number and start_row and end_row and not truncate_target
                is_keyset_partition = where_clause and not truncate_target and not use_row_number

                if (is_row_number_partition or is_keyset_partition) and pk_columns:
                    logger.info(f"Cleaning up existing partition data before transfer (idempotent retry)")

                    if is_row_number_partition:
                        # ROW_NUMBER partition: get bounds from row range
                        first_pk, last_pk = self._get_partition_pk_bounds(
                            mssql_conn,
                            source_schema,
                            source_table,
                            pk_columns,
                            start_row,
                            end_row,
                            where_clause
                        )
                    else:
                        # Keyset partition: get bounds from where_clause query
                        first_pk, last_pk = self._get_keyset_partition_pk_bounds(
                            mssql_conn,
                            source_schema,
                            source_table,
                            pk_columns,
                            where_clause
                        )

                    if first_pk and last_pk:
                        # Log the PK bounds for debugging/verification
                        pk_cols_str = ', '.join(pk_columns)
                        first_pk_str = ', '.join(repr(v) for v in first_pk)
                        last_pk_str = ', '.join(repr(v) for v in last_pk)
                        logger.info(f"Partition PK bounds: ({pk_cols_str}) from ({first_pk_str}) to ({last_pk_str})")

                        deleted = self._delete_partition_data(
                            target_schema,
                            target_table,
                            pk_columns,
                            first_pk,
                            last_pk
                        )
                        if deleted > 0:
                            logger.info(f"Deleted {deleted:,} existing rows from partition range")
                        else:
                            logger.info("No existing rows to delete (clean partition)")
                    else:
                        logger.warning("Could not determine partition PK bounds - skipping cleanup")

                if use_row_number:
                    # ROW_NUMBER mode: process in chunks within the specified row range
                    current_start = start_row if start_row else 1
                    final_end = end_row if end_row else source_row_count
                    total_expected = final_end - current_start + 1

                    # Check if parallel readers are enabled
                    num_readers, queue_size = _get_parallel_reader_config()

                    if num_readers > 1 and total_expected >= chunk_size * 2:
                        # Use parallel readers
                        logger.info(
                            f"Using {num_readers} parallel readers "
                            f"(queue_size={queue_size}) for {total_expected:,} rows"
                        )

                        parallel_reader = ParallelReader(
                            mssql_config=self._mssql_config,
                            num_readers=num_readers,
                            queue_size=queue_size,
                        )

                        try:
                            parallel_reader.start_readers(
                                schema_name=source_schema,
                                table_name=source_table,
                                columns=columns,
                                order_by_columns=order_by_columns,
                                total_start_row=current_start,
                                total_end_row=final_end,
                                chunk_size=chunk_size,
                                where_clause=where_clause,
                                read_func=self._read_chunk_row_number,
                            )

                            # Writer consumes chunks from queue
                            for reader_id, chunk_start, chunk_end, rows in parallel_reader.get_chunks():
                                chunk_start_time = time.time()

                                rows_written = self._write_chunk(
                                    rows,
                                    target_schema,
                                    target_table,
                                    columns,
                                    postgres_conn
                                )

                                rows_transferred += rows_written
                                chunks_processed += 1

                                chunk_time = time.time() - chunk_start_time
                                rows_per_second = rows_written / chunk_time if chunk_time > 0 else 0

                                logger.info(
                                    f"Chunk {chunks_processed} (reader {reader_id}): "
                                    f"Transferred {rows_written:,} rows "
                                    f"({rows_transferred:,}/{total_expected:,} total) "
                                    f"at {rows_per_second:,.0f} rows/sec"
                                )

                                postgres_conn.commit()
                        finally:
                            parallel_reader.shutdown()
                    else:
                        # Sequential mode (default or small row count)
                        if num_readers > 1:
                            logger.info(
                                f"Row count {total_expected:,} too small for parallel readers, "
                                f"using sequential mode"
                            )

                        while current_start <= final_end:
                            chunk_start_time = time.time()
                            current_end = min(current_start + chunk_size - 1, final_end)

                            rows = self._read_chunk_row_number(
                                mssql_conn,
                                source_schema,
                                source_table,
                                columns,
                                order_by_columns,
                                current_start,
                                current_end,
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
                            current_start = current_end + 1

                            chunk_time = time.time() - chunk_start_time
                            rows_per_second = rows_written / chunk_time if chunk_time > 0 else 0

                            logger.info(
                                f"Chunk {chunks_processed}: Transferred {rows_written:,} rows "
                                f"({rows_transferred:,}/{total_expected:,} total) "
                                f"at {rows_per_second:,.0f} rows/sec"
                            )

                            postgres_conn.commit()
                else:
                    # Keyset mode: check if parallel readers are enabled
                    num_readers, queue_size = _get_parallel_reader_config()

                    if num_readers > 1 and source_row_count >= chunk_size * 2:
                        # Use parallel keyset readers
                        logger.info(
                            f"Using {num_readers} parallel keyset readers "
                            f"(queue_size={queue_size}) for {source_row_count:,} rows"
                        )

                        # Get balanced PK boundaries using NTILE
                        pk_boundaries = self._get_pk_ntile_boundaries(
                            mssql_conn,
                            source_schema,
                            source_table,
                            pk_column,
                            num_readers,
                            where_clause,
                        )

                        if pk_boundaries and len(pk_boundaries) > 1:
                            parallel_reader = ParallelReader(
                                mssql_config=self._mssql_config,
                                num_readers=len(pk_boundaries),
                                queue_size=queue_size,
                            )

                            try:
                                parallel_reader.start_keyset_readers(
                                    schema_name=source_schema,
                                    table_name=source_table,
                                    columns=columns,
                                    pk_column=pk_column,
                                    pk_index=pk_index,
                                    pk_boundaries=pk_boundaries,
                                    chunk_size=chunk_size,
                                    base_where_clause=where_clause,
                                    read_func=self._read_chunk_keyset_bounded,
                                )

                                # Writer consumes chunks from queue
                                for reader_id, chunk_num, reader_total, rows in parallel_reader.get_chunks():
                                    chunk_start_time = time.time()

                                    rows_written = self._write_chunk(
                                        rows,
                                        target_schema,
                                        target_table,
                                        columns,
                                        postgres_conn
                                    )

                                    rows_transferred += rows_written
                                    chunks_processed += 1

                                    chunk_time = time.time() - chunk_start_time
                                    rows_per_second = rows_written / chunk_time if chunk_time > 0 else 0

                                    logger.info(
                                        f"Chunk {chunks_processed} (reader {reader_id}): "
                                        f"Transferred {rows_written:,} rows "
                                        f"({rows_transferred:,}/{source_row_count:,} total) "
                                        f"at {rows_per_second:,.0f} rows/sec"
                                    )

                                    postgres_conn.commit()
                            finally:
                                parallel_reader.shutdown()
                        else:
                            # Fall back to sequential if NTILE failed
                            logger.warning(
                                "Could not compute NTILE boundaries, falling back to sequential"
                            )
                            num_readers = 1  # Force sequential mode below

                    if num_readers == 1 or source_row_count < chunk_size * 2:
                        # Sequential keyset mode
                        if num_readers > 1:
                            logger.info(
                                f"Row count {source_row_count:,} too small for parallel readers, "
                                f"using sequential keyset mode"
                            )

                        last_key_value = None
                        while rows_transferred < source_row_count:
                            chunk_start_time = time.time()

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

                            chunk_time = time.time() - chunk_start_time
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

        partial_load = bool(where_clause) or use_row_number

        success = len(errors) == 0 and (
            rows_transferred > 0 if partial_load else target_row_count == source_row_count
        )

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
            'success': success,
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
            query = f"SELECT COUNT(*) FROM [{schema_name}].[{table_name}]"
            if where_clause:
                query += f" WHERE {where_clause}"
            count = self.mssql_hook.get_first(query)[0]
            return count or 0

        query = sql.SQL('SELECT COUNT(*) FROM {}.{}').format(
            sql.Identifier(schema_name),
            sql.Identifier(table_name)
        )
        count = 0
        conn = None
        try:
            conn = self.postgres_hook.get_conn()
            with conn.cursor() as cursor:
                # Disable statement timeout for COUNT on large tables
                cursor.execute("SET statement_timeout = 0")
                cursor.execute(query)
                count = cursor.fetchone()[0] or 0
        finally:
            if conn:
                conn.close()

        return count

    def _truncate_table(self, schema_name: str, table_name: str) -> None:
        """
        Truncate a PostgreSQL table.

        Args:
            schema_name: Schema name
            table_name: Table name
        """
        query = sql.SQL('TRUNCATE TABLE {}.{} CASCADE').format(
            sql.Identifier(schema_name),
            sql.Identifier(table_name)
        )
        conn = None
        try:
            conn = self.postgres_hook.get_conn()
            with conn.cursor() as cursor:
                cursor.execute(query)
            conn.commit()
        finally:
            if conn:
                conn.close()

    def _get_partition_pk_bounds(
        self,
        mssql_conn,
        schema_name: str,
        table_name: str,
        pk_columns: List[str],
        start_row: int,
        end_row: int,
        where_clause: Optional[str] = None
    ) -> Tuple[Optional[Tuple], Optional[Tuple]]:
        """
        Get the first and last PK values for a partition's row range.

        This is used to make partition transfers idempotent by identifying
        which rows to delete before re-inserting.

        Args:
            mssql_conn: Active MSSQL connection
            schema_name: Source schema name
            table_name: Source table name
            pk_columns: Primary key column names
            start_row: First row number in partition (1-indexed)
            end_row: Last row number in partition
            where_clause: Optional WHERE clause for filtering

        Returns:
            Tuple of (first_pk_tuple, last_pk_tuple), or (None, None) if no data
        """
        # P0.4: Conditionally use NOLOCK based on strict consistency mode
        table_hint = "" if _is_strict_consistency_mode() else " WITH (NOLOCK)"

        pk_cols_quoted = ', '.join([f'[{col}]' for col in pk_columns])
        order_by = ', '.join([f'[{col}]' for col in pk_columns])

        # Build inner query with optional WHERE clause
        inner_query = f"""
        SELECT {pk_cols_quoted},
               ROW_NUMBER() OVER (ORDER BY {order_by}) as _rn
        FROM [{schema_name}].[{table_name}]{table_hint}
        """
        if where_clause:
            inner_query += f"\nWHERE {where_clause}"

        # Get first PK in partition
        first_query = f"""
        SELECT TOP 1 {pk_cols_quoted}
        FROM ({inner_query}) sub
        WHERE _rn = ?
        """

        # Get last PK in partition
        last_query = f"""
        SELECT TOP 1 {pk_cols_quoted}
        FROM ({inner_query}) sub
        WHERE _rn = ?
        """

        try:
            with mssql_conn.cursor() as cursor:
                cursor.execute(first_query, (start_row,))
                first_row = cursor.fetchone()

                cursor.execute(last_query, (end_row,))
                last_row = cursor.fetchone()

                if first_row and last_row:
                    return tuple(first_row), tuple(last_row)
                return None, None
        except Exception as e:
            logger.warning(f"Could not get partition PK bounds: {e}")
            return None, None

    def _get_keyset_partition_pk_bounds(
        self,
        mssql_conn,
        schema_name: str,
        table_name: str,
        pk_columns: List[str],
        where_clause: str
    ) -> Tuple[Optional[Tuple], Optional[Tuple]]:
        """
        Get the MIN and MAX PK values for a keyset partition.

        For keyset partitions that use WHERE clause filtering (e.g., [Id] >= 1 AND [Id] <= 1000000),
        this queries the source table to get the actual MIN and MAX PK values.

        Args:
            mssql_conn: Active MSSQL connection
            schema_name: Source schema name
            table_name: Source table name
            pk_columns: Primary key column names
            where_clause: WHERE clause defining the partition boundaries

        Returns:
            Tuple of (min_pk_tuple, max_pk_tuple), or (None, None) if no data
        """
        # P0.4: Conditionally use NOLOCK based on strict consistency mode
        table_hint = "" if _is_strict_consistency_mode() else " WITH (NOLOCK)"

        pk_cols_quoted = ', '.join([f'[{col}]' for col in pk_columns])

        # Build MIN and MAX queries for each PK column
        min_selects = ', '.join([f'MIN([{col}])' for col in pk_columns])
        max_selects = ', '.join([f'MAX([{col}])' for col in pk_columns])

        query = f"""
        SELECT {min_selects}, {max_selects}
        FROM [{schema_name}].[{table_name}]{table_hint}
        WHERE {where_clause}
        """

        try:
            with mssql_conn.cursor() as cursor:
                cursor.execute(query)
                row = cursor.fetchone()

                if row:
                    num_pk_cols = len(pk_columns)
                    min_values = row[:num_pk_cols]
                    max_values = row[num_pk_cols:]

                    # Check if any values are None (no data)
                    if any(v is None for v in min_values) or any(v is None for v in max_values):
                        return None, None

                    return tuple(min_values), tuple(max_values)
                return None, None
        except Exception as e:
            logger.warning(f"Could not get keyset partition PK bounds: {e}")
            return None, None

    def _delete_partition_data(
        self,
        schema_name: str,
        table_name: str,
        pk_columns: List[str],
        first_pk: Tuple,
        last_pk: Tuple
    ) -> int:
        """
        Delete rows from target table within a PK range.

        Uses PostgreSQL tuple comparison for composite PKs:
        WHERE (col1, col2) >= (val1, val2) AND (col1, col2) <= (val3, val4)

        Args:
            schema_name: Target schema name
            table_name: Target table name
            pk_columns: Primary key column names
            first_pk: First PK tuple (inclusive)
            last_pk: Last PK tuple (inclusive)

        Returns:
            Number of rows deleted
        """
        # Build tuple comparison for composite PKs
        pk_tuple = sql.SQL('({})').format(
            sql.SQL(', ').join([sql.Identifier(col) for col in pk_columns])
        )

        # Build placeholders for values
        placeholders = sql.SQL('({})').format(
            sql.SQL(', ').join([sql.Placeholder() for _ in pk_columns])
        )

        query = sql.SQL('DELETE FROM {}.{} WHERE {} >= {} AND {} <= {}').format(
            sql.Identifier(schema_name),
            sql.Identifier(table_name),
            pk_tuple,
            placeholders,
            pk_tuple,
            placeholders
        )

        # Combine first and last PK values for the query
        params = list(first_pk) + list(last_pk)

        conn = None
        try:
            conn = self.postgres_hook.get_conn()
            with conn.cursor() as cursor:
                # Disable statement timeout for large deletes
                cursor.execute("SET statement_timeout = 0")
                cursor.execute(query, params)
                deleted = cursor.rowcount
            conn.commit()
            return deleted
        except Exception as e:
            logger.error(f"Error deleting partition data: {e}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                conn.close()

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
        WHERE s.name = ? AND t.name = ?
        ORDER BY c.column_id
        """
        columns = self.mssql_hook.get_records(query, parameters=[schema_name, table_name])
        return [col[0] for col in columns]

    def _get_primary_key_columns(
        self,
        schema_name: str,
        table_name: str,
        columns: List[str]
    ) -> List[str]:
        """
        Get all primary key columns for keyset pagination (supports composite PKs).

        Args:
            schema_name: Schema name
            table_name: Table name
            columns: List of available columns

        Returns:
            List of PK column names in ordinal order, or fallback to single column
        """
        # Try to get actual primary key from database
        query = """
        SELECT c.name
        FROM sys.index_columns ic
        INNER JOIN sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id
        INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        INNER JOIN sys.tables t ON i.object_id = t.object_id
        INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name = ? AND t.name = ? AND i.is_primary_key = 1
        ORDER BY ic.key_ordinal
        """
        try:
            pk_cols = self.mssql_hook.get_records(query, parameters=[schema_name, table_name])
            if pk_cols:
                # Return all PK columns in order
                return [col[0] for col in pk_cols]
        except Exception:
            pass

        # Fallback: prefer 'Id' column if it exists (case-insensitive)
        for col in columns:
            if col.lower() == 'id':
                return [col]

        # Final fallback to first column
        return [columns[0]] if columns else ['Id']

    def _get_primary_key_column(
        self,
        schema_name: str,
        table_name: str,
        columns: List[str]
    ) -> str:
        """
        Get the primary key column for keyset pagination (legacy, single PK).

        Deprecated: Use _get_primary_key_columns() for composite PK support.

        Args:
            schema_name: Schema name
            table_name: Table name
            columns: List of available columns

        Returns:
            Column name to use for keyset pagination
        """
        pk_cols = self._get_primary_key_columns(schema_name, table_name, columns)
        return pk_cols[0] if pk_cols else (columns[0] if columns else 'Id')

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

    def _get_pk_ntile_boundaries(
        self,
        mssql_conn,
        schema_name: str,
        table_name: str,
        pk_column: str,
        num_partitions: int,
        where_clause: Optional[str] = None
    ) -> List[Tuple[Any, Any]]:
        """
        Get balanced PK boundaries using NTILE for parallel readers.

        Uses NTILE to divide rows into equal groups by row count (not by PK value),
        ensuring balanced distribution even with sparse PKs.

        Args:
            mssql_conn: Active MSSQL connection
            schema_name: Source schema name
            table_name: Source table name
            pk_column: Primary key column name
            num_partitions: Number of partitions to create
            where_clause: Optional filter

        Returns:
            List of (min_pk, max_pk) tuples for each partition
        """
        table_hint = "" if _is_strict_consistency_mode() else " WITH (NOLOCK)"

        # Build NTILE query to get partition boundaries
        inner_where = f"\nWHERE {where_clause}" if where_clause else ""

        query = f"""
        WITH numbered AS (
            SELECT [{pk_column}],
                   NTILE({num_partitions}) OVER (ORDER BY [{pk_column}]) as partition_id
            FROM [{schema_name}].[{table_name}]{table_hint}{inner_where}
        )
        SELECT partition_id,
               MIN([{pk_column}]) as min_pk,
               MAX([{pk_column}]) as max_pk,
               COUNT(*) as row_count
        FROM numbered
        GROUP BY partition_id
        ORDER BY partition_id
        """

        try:
            with mssql_conn.cursor() as cursor:
                cursor.execute(query)
                rows = cursor.fetchall()

                if not rows:
                    return []

                boundaries = []
                for row in rows:
                    partition_id, min_pk, max_pk, row_count = row
                    boundaries.append((min_pk, max_pk))
                    logger.debug(
                        f"NTILE partition {partition_id}: pk {min_pk}-{max_pk}, "
                        f"{row_count:,} rows"
                    )

                return boundaries

        except Exception as e:
            logger.warning(f"Could not get NTILE boundaries: {e}")
            return []

    def _read_chunk_keyset_bounded(
        self,
        conn,
        schema_name: str,
        table_name: str,
        columns: List[str],
        pk_column: str,
        start_key: Any,
        limit: int,
        pk_index: int,
        where_clause: str,
        max_pk: Any,
    ) -> Tuple[List[Tuple[Any, ...]], Optional[Any]]:
        """
        Read rows using keyset pagination within a bounded PK range.

        This is used by parallel keyset readers to read within their assigned range.

        Args:
            conn: MSSQL connection
            schema_name: Source schema
            table_name: Source table
            columns: Columns to select
            pk_column: Primary key column
            start_key: Starting PK value (inclusive for first call, exclusive after)
            limit: Max rows to fetch
            pk_index: Index of PK column in columns list
            where_clause: WHERE clause including PK range conditions
            max_pk: Maximum PK value (for logging)

        Returns:
            Tuple of (rows, last_key_value)
        """
        table_hint = "" if _is_strict_consistency_mode() else " WITH (NOLOCK)"

        quoted_columns = ', '.join([f'[{col}]' for col in columns])
        query = f"""
        SELECT TOP {limit} {quoted_columns}
        FROM [{schema_name}].[{table_name}]{table_hint}
        WHERE {where_clause}
        ORDER BY [{pk_column}]
        """

        try:
            with conn.cursor() as cursor:
                # Execute with both start and max pk parameters
                cursor.execute(query, (start_key, max_pk))
                rows = cursor.fetchall()

                if not rows:
                    return [], None

                next_key = rows[-1][pk_index]
                return rows, next_key

        except Exception as e:
            logger.error(f"Error reading keyset chunk (pk >= {start_key}): {e}")
            raise

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

        # P0.4: Conditionally use NOLOCK based on strict consistency mode
        table_hint = "" if _is_strict_consistency_mode() else " WITH (NOLOCK)"

        quoted_columns = ', '.join([f'[{col}]' for col in columns])
        base_query = f"""
        SELECT TOP {limit} {quoted_columns}
        FROM [{schema_name}].[{table_name}]{table_hint}
        """
        order_by = f"ORDER BY [{pk_column}]"

        # Build WHERE clause combining filter and pagination
        where_conditions = []
        if where_clause:
            where_conditions.append(f"({where_clause})")
        if last_key_value is not None:
            where_conditions.append(f"[{pk_column}] > ?")

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

    def _read_chunk_row_number(
        self,
        conn,
        schema_name: str,
        table_name: str,
        columns: List[str],
        order_by_columns: List[str],
        start_row: int,
        end_row: int,
        where_clause: Optional[str] = None,
    ) -> List[Tuple[Any, ...]]:
        """
        Read rows using ROW_NUMBER pagination for composite primary keys.

        This is slower than keyset pagination but works correctly for any PK structure.

        Args:
            conn: MSSQL connection
            schema_name: Source schema
            table_name: Source table
            columns: Columns to select
            order_by_columns: All PK columns for deterministic ordering
            start_row: First row number to include (1-indexed)
            end_row: Last row number to include
            where_clause: Optional filter

        Returns:
            List of rows
        """
        # P0.4: Conditionally use NOLOCK based on strict consistency mode
        table_hint = "" if _is_strict_consistency_mode() else " WITH (NOLOCK)"

        quoted_columns = ', '.join([f'[{col}]' for col in columns])
        order_by = ', '.join([f'[{col}]' for col in order_by_columns])

        # Build inner query with optional WHERE clause
        inner_query = f"""
        SELECT {quoted_columns},
               ROW_NUMBER() OVER (ORDER BY {order_by}) as _rn
        FROM [{schema_name}].[{table_name}]{table_hint}
        """
        if where_clause:
            inner_query += f"\nWHERE {where_clause}"

        query = f"""
        SELECT {quoted_columns}
        FROM ({inner_query}) sub
        WHERE _rn BETWEEN ? AND ?
        ORDER BY _rn
        """

        try:
            with conn.cursor() as cursor:
                cursor.execute(query, (start_row, end_row))
                rows = cursor.fetchall()
                return rows
        except Exception as e:
            logger.error(f"Error reading rows {start_row}-{end_row}: {str(e)}")
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

        # Use safe identifier quoting for schema, table, and columns
        # P0.1 FIX: Use \N as NULL marker to distinguish NULL from empty strings
        # The \N marker is PostgreSQL's default and unlikely to appear in real data
        quoted_columns = sql.SQL(', ').join([sql.Identifier(col) for col in columns])
        copy_sql = sql.SQL('COPY {}.{} ({}) FROM STDIN WITH (FORMAT CSV, DELIMITER E\'\\t\', QUOTE \'"\', NULL \'\\N\')').format(
            sql.Identifier(schema_name),
            sql.Identifier(table_name),
            quoted_columns
        )

        stream = _CSVRowStream(rows, self._normalize_value)
        with postgres_conn.cursor() as cursor:
            cursor.copy_expert(copy_sql, stream)

        return len(rows)

    def _normalize_value(self, value: Any) -> Any:
        """
        Normalize Python values for COPY consumption.

        P0.1 FIX: NULL values are represented as the literal string '\\N' which
        PostgreSQL COPY interprets as NULL (via NULL '\\N' option). Empty strings
        remain as empty strings and are properly distinguished from NULL.
        """
        if value is None:
            # Return the NULL marker - COPY will interpret this as NULL
            return '\\N'

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
            # P0.2 FIX: Emit bytea in PostgreSQL hex format for correct binary transfer
            # Previously decoded as UTF-8 with 'ignore' which silently corrupted data
            return '\\x' + bytes(value).hex()
        if isinstance(value, float) and not math.isfinite(value):
            return '\\N'  # Return NULL for non-finite floats (NaN, Inf)

        return value


def transfer_table_data(
    mssql_conn_id: str,
    postgres_conn_id: str,
    table_info: Dict[str, Any],
    chunk_size: int = 200000,
    truncate: bool = True,
    where_clause: Optional[str] = None
) -> Dict[str, Any]:
    """
    Convenience function to transfer a single table.

    Args:
        mssql_conn_id: SQL Server connection ID
        postgres_conn_id: PostgreSQL connection ID
        table_info: Table information dictionary with schema and table names.
            May include optional keys:
            - use_row_number: Use ROW_NUMBER pagination for composite PKs
            - order_by_columns: List of columns for ORDER BY
            - start_row: Starting row number for partition
            - end_row: Ending row number for partition
        chunk_size: Rows per chunk
        truncate: Whether to truncate target before transfer
        where_clause: Optional WHERE clause for filtering source data

    Returns:
        Transfer result dictionary
    """
    import os

    # Simulated failure for testing resilience
    # Set SIMULATE_FAILURE_TABLES=Table1,Table2 to simulate failures for specific tables
    simulate_failures = os.environ.get('SIMULATE_FAILURE_TABLES', '')
    if simulate_failures:
        failure_tables = [t.strip().upper() for t in simulate_failures.split(',') if t.strip()]
        table_name = table_info['table_name'].upper()
        if table_name in failure_tables:
            logger.error(f"SIMULATED FAILURE for table {table_info['table_name']} (testing resilience)")
            raise Exception(f"Simulated failure for table {table_info['table_name']}")

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
        where_clause=where_clause,
        use_row_number=table_info.get('use_row_number', False),
        order_by_columns=table_info.get('order_by_columns'),
        start_row=table_info.get('start_row'),
        end_row=table_info.get('end_row'),
    )


def parallel_transfer_tables(
    mssql_conn_id: str,
    postgres_conn_id: str,
    tables: List[Dict[str, Any]],
    chunk_size: int = 200000,
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
