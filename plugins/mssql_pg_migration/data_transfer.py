"""
Data Transfer Module

This module handles the actual data migration from SQL Server to PostgreSQL,
including chunked reading, bulk loading, and progress tracking.

Uses direct pyodbc connections for keyset pagination to avoid issues with
Airflow MSSQL hook's get_pandas_df method on large datasets.
"""

from typing import Dict, Any, Optional, List, Tuple, Iterable, Iterator
from mssql_pg_migration.odbc_helper import OdbcConnectionHelper
from mssql_pg_migration.type_mapping import sanitize_identifier
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

# Binary COPY support - import lazily to avoid import errors if not used
_binary_copy_module = None

def _get_binary_copy_module():
    """Lazily import binary_copy module."""
    global _binary_copy_module
    if _binary_copy_module is None:
        from mssql_pg_migration import binary_copy
        _binary_copy_module = binary_copy
    return _binary_copy_module

logger = logging.getLogger(__name__)

# Environment variable to control UNLOGGED staging tables
# Default to True for speed (staging tables are temporary and don't need crash safety)
USE_UNLOGGED_STAGING = os.environ.get('USE_UNLOGGED_STAGING', 'true').lower() == 'true'


# =============================================================================
# AUTO-TUNING UTILITIES
# =============================================================================

def calculate_optimal_chunk_size(
    num_columns: int,
    avg_row_width_bytes: Optional[int] = None,
    base_chunk_size: int = 200000,
) -> int:
    """
    Calculate optimal chunk size based on table characteristics.

    Narrow tables (few columns) can use larger chunks for better throughput.
    Wide tables (many columns) need smaller chunks to avoid memory pressure.

    Args:
        num_columns: Number of columns in the table
        avg_row_width_bytes: Average row size in bytes (if known)
        base_chunk_size: Base chunk size to adjust from

    Returns:
        Optimal chunk size for this table
    """
    if not os.environ.get('AUTO_TUNE_CHUNK_SIZE', 'false').lower() == 'true':
        return base_chunk_size

    # Heuristic based on column count
    if num_columns <= 5:
        # Very narrow table - can use large chunks
        multiplier = 2.0
    elif num_columns <= 10:
        # Narrow table
        multiplier = 1.5
    elif num_columns <= 20:
        # Medium table - use base
        multiplier = 1.0
    elif num_columns <= 40:
        # Wide table - reduce chunk size
        multiplier = 0.5
    else:
        # Very wide table - use small chunks
        multiplier = 0.25

    # If we know the row width, refine further
    if avg_row_width_bytes:
        # Target ~50MB per chunk in memory
        target_chunk_bytes = 50 * 1024 * 1024
        width_based_chunk = target_chunk_bytes // avg_row_width_bytes
        # Blend column-based and width-based estimates
        optimal = int((base_chunk_size * multiplier + width_based_chunk) / 2)
    else:
        optimal = int(base_chunk_size * multiplier)

    # Clamp to reasonable bounds
    min_chunk = 10000
    max_chunk = 500000
    result = max(min_chunk, min(max_chunk, optimal))

    logger.debug(
        f"Auto-tuned chunk size: {result} (columns={num_columns}, "
        f"base={base_chunk_size}, multiplier={multiplier:.2f})"
    )

    return result


def get_table_row_width(columns: List[Dict[str, Any]]) -> int:
    """
    Estimate average row width in bytes based on column types.

    Args:
        columns: List of column definitions with 'data_type' key

    Returns:
        Estimated row width in bytes
    """
    type_sizes = {
        # Integer types
        'bit': 1,
        'tinyint': 1,
        'smallint': 2,
        'int': 4,
        'bigint': 8,
        # Float types
        'real': 4,
        'float': 8,
        # Date/time types
        'date': 4,
        'time': 8,
        'datetime': 8,
        'datetime2': 8,
        'smalldatetime': 4,
        'datetimeoffset': 10,
        # Other fixed types
        'uniqueidentifier': 16,
        'money': 8,
        'smallmoney': 4,
        # Variable types - estimate
        'varchar': 50,
        'nvarchar': 100,
        'char': 20,
        'nchar': 40,
        'text': 200,
        'ntext': 400,
        'binary': 50,
        'varbinary': 100,
        'image': 500,
        'xml': 500,
        # Decimal - estimate
        'decimal': 17,
        'numeric': 17,
    }

    total = 0
    for col in columns:
        data_type = col.get('data_type', 'varchar').lower().split('(')[0]
        total += type_sizes.get(data_type, 50)  # Default 50 bytes for unknown

    return total


# =============================================================================
# PARALLEL PROCESSING UTILITIES
# =============================================================================

def calculate_partition_ranges(
    total_rows: int,
    num_partitions: int,
    pk_min: Any,
    pk_max: Any,
) -> List[Tuple[Any, Any]]:
    """
    Calculate partition ranges for parallel processing.

    Args:
        total_rows: Total number of rows in the table
        num_partitions: Desired number of partitions
        pk_min: Minimum primary key value
        pk_max: Maximum primary key value

    Returns:
        List of (start_pk, end_pk) tuples for each partition
    """
    if isinstance(pk_min, int) and isinstance(pk_max, int):
        # Integer PK - calculate even ranges
        pk_range = pk_max - pk_min + 1
        partition_size = pk_range // num_partitions

        ranges = []
        for i in range(num_partitions):
            start = pk_min + (i * partition_size)
            if i == num_partitions - 1:
                end = pk_max  # Last partition gets remainder
            else:
                end = start + partition_size - 1
            ranges.append((start, end))

        return ranges
    else:
        # Non-integer PK - fall back to single partition
        return [(pk_min, pk_max)]


class MssqlConnectionPool:
    """
    Thread-safe connection pool for SQL Server pyodbc connections.

    Since pyodbc connections are not thread-safe, this pool ensures each
    connection is used by only one thread at a time. Uses a semaphore to
    limit total connections and a queue for connection reuse.

    Usage:
        pool = MssqlConnectionPool(config, max_conn=12)
        conn = pool.acquire()
        try:
            cursor = conn.cursor()
            # use connection
        finally:
            pool.release(conn)
    """

    def __init__(
        self,
        mssql_config: Dict[str, str],
        min_conn: int = 2,
        max_conn: int = 12,
        acquire_timeout: float = 120.0,
    ):
        """
        Initialize the MSSQL connection pool.

        Args:
            mssql_config: ODBC connection config dict with keys like
                          DRIVER, SERVER, DATABASE, UID, PWD, TrustServerCertificate
            min_conn: Minimum connections to pre-create (warm start)
            max_conn: Maximum concurrent connections (hard limit)
            acquire_timeout: Seconds to wait when pool is exhausted
        """
        self._config = mssql_config
        self._min_conn = min_conn
        self._max_conn = max_conn
        self._acquire_timeout = acquire_timeout

        # Thread-safe queue for available connections (LIFO for reuse)
        self._available: queue.Queue[pyodbc.Connection] = queue.Queue()

        # Semaphore limits total concurrent connections
        self._semaphore = threading.Semaphore(max_conn)

        # Track all connections for cleanup
        self._all_connections: List[pyodbc.Connection] = []
        self._lock = threading.Lock()
        self._closed = False

        logger.info(f"Initializing MSSQL connection pool: min={min_conn}, max={max_conn}")

        # Pre-warm pool with minimum connections
        for _ in range(min_conn):
            try:
                conn = self._create_connection()
                self._available.put(conn)
            except Exception as e:
                logger.warning(f"Failed to pre-warm MSSQL pool: {e}")

    def _create_connection(self) -> pyodbc.Connection:
        """Create a new pyodbc connection."""
        conn_str = ';'.join([f"{k}={v}" for k, v in self._config.items() if v])
        conn = pyodbc.connect(conn_str, timeout=30)
        with self._lock:
            self._all_connections.append(conn)
        logger.debug(f"Created new MSSQL connection (pool size: {len(self._all_connections)})")
        return conn

    def _validate_connection(self, conn: pyodbc.Connection) -> bool:
        """Check if connection is still valid."""
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            return True
        except Exception:
            return False

    def _close_connection(self, conn: pyodbc.Connection) -> None:
        """Safely close a connection."""
        try:
            conn.close()
        except Exception:
            pass
        with self._lock:
            if conn in self._all_connections:
                self._all_connections.remove(conn)

    def acquire(self) -> pyodbc.Connection:
        """
        Acquire a connection from the pool.

        Blocks if all connections are in use until one becomes available
        or acquire_timeout is reached.

        Returns:
            Active pyodbc connection

        Raises:
            TimeoutError: If no connection available within timeout
            RuntimeError: If pool has been closed
        """
        if self._closed:
            raise RuntimeError("Connection pool has been closed")

        # Wait for available slot (blocks if pool exhausted)
        acquired = self._semaphore.acquire(timeout=self._acquire_timeout)
        if not acquired:
            raise TimeoutError(
                f"Could not acquire MSSQL connection within {self._acquire_timeout}s "
                f"(pool max: {self._max_conn})"
            )

        try:
            # Try to get existing connection from queue
            try:
                conn = self._available.get_nowait()
                if self._validate_connection(conn):
                    return conn
                # Connection is stale, replace it
                self._close_connection(conn)
                return self._create_connection()
            except queue.Empty:
                # No available connection, create new one
                return self._create_connection()
        except Exception:
            self._semaphore.release()
            raise

    def release(self, conn: pyodbc.Connection) -> None:
        """
        Return a connection to the pool.

        Args:
            conn: Connection to return (can be None, will be ignored)
        """
        if conn is None:
            return

        if self._closed:
            self._close_connection(conn)
            return

        # Return connection to available queue
        self._available.put(conn)
        self._semaphore.release()

    def close(self) -> None:
        """Close all connections and shut down the pool."""
        self._closed = True

        # Drain and close available connections
        while True:
            try:
                conn = self._available.get_nowait()
                self._close_connection(conn)
            except queue.Empty:
                break

        # Close any remaining connections
        with self._lock:
            for conn in list(self._all_connections):
                self._close_connection(conn)

        logger.info("MSSQL connection pool closed")

    @property
    def stats(self) -> Dict[str, int]:
        """Get pool statistics."""
        return {
            "total": len(self._all_connections),
            "available": self._available.qsize(),
            "max": self._max_conn,
        }


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

    Each reader thread acquires a connection from the global MSSQL pool
    (pyodbc connections are not thread-safe) and reads a disjoint row range.
    Chunks are placed in a bounded queue that the writer consumes.

    Uses backpressure: if queue is full, readers block until writer consumes.
    """

    # Sentinel value to signal end of data
    _DONE = object()

    def __init__(
        self,
        mssql_pool: MssqlConnectionPool,
        num_readers: int,
        queue_size: int,
    ):
        """
        Initialize the parallel reader manager.

        Args:
            mssql_pool: MSSQL connection pool to acquire connections from
            num_readers: Number of parallel reader threads
            queue_size: Maximum chunks buffered in queue (backpressure)
        """
        self.mssql_pool = mssql_pool
        self.num_readers = num_readers
        self.chunk_queue: queue.Queue = queue.Queue(maxsize=queue_size)
        self.executor: Optional[ThreadPoolExecutor] = None
        self.futures: List[Future] = []
        self.cancel_event = threading.Event()
        self.error: Optional[Exception] = None
        self._error_lock = threading.Lock()
        self._readers_done = 0
        self._done_lock = threading.Lock()

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
            conn = self.mssql_pool.acquire()
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
            self.mssql_pool.release(conn)
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
            conn = self.mssql_pool.acquire()
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
            self.mssql_pool.release(conn)
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

    # PostgreSQL connection pools (class-level, shared across instances)
    _postgres_pools: Dict[str, pg_pool.ThreadedConnectionPool] = {}
    _pg_pool_lock = threading.Lock()

    # MSSQL connection pools (class-level, shared across instances)
    _mssql_pools: Dict[str, MssqlConnectionPool] = {}
    _mssql_pool_lock = threading.Lock()

    def __init__(self, mssql_conn_id: str, postgres_conn_id: str):
        """
        Initialize the data transfer handler.

        Args:
            mssql_conn_id: Airflow connection ID for SQL Server
            postgres_conn_id: Airflow connection ID for PostgreSQL
        """
        self.mssql_hook = OdbcConnectionHelper(odbc_conn_id=mssql_conn_id)
        self.postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        self._mssql_conn_id = mssql_conn_id
        self._postgres_conn_id = postgres_conn_id

        # Binary COPY configuration
        # Set USE_BINARY_COPY=true to enable PostgreSQL binary COPY format
        # Binary COPY is ~20-30% faster than CSV COPY
        self._use_binary_copy = os.environ.get('USE_BINARY_COPY', 'false').lower() == 'true'
        if self._use_binary_copy:
            logger.info("Binary COPY format enabled for PostgreSQL writes")

        # Column type cache for binary COPY (populated per table)
        self._column_types_cache: Dict[str, List[str]] = {}

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
            with DataTransfer._pg_pool_lock:
                if postgres_conn_id not in DataTransfer._postgres_pools:
                    pg_conn = self.postgres_hook.get_connection(postgres_conn_id)
                    pg_max_conn = int(os.environ.get('MAX_PG_CONNECTIONS', '8'))
                    pg_min_conn = max(1, pg_max_conn // 4)
                    DataTransfer._postgres_pools[postgres_conn_id] = pg_pool.ThreadedConnectionPool(
                        minconn=pg_min_conn,
                        maxconn=pg_max_conn,
                        host=pg_conn.host,
                        port=pg_conn.port or 5432,
                        database=pg_conn.schema or pg_conn.login,
                        user=pg_conn.login,
                        password=pg_conn.password,
                    )
                    logger.info(f"Created PostgreSQL pool for {postgres_conn_id}: max={pg_max_conn}")

        # Initialize shared MSSQL connection pool for this connection ID
        self._init_mssql_pool()

        # Share the pool with mssql_hook so OdbcConnectionHelper uses pooled connections
        self.mssql_hook.set_pool(self._get_mssql_pool())

    def _init_mssql_pool(self) -> None:
        """Initialize shared MSSQL connection pool for this connection ID."""
        conn_id = self._mssql_conn_id

        if conn_id not in DataTransfer._mssql_pools:
            with DataTransfer._mssql_pool_lock:
                # Double-check after acquiring lock
                if conn_id not in DataTransfer._mssql_pools:
                    max_conn = int(os.environ.get('MAX_MSSQL_CONNECTIONS', '12'))
                    min_conn = max(1, max_conn // 4)

                    DataTransfer._mssql_pools[conn_id] = MssqlConnectionPool(
                        mssql_config=self._mssql_config,
                        min_conn=min_conn,
                        max_conn=max_conn,
                    )
                    logger.info(f"Created MSSQL pool for {conn_id}: max={max_conn}")

    def _get_mssql_pool(self) -> MssqlConnectionPool:
        """Get the MSSQL pool for this connection ID."""
        return DataTransfer._mssql_pools.get(self._mssql_conn_id)

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
        """
        Context manager for MSSQL connection from the pool.

        Uses the global MSSQL connection pool for consistency and to limit
        total concurrent connections.
        """
        pool = self._get_mssql_pool()
        conn = pool.acquire()
        try:
            yield conn
        finally:
            pool.release(conn)

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
        end_row: Optional[int] = None,
        resume_from_pk: Any = None,
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
            resume_from_pk: Resume from this PK value (keyset mode only, ignored for ROW_NUMBER/parallel)

        Returns:
            Transfer result dictionary with statistics and last_pk_synced for checkpoint
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

        # Get column list if not specified (source columns have original names)
        if not columns:
            columns = self._get_table_columns(source_schema, source_table)
            logger.info(f"Transferring {len(columns)} columns")

        # Create mapping from source columns to sanitized target columns
        # Source columns: original names (e.g., "User Name", "Log ID")
        # Target columns: sanitized names (e.g., "user_name", "log_id")
        source_columns = columns  # Keep original names for SQL Server queries
        target_columns = [sanitize_identifier(col) for col in columns]
        column_mapping = dict(zip(source_columns, target_columns))

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
        # Track last PK synced for checkpoint resume (initialized to resume point if provided)
        last_pk_synced = resume_from_pk

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
            # Log if resuming from checkpoint
            if resume_from_pk is not None:
                logger.info(f"RESUMING from checkpoint: PK > {resume_from_pk}")

        try:
            with self._mssql_connection() as mssql_conn, self._postgres_connection() as postgres_conn:
                # Disable statement timeout for the entire transfer operation
                with postgres_conn.cursor() as cursor:
                    cursor.execute("SET statement_timeout = '2h'")

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
                            mssql_pool=self._get_mssql_pool(),
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
                                    target_columns,
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
                                target_columns,
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
                                mssql_pool=self._get_mssql_pool(),
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
                                        target_columns,
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

                        # Use resume_from_pk if provided (checkpoint resume)
                        last_key_value = resume_from_pk if resume_from_pk is not None else None
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
                                target_columns,
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
                            # Update checkpoint after successful commit
                            last_pk_synced = last_key_value

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
        # Only include last_pk_synced if set (avoid XCom None serialization issues)
        if last_pk_synced is not None:
            result['last_pk_synced'] = last_pk_synced

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
                cursor.execute("SET statement_timeout = '2h'")
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
                cursor.execute("SET statement_timeout = '2h'")
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

        # Escape identifiers to prevent SQL injection (double brackets)
        safe_schema = schema_name.replace(']', ']]')
        safe_table = table_name.replace(']', ']]')
        safe_pk = pk_column.replace(']', ']]')
        quoted_columns = ', '.join([f'[{col.replace("]", "]]")}]' for col in columns])

        query = f"""
        SELECT TOP {limit} {quoted_columns}
        FROM [{safe_schema}].[{safe_table}]{table_hint}
        WHERE {where_clause}
        ORDER BY [{safe_pk}]
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

        # Escape identifiers to prevent SQL injection (double brackets)
        safe_schema = schema_name.replace(']', ']]')
        safe_table = table_name.replace(']', ']]')
        safe_pk = pk_column.replace(']', ']]')
        quoted_columns = ', '.join([f'[{col.replace("]", "]]")}]' for col in columns])

        base_query = f"""
        SELECT TOP {limit} {quoted_columns}
        FROM [{safe_schema}].[{safe_table}]{table_hint}
        """
        order_by = f"ORDER BY [{safe_pk}]"

        # Build WHERE clause combining filter and pagination
        where_conditions = []
        if where_clause:
            where_conditions.append(f"({where_clause})")
        if last_key_value is not None:
            where_conditions.append(f"[{safe_pk}] > ?")

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

        # Escape identifiers to prevent SQL injection (double brackets)
        safe_schema = schema_name.replace(']', ']]')
        safe_table = table_name.replace(']', ']]')
        quoted_columns = ', '.join([f'[{col.replace("]", "]]")}]' for col in columns])
        order_by = ', '.join([f'[{col.replace("]", "]]")}]' for col in order_by_columns])

        # Build inner query with optional WHERE clause
        inner_query = f"""
        SELECT {quoted_columns},
               ROW_NUMBER() OVER (ORDER BY {order_by}) as _rn
        FROM [{safe_schema}].[{safe_table}]{table_hint}
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

        Supports two formats:
        - CSV COPY (default): Text-based, universal compatibility
        - Binary COPY (USE_BINARY_COPY=true): ~20-30% faster, more compact

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

        # Use binary COPY if enabled
        if self._use_binary_copy:
            return self._write_chunk_binary(rows, schema_name, table_name, columns, postgres_conn)

        # Default: CSV COPY
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

    def _write_chunk_binary(
        self,
        rows: List[Tuple[Any, ...]],
        schema_name: str,
        table_name: str,
        columns: List[str],
        postgres_conn
    ) -> int:
        """
        Stream rows to PostgreSQL using Binary COPY format.

        Binary COPY is ~20-30% faster than CSV COPY because:
        - No text encoding/decoding overhead
        - More compact wire format
        - PostgreSQL's binary parser is highly optimized

        Args:
            rows: Sequence of rows to write
            schema_name: Target schema name
            table_name: Target table name
            columns: List of column names
            postgres_conn: Active PostgreSQL connection

        Returns:
            Number of rows written
        """
        binary_copy = _get_binary_copy_module()

        # Get column types (cached per table for performance)
        cache_key = f"{schema_name}.{table_name}"
        if cache_key not in self._column_types_cache:
            # Query column types from PostgreSQL
            column_types = self._get_pg_column_types(schema_name, table_name, columns, postgres_conn)
            self._column_types_cache[cache_key] = column_types
        else:
            column_types = self._column_types_cache[cache_key]

        # Use binary COPY
        return binary_copy.stream_binary_copy(
            rows=rows,
            schema_name=schema_name,
            table_name=table_name,
            columns=columns,
            column_types=column_types,
            postgres_conn=postgres_conn,
        )

    def _get_pg_column_types(
        self,
        schema_name: str,
        table_name: str,
        columns: List[str],
        postgres_conn
    ) -> List[str]:
        """
        Get PostgreSQL column types for binary encoding.

        Args:
            schema_name: Schema name
            table_name: Table name
            columns: List of column names
            postgres_conn: PostgreSQL connection

        Returns:
            List of PostgreSQL type names in same order as columns
        """
        # Query column types from information_schema
        query = """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
        """

        with postgres_conn.cursor() as cursor:
            cursor.execute(query, (schema_name, table_name))
            type_map = {row[0]: row[1] for row in cursor.fetchall()}

        # Return types in column order
        return [type_map.get(col, 'text') for col in columns]

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

    def _normalize_value_for_insert(self, value: Any) -> Any:
        """
        Normalize Python values for parameterized INSERT/UPDATE queries.

        Unlike _normalize_value (for COPY), this preserves Python None for NULL
        so psycopg2 parameter binding works correctly.
        """
        if value is None:
            return None

        if isinstance(value, datetime):
            return value
        if isinstance(value, date):
            return value
        if isinstance(value, dt_time):
            return value
        if isinstance(value, Decimal):
            return float(value)
        if isinstance(value, bool):
            return value
        if isinstance(value, (bytes, bytearray, memoryview)):
            return bytes(value)
        if isinstance(value, float) and not math.isfinite(value):
            return None

        return value


def transfer_table_data(
    mssql_conn_id: str,
    postgres_conn_id: str,
    table_info: Dict[str, Any],
    chunk_size: int = 200000,
    truncate: bool = True,
    where_clause: Optional[str] = None,
    resume_from_pk: Any = None,
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
        resume_from_pk: Resume from this PK value (for checkpoint resume)

    Returns:
        Transfer result dictionary with last_pk_synced for checkpoint
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
        resume_from_pk=resume_from_pk,
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


def upsert_rows(
    postgres_conn,
    schema_name: str,
    table_name: str,
    columns: List[str],
    pk_columns: List[str],
    rows: List[Tuple[Any, ...]],
) -> Tuple[int, int]:
    """
    Upsert rows using INSERT...ON CONFLICT DO UPDATE.

    Uses PostgreSQL's INSERT ... ON CONFLICT to efficiently handle
    both inserts and updates in a single statement.

    The function uses the xmax system column trick to count inserts vs updates:
    - xmax = 0 means the row was inserted
    - xmax > 0 means the row was updated

    Args:
        postgres_conn: Active PostgreSQL connection
        schema_name: Target schema name
        table_name: Target table name
        columns: List of all column names
        pk_columns: List of primary key column names
        rows: List of row tuples to upsert

    Returns:
        Tuple of (inserted_count, updated_count)
    """
    if not rows:
        return 0, 0

    # Validate pk_columns are subset of columns
    missing_pks = set(pk_columns) - set(columns)
    if missing_pks:
        raise ValueError(f"PK columns not in column list: {missing_pks}")

    # Build column lists
    all_cols = sql.SQL(', ').join([sql.Identifier(c) for c in columns])
    pk_cols = sql.SQL(', ').join([sql.Identifier(c) for c in pk_columns])

    # Non-PK columns for UPDATE SET clause
    non_pk_columns = [c for c in columns if c not in pk_columns]

    if non_pk_columns:
        update_set = sql.SQL(', ').join([
            sql.SQL('{} = EXCLUDED.{}').format(
                sql.Identifier(c), sql.Identifier(c)
            )
            for c in non_pk_columns
        ])
    else:
        # If all columns are PK, just do nothing on conflict
        update_set = None

    # Build placeholders for VALUES
    row_placeholder = sql.SQL('({})').format(
        sql.SQL(', ').join([sql.Placeholder()] * len(columns))
    )

    # Build the upsert query
    if update_set:
        query = sql.SQL("""
            INSERT INTO {schema}.{table} ({columns})
            VALUES {values}
            ON CONFLICT ({pk}) DO UPDATE SET {update_set}
            RETURNING (xmax = 0) AS inserted
        """).format(
            schema=sql.Identifier(schema_name),
            table=sql.Identifier(table_name),
            columns=all_cols,
            values=sql.SQL(', ').join([row_placeholder] * len(rows)),
            pk=pk_cols,
            update_set=update_set,
        )
    else:
        # All columns are PK - do nothing on conflict
        query = sql.SQL("""
            INSERT INTO {schema}.{table} ({columns})
            VALUES {values}
            ON CONFLICT ({pk}) DO NOTHING
            RETURNING (xmax = 0) AS inserted
        """).format(
            schema=sql.Identifier(schema_name),
            table=sql.Identifier(table_name),
            columns=all_cols,
            values=sql.SQL(', ').join([row_placeholder] * len(rows)),
            pk=pk_cols,
        )

    # Flatten row tuples for parameter binding
    params = []
    for row in rows:
        params.extend(row)

    try:
        with postgres_conn.cursor() as cursor:
            cursor.execute(query, params)
            results = cursor.fetchall()

            # Count inserts vs updates (xmax = 0 means row was inserted)
            inserted_count = sum(1 for r in results if r[0])
            updated_count = len(results) - inserted_count

            return inserted_count, updated_count
    except Exception as e:
        logger.error(f"Error upserting rows: {e}")
        raise


def upsert_from_staging(
    postgres_conn,
    staging_schema: str,
    staging_table: str,
    target_schema: str,
    target_table: str,
    columns: List[str],
    pk_columns: List[str],
) -> Tuple[int, int]:
    """
    Upsert from staging table using IS DISTINCT FROM for efficient change detection.

    This is the key optimization over hash-based comparison:
    - PostgreSQL's IS DISTINCT FROM handles NULL-safe comparison
    - Only rows that actually changed are updated (via WHERE clause)
    - The optimizer can use indexes efficiently

    The approach:
    1. INSERT from staging ON CONFLICT DO UPDATE
    2. WHERE clause checks if any non-PK column IS DISTINCT FROM EXCLUDED
    3. Only rows with actual changes trigger updates

    Args:
        postgres_conn: Active PostgreSQL connection
        staging_schema: Schema containing staging table
        staging_table: Staging table name (with new data)
        target_schema: Target schema name
        target_table: Target table name
        columns: List of all column names
        pk_columns: List of primary key column names

    Returns:
        Tuple of (inserted_count, updated_count)
    """
    # Build column lists
    all_cols = sql.SQL(', ').join([sql.Identifier(c) for c in columns])
    pk_cols = sql.SQL(', ').join([sql.Identifier(c) for c in pk_columns])

    # Non-PK columns for UPDATE SET and WHERE clause
    non_pk_columns = [c for c in columns if c not in pk_columns]

    if non_pk_columns:
        # UPDATE SET clause
        update_set = sql.SQL(', ').join([
            sql.SQL('{} = EXCLUDED.{}').format(
                sql.Identifier(c), sql.Identifier(c)
            )
            for c in non_pk_columns
        ])

        # WHERE clause: only update if at least one column IS DISTINCT FROM
        # This is the key optimization - skip updates for unchanged rows
        where_conditions = sql.SQL(' OR ').join([
            sql.SQL('{}.{} IS DISTINCT FROM EXCLUDED.{}').format(
                sql.Identifier(target_table),
                sql.Identifier(c),
                sql.Identifier(c)
            )
            for c in non_pk_columns
        ])

        query = sql.SQL("""
            INSERT INTO {target_schema}.{target_table} ({columns})
            SELECT {columns} FROM {staging_schema}.{staging_table}
            ON CONFLICT ({pk}) DO UPDATE SET {update_set}
            WHERE {where_clause}
            RETURNING (xmax = 0) AS inserted
        """).format(
            target_schema=sql.Identifier(target_schema),
            target_table=sql.Identifier(target_table),
            columns=all_cols,
            staging_schema=sql.Identifier(staging_schema),
            staging_table=sql.Identifier(staging_table),
            pk=pk_cols,
            update_set=update_set,
            where_clause=where_conditions,
        )
    else:
        # All columns are PK - do nothing on conflict
        query = sql.SQL("""
            INSERT INTO {target_schema}.{target_table} ({columns})
            SELECT {columns} FROM {staging_schema}.{staging_table}
            ON CONFLICT ({pk}) DO NOTHING
            RETURNING (xmax = 0) AS inserted
        """).format(
            target_schema=sql.Identifier(target_schema),
            target_table=sql.Identifier(target_table),
            columns=all_cols,
            staging_schema=sql.Identifier(staging_schema),
            staging_table=sql.Identifier(staging_table),
            pk=pk_cols,
        )

    try:
        with postgres_conn.cursor() as cursor:
            cursor.execute(query)
            results = cursor.fetchall()

            # Count inserts vs updates (xmax = 0 means row was inserted)
            inserted_count = sum(1 for r in results if r[0])
            updated_count = len(results) - inserted_count

            return inserted_count, updated_count
    except Exception as e:
        logger.error(f"Error upserting from staging: {e}")
        raise


def _create_staging_table(
    postgres_conn,
    schema_name: str,
    source_table: str,
    staging_table: str,
    use_unlogged: bool = False,
) -> None:
    """
    Create a staging table with same structure as source.

    Args:
        postgres_conn: PostgreSQL connection
        schema_name: Target schema name
        source_table: Source table to copy structure from
        staging_table: Name for the staging table
        use_unlogged: If True, create as UNLOGGED table (faster but not crash-safe).
                      Defaults to False for data safety.
    """
    table_type = "UNLOGGED TABLE" if use_unlogged else "TABLE"
    query = sql.SQL(
        "CREATE " + table_type + " {schema}.{staging} "
        "(LIKE {schema}.{source} INCLUDING DEFAULTS)"
    ).format(
        schema=sql.Identifier(schema_name),
        staging=sql.Identifier(staging_table),
        source=sql.Identifier(source_table),
    )

    with postgres_conn.cursor() as cursor:
        cursor.execute(query)


def _drop_staging_table(
    postgres_conn,
    schema_name: str,
    staging_table: str,
) -> None:
    """Drop the staging table if it exists."""
    query = sql.SQL("DROP TABLE IF EXISTS {schema}.{table}").format(
        schema=sql.Identifier(schema_name),
        table=sql.Identifier(staging_table),
    )

    with postgres_conn.cursor() as cursor:
        cursor.execute(query)


def _write_to_staging(
    postgres_conn,
    schema_name: str,
    table_name: str,
    columns: List[str],
    rows: List[Tuple[Any, ...]],
    normalize_value,
    column_types: Optional[List[str]] = None,
    use_binary: bool = False,
) -> int:
    """
    Write rows to staging table using COPY for speed.

    Supports both CSV COPY (default) and binary COPY (faster).

    Args:
        postgres_conn: PostgreSQL connection
        schema_name: Schema name
        table_name: Table name (staging table)
        columns: Column names
        rows: Rows to write
        normalize_value: Value normalization function
        column_types: PostgreSQL column types (required for binary COPY)
        use_binary: Use binary COPY format (default: False)

    Returns:
        Number of rows written
    """
    if not rows:
        return 0

    # Use binary COPY if enabled and column types are available
    if use_binary and column_types:
        binary_copy = _get_binary_copy_module()
        return binary_copy.stream_binary_copy(
            rows=rows,
            schema_name=schema_name,
            table_name=table_name,
            columns=columns,
            column_types=column_types,
            postgres_conn=postgres_conn,
        )

    # Default: CSV COPY
    quoted_columns = sql.SQL(', ').join([sql.Identifier(col) for col in columns])
    copy_sql = sql.SQL(
        "COPY {}.{} ({}) FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', QUOTE '\"', NULL '\\N')"
    ).format(
        sql.Identifier(schema_name),
        sql.Identifier(table_name),
        quoted_columns
    )

    stream = _CSVRowStream(rows, normalize_value)
    with postgres_conn.cursor() as cursor:
        cursor.copy_expert(copy_sql, stream)

    return len(rows)


def transfer_incremental_staging(
    mssql_conn_id: str,
    postgres_conn_id: str,
    table_info: Dict[str, Any],
    chunk_size: int = 100000,
    resume_from_pk: Any = None,
) -> Dict[str, Any]:
    """
    Transfer table data using staging table pattern for efficient incremental sync.

    This approach is significantly faster than hash-based change detection:
    1. Create staging table (UNLOGGED if USE_UNLOGGED_STAGING=true)
    2. COPY all source rows to staging (bulk load)
    3. Upsert from staging using INSERT...ON CONFLICT with IS DISTINCT FROM
    4. Drop staging table

    The IS DISTINCT FROM clause ensures only rows that actually changed
    are updated, eliminating the need for expensive hash comparisons.

    Args:
        mssql_conn_id: SQL Server connection ID
        postgres_conn_id: PostgreSQL connection ID
        table_info: Table information including schema, columns, and pk_columns
        chunk_size: Rows per chunk for COPY to staging
        resume_from_pk: Optional PK value to resume from (only sync rows with PK > this value)

    Returns:
        Transfer result dictionary with rows_inserted, rows_updated, etc.
    """
    import uuid

    start_time = time.time()

    source_schema = table_info.get('source_schema', 'dbo')
    source_table = table_info['table_name']
    target_schema = table_info.get('target_schema', 'public')
    target_table = table_info.get('target_table', source_table)
    columns = table_info.get('columns', [])
    pk_columns = table_info.get('pk_columns', [])

    # Handle pk_columns as dict from schema extractor
    if isinstance(pk_columns, dict):
        pk_columns = [col['name'] for col in pk_columns.get('columns', [])]

    transfer = DataTransfer(mssql_conn_id, postgres_conn_id)

    # Get columns if not provided
    if not columns:
        columns = transfer._get_table_columns(source_schema, source_table)

    # Get source row count
    source_count = transfer._get_row_count(source_schema, source_table, is_source=True)

    logger.info(
        f"Staging table sync: {source_schema}.{source_table} -> {target_schema}.{target_table}, "
        f"{source_count:,} source rows"
    )

    if source_count == 0:
        return {
            'table_name': source_table,
            'source_table': f"{source_schema}.{source_table}",
            'target_table': f"{target_schema}.{target_table}",
            'rows_transferred': 0,
            'rows_inserted': 0,
            'rows_updated': 0,
            'rows_unchanged': 0,
            'elapsed_time_seconds': time.time() - start_time,
            'success': True,
            'errors': [],
        }

    total_inserted = 0
    total_updated = 0
    rows_copied = 0
    errors = []
    last_key_value = resume_from_pk  # Track for resume support

    # Generate unique staging table name
    staging_table = f"_staging_{source_table}_{uuid.uuid4().hex[:8]}"

    try:
        with transfer._postgres_connection() as postgres_conn:
            # Disable statement timeout
            with postgres_conn.cursor() as cursor:
                cursor.execute("SET statement_timeout = '2h'")

            # Step 1: Create staging table
            logger.info(f"Creating staging table: {target_schema}.{staging_table} (unlogged={USE_UNLOGGED_STAGING})")
            _create_staging_table(
                postgres_conn, target_schema, target_table, staging_table,
                use_unlogged=USE_UNLOGGED_STAGING
            )
            postgres_conn.commit()

            # Step 2: Copy source data to staging in chunks
            logger.info(f"Copying {source_count:,} rows to staging table...")

            pk_column = pk_columns[0] if pk_columns else columns[0]
            pk_index = columns.index(pk_column) if pk_column in columns else 0

            # Check if binary COPY is enabled
            use_binary = transfer._use_binary_copy
            column_types = None
            if use_binary:
                # Get column types for binary encoding
                column_types = transfer._get_pg_column_types(
                    target_schema, target_table, columns, postgres_conn
                )
                logger.info(f"Using binary COPY for staging (column types: {len(column_types)})")

            with transfer._mssql_connection() as mssql_conn:
                # last_key_value already initialized for resume support
                chunks_processed = 0

                if resume_from_pk is not None:
                    logger.info(f"Resuming from PK > {resume_from_pk}")
                    # Recalculate remaining rows for progress tracking
                    remaining_query = f"""
                        SELECT COUNT(*) FROM [{source_schema}].[{source_table}]
                        WHERE [{pk_column}] > ?
                    """
                    with mssql_conn.cursor() as cursor:
                        cursor.execute(remaining_query, (resume_from_pk,))
                        source_count = cursor.fetchone()[0]
                    logger.info(f"Remaining rows to sync: {source_count:,}")

                while rows_copied < source_count:
                    chunk_start_time = time.time()

                    rows, last_key_value = transfer._read_chunk_keyset(
                        mssql_conn,
                        source_schema,
                        source_table,
                        columns,
                        pk_column,
                        last_key_value,
                        chunk_size,
                        pk_index,
                    )

                    if not rows:
                        break

                    # Write chunk to staging table
                    rows_written = _write_to_staging(
                        postgres_conn,
                        target_schema,
                        staging_table,
                        columns,
                        rows,
                        transfer._normalize_value,
                        column_types=column_types,
                        use_binary=use_binary,
                    )

                    rows_copied += rows_written
                    chunks_processed += 1
                    postgres_conn.commit()

                    chunk_time = time.time() - chunk_start_time
                    rows_per_second = rows_written / chunk_time if chunk_time > 0 else 0

                    if chunks_processed % 10 == 0:
                        logger.info(
                            f"Staging progress: {rows_copied:,}/{source_count:,} rows "
                            f"({rows_per_second:,.0f} rows/sec)"
                        )

            # Step 3: Upsert from staging to target
            logger.info(f"Upserting {rows_copied:,} rows from staging to target...")
            upsert_start = time.time()

            total_inserted, total_updated = upsert_from_staging(
                postgres_conn,
                target_schema,
                staging_table,
                target_schema,
                target_table,
                columns,
                pk_columns,
            )
            postgres_conn.commit()

            upsert_time = time.time() - upsert_start
            logger.info(
                f"Upsert complete: {total_inserted:,} inserted, {total_updated:,} updated "
                f"in {upsert_time:.2f}s"
            )

            # Step 4: Drop staging table
            _drop_staging_table(postgres_conn, target_schema, staging_table)
            postgres_conn.commit()

    except Exception as e:
        error_msg = f"Error in staging transfer: {str(e)}"
        logger.error(error_msg)
        errors.append(error_msg)

        # Cleanup staging table on error
        try:
            with transfer._postgres_connection() as cleanup_conn:
                _drop_staging_table(cleanup_conn, target_schema, staging_table)
                cleanup_conn.commit()
        except Exception:
            pass

    elapsed_time = time.time() - start_time
    total_rows = total_inserted + total_updated
    rows_unchanged = rows_copied - total_rows

    result = {
        'table_name': source_table,
        'source_table': f"{source_schema}.{source_table}",
        'target_table': f"{target_schema}.{target_table}",
        'rows_transferred': total_rows,
        'rows_inserted': total_inserted,
        'rows_updated': total_updated,
        'rows_unchanged': rows_unchanged,
        'rows_copied_to_staging': rows_copied,
        'elapsed_time_seconds': elapsed_time,
        'avg_rows_per_second': rows_copied / elapsed_time if elapsed_time > 0 else 0,
        'success': len(errors) == 0,
        'errors': errors,
    }
    # Only include last_pk_synced if set (avoid XCom None serialization issues)
    if last_key_value is not None:
        result['last_pk_synced'] = last_key_value

    if result['success']:
        logger.info(
            f"Staging sync complete: {total_inserted:,} inserted, {total_updated:,} updated, "
            f"{rows_unchanged:,} unchanged in {elapsed_time:.2f}s"
        )
    else:
        logger.error(f"Staging sync failed: {errors}")

    return result


def transfer_incremental_staging_partitioned(
    mssql_conn_id: str,
    postgres_conn_id: str,
    table_info: Dict[str, Any],
    pk_start: Any,
    pk_end: Any,
    partition_id: int,
    chunk_size: int = 100000,
    resume_from_pk: Any = None,
) -> Dict[str, Any]:
    """
    Transfer a partition of table data using staging table pattern.

    This is used for parallel processing of large tables. Each partition
    handles a range of primary keys independently.

    Args:
        mssql_conn_id: SQL Server connection ID
        postgres_conn_id: PostgreSQL connection ID
        table_info: Table information including schema, columns, and pk_columns
        pk_start: Start of primary key range (inclusive)
        pk_end: End of primary key range (inclusive)
        partition_id: Partition identifier for logging
        chunk_size: Rows per chunk for COPY to staging
        resume_from_pk: Resume from this PK value (rows with PK > resume_from_pk)

    Returns:
        Transfer result dictionary with rows_inserted, rows_updated, last_pk_synced, etc.
    """
    import uuid

    start_time = time.time()

    source_schema = table_info.get('source_schema', 'dbo')
    source_table = table_info['table_name']
    target_schema = table_info.get('target_schema', 'public')
    target_table = table_info.get('target_table', source_table)
    columns = table_info.get('columns', [])
    pk_columns = table_info.get('pk_columns', [])

    # Handle pk_columns as dict from schema extractor
    if isinstance(pk_columns, dict):
        pk_columns = [col['name'] for col in pk_columns.get('columns', [])]

    if not pk_columns:
        logger.error(f"No primary key columns for {source_table}")
        return {
            'table_name': source_table,
            'partition_id': partition_id,
            'success': False,
            'errors': ['No primary key columns defined'],
        }

    pk_column = pk_columns[0]

    # Validate pk_start and pk_end are integers to prevent SQL injection
    if not isinstance(pk_start, int) or not isinstance(pk_end, int):
        logger.error(f"Partition bounds must be integers, got {type(pk_start)} and {type(pk_end)}")
        return {
            'table_name': source_table,
            'partition_id': partition_id,
            'success': False,
            'errors': ['Partition bounds must be integers'],
        }

    # Determine starting point (resume_from_pk takes precedence)
    is_resuming = resume_from_pk is not None
    if is_resuming:
        logger.info(
            f"Partition {partition_id}: RESUMING {source_schema}.{source_table} "
            f"from PK > {resume_from_pk} (original range: {pk_start} - {pk_end})"
        )
    else:
        logger.info(
            f"Partition {partition_id}: Syncing {source_schema}.{source_table} "
            f"(PK range: {pk_start} - {pk_end})"
        )

    transfer = DataTransfer(mssql_conn_id, postgres_conn_id)

    # Get columns if not provided
    if not columns:
        columns = transfer._get_table_columns(source_schema, source_table)

    total_inserted = 0
    total_updated = 0
    rows_copied = 0
    errors = []
    # Track last successfully processed PK for checkpoint (initialize to resume point or start-1)
    last_pk_synced = resume_from_pk if resume_from_pk is not None else (pk_start - 1 if isinstance(pk_start, int) else None)

    # Generate unique staging table name for this partition
    staging_table = f"_staging_{source_table}_p{partition_id}_{uuid.uuid4().hex[:6]}"

    try:
        with transfer._postgres_connection() as postgres_conn:
            # Set high statement timeout (2 hours) instead of infinite
            with postgres_conn.cursor() as cursor:
                cursor.execute("SET statement_timeout = '2h'")

            # Step 1: Create staging table
            _create_staging_table(
                postgres_conn, target_schema, target_table, staging_table,
                use_unlogged=USE_UNLOGGED_STAGING
            )
            postgres_conn.commit()

            # Step 2: Copy partition data to staging
            # Build WHERE clause for partition - pk_column is escaped, values are validated integers
            safe_pk = pk_column.replace(']', ']]')
            where_clause = f"[{safe_pk}] >= {int(pk_start)} AND [{safe_pk}] <= {int(pk_end)}"

            # Check if binary COPY is enabled
            use_binary = transfer._use_binary_copy
            column_types = None
            if use_binary:
                column_types = transfer._get_pg_column_types(
                    target_schema, target_table, columns, postgres_conn
                )

            pk_index = columns.index(pk_column) if pk_column in columns else 0

            with transfer._mssql_connection() as mssql_conn:
                # Use resume_from_pk if resuming, otherwise start from pk_start - 1
                if resume_from_pk is not None:
                    last_key_value = resume_from_pk
                else:
                    last_key_value = pk_start - 1 if isinstance(pk_start, int) else None
                chunks_processed = 0

                while True:
                    rows, last_key_value = transfer._read_chunk_keyset(
                        mssql_conn,
                        source_schema,
                        source_table,
                        columns,
                        pk_column,
                        last_key_value,
                        chunk_size,
                        pk_index,
                        where_clause=where_clause,
                    )

                    if not rows:
                        break

                    # Check if we've passed the end of our partition
                    if isinstance(pk_end, int) and last_key_value and last_key_value > pk_end:
                        # Filter out rows beyond our partition
                        rows = [r for r in rows if r[pk_index] <= pk_end]

                    if not rows:
                        break

                    # Write chunk to staging table
                    rows_written = _write_to_staging(
                        postgres_conn,
                        target_schema,
                        staging_table,
                        columns,
                        rows,
                        transfer._normalize_value,
                        column_types=column_types,
                        use_binary=use_binary,
                    )

                    rows_copied += rows_written
                    chunks_processed += 1
                    postgres_conn.commit()
                    # Update checkpoint after successful commit
                    last_pk_synced = last_key_value

                    # Check if we've reached the end of our partition
                    if isinstance(pk_end, int) and last_key_value and last_key_value >= pk_end:
                        break

            # Step 3: Upsert from staging to target
            total_inserted, total_updated = upsert_from_staging(
                postgres_conn,
                target_schema,
                staging_table,
                target_schema,
                target_table,
                columns,
                pk_columns,
            )
            postgres_conn.commit()

            # Step 4: Drop staging table
            _drop_staging_table(postgres_conn, target_schema, staging_table)
            postgres_conn.commit()

    except Exception as e:
        error_msg = f"Error in partition {partition_id}: {str(e)}"
        logger.error(error_msg)
        errors.append(error_msg)

        # Cleanup staging table on error
        try:
            with transfer._postgres_connection() as cleanup_conn:
                _drop_staging_table(cleanup_conn, target_schema, staging_table)
                cleanup_conn.commit()
        except Exception:
            pass

    elapsed_time = time.time() - start_time
    rows_unchanged = rows_copied - (total_inserted + total_updated)

    result = {
        'table_name': source_table,
        'partition_id': partition_id,
        'pk_range': (pk_start, pk_end),
        'rows_copied_to_staging': rows_copied,
        'rows_inserted': total_inserted,
        'rows_updated': total_updated,
        'rows_unchanged': rows_unchanged,
        'elapsed_time_seconds': elapsed_time,
        'success': len(errors) == 0,
        'errors': errors,
    }
    # Only include last_pk_synced if set (avoid XCom None serialization issues)
    if last_pk_synced is not None:
        result['last_pk_synced'] = last_pk_synced

    if result['success']:
        logger.info(
            f"Partition {partition_id} complete: {total_inserted:,} inserted, "
            f"{total_updated:,} updated in {elapsed_time:.2f}s"
        )

    return result


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
