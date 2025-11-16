"""
Data Transfer Module

This module handles the actual data migration from SQL Server to PostgreSQL,
including chunked reading, bulk loading, and progress tracking.
"""

from typing import Dict, Any, Optional, List, Tuple
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import numpy as np
from io import StringIO
import logging
import time

logger = logging.getLogger(__name__)


class DataTransfer:
    """Handle data transfer from SQL Server to PostgreSQL."""

    def __init__(self, mssql_conn_id: str, postgres_conn_id: str):
        """
        Initialize the data transfer handler.

        Args:
            mssql_conn_id: Airflow connection ID for SQL Server
            postgres_conn_id: Airflow connection ID for PostgreSQL
        """
        self.mssql_hook = MsSqlHook(mssql_conn_id=mssql_conn_id)
        self.postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)

    def transfer_table(
        self,
        source_schema: str,
        source_table: str,
        target_schema: str,
        target_table: str,
        chunk_size: int = 10000,
        truncate_target: bool = True,
        columns: Optional[List[str]] = None
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

        Returns:
            Transfer result dictionary with statistics
        """
        start_time = time.time()
        logger.info(f"Starting transfer: {source_schema}.{source_table} -> {target_schema}.{target_table}")

        # Get source row count
        source_row_count = self._get_row_count(source_schema, source_table, is_source=True)
        logger.info(f"Source table has {source_row_count:,} rows")

        # Truncate target if requested
        if truncate_target:
            self._truncate_table(target_schema, target_table)
            logger.info(f"Truncated target table {target_schema}.{target_table}")

        # Get column list if not specified
        if not columns:
            columns = self._get_table_columns(source_schema, source_table)
            logger.info(f"Transferring {len(columns)} columns")

        # Transfer data in chunks
        rows_transferred = 0
        chunks_processed = 0
        errors = []

        try:
            # Use server-side cursor for memory efficiency
            offset = 0
            while offset < source_row_count:
                chunk_start = time.time()

                # Read chunk from SQL Server
                chunk_df = self._read_chunk(
                    source_schema,
                    source_table,
                    columns,
                    offset,
                    chunk_size
                )

                if chunk_df.empty:
                    break  # No more data

                # Process data types for PostgreSQL compatibility
                chunk_df = self._process_dataframe(chunk_df)

                # Write chunk to PostgreSQL
                rows_written = self._write_chunk(
                    chunk_df,
                    target_schema,
                    target_table,
                    columns
                )

                rows_transferred += rows_written
                chunks_processed += 1
                offset += chunk_size

                chunk_time = time.time() - chunk_start
                rows_per_second = rows_written / chunk_time if chunk_time > 0 else 0

                logger.info(
                    f"Chunk {chunks_processed}: Transferred {rows_written:,} rows "
                    f"({rows_transferred:,}/{source_row_count:,} total) "
                    f"at {rows_per_second:,.0f} rows/sec"
                )

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

    def _get_row_count(self, schema_name: str, table_name: str, is_source: bool = True) -> int:
        """
        Get row count from a table.

        Args:
            schema_name: Schema name
            table_name: Table name
            is_source: Whether this is the source (SQL Server) or target (PostgreSQL)

        Returns:
            Row count
        """
        if is_source:
            # SQL Server
            query = f"SELECT COUNT(*) FROM [{schema_name}].[{table_name}]"
            count = self.mssql_hook.get_first(query)[0]
        else:
            # PostgreSQL
            query = f'SELECT COUNT(*) FROM {schema_name}."{table_name}"'
            count = self.postgres_hook.get_first(query)[0]

        return count or 0

    def _truncate_table(self, schema_name: str, table_name: str) -> None:
        """
        Truncate a PostgreSQL table.

        Args:
            schema_name: Schema name
            table_name: Table name
        """
        query = f'TRUNCATE TABLE {schema_name}."{table_name}" CASCADE'
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

    def _read_chunk(
        self,
        schema_name: str,
        table_name: str,
        columns: List[str],
        offset: int,
        limit: int
    ) -> pd.DataFrame:
        """
        Read a chunk of data from SQL Server.

        Args:
            schema_name: Schema name
            table_name: Table name
            columns: List of columns to read
            offset: Row offset
            limit: Maximum rows to read

        Returns:
            DataFrame with the chunk data
        """
        # Quote column names for SQL Server
        quoted_columns = ', '.join([f'[{col}]' for col in columns])

        # Use ORDER BY (SELECT NULL) for consistent but efficient ordering
        query = f"""
        SELECT {quoted_columns}
        FROM [{schema_name}].[{table_name}]
        ORDER BY (SELECT NULL)
        OFFSET {offset} ROWS
        FETCH NEXT {limit} ROWS ONLY
        """

        try:
            df = self.mssql_hook.get_pandas_df(query)
            return df
        except Exception as e:
            logger.error(f"Error reading chunk at offset {offset}: {str(e)}")
            raise

    def _process_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Process DataFrame for PostgreSQL compatibility.

        Args:
            df: Input DataFrame

        Returns:
            Processed DataFrame
        """
        # Create a copy to avoid modifying the original
        processed_df = df.copy()

        for column in processed_df.columns:
            dtype = processed_df[column].dtype

            # Handle datetime columns
            if pd.api.types.is_datetime64_any_dtype(dtype):
                # Replace NaT with None for PostgreSQL NULL
                processed_df[column] = processed_df[column].where(pd.notnull(processed_df[column]), None)

            # Handle boolean columns
            elif dtype == 'bool':
                # Ensure proper boolean values
                processed_df[column] = processed_df[column].astype(bool)

            # Handle object columns (strings, etc.)
            elif dtype == 'object':
                # Replace NaN with None for PostgreSQL NULL
                processed_df[column] = processed_df[column].where(pd.notnull(processed_df[column]), None)

                # Handle potential encoding issues
                if processed_df[column].dtype == 'object':
                    try:
                        # Attempt to clean string data
                        mask = processed_df[column].notna()
                        processed_df.loc[mask, column] = processed_df.loc[mask, column].apply(
                            lambda x: x.encode('utf-8', 'ignore').decode('utf-8') if isinstance(x, str) else x
                        )
                    except Exception as e:
                        logger.warning(f"Could not process column {column}: {str(e)}")

            # Handle numeric columns with infinity values
            elif pd.api.types.is_numeric_dtype(dtype):
                # Replace inf/-inf with None
                processed_df[column] = processed_df[column].replace([np.inf, -np.inf], None)

        return processed_df

    def _write_chunk(
        self,
        df: pd.DataFrame,
        schema_name: str,
        table_name: str,
        columns: List[str]
    ) -> int:
        """
        Write a chunk of data to PostgreSQL using COPY.

        Args:
            df: DataFrame to write
            schema_name: Target schema name
            table_name: Target table name
            columns: List of column names

        Returns:
            Number of rows written
        """
        # Use COPY for efficient bulk insert
        with self.postgres_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                # Create a CSV buffer
                buffer = StringIO()
                df.to_csv(
                    buffer,
                    index=False,
                    header=False,
                    sep='\\t',
                    na_rep='\\\\N',
                    escapechar='\\\\',
                    quoting=3  # QUOTE_NONE
                )
                buffer.seek(0)

                # Quote column names for PostgreSQL
                quoted_columns = ', '.join([f'"{col}"' for col in columns])

                # Use COPY FROM for bulk insert
                cursor.copy_expert(
                    f"COPY {schema_name}.\"{table_name}\" ({quoted_columns}) "
                    f"FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\\\t', NULL '\\\\N', ESCAPE E'\\\\\\\\')",
                    buffer
                )

            conn.commit()

        return len(df)


def transfer_table_data(
    mssql_conn_id: str,
    postgres_conn_id: str,
    table_info: Dict[str, Any],
    chunk_size: int = 10000,
    truncate: bool = True
) -> Dict[str, Any]:
    """
    Convenience function to transfer a single table.

    Args:
        mssql_conn_id: SQL Server connection ID
        postgres_conn_id: PostgreSQL connection ID
        table_info: Table information dictionary with schema and table names
        chunk_size: Rows per chunk
        truncate: Whether to truncate target before transfer

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
        columns=table_info.get('columns')
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