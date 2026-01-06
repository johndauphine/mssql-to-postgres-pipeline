"""
Incremental State Management Module

This module manages sync state for incremental loading, including:
- State table creation and management
- Checkpoint tracking for resumability
- Sync statistics and error recording

State is stored in a PostgreSQL table (_migration_state) in the target database.
"""

from typing import Dict, Any, Optional, List
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json
import logging

logger = logging.getLogger(__name__)

# Max error message length to store in database
# Keeps error_message column manageable while preserving useful context
MAX_ERROR_MESSAGE_LENGTH = 4000

# State table DDL
STATE_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS _migration_state (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(255) NOT NULL,
    source_schema VARCHAR(128) NOT NULL,
    target_schema VARCHAR(128) NOT NULL,

    -- Sync metadata
    sync_status VARCHAR(20) NOT NULL DEFAULT 'pending',
    last_sync_start TIMESTAMP WITH TIME ZONE,
    last_sync_end TIMESTAMP WITH TIME ZONE,
    sync_duration_seconds NUMERIC(12, 2),

    -- Row statistics
    source_row_count BIGINT,
    target_row_count BIGINT,
    rows_inserted BIGINT DEFAULT 0,
    rows_updated BIGINT DEFAULT 0,
    rows_unchanged BIGINT DEFAULT 0,

    -- Resumability
    last_pk_synced JSONB,
    checkpoint_batch_num INTEGER DEFAULT 0,

    -- Error tracking
    error_message TEXT,
    retry_count INTEGER DEFAULT 0,

    -- Restartability (added for full migration support)
    migration_type VARCHAR(20) DEFAULT 'incremental',  -- 'full' or 'incremental'
    dag_run_id VARCHAR(255),                           -- correlate state to DAG run
    partition_info JSONB,                              -- track partition completion

    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT uq_migration_state UNIQUE (table_name, source_schema, target_schema)
);

CREATE INDEX IF NOT EXISTS idx_migration_state_lookup
    ON _migration_state(source_schema, table_name);

CREATE INDEX IF NOT EXISTS idx_migration_state_status
    ON _migration_state(sync_status, migration_type);
"""

# Schema migration for existing tables (adds new columns if they don't exist)
SCHEMA_MIGRATION_DDL = """
DO $$
BEGIN
    -- Add migration_type column if not exists
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = '_migration_state' AND column_name = 'migration_type'
    ) THEN
        ALTER TABLE _migration_state ADD COLUMN migration_type VARCHAR(20) DEFAULT 'incremental';
    END IF;

    -- Add dag_run_id column if not exists
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = '_migration_state' AND column_name = 'dag_run_id'
    ) THEN
        ALTER TABLE _migration_state ADD COLUMN dag_run_id VARCHAR(255);
    END IF;

    -- Add partition_info column if not exists
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = '_migration_state' AND column_name = 'partition_info'
    ) THEN
        ALTER TABLE _migration_state ADD COLUMN partition_info JSONB;
    END IF;
END;
$$;

-- Create index for status/type lookup if not exists
CREATE INDEX IF NOT EXISTS idx_migration_state_status
    ON _migration_state(sync_status, migration_type);
"""

UPDATE_TIMESTAMP_FUNCTION = """
CREATE OR REPLACE FUNCTION update_migration_state_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
"""

UPDATE_TIMESTAMP_TRIGGER = """
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger WHERE tgname = 'trg_migration_state_updated'
    ) THEN
        CREATE TRIGGER trg_migration_state_updated
            BEFORE UPDATE ON _migration_state
            FOR EACH ROW
            EXECUTE FUNCTION update_migration_state_timestamp();
    END IF;
END;
$$;
"""


class IncrementalStateManager:
    """
    Manages sync state for incremental loading.

    Provides methods for:
    - Creating and initializing the state table
    - Starting, checkpointing, and completing syncs
    - Resuming interrupted syncs
    - Recording errors and retries
    """

    def __init__(self, postgres_conn_id: str):
        """
        Initialize the state manager.

        Args:
            postgres_conn_id: Airflow connection ID for the target PostgreSQL database
        """
        self.postgres_conn_id = postgres_conn_id
        self._hook = PostgresHook(postgres_conn_id=postgres_conn_id)

    def ensure_state_table_exists(self) -> None:
        """
        Create the _migration_state table if it doesn't exist.

        This should be called once at the start of a migration run.
        Safe to call multiple times (idempotent).
        Also migrates schema for existing tables (adds new columns).
        """
        conn = None
        try:
            conn = self._hook.get_conn()
            with conn.cursor() as cursor:
                # Create table and index
                cursor.execute(STATE_TABLE_DDL)
                # Migrate schema for existing tables (add new columns)
                cursor.execute(SCHEMA_MIGRATION_DDL)
                # Create update timestamp function
                cursor.execute(UPDATE_TIMESTAMP_FUNCTION)
                # Create trigger
                cursor.execute(UPDATE_TIMESTAMP_TRIGGER)
            conn.commit()
            logger.info("Ensured _migration_state table exists (with schema migration)")
        except Exception as e:
            logger.error(f"Error creating state table: {e}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                conn.close()

    def start_sync(
        self,
        table_name: str,
        source_schema: str,
        target_schema: str,
        source_row_count: Optional[int] = None,
        migration_type: str = 'incremental',
        dag_run_id: Optional[str] = None,
        partition_info: Optional[Dict[str, Any]] = None,
    ) -> int:
        """
        Start or restart a sync for a table.

        Creates a new state record or updates an existing one.
        Resets checkpoint and error state for a fresh sync.

        Args:
            table_name: Name of the table being synced
            source_schema: Source schema name
            target_schema: Target schema name
            source_row_count: Optional row count from source
            migration_type: 'full' or 'incremental' (default: 'incremental')
            dag_run_id: Optional DAG run ID for correlation
            partition_info: Optional partition tracking info (for partitioned tables)

        Returns:
            The state record ID (sync_id)
        """
        conn = None
        try:
            conn = self._hook.get_conn()
            with conn.cursor() as cursor:
                # Upsert state record
                cursor.execute(
                    """
                    INSERT INTO _migration_state (
                        table_name, source_schema, target_schema,
                        sync_status, last_sync_start, source_row_count,
                        rows_inserted, rows_updated, rows_unchanged,
                        last_pk_synced, checkpoint_batch_num, error_message,
                        migration_type, dag_run_id, partition_info
                    ) VALUES (%s, %s, %s, 'running', CURRENT_TIMESTAMP, %s, 0, 0, 0, NULL, 0, NULL, %s, %s, %s)
                    ON CONFLICT (table_name, source_schema, target_schema)
                    DO UPDATE SET
                        sync_status = 'running',
                        last_sync_start = CURRENT_TIMESTAMP,
                        source_row_count = EXCLUDED.source_row_count,
                        rows_inserted = 0,
                        rows_updated = 0,
                        rows_unchanged = 0,
                        last_pk_synced = NULL,
                        checkpoint_batch_num = 0,
                        error_message = NULL,
                        migration_type = EXCLUDED.migration_type,
                        dag_run_id = EXCLUDED.dag_run_id,
                        partition_info = EXCLUDED.partition_info,
                        retry_count = _migration_state.retry_count +
                            CASE WHEN _migration_state.sync_status IN ('failed', 'running') THEN 1 ELSE 0 END
                    RETURNING id
                    """,
                    (
                        table_name, source_schema, target_schema, source_row_count,
                        migration_type, dag_run_id,
                        json.dumps(partition_info) if partition_info else None
                    )
                )
                sync_id = cursor.fetchone()[0]
            conn.commit()
            logger.info(f"Started sync for {source_schema}.{table_name} (sync_id={sync_id}, type={migration_type})")
            return sync_id
        except Exception as e:
            logger.error(f"Error starting sync: {e}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                conn.close()

    def save_checkpoint(
        self,
        sync_id: int,
        last_pk: Dict[str, Any],
        batch_num: int,
        rows_inserted: int,
        rows_updated: int,
        rows_unchanged: int,
    ) -> None:
        """
        Save a checkpoint for resumability.

        Called periodically during sync to record progress.
        If sync is interrupted, can resume from last checkpoint.

        Args:
            sync_id: State record ID
            last_pk: Last PK value processed (as dict for composite PKs)
            batch_num: Current batch number
            rows_inserted: Cumulative rows inserted so far
            rows_updated: Cumulative rows updated so far
            rows_unchanged: Cumulative rows unchanged so far
        """
        conn = None
        try:
            conn = self._hook.get_conn()
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE _migration_state SET
                        last_pk_synced = %s,
                        checkpoint_batch_num = %s,
                        rows_inserted = %s,
                        rows_updated = %s,
                        rows_unchanged = %s
                    WHERE id = %s
                    """,
                    (
                        json.dumps(last_pk) if last_pk else None,
                        batch_num,
                        rows_inserted,
                        rows_updated,
                        rows_unchanged,
                        sync_id,
                    )
                )
            conn.commit()
            logger.debug(f"Saved checkpoint: sync_id={sync_id}, batch={batch_num}")
        except Exception as e:
            logger.warning(f"Error saving checkpoint: {e}")
            if conn:
                conn.rollback()
            # Don't raise - checkpoint failure shouldn't stop sync
        finally:
            if conn:
                conn.close()

    def get_resume_point(
        self,
        table_name: str,
        source_schema: str,
        target_schema: str,
    ) -> Optional[Dict[str, Any]]:
        """
        Get the last checkpoint for resuming an interrupted sync.

        Args:
            table_name: Name of the table
            source_schema: Source schema name
            target_schema: Target schema name

        Returns:
            Dict with checkpoint info if found and sync was interrupted,
            None if no checkpoint or sync completed successfully
        """
        conn = None
        try:
            conn = self._hook.get_conn()
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT id, last_pk_synced, checkpoint_batch_num,
                           rows_inserted, rows_updated, rows_unchanged,
                           sync_status
                    FROM _migration_state
                    WHERE table_name = %s
                      AND source_schema = %s
                      AND target_schema = %s
                    """,
                    (table_name, source_schema, target_schema)
                )
                row = cursor.fetchone()

                if not row:
                    return None

                sync_id, last_pk_json, batch_num, inserted, updated, unchanged, status = row

                # Only resume if sync was running (interrupted) or failed
                if status not in ('running', 'failed'):
                    return None

                # Must have a checkpoint to resume from
                if not last_pk_json:
                    return None

                # Handle JSONB: psycopg2 returns dict for JSONB, but handle str for safety
                if isinstance(last_pk_json, (str, bytes)):
                    last_pk = json.loads(last_pk_json)
                else:
                    last_pk = last_pk_json

                return {
                    'sync_id': sync_id,
                    'last_pk': last_pk,
                    'batch_num': batch_num,
                    'rows_inserted': inserted,
                    'rows_updated': updated,
                    'rows_unchanged': unchanged,
                }
        except Exception as e:
            logger.warning(f"Error getting resume point: {e}")
            return None
        finally:
            if conn:
                conn.close()

    def complete_sync(
        self,
        sync_id: int,
        rows_inserted: int,
        rows_updated: int,
        rows_unchanged: int,
        target_row_count: Optional[int] = None,
    ) -> None:
        """
        Mark a sync as successfully completed.

        Args:
            sync_id: State record ID
            rows_inserted: Total rows inserted
            rows_updated: Total rows updated
            rows_unchanged: Total rows unchanged
            target_row_count: Optional final target row count
        """
        conn = None
        try:
            conn = self._hook.get_conn()
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE _migration_state SET
                        sync_status = 'completed',
                        last_sync_end = CURRENT_TIMESTAMP,
                        sync_duration_seconds = EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - last_sync_start)),
                        rows_inserted = %s,
                        rows_updated = %s,
                        rows_unchanged = %s,
                        target_row_count = %s,
                        error_message = NULL
                    WHERE id = %s
                    """,
                    (rows_inserted, rows_updated, rows_unchanged, target_row_count, sync_id)
                )
            conn.commit()
            logger.info(
                f"Completed sync: sync_id={sync_id}, "
                f"inserted={rows_inserted}, updated={rows_updated}, unchanged={rows_unchanged}"
            )
        except Exception as e:
            logger.error(f"Error completing sync: {e}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                conn.close()

    def fail_sync(self, sync_id: int, error: str) -> None:
        """
        Mark a sync as failed.

        Args:
            sync_id: State record ID
            error: Error message to record
        """
        conn = None
        try:
            conn = self._hook.get_conn()
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE _migration_state SET
                        sync_status = 'failed',
                        last_sync_end = CURRENT_TIMESTAMP,
                        sync_duration_seconds = EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - last_sync_start)),
                        error_message = %s
                    WHERE id = %s
                    """,
                    (error[:MAX_ERROR_MESSAGE_LENGTH] if error else None, sync_id)
                )
            conn.commit()
            logger.warning(f"Failed sync: sync_id={sync_id}, error={error[:100]}...")
        except Exception as e:
            logger.error(f"Error recording sync failure: {e}")
            if conn:
                conn.rollback()
        finally:
            if conn:
                conn.close()

    def get_last_sync_info(
        self,
        table_name: str,
        source_schema: str,
        target_schema: str,
    ) -> Optional[Dict[str, Any]]:
        """
        Get information about the last sync for a table.

        Useful for determining if incremental mode is appropriate.

        Args:
            table_name: Name of the table
            source_schema: Source schema name
            target_schema: Target schema name

        Returns:
            Dict with last sync info, or None if never synced
        """
        conn = None
        try:
            conn = self._hook.get_conn()
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT sync_status, last_sync_start, last_sync_end,
                           sync_duration_seconds, source_row_count, target_row_count,
                           rows_inserted, rows_updated, rows_unchanged,
                           retry_count
                    FROM _migration_state
                    WHERE table_name = %s
                      AND source_schema = %s
                      AND target_schema = %s
                    """,
                    (table_name, source_schema, target_schema)
                )
                row = cursor.fetchone()

                if not row:
                    return None

                return {
                    'sync_status': row[0],
                    'last_sync_start': row[1],
                    'last_sync_end': row[2],
                    'sync_duration_seconds': float(row[3]) if row[3] else None,
                    'source_row_count': row[4],
                    'target_row_count': row[5],
                    'rows_inserted': row[6],
                    'rows_updated': row[7],
                    'rows_unchanged': row[8],
                    'retry_count': row[9],
                }
        except Exception as e:
            logger.warning(f"Error getting last sync info: {e}")
            return None
        finally:
            if conn:
                conn.close()

    def get_all_sync_states(
        self,
        source_schema: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get sync state for all tables.

        Args:
            source_schema: Optional filter by source schema

        Returns:
            List of sync state records
        """
        conn = None
        try:
            conn = self._hook.get_conn()
            with conn.cursor() as cursor:
                query = """
                    SELECT table_name, source_schema, target_schema,
                           sync_status, last_sync_end, rows_inserted, rows_updated
                    FROM _migration_state
                """
                params = []
                if source_schema:
                    query += " WHERE source_schema = %s"
                    params.append(source_schema)
                query += " ORDER BY table_name"

                cursor.execute(query, params if params else None)
                rows = cursor.fetchall()

                return [
                    {
                        'table_name': row[0],
                        'source_schema': row[1],
                        'target_schema': row[2],
                        'sync_status': row[3],
                        'last_sync_end': row[4],
                        'rows_inserted': row[5],
                        'rows_updated': row[6],
                    }
                    for row in rows
                ]
        except Exception as e:
            logger.warning(f"Error getting sync states: {e}")
            return []
        finally:
            if conn:
                conn.close()

    def target_table_has_data(
        self,
        table_name: str,
        target_schema: str,
    ) -> bool:
        """
        Check if a target table exists and has data.

        Used to determine if incremental mode is appropriate.
        Uses a single query that returns False if table doesn't exist.

        Args:
            table_name: Name of the table
            target_schema: Target schema name

        Returns:
            True if table exists and has at least one row
        """
        from psycopg2 import sql as psql
        from psycopg2 import errors as psy_errors

        conn = None
        try:
            conn = self._hook.get_conn()
            with conn.cursor() as cursor:
                # Single query: check if table exists and has at least one row
                # If table doesn't exist, psycopg2 raises UndefinedTable
                try:
                    cursor.execute(
                        psql.SQL("SELECT EXISTS (SELECT 1 FROM {}.{} LIMIT 1)").format(
                            psql.Identifier(target_schema),
                            psql.Identifier(table_name)
                        )
                    )
                    return cursor.fetchone()[0]
                except psy_errors.UndefinedTable:
                    # Table does not exist
                    conn.rollback()  # Clear the error state
                    return False
        except Exception as e:
            logger.warning(f"Error checking target table: {e}")
            return False
        finally:
            if conn:
                conn.close()

    # =========================================================================
    # Restartability Methods (for full migration DAG support)
    # =========================================================================

    def get_tables_by_status(
        self,
        statuses: List[str],
        source_schema: Optional[str] = None,
        migration_type: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get tables filtered by sync status.

        Args:
            statuses: List of statuses to filter (e.g., ['completed', 'failed'])
            source_schema: Optional filter by source schema
            migration_type: Optional filter by migration type ('full' or 'incremental')

        Returns:
            List of state records matching the criteria
        """
        conn = None
        try:
            conn = self._hook.get_conn()
            with conn.cursor() as cursor:
                query = """
                    SELECT id, table_name, source_schema, target_schema,
                           sync_status, last_sync_end, rows_inserted,
                           migration_type, dag_run_id, partition_info,
                           error_message, retry_count
                    FROM _migration_state
                    WHERE sync_status = ANY(%s)
                """
                params = [statuses]

                if source_schema:
                    query += " AND source_schema = %s"
                    params.append(source_schema)

                if migration_type:
                    query += " AND migration_type = %s"
                    params.append(migration_type)

                query += " ORDER BY table_name"
                cursor.execute(query, params)
                rows = cursor.fetchall()

                return [
                    {
                        'id': row[0],
                        'table_name': row[1],
                        'source_schema': row[2],
                        'target_schema': row[3],
                        'sync_status': row[4],
                        'last_sync_end': row[5],
                        'rows_inserted': row[6],
                        'migration_type': row[7],
                        'dag_run_id': row[8],
                        'partition_info': row[9],
                        'error_message': row[10],
                        'retry_count': row[11],
                    }
                    for row in rows
                ]
        except Exception as e:
            logger.warning(f"Error getting tables by status: {e}")
            return []
        finally:
            if conn:
                conn.close()

    def reset_table_state(
        self,
        table_name: str,
        source_schema: str,
        target_schema: str,
    ) -> bool:
        """
        Reset state for a specific table to allow re-processing.

        Sets status to 'pending' and clears checkpoint/error info.

        Args:
            table_name: Name of the table
            source_schema: Source schema name
            target_schema: Target schema name

        Returns:
            True if state was reset, False if no matching record
        """
        conn = None
        try:
            conn = self._hook.get_conn()
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE _migration_state SET
                        sync_status = 'pending',
                        last_pk_synced = NULL,
                        checkpoint_batch_num = 0,
                        error_message = NULL,
                        partition_info = NULL
                    WHERE table_name = %s
                      AND source_schema = %s
                      AND target_schema = %s
                    """,
                    (table_name, source_schema, target_schema)
                )
                affected = cursor.rowcount
            conn.commit()
            if affected > 0:
                logger.info(f"Reset state for {source_schema}.{table_name}")
            return affected > 0
        except Exception as e:
            logger.error(f"Error resetting table state: {e}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                conn.close()

    def reset_all_state(
        self,
        source_schema: Optional[str] = None,
        migration_type: Optional[str] = None,
    ) -> int:
        """
        Reset state for all tables (bulk reset).

        Args:
            source_schema: Optional filter by source schema
            migration_type: Optional filter by migration type

        Returns:
            Number of records reset
        """
        conn = None
        try:
            conn = self._hook.get_conn()
            with conn.cursor() as cursor:
                query = """
                    UPDATE _migration_state SET
                        sync_status = 'pending',
                        last_pk_synced = NULL,
                        checkpoint_batch_num = 0,
                        error_message = NULL,
                        partition_info = NULL
                    WHERE 1=1
                """
                params = []

                if source_schema:
                    query += " AND source_schema = %s"
                    params.append(source_schema)

                if migration_type:
                    query += " AND migration_type = %s"
                    params.append(migration_type)

                cursor.execute(query, params if params else None)
                affected = cursor.rowcount
            conn.commit()
            logger.info(f"Reset state for {affected} tables")
            return affected
        except Exception as e:
            logger.error(f"Error resetting all state: {e}")
            if conn:
                conn.rollback()
            return 0
        finally:
            if conn:
                conn.close()

    def reset_stale_running_states(
        self,
        current_dag_run_id: str,
        source_schema: Optional[str] = None,
        migration_type: Optional[str] = None,
    ) -> int:
        """
        Reset 'running' states from previous DAG runs (zombie cleanup).

        When a DAG crashes, tasks may be left in 'running' state.
        This marks them as 'failed' so they can be retried.

        Args:
            current_dag_run_id: The current DAG run ID (don't reset these)
            source_schema: Optional filter by source schema
            migration_type: Optional filter by migration type

        Returns:
            Number of zombie states reset
        """
        conn = None
        try:
            conn = self._hook.get_conn()
            with conn.cursor() as cursor:
                query = """
                    UPDATE _migration_state SET
                        sync_status = 'failed',
                        error_message = 'Zombie state: previous DAG run did not complete'
                    WHERE sync_status = 'running'
                      AND (dag_run_id IS NULL OR dag_run_id != %s)
                """
                params = [current_dag_run_id]

                if source_schema:
                    query += " AND source_schema = %s"
                    params.append(source_schema)

                if migration_type:
                    query += " AND migration_type = %s"
                    params.append(migration_type)

                cursor.execute(query, params)
                affected = cursor.rowcount
            conn.commit()
            if affected > 0:
                logger.warning(f"Reset {affected} zombie 'running' states from previous runs")
            return affected
        except Exception as e:
            logger.error(f"Error resetting stale running states: {e}")
            if conn:
                conn.rollback()
            return 0
        finally:
            if conn:
                conn.close()

    def update_partition_status(
        self,
        sync_id: int,
        partition_id: int,
        status: str,
        rows_transferred: int = 0,
        error: Optional[str] = None,
    ) -> None:
        """
        Update the status of a specific partition (with row locking).

        Uses SELECT FOR UPDATE to prevent concurrent updates from
        overwriting each other when multiple partitions complete simultaneously.

        Args:
            sync_id: State record ID
            partition_id: Partition index to update
            status: New status ('pending', 'running', 'completed', 'failed')
            rows_transferred: Number of rows transferred (for progress tracking)
            error: Optional error message for failed partitions
        """
        conn = None
        try:
            conn = self._hook.get_conn()
            with conn.cursor() as cursor:
                # Lock the row for update
                cursor.execute(
                    "SELECT partition_info FROM _migration_state WHERE id = %s FOR UPDATE",
                    (sync_id,)
                )
                row = cursor.fetchone()
                if not row:
                    logger.warning(f"No state record found for sync_id={sync_id}")
                    return

                partition_info = row[0] or {'total_partitions': 0, 'partitions': []}

                # Handle JSONB: psycopg2 returns dict for JSONB
                if isinstance(partition_info, str):
                    partition_info = json.loads(partition_info)

                # Ensure partitions list exists and is long enough
                partitions = partition_info.get('partitions', [])
                while len(partitions) <= partition_id:
                    partitions.append({'id': len(partitions), 'status': 'pending'})

                # Update the partition status
                partitions[partition_id]['status'] = status
                partitions[partition_id]['rows'] = rows_transferred
                if error:
                    partitions[partition_id]['error'] = error[:500]  # Truncate long errors
                elif 'error' in partitions[partition_id]:
                    del partitions[partition_id]['error']

                partition_info['partitions'] = partitions

                # Update the record
                cursor.execute(
                    "UPDATE _migration_state SET partition_info = %s WHERE id = %s",
                    (json.dumps(partition_info), sync_id)
                )
            conn.commit()
            logger.debug(f"Updated partition {partition_id} status to '{status}' for sync_id={sync_id}")
        except Exception as e:
            logger.error(f"Error updating partition status: {e}")
            if conn:
                conn.rollback()
        finally:
            if conn:
                conn.close()

    def get_incomplete_partitions(
        self,
        sync_id: int,
    ) -> List[Dict[str, Any]]:
        """
        Get partitions that need to be (re)processed.

        Returns partitions with status 'pending', 'running', or 'failed'.

        Args:
            sync_id: State record ID

        Returns:
            List of partition info dicts that need processing
        """
        conn = None
        try:
            conn = self._hook.get_conn()
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT partition_info FROM _migration_state WHERE id = %s",
                    (sync_id,)
                )
                row = cursor.fetchone()
                if not row or not row[0]:
                    return []

                partition_info = row[0]
                if isinstance(partition_info, str):
                    partition_info = json.loads(partition_info)

                partitions = partition_info.get('partitions', [])
                incomplete = [
                    p for p in partitions
                    if p.get('status') in ('pending', 'running', 'failed')
                ]
                return incomplete
        except Exception as e:
            logger.warning(f"Error getting incomplete partitions: {e}")
            return []
        finally:
            if conn:
                conn.close()

    def get_migration_summary(
        self,
        source_schema: Optional[str] = None,
        migration_type: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Get a summary of migration state for resume planning.

        Args:
            source_schema: Optional filter by source schema
            migration_type: Optional filter by migration type

        Returns:
            Dict with counts by status and list of tables per status
        """
        conn = None
        try:
            conn = self._hook.get_conn()
            with conn.cursor() as cursor:
                query = """
                    SELECT sync_status, COUNT(*), ARRAY_AGG(table_name ORDER BY table_name)
                    FROM _migration_state
                    WHERE 1=1
                """
                params = []

                if source_schema:
                    query += " AND source_schema = %s"
                    params.append(source_schema)

                if migration_type:
                    query += " AND migration_type = %s"
                    params.append(migration_type)

                query += " GROUP BY sync_status"
                cursor.execute(query, params if params else None)
                rows = cursor.fetchall()

                summary = {
                    'completed': {'count': 0, 'tables': []},
                    'failed': {'count': 0, 'tables': []},
                    'running': {'count': 0, 'tables': []},
                    'pending': {'count': 0, 'tables': []},
                }

                for status, count, tables in rows:
                    if status in summary:
                        summary[status] = {'count': count, 'tables': tables or []}

                return summary
        except Exception as e:
            logger.warning(f"Error getting migration summary: {e}")
            return {
                'completed': {'count': 0, 'tables': []},
                'failed': {'count': 0, 'tables': []},
                'running': {'count': 0, 'tables': []},
                'pending': {'count': 0, 'tables': []},
            }
        finally:
            if conn:
                conn.close()
