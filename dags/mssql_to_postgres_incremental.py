"""
SQL Server to PostgreSQL Incremental Migration DAG

This DAG performs incremental data synchronization from SQL Server to PostgreSQL.
It uses a full-diff comparison strategy:

1. Compare source vs target primary keys to find new rows
2. Optionally compare row hashes to find changed rows
3. Upsert only new/changed rows (ignores deletes - append-only)

Requires:
- Target tables already exist (run full migration first)
- Tables must have primary keys for diff detection

State is tracked in _migration_state table in target database.
"""

from airflow.decorators import dag, task
from airflow.models.param import Param
from pendulum import datetime
from datetime import timedelta
from typing import List, Dict, Any
import logging
import os

# Configuration from environment
MAX_PARALLEL_TRANSFERS = int(os.environ.get('MAX_PARALLEL_TRANSFERS', '8'))
MAX_ACTIVE_TASKS = int(os.environ.get('MAX_ACTIVE_TASKS', '16'))
DEFAULT_BATCH_SIZE = int(os.environ.get('DEFAULT_INCREMENTAL_BATCH_SIZE', '10000'))

# Import migration modules
from mssql_pg_migration import schema_extractor
from mssql_pg_migration.incremental_state import IncrementalStateManager
from mssql_pg_migration.diff_detector import DiffDetector
from mssql_pg_migration.data_transfer import transfer_incremental

logger = logging.getLogger(__name__)


@dag(
    start_date=datetime(2025, 1, 1),
    schedule=None,  # Run manually or on schedule
    catchup=False,
    max_active_runs=1,
    max_active_tasks=MAX_ACTIVE_TASKS,
    is_paused_upon_creation=False,
    doc_md=__doc__,
    default_args={
        "owner": "data-team",
        "retries": 3,
        "retry_delay": timedelta(seconds=30),
        "retry_exponential_backoff": False,
        "max_retry_delay": timedelta(minutes=30),
        "pool": "default_pool",
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
        "include_tables": Param(
            default=[],
            type="array",
            description="List of specific tables to sync (if empty, all tables)"
        ),
        "exclude_tables": Param(
            default=[],
            type="array",
            description="List of table patterns to exclude"
        ),
        "detect_changes": Param(
            default=False,
            type="boolean",
            description="Compare row hashes to detect changes (slower but finds updates)"
        ),
        "batch_size": Param(
            default=DEFAULT_BATCH_SIZE,
            type="integer",
            minimum=100,
            maximum=100000,
            description="Rows per batch for upsert operations"
        ),
        "diff_batch_size": Param(
            default=100000,
            type="integer",
            minimum=10000,
            maximum=1000000,
            description="PKs per batch during diff detection"
        ),
    },
    tags=["migration", "mssql", "postgres", "etl", "incremental"],
)
def mssql_to_postgres_incremental():
    """
    Incremental sync DAG for SQL Server to PostgreSQL.

    Uses full-diff comparison to find and sync only new/changed rows.
    """

    @task
    def initialize_state(**context) -> str:
        """
        Ensure migration state table exists in target database.

        Returns:
            Status message
        """
        params = context["params"]
        state_manager = IncrementalStateManager(params["target_conn_id"])
        state_manager.ensure_state_table_exists()
        return "State table ready"

    @task
    def discover_tables(**context) -> List[Dict[str, Any]]:
        """
        Discover tables to sync from source database.

        Filters to tables that:
        - Have a primary key (required for diff detection)
        - Exist in target database

        Returns:
            List of table info dicts with PK information
        """
        params = context["params"]
        source_schema = params["source_schema"]
        target_schema = params["target_schema"]

        # Parse include_tables - handle various formats
        include_tables_raw = params.get("include_tables", [])
        if isinstance(include_tables_raw, str):
            import json
            try:
                include_tables = json.loads(include_tables_raw)
            except json.JSONDecodeError:
                include_tables = [t.strip() for t in include_tables_raw.split(',') if t.strip()]
        else:
            include_tables = include_tables_raw

        # Handle nested comma-separated items
        if include_tables and isinstance(include_tables, list):
            expanded = []
            for item in include_tables:
                if isinstance(item, str) and ',' in item:
                    expanded.extend([t.strip() for t in item.split(',') if t.strip()])
                elif isinstance(item, str) and item.strip():
                    expanded.append(item.strip())
            include_tables = expanded

        # Get source tables with schema info
        tables = schema_extractor.extract_schema_info(
            mssql_conn_id=params["source_conn_id"],
            schema_name=source_schema,
            exclude_tables=params.get("exclude_tables", []),
            include_tables=include_tables or None
        )

        logger.info(f"Found {len(tables)} tables in source")

        # Check which tables exist in target and have PKs
        state_manager = IncrementalStateManager(params["target_conn_id"])
        syncable_tables = []

        for table in tables:
            table_name = table["table_name"]
            pk_columns = table.get("pk_columns", {})

            # Skip tables without primary keys
            if not pk_columns or not pk_columns.get("columns"):
                logger.warning(
                    f"Skipping {table_name}: no primary key (required for incremental sync)"
                )
                continue

            # Check if target table exists and has data
            if not state_manager.target_table_has_data(table_name, target_schema):
                logger.warning(
                    f"Skipping {table_name}: target table is empty or doesn't exist. "
                    "Run full migration first."
                )
                continue

            # Build table info for sync
            table_info = {
                "table_name": table_name,
                "source_schema": source_schema,
                "target_schema": target_schema,
                "target_table": table_name,
                "row_count": table.get("row_count", 0),
                "columns": [col["column_name"] for col in table["columns"]],
                "pk_columns": [col["name"] for col in pk_columns.get("columns", [])],
            }
            syncable_tables.append(table_info)

        logger.info(f"Prepared {len(syncable_tables)} tables for incremental sync")

        if not syncable_tables:
            logger.warning(
                "No tables eligible for incremental sync. "
                "Ensure tables have primary keys and exist in target with data."
            )

        return syncable_tables

    @task(max_active_tis_per_dagrun=MAX_PARALLEL_TRANSFERS)
    def sync_table(table_info: Dict[str, Any], **context) -> Dict[str, Any]:
        """
        Sync a single table incrementally.

        1. Start sync in state table
        2. Detect new/changed rows via diff
        3. Transfer only those rows via upsert
        4. Update state with results

        Args:
            table_info: Table configuration dict

        Returns:
            Sync result dict
        """
        params = context["params"]
        source_conn_id = params["source_conn_id"]
        target_conn_id = params["target_conn_id"]
        detect_changes = params.get("detect_changes", False)
        batch_size = params.get("batch_size", DEFAULT_BATCH_SIZE)
        diff_batch_size = params.get("diff_batch_size", 100000)

        table_name = table_info["table_name"]
        source_schema = table_info["source_schema"]
        target_schema = table_info["target_schema"]
        pk_columns = table_info["pk_columns"]
        columns = table_info.get("columns", [])

        logger.info(f"Starting incremental sync for {source_schema}.{table_name}")

        # Initialize state tracking
        state_manager = IncrementalStateManager(target_conn_id)
        sync_id = state_manager.start_sync(
            table_name=table_name,
            source_schema=source_schema,
            target_schema=target_schema,
            source_row_count=table_info.get("row_count"),
        )

        try:
            # Detect changes
            detector = DiffDetector(source_conn_id, target_conn_id, diff_batch_size)

            # Determine columns to compare for change detection
            compare_columns = None
            if detect_changes:
                # Compare all non-PK columns
                compare_columns = [c for c in columns if c not in pk_columns]

            diff_result = detector.detect_changes(
                source_schema=source_schema,
                source_table=table_name,
                target_schema=target_schema,
                target_table=table_name,
                pk_columns=pk_columns,
                compare_columns=compare_columns,
            )

            new_count = diff_result["new_count"]
            changed_count = diff_result["changed_count"]
            unchanged_count = diff_result["unchanged_count"]

            logger.info(
                f"{table_name}: {new_count:,} new, {changed_count:,} changed, "
                f"{unchanged_count:,} unchanged"
            )

            # If no changes, mark complete and return early
            if new_count == 0 and changed_count == 0:
                state_manager.complete_sync(
                    sync_id=sync_id,
                    rows_inserted=0,
                    rows_updated=0,
                    rows_unchanged=unchanged_count,
                    target_row_count=diff_result.get("target_count"),
                )
                return {
                    "table_name": table_name,
                    "status": "no_changes",
                    "new_count": 0,
                    "changed_count": 0,
                    "unchanged_count": unchanged_count,
                    "rows_inserted": 0,
                    "rows_updated": 0,
                    "success": True,
                }

            # Combine new and changed PKs for transfer
            pks_to_sync = diff_result["new_pks"] + diff_result["changed_pks"]

            # Transfer rows
            transfer_result = transfer_incremental(
                mssql_conn_id=source_conn_id,
                postgres_conn_id=target_conn_id,
                table_info=table_info,
                pk_values=pks_to_sync,
                batch_size=batch_size,
            )

            # Update state
            if transfer_result["success"]:
                state_manager.complete_sync(
                    sync_id=sync_id,
                    rows_inserted=transfer_result["rows_inserted"],
                    rows_updated=transfer_result["rows_updated"],
                    rows_unchanged=unchanged_count,
                )
            else:
                state_manager.fail_sync(
                    sync_id=sync_id,
                    error="; ".join(transfer_result.get("errors", ["Unknown error"])),
                )

            return {
                "table_name": table_name,
                "status": "synced" if transfer_result["success"] else "failed",
                "new_count": new_count,
                "changed_count": changed_count,
                "unchanged_count": unchanged_count,
                "rows_inserted": transfer_result["rows_inserted"],
                "rows_updated": transfer_result["rows_updated"],
                "elapsed_seconds": transfer_result.get("elapsed_time_seconds", 0),
                "success": transfer_result["success"],
                "errors": transfer_result.get("errors", []),
            }

        except Exception as e:
            error_msg = str(e)
            logger.error(f"Error syncing {table_name}: {error_msg}")
            state_manager.fail_sync(sync_id=sync_id, error=error_msg)
            return {
                "table_name": table_name,
                "status": "failed",
                "success": False,
                "errors": [error_msg],
            }

    @task(trigger_rule="all_done")
    def collect_results(sync_results: List[Dict[str, Any]], **context) -> Dict[str, Any]:
        """
        Collect and summarize sync results.

        Args:
            sync_results: List of per-table sync results

        Returns:
            Summary dict
        """
        if not sync_results:
            return {
                "status": "no_tables",
                "message": "No tables were synced",
                "tables_synced": 0,
                "total_inserted": 0,
                "total_updated": 0,
            }

        tables_synced = len([r for r in sync_results if r.get("success")])
        tables_failed = len([r for r in sync_results if not r.get("success")])
        tables_no_changes = len([r for r in sync_results if r.get("status") == "no_changes"])

        total_inserted = sum(r.get("rows_inserted", 0) for r in sync_results)
        total_updated = sum(r.get("rows_updated", 0) for r in sync_results)
        total_unchanged = sum(r.get("unchanged_count", 0) for r in sync_results)

        summary = {
            "status": "success" if tables_failed == 0 else "partial_failure",
            "tables_synced": tables_synced,
            "tables_failed": tables_failed,
            "tables_no_changes": tables_no_changes,
            "total_inserted": total_inserted,
            "total_updated": total_updated,
            "total_unchanged": total_unchanged,
            "details": sync_results,
        }

        logger.info(
            f"Incremental sync complete: {tables_synced} tables synced, "
            f"{tables_failed} failed, {tables_no_changes} had no changes. "
            f"Total: {total_inserted:,} inserted, {total_updated:,} updated"
        )

        if tables_failed > 0:
            failed_tables = [r["table_name"] for r in sync_results if not r.get("success")]
            logger.error(f"Failed tables: {', '.join(failed_tables)}")

        return summary

    # Define task flow
    state_ready = initialize_state()
    tables = discover_tables()

    # Ensure state is ready before discovering tables
    state_ready >> tables

    # Sync tables in parallel
    sync_results = sync_table.expand(table_info=tables)

    # Collect results
    collect_results(sync_results)


# Instantiate the DAG
mssql_to_postgres_incremental()
