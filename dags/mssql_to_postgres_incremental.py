"""
SQL Server to PostgreSQL Incremental Migration DAG

This DAG performs incremental data synchronization from SQL Server to PostgreSQL.

Tables must be explicitly specified in 'schema.table' format in include_tables.
Target PostgreSQL schema is derived as: {sourcedb}__{sourceschema} (lowercase)

Sync process:
1. COPY all source rows to an UNLOGGED staging table
2. Upsert from staging using INSERT...ON CONFLICT with IS DISTINCT FROM
3. Only rows that actually changed are updated (PostgreSQL handles comparison)

Large tables (>1M rows) are automatically partitioned for parallel processing.

Requires:
- Target tables already exist (run full migration first)
- Tables must have primary keys for upsert operations

State is tracked in public._migration_state table in target database.
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
# Batch size increased from 10K to 100K for staging table approach which uses bulk COPY.
# Environments with very wide rows may need to lower via DEFAULT_INCREMENTAL_BATCH_SIZE env var.
DEFAULT_BATCH_SIZE = int(os.environ.get('DEFAULT_INCREMENTAL_BATCH_SIZE', '100000'))
# Partitioning settings for large tables
PARTITION_THRESHOLD = int(os.environ.get('PARTITION_THRESHOLD', '1000000'))  # Tables > 1M rows get partitioned
MAX_PARTITIONS_PER_TABLE = int(os.environ.get('MAX_PARTITIONS_PER_TABLE', '6'))

# Import migration modules
from mssql_pg_migration import schema_extractor
from mssql_pg_migration.incremental_state import IncrementalStateManager
from mssql_pg_migration.data_transfer import (
    transfer_incremental_staging,
    transfer_incremental_staging_partitioned,
)
from mssql_pg_migration.table_config import (
    expand_include_tables_param,
    validate_include_tables,
    parse_include_tables,
    get_source_database,
    derive_target_schema,
    get_default_include_tables,
    load_include_tables_from_config,
)

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
        "include_tables": Param(
            default=get_default_include_tables(),
            description="Tables to include in 'schema.table' format (e.g., ['dbo.Users', 'dbo.Posts']). "
                        "Defaults from config/{database}_include_tables.txt or INCLUDE_TABLES env var."
        ),
        "batch_size": Param(
            default=DEFAULT_BATCH_SIZE,
            type="integer",
            minimum=1000,
            maximum=500000,
            description="Rows per batch for COPY to staging table"
        ),
        # Restartability parameter
        "resume_from_checkpoint": Param(
            default=True,
            type="boolean",
            description="Resume interrupted syncs from last checkpoint. Set False to restart from beginning."
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

        State is stored in public._migration_state (fixed schema).
        Note: This requires the 'public' schema to exist and be accessible.
        The schema is intentionally fixed to preserve state across migrations
        regardless of target schema naming.

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

        Parses include_tables in schema.table format and derives target schemas.
        Filters to tables that:
        - Have a primary key (required for diff detection)
        - Exist in target database

        Returns:
            List of table info dicts with PK information
        """
        params = context["params"]
        source_conn_id = params["source_conn_id"]

        # Parse and expand include_tables parameter
        include_tables_raw = params.get("include_tables", [])
        include_tables = expand_include_tables_param(include_tables_raw)

        # If empty, try loading from config file at runtime
        if not include_tables:
            include_tables = load_include_tables_from_config(source_conn_id)

        # Validate include_tables
        validate_include_tables(include_tables)

        # Parse into {schema: [tables]} dict
        schema_tables = parse_include_tables(include_tables)

        # Get source database name for deriving target schemas
        source_db = get_source_database(source_conn_id)

        logger.info(f"Discovering tables from schemas: {list(schema_tables.keys())}")

        # Build target schema map
        target_schema_map = {
            src_schema: derive_target_schema(source_db, src_schema)
            for src_schema in schema_tables.keys()
        }

        # Initialize state manager for checking target tables
        state_manager = IncrementalStateManager(params["target_conn_id"])
        syncable_tables = []

        for source_schema, tables in schema_tables.items():
            target_schema = target_schema_map[source_schema]

            # Get source tables with schema info for this schema
            source_tables = schema_extractor.extract_schema_info(
                mssql_conn_id=source_conn_id,
                schema_name=source_schema,
                exclude_tables=[],
                include_tables=tables
            )

            logger.info(f"Found {len(source_tables)} tables in {source_schema}")

            for table in source_tables:
                table_name = table["table_name"]
                pk_columns = table.get("pk_columns", {})

                # Skip tables without primary keys
                if not pk_columns or not pk_columns.get("columns"):
                    logger.warning(
                        f"Skipping {source_schema}.{table_name}: no primary key (required for incremental sync)"
                    )
                    continue

                # Check if target table exists and has data
                if not state_manager.target_table_has_data(table_name, target_schema):
                    logger.warning(
                        f"Skipping {source_schema}.{table_name}: target table {target_schema}.{table_name} "
                        "is empty or doesn't exist. Run full migration first."
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

    @task
    def create_sync_tasks(tables: List[Dict[str, Any]], **context) -> List[Dict[str, Any]]:
        """
        Create sync tasks, partitioning large tables for parallel processing.

        Tables larger than PARTITION_THRESHOLD get split into multiple partitions.
        Each partition is processed as a separate task for better parallelism.

        Args:
            tables: List of table info dicts

        Returns:
            List of sync task configs (may be more than input tables due to partitioning)
        """
        params = context["params"]
        source_conn_id = params["source_conn_id"]

        sync_tasks = []

        for table in tables:
            row_count = table.get("row_count", 0)
            table_name = table["table_name"]
            pk_columns = table.get("pk_columns", [])

            # Check if table should be partitioned
            if row_count >= PARTITION_THRESHOLD and pk_columns:
                pk_column = pk_columns[0]

                # Get PK min/max for partitioning
                try:
                    from mssql_pg_migration.odbc_helper import OdbcConnectionHelper
                    helper = OdbcConnectionHelper(odbc_conn_id=source_conn_id)

                    with helper.get_conn() as conn:
                        cursor = conn.cursor()
                        # Use parameterized identifiers via bracket escaping
                        # Note: pyodbc doesn't support parameterized identifiers, but we
                        # validate that pk_column comes from schema discovery (trusted source)
                        # and double any brackets to prevent SQL injection
                        safe_pk = pk_column.replace(']', ']]')
                        safe_schema = table['source_schema'].replace(']', ']]')
                        safe_table = table_name.replace(']', ']]')
                        query = f"SELECT MIN([{safe_pk}]), MAX([{safe_pk}]) FROM [{safe_schema}].[{safe_table}]"
                        cursor.execute(query)
                        row = cursor.fetchone()
                        pk_min, pk_max = row[0], row[1]
                        cursor.close()

                    if pk_min is not None and pk_max is not None and isinstance(pk_min, int):
                        # Calculate partitions
                        num_partitions = min(
                            MAX_PARTITIONS_PER_TABLE,
                            max(2, row_count // 500000)  # ~500K rows per partition
                        )

                        pk_range = pk_max - pk_min + 1
                        partition_size = pk_range // num_partitions

                        logger.info(
                            f"Partitioning {table_name}: {row_count:,} rows into "
                            f"{num_partitions} partitions"
                        )

                        for i in range(num_partitions):
                            start_pk = pk_min + (i * partition_size)
                            end_pk = pk_max if i == num_partitions - 1 else start_pk + partition_size - 1

                            sync_tasks.append({
                                **table,
                                "is_partition": True,
                                "partition_id": i,
                                "pk_start": start_pk,
                                "pk_end": end_pk,
                                "partition_row_estimate": partition_size,
                            })

                        continue  # Skip adding as single task

                except Exception as e:
                    logger.warning(f"Could not partition {table_name}: {e}")

            # Add as single task (small table or partitioning failed)
            sync_tasks.append({
                **table,
                "is_partition": False,
            })

        logger.info(
            f"Created {len(sync_tasks)} sync tasks from {len(tables)} tables "
            f"(partitioned: {len(sync_tasks) - len(tables)} extra tasks)"
        )

        return sync_tasks

    @task(max_active_tis_per_dagrun=MAX_PARALLEL_TRANSFERS)
    def sync_table(task_info: Dict[str, Any], **context) -> Dict[str, Any]:
        """
        Sync a single table or partition incrementally using staging table pattern.

        For partitioned tables:
        1. Create partition-specific staging table
        2. COPY partition rows to staging
        3. Upsert from staging to target
        4. Drop staging table

        For non-partitioned tables:
        1. Check for resume point if resume_from_checkpoint=True
        2. Start sync in state table (or resume from checkpoint)
        3. COPY source rows to staging table (from last PK if resuming)
        4. Upsert from staging with IS DISTINCT FROM (only changed rows update)
        5. Update state with results

        Args:
            task_info: Table or partition configuration dict

        Returns:
            Sync result dict
        """
        params = context["params"]
        source_conn_id = params["source_conn_id"]
        target_conn_id = params["target_conn_id"]
        batch_size = params.get("batch_size", DEFAULT_BATCH_SIZE)
        resume_from_checkpoint = params.get("resume_from_checkpoint", True)

        is_partition = task_info.get("is_partition", False)
        table_name = task_info["table_name"]
        source_schema = task_info["source_schema"]
        target_schema = task_info["target_schema"]

        # Handle partitioned transfer
        if is_partition:
            partition_id = task_info["partition_id"]
            pk_start = task_info["pk_start"]
            pk_end = task_info["pk_end"]

            logger.info(
                f"Syncing partition {partition_id} of {source_schema}.{table_name} "
                f"(PK range: {pk_start} - {pk_end})"
            )

            try:
                result = transfer_incremental_staging_partitioned(
                    mssql_conn_id=source_conn_id,
                    postgres_conn_id=target_conn_id,
                    table_info=task_info,
                    pk_start=pk_start,
                    pk_end=pk_end,
                    partition_id=partition_id,
                    chunk_size=batch_size,
                )

                return {
                    "table_name": table_name,
                    "partition_id": partition_id,
                    "is_partition": True,
                    "status": "synced" if result["success"] else "failed",
                    "rows_inserted": result.get("rows_inserted", 0),
                    "rows_updated": result.get("rows_updated", 0),
                    "rows_copied_to_staging": result.get("rows_copied_to_staging", 0),
                    "elapsed_seconds": result.get("elapsed_time_seconds", 0),
                    "success": result["success"],
                    "errors": result.get("errors", []),
                }

            except Exception as e:
                error_msg = str(e)
                logger.error(f"Error syncing partition {partition_id} of {table_name}: {error_msg}")
                return {
                    "table_name": table_name,
                    "partition_id": partition_id,
                    "is_partition": True,
                    "status": "failed",
                    "success": False,
                    "errors": [error_msg],
                }

        # Non-partitioned table sync
        logger.info(
            f"Starting incremental sync for {source_schema}.{table_name} -> {target_schema}.{table_name}"
        )

        # For non-partitioned tables, we need the full table_info
        table_info = task_info

        # Initialize state tracking
        state_manager = IncrementalStateManager(target_conn_id)

        # Check for resume point if enabled
        resume_point = None
        resumed = False
        if resume_from_checkpoint:
            resume_point = state_manager.get_resume_point(
                table_name=table_name,
                source_schema=source_schema,
                target_schema=target_schema,
            )
            if resume_point:
                logger.info(
                    f"Resuming {source_schema}.{table_name} from checkpoint: "
                    f"batch={resume_point['batch_num']}, last_pk={resume_point['last_pk']}, "
                    f"inserted={resume_point['rows_inserted']}, updated={resume_point['rows_updated']}"
                )
                resumed = True

        # Start or resume sync
        if resumed and resume_point:
            sync_id = resume_point['sync_id']
            # Don't reset counters - we're resuming
            initial_inserted = resume_point['rows_inserted']
            initial_updated = resume_point['rows_updated']
            initial_unchanged = resume_point['rows_unchanged']
        else:
            sync_id = state_manager.start_sync(
                table_name=table_name,
                source_schema=source_schema,
                target_schema=target_schema,
                source_row_count=table_info.get("row_count"),
            )
            initial_inserted = 0
            initial_updated = 0
            initial_unchanged = 0

        try:
            # Staging table approach - fast, handles change detection via IS DISTINCT FROM
            # Now supports resume: if resume_point exists, skip rows already synced
            resume_pk = None
            if resumed and resume_point and resume_point.get('last_pk'):
                last_pk_dict = resume_point['last_pk']
                # Extract the pk value (stored as {"pk": value})
                resume_pk = last_pk_dict.get('pk') if isinstance(last_pk_dict, dict) else last_pk_dict

            transfer_result = transfer_incremental_staging(
                mssql_conn_id=source_conn_id,
                postgres_conn_id=target_conn_id,
                table_info=table_info,
                chunk_size=batch_size,
                resume_from_pk=resume_pk,
            )

            rows_inserted = transfer_result["rows_inserted"]
            rows_updated = transfer_result["rows_updated"]
            rows_unchanged = transfer_result.get("rows_unchanged", 0)
            last_pk_synced = transfer_result.get("last_pk_synced")

            # Update state
            if transfer_result["success"]:
                # Save checkpoint with last_pk for resume support
                if last_pk_synced is not None:
                    state_manager.save_checkpoint(
                        sync_id=sync_id,
                        last_pk={"pk": last_pk_synced},  # Wrap in dict for JSON
                        batch_num=1,  # Staging uses single batch
                        rows_inserted=rows_inserted + initial_inserted,
                        rows_updated=rows_updated + initial_updated,
                        rows_unchanged=rows_unchanged + initial_unchanged,
                    )
                state_manager.complete_sync(
                    sync_id=sync_id,
                    rows_inserted=rows_inserted + initial_inserted,
                    rows_updated=rows_updated + initial_updated,
                    rows_unchanged=rows_unchanged + initial_unchanged,
                )
            else:
                # Save checkpoint even on failure for partial progress
                if last_pk_synced is not None:
                    state_manager.save_checkpoint(
                        sync_id=sync_id,
                        last_pk={"pk": last_pk_synced},
                        batch_num=1,
                        rows_inserted=rows_inserted + initial_inserted,
                        rows_updated=rows_updated + initial_updated,
                        rows_unchanged=rows_unchanged + initial_unchanged,
                    )
                state_manager.fail_sync(
                    sync_id=sync_id,
                    error="; ".join(transfer_result.get("errors", ["Unknown error"])),
                )

            return {
                "table_name": table_name,
                "source_schema": source_schema,
                "target_schema": target_schema,
                "status": "synced" if transfer_result["success"] else "failed",
                "rows_inserted": rows_inserted,
                "rows_updated": rows_updated,
                "unchanged_count": rows_unchanged,
                "rows_copied_to_staging": transfer_result.get("rows_copied_to_staging", 0),
                "elapsed_seconds": transfer_result.get("elapsed_time_seconds", 0),
                "success": transfer_result["success"],
                "errors": transfer_result.get("errors", []),
                "resumed": resumed,
            }

        except Exception as e:
            error_msg = str(e)
            logger.error(f"Error syncing {source_schema}.{table_name}: {error_msg}")
            state_manager.fail_sync(sync_id=sync_id, error=error_msg)
            return {
                "table_name": table_name,
                "source_schema": source_schema,
                "target_schema": target_schema,
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
        # Convert to list to avoid Airflow 3.0 lazy sequence __len__ bug
        results_list = list(sync_results) if sync_results else []

        if not results_list:
            return {
                "status": "no_tables",
                "message": "No tables were synced",
                "tables_synced": 0,
                "total_inserted": 0,
                "total_updated": 0,
            }

        tables_synced = len([r for r in results_list if r.get("success")])
        tables_failed = len([r for r in results_list if not r.get("success")])
        tables_no_changes = len([r for r in results_list if r.get("status") == "no_changes"])

        total_inserted = sum(r.get("rows_inserted", 0) for r in results_list)
        total_updated = sum(r.get("rows_updated", 0) for r in results_list)
        total_unchanged = sum(r.get("unchanged_count", 0) for r in results_list)

        summary = {
            "status": "success" if tables_failed == 0 else "partial_failure",
            "tables_synced": tables_synced,
            "tables_failed": tables_failed,
            "tables_no_changes": tables_no_changes,
            "total_inserted": total_inserted,
            "total_updated": total_updated,
            "total_unchanged": total_unchanged,
            # Note: details removed to avoid XCom serialization issues with large results
        }

        logger.info(
            f"Incremental sync complete: {tables_synced} tables synced, "
            f"{tables_failed} failed, {tables_no_changes} had no changes. "
            f"Total: {total_inserted:,} inserted, {total_updated:,} updated"
        )

        if tables_failed > 0:
            failed_tables = [
                f"{r.get('source_schema', '')}.{r['table_name']}"
                for r in results_list if not r.get("success")
            ]
            logger.error(f"Failed tables: {', '.join(failed_tables)}")

        return summary

    # Define task flow
    state_ready = initialize_state()
    tables = discover_tables()

    # Ensure state is ready before discovering tables
    state_ready >> tables

    # Create sync tasks (with partitioning for large tables)
    sync_tasks = create_sync_tasks(tables)

    # Sync tables/partitions in parallel
    sync_results = sync_table.expand(task_info=sync_tasks)

    # Collect results
    collect_results(sync_results)


# Instantiate the DAG
mssql_to_postgres_incremental()
