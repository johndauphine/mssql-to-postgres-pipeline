"""
SQL Server to PostgreSQL Migration DAG (Data Transfer Only)

This DAG performs data transfer from SQL Server to PostgreSQL.
It assumes tables already exist in the target (created by schema DAG).

Tables must be explicitly specified in 'schema.table' format in include_tables.
Target PostgreSQL schema is derived as: {sourcedb}__{sourceschema} (lowercase)

Workflow:
1. Trigger schema DAG (ensures tables exist with PKs)
2. Discover tables from target PostgreSQL (using derived schemas)
3. Get row counts from source SQL Server
4. Partition large tables for parallel transfer
5. Transfer data via TRUNCATE + COPY
6. Reset sequences for SERIAL columns
7. Trigger validation DAG

This DAG does NOT:
- Create or drop tables (schema DAG does this)
- Create primary keys (schema DAG does this)
- Create foreign keys or indexes (not supported)
"""

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from pendulum import datetime
import pendulum
from datetime import timedelta
from typing import List, Dict, Any
import logging
import re
import os

from mssql_pg_migration import data_transfer
from mssql_pg_migration.notifications import send_success_notification
from mssql_pg_migration.incremental_state import IncrementalStateManager
from mssql_pg_migration.table_config import (
    expand_include_tables_param,
    validate_include_tables,
    parse_include_tables,
    get_source_database,
    derive_target_schema,
    get_default_include_tables,
)

logger = logging.getLogger(__name__)

# Configuration from environment
MAX_PARALLEL_TRANSFERS = int(os.environ.get('MAX_PARALLEL_TRANSFERS', '8'))
MAX_ACTIVE_TASKS = int(os.environ.get('MAX_ACTIVE_TASKS', '16'))
DEFAULT_CHUNK_SIZE = int(os.environ.get('DEFAULT_CHUNK_SIZE', '200000'))
LARGE_TABLE_THRESHOLD = 1_000_000
DEFAULT_INCLUDE_TABLES = get_default_include_tables()


def validate_sql_identifier(identifier: str, identifier_type: str = "identifier") -> str:
    """Validate SQL identifiers to prevent injection."""
    if not identifier:
        raise ValueError(f"Invalid {identifier_type}: cannot be empty")
    if len(identifier) > 128:
        raise ValueError(f"Invalid {identifier_type}: exceeds maximum length")
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', identifier):
        raise ValueError(f"Invalid {identifier_type} '{identifier}'")
    return identifier


def get_partition_count(row_count: int) -> int:
    """Calculate optimal partition count based on table size."""
    max_partitions = int(os.environ.get('MAX_PARTITIONS', '8'))
    if row_count < 2_000_000:
        return min(2, max_partitions)
    elif row_count < 5_000_000:
        return min(4, max_partitions)
    else:
        return max_partitions


@dag(
    dag_id="mssql_to_postgres_migration",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=MAX_ACTIVE_TASKS,
    is_paused_upon_creation=False,
    doc_md=__doc__,
    default_args={
        "owner": "data-team",
        "retries": 3,
        "retry_delay": timedelta(seconds=30),
        "pool": "default_pool",
    },
    params={
        "source_conn_id": Param(default="mssql_source", type="string"),
        "target_conn_id": Param(default="postgres_target", type="string"),
        "chunk_size": Param(default=DEFAULT_CHUNK_SIZE, type="integer", minimum=100, maximum=500000),
        "include_tables": Param(
            default=get_default_include_tables(),
            description="Tables to include in 'schema.table' format (e.g., ['dbo.Users', 'dbo.Posts']). "
                        "Defaults from config/{database}_include_tables.txt or INCLUDE_TABLES env var."
        ),
        "skip_schema_dag": Param(default=False, type="boolean", description="Skip schema DAG trigger"),
        # Restartability parameters
        "resume_mode": Param(
            default=False, type="boolean",
            description="Skip tables that completed in previous runs. Retry failed/running tables."
        ),
        "reset_state": Param(
            default=False, type="boolean",
            description="Clear all migration state before starting (fresh start)."
        ),
        "retry_failed_only": Param(
            default=False, type="boolean",
            description="Only retry tables that failed (skip pending tables). Requires resume_mode=True."
        ),
        "force_refresh_tables": Param(
            default=[],
            description="Force re-run specific tables even if completed (e.g., ['Users', 'Posts'])."
        ),
    },
    tags=["migration", "mssql", "postgres", "etl", "full-refresh"],
)
def mssql_to_postgres_migration():
    """Migration DAG: Trigger schema DAG, then transfer data."""

    from airflow.operators.empty import EmptyOperator

    @task.branch
    def check_skip_schema(**context) -> str:
        """Branch based on skip_schema_dag parameter."""
        params = context["params"]
        if params.get("skip_schema_dag", False):
            logger.info("Skipping schema DAG trigger (skip_schema_dag=True)")
            return "skip_schema_dag_task"
        return "trigger_schema_dag"

    # Branch decision
    branch = check_skip_schema()

    # Step 1a: Trigger schema DAG to ensure tables exist
    trigger_schema = TriggerDagRunOperator(
        task_id="trigger_schema_dag",
        trigger_dag_id="mssql_to_postgres_schema",
        wait_for_completion=True,
        poke_interval=10,
        conf={
            "source_conn_id": "{{ params.source_conn_id }}",
            "target_conn_id": "{{ params.target_conn_id }}",
            "include_tables": "{{ params.include_tables | tojson }}",
            "drop_existing": True,
        },
    )

    # Step 1b: Skip trigger (dummy task for branching)
    skip_schema = EmptyOperator(task_id="skip_schema_dag_task")

    branch >> [trigger_schema, skip_schema]

    @task(trigger_rule="none_failed_min_one_success")
    def initialize_migration_state(**context) -> Dict[str, Any]:
        """
        Initialize migration state for restartability.

        - Creates state table if not exists (with schema migration)
        - Clears state if reset_state=True
        - Resets zombie 'running' states from crashed DAG runs
        - Returns migration summary for resume planning
        """
        params = context["params"]
        dag_run = context["dag_run"]

        target_conn_id = params["target_conn_id"]
        resume_mode = params.get("resume_mode", False)
        reset_state = params.get("reset_state", False)
        retry_failed_only = params.get("retry_failed_only", False)
        force_refresh_tables = params.get("force_refresh_tables", [])

        dag_run_id = dag_run.run_id if dag_run else "unknown"

        # Initialize state manager
        state_mgr = IncrementalStateManager(postgres_conn_id=target_conn_id)
        state_mgr.ensure_state_table_exists()

        # Handle reset_state
        if reset_state:
            count = state_mgr.reset_all_state(migration_type='full')
            logger.info(f"[STATE] Reset state for {count} tables (reset_state=True)")

        # Handle zombie running states
        if resume_mode:
            zombies = state_mgr.reset_stale_running_states(
                current_dag_run_id=dag_run_id,
                migration_type='full'
            )
            if zombies > 0:
                logger.warning(f"[STATE] Reset {zombies} zombie 'running' states from previous DAG runs")

        # Handle force_refresh_tables (reset specific tables to pending)
        if force_refresh_tables:
            # Parse include_tables to get source_schema -> target_schema mapping
            include_tables_raw = params.get("include_tables", [])
            include_tables = expand_include_tables_param(include_tables_raw)
            schema_tables = parse_include_tables(include_tables)
            source_db = get_source_database(params["source_conn_id"])

            for table_name in force_refresh_tables:
                # Find source_schema for this table
                for src_schema, tables in schema_tables.items():
                    if table_name in tables:
                        target_schema = derive_target_schema(source_db, src_schema)
                        state_mgr.reset_table_state(table_name, src_schema, target_schema)
                        logger.info(f"[STATE] Force reset state for {src_schema}.{table_name}")
                        break

        # Get migration summary
        summary = state_mgr.get_migration_summary(migration_type='full')

        # Log resume plan
        logger.info("=" * 60)
        logger.info("[PLAN] Migration State Summary")
        logger.info("=" * 60)
        logger.info(f"  resume_mode: {resume_mode}")
        logger.info(f"  reset_state: {reset_state}")
        logger.info(f"  retry_failed_only: {retry_failed_only}")
        logger.info(f"  force_refresh_tables: {force_refresh_tables}")
        logger.info("-" * 60)
        logger.info(f"  Completed: {summary['completed']['count']} tables")
        logger.info(f"  Failed:    {summary['failed']['count']} tables")
        logger.info(f"  Running:   {summary['running']['count']} tables")
        logger.info(f"  Pending:   {summary['pending']['count']} tables")
        logger.info("=" * 60)

        if resume_mode:
            skip_count = summary['completed']['count'] - len(force_refresh_tables)
            retry_count = summary['failed']['count'] + summary['running']['count']
            logger.info(f"[PLAN] Skipping: {skip_count} completed tables")
            logger.info(f"[PLAN] Retrying: {retry_count} failed/running tables")
            if summary['completed']['tables']:
                logger.info(f"[PLAN] Completed tables: {', '.join(summary['completed']['tables'][:10])}" +
                           ("..." if len(summary['completed']['tables']) > 10 else ""))
            if summary['failed']['tables']:
                logger.info(f"[PLAN] Failed tables: {', '.join(summary['failed']['tables'])}")

        return {
            "dag_run_id": dag_run_id,
            "resume_mode": resume_mode,
            "reset_state": reset_state,
            "retry_failed_only": retry_failed_only,
            "force_refresh_tables": force_refresh_tables,
            "summary": summary,
        }

    @task(trigger_rule="none_failed_min_one_success")
    def discover_target_tables(**context) -> List[Dict[str, Any]]:
        """
        Discover tables from target PostgreSQL based on include_tables.

        Parses include_tables, derives target schemas, and queries
        information_schema to find tables, columns, and PK info.
        """
        params = context["params"]
        source_conn_id = params["source_conn_id"]

        # Parse and expand include_tables parameter
        include_tables_raw = params.get("include_tables", [])
        include_tables = expand_include_tables_param(include_tables_raw)

        # Fall back to environment variable if empty
        if not include_tables and DEFAULT_INCLUDE_TABLES:
            include_tables = expand_include_tables_param(DEFAULT_INCLUDE_TABLES)

        # Validate include_tables
        validate_include_tables(include_tables)

        # Parse into {schema: [tables]} dict
        schema_tables = parse_include_tables(include_tables)

        # Get source database name for deriving target schemas
        source_db = get_source_database(source_conn_id)

        # Build mapping: source_schema -> target_schema
        target_schema_map = {
            src_schema: derive_target_schema(source_db, src_schema)
            for src_schema in schema_tables.keys()
        }

        logger.info(f"Target schemas to query: {list(target_schema_map.values())}")

        from airflow.providers.postgres.hooks.postgres import PostgresHook
        pg_hook = PostgresHook(postgres_conn_id=params["target_conn_id"])

        discovered_tables = []

        # Query each target schema
        for source_schema, tables in schema_tables.items():
            target_schema = target_schema_map[source_schema]

            # Get columns for tables in this schema
            columns_query = """
                SELECT table_name, column_name, ordinal_position
                FROM information_schema.columns
                WHERE table_schema = %s
                ORDER BY table_name, ordinal_position
            """
            columns_result = pg_hook.get_records(columns_query, parameters=[target_schema])

            # Group columns by table
            table_columns = {}
            for row in columns_result:
                tbl, col, _ = row
                if tbl not in table_columns:
                    table_columns[tbl] = []
                table_columns[tbl].append(col)

            # Get primary key columns
            pk_query = """
                SELECT tc.table_name, kcu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_schema = %s
                ORDER BY tc.table_name, kcu.ordinal_position
            """
            pk_result = pg_hook.get_records(pk_query, parameters=[target_schema])

            # Group PKs by table
            table_pks = {}
            for row in pk_result:
                tbl, col = row
                if tbl not in table_pks:
                    table_pks[tbl] = []
                table_pks[tbl].append(col)

            # Build table info for tables in this schema
            for table_name in tables:
                # Check if table exists in target (case-insensitive match)
                table_lower = table_name.lower()
                found_table = None
                for t in table_columns.keys():
                    if t.lower() == table_lower:
                        found_table = t
                        break

                if not found_table:
                    logger.warning(f"Table {target_schema}.{table_name} not found in target, skipping")
                    continue

                discovered_tables.append({
                    "table_name": found_table,
                    "source_schema": source_schema,
                    "target_schema": target_schema,
                    "columns": table_columns.get(found_table, []),
                    "pk_columns": table_pks.get(found_table, []),
                })

        logger.info(f"Discovered {len(discovered_tables)} tables for migration")
        return discovered_tables

    @task
    def get_source_row_counts(tables: List[Dict[str, Any]], **context) -> List[Dict[str, Any]]:
        """
        Get row counts from source SQL Server for partitioning decisions.

        Uses source_schema from each table's info dict.
        """
        params = context["params"]

        from mssql_pg_migration.odbc_helper import OdbcConnectionHelper
        mssql_hook = OdbcConnectionHelper(odbc_conn_id=params["source_conn_id"])

        tables_with_counts = []

        for table_info in tables:
            table_name = table_info["table_name"]
            source_schema = table_info["source_schema"]

            try:
                # Get row count from SQL Server
                count_query = f"""
                    SELECT SUM(p.rows) as row_count
                    FROM sys.partitions p
                    INNER JOIN sys.tables t ON p.object_id = t.object_id
                    INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                    WHERE s.name = ? AND t.name = ? AND p.index_id IN (0, 1)
                """
                result = mssql_hook.get_first(count_query, parameters=[source_schema, table_name])
                row_count = result[0] if result and result[0] else 0

                # Get PK column info for partitioning
                pk_query = """
                    SELECT c.name, t.name as data_type
                    FROM sys.indexes i
                    JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
                    JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                    JOIN sys.types t ON c.user_type_id = t.user_type_id
                    JOIN sys.tables tbl ON i.object_id = tbl.object_id
                    JOIN sys.schemas s ON tbl.schema_id = s.schema_id
                    WHERE i.is_primary_key = 1 AND s.name = ? AND tbl.name = ?
                    ORDER BY ic.key_ordinal
                """
                pk_result = mssql_hook.get_records(pk_query, parameters=[source_schema, table_name])
                pk_columns_info = {
                    'columns': [{'name': r[0], 'data_type': r[1]} for r in pk_result] if pk_result else [],
                    'is_composite': len(pk_result) > 1 if pk_result else False,
                }

                tables_with_counts.append({
                    **table_info,
                    "row_count": row_count,
                    "pk_columns_info": pk_columns_info,
                })

                logger.info(f"  {source_schema}.{table_name}: {row_count:,} rows")

            except Exception as e:
                logger.warning(f"Could not get row count for {source_schema}.{table_name}: {e}")
                tables_with_counts.append({
                    **table_info,
                    "row_count": 0,
                    "pk_columns_info": {'columns': [], 'is_composite': False},
                })

        total_rows = sum(t["row_count"] for t in tables_with_counts)
        logger.info(f"Total rows to transfer: {total_rows:,}")

        return tables_with_counts

    @task
    def prepare_transfer_plan(
        tables: List[Dict[str, Any]],
        migration_state: Dict[str, Any],
        **context
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Prepare transfer plan: split into regular tables and partitioned tables.

        Uses source_schema from each table's info dict.
        Large tables (>1M rows) are partitioned for parallel transfer.

        In resume_mode, filters out completed tables unless in force_refresh_tables.
        """
        params = context["params"]
        dag_run = context["dag_run"]
        dag_run_id = dag_run.run_id if dag_run else "unknown"

        resume_mode = migration_state.get("resume_mode", False)
        retry_failed_only = migration_state.get("retry_failed_only", False)
        force_refresh_tables = migration_state.get("force_refresh_tables", [])
        summary = migration_state.get("summary", {})

        # Get sets of tables by status for quick lookup
        completed_tables = set(summary.get('completed', {}).get('tables', []))
        failed_tables = set(summary.get('failed', {}).get('tables', []))
        running_tables = set(summary.get('running', {}).get('tables', []))
        force_refresh_set = set(force_refresh_tables)

        regular_tables = []
        partitions = []
        skipped_tables = []

        from mssql_pg_migration.odbc_helper import OdbcConnectionHelper
        mssql_hook = OdbcConnectionHelper(odbc_conn_id=params["source_conn_id"])

        # Initialize state manager for recording state
        target_conn_id = params["target_conn_id"]
        state_mgr = IncrementalStateManager(postgres_conn_id=target_conn_id)

        for table_info in tables:
            table_name = table_info["table_name"]
            source_schema = table_info["source_schema"]
            target_schema = table_info.get("target_schema", "unknown")
            row_count = table_info.get("row_count", 0)
            pk_info = table_info.get("pk_columns_info", {})
            pk_columns = pk_info.get("columns", [])

            # Check if we should skip this table in resume mode
            if resume_mode:
                if table_name in completed_tables and table_name not in force_refresh_set:
                    skipped_tables.append(table_name)
                    logger.info(f"  {source_schema}.{table_name} -> SKIPPED (completed in previous run)")
                    continue

                if retry_failed_only:
                    # Only process failed/running tables
                    if table_name not in failed_tables and table_name not in running_tables:
                        if table_name not in force_refresh_set:
                            skipped_tables.append(table_name)
                            logger.info(f"  {source_schema}.{table_name} -> SKIPPED (not failed, retry_failed_only=True)")
                            continue

            # Add state tracking info to table_info
            table_info_with_state = {
                **table_info,
                "dag_run_id": dag_run_id,
            }

            # Check for partition-level resume in resume mode
            # If table was previously partitioned and has incomplete partitions, resume from there
            if resume_mode and (table_name in running_tables or table_name in failed_tables):
                table_state = state_mgr.get_partitioned_table_state(
                    table_name, source_schema, target_schema, migration_type='full'
                )
                if table_state and table_state.get('partition_info'):
                    stored_partition_info = table_state['partition_info']
                    stored_partitions = stored_partition_info.get('partitions', [])

                    # Find incomplete partitions (pending, running, or failed)
                    incomplete = [
                        p for p in stored_partitions
                        if p.get('status') in ('pending', 'running', 'failed')
                    ]
                    completed_partitions = [
                        p for p in stored_partitions
                        if p.get('status') == 'completed'
                    ]

                    if incomplete and completed_partitions:
                        # Have both completed and incomplete - do partition resume
                        logger.info(f"  {source_schema}.{table_name} -> PARTITION RESUME "
                                   f"({len(completed_partitions)} completed, {len(incomplete)} to retry)")

                        # Get pk_column from table_info for building WHERE clause
                        pk_column = pk_columns[0]['name'] if pk_columns else None

                        # Check if table uses composite PK (ROW_NUMBER mode) - no checkpoint resume for that
                        is_composite = pk_info.get("is_composite", False)

                        for stored_p in incomplete:
                            partition_idx = stored_p.get('id', 0)
                            min_pk = stored_p.get('min_pk')
                            max_pk = stored_p.get('max_pk')
                            was_failed = stored_p.get('status') == 'failed'
                            last_pk_synced = stored_p.get('last_pk_synced')

                            # Checkpoint resume only for keyset mode (single PK, sequential reader)
                            # ROW_NUMBER mode and parallel readers don't support mid-partition resume
                            resume_from_pk = None
                            if last_pk_synced is not None and not is_composite:
                                resume_from_pk = last_pk_synced
                                logger.info(f"    partition_{partition_idx + 1}: checkpoint at PK={last_pk_synced}")

                            # Build WHERE clause if we have PK bounds
                            where_clause = None
                            if pk_column and min_pk is not None and max_pk is not None:
                                safe_pk = validate_sql_identifier(pk_column, "pk column")
                                # Format values for SQL
                                if isinstance(min_pk, str):
                                    min_sql = f"'{min_pk.replace(chr(39), chr(39)+chr(39))}'"
                                    max_sql = f"'{max_pk.replace(chr(39), chr(39)+chr(39))}'"
                                else:
                                    min_sql = str(min_pk)
                                    max_sql = str(max_pk)

                                # Last partition uses >= only (no upper bound)
                                total_parts = stored_partition_info.get('total_partitions', len(stored_partitions))
                                if partition_idx == total_parts - 1:
                                    where_clause = f"[{safe_pk}] >= {min_sql}"
                                elif partition_idx == 0:
                                    where_clause = f"[{safe_pk}] <= {max_sql}"
                                else:
                                    where_clause = f"[{safe_pk}] >= {min_sql} AND [{safe_pk}] <= {max_sql}"

                            # Cleanup logic (Gemini fix for stale checkpoints):
                            # - If resuming from checkpoint: delete rows AFTER checkpoint (handles stale checkpoint)
                            # - If no checkpoint (full retry): delete entire partition range
                            # - If not failed: no cleanup needed
                            cleanup_required = was_failed and resume_from_pk is None

                            resume_partition = {
                                **table_info_with_state,
                                "partition_name": f"partition_{partition_idx + 1}",
                                "partition_index": partition_idx,
                                "where_clause": where_clause,
                                "pk_column": pk_column,
                                "min_pk": min_pk,
                                "max_pk": max_pk,
                                "estimated_rows": stored_p.get('rows', 0),
                                "truncate_first": False,  # Never truncate on resume
                                "total_partitions": stored_partition_info.get('total_partitions', len(stored_partitions)),
                                "is_resume": True,
                                "cleanup_required": cleanup_required,
                                "resume_from_pk": resume_from_pk,  # Checkpoint resume
                                "sync_id": table_state.get('sync_id'),  # Preserve existing sync_id
                            }
                            partitions.append(resume_partition)

                            if resume_from_pk:
                                status_msg = f"CHECKPOINT (pk > {resume_from_pk})"
                            elif was_failed:
                                status_msg = "RETRY (cleanup)"
                            else:
                                status_msg = "RESUME"
                            logger.info(f"    partition_{partition_idx + 1}: {status_msg}")

                        # Skip normal table processing - we've built resume partitions
                        continue

            if row_count < LARGE_TABLE_THRESHOLD:
                # Small table: regular transfer
                regular_tables.append({
                    **table_info_with_state,
                    "truncate_first": True,
                })
                logger.info(f"  {source_schema}.{table_name} ({row_count:,} rows) -> regular transfer")
                continue

            # Large table: partition it
            if not pk_columns:
                # No PK: fall back to regular transfer
                logger.warning(f"  {source_schema}.{table_name} has no PK, using regular transfer")
                regular_tables.append({
                    **table_info_with_state,
                    "truncate_first": True,
                })
                continue

            is_composite = pk_info.get("is_composite", False)
            partition_count = get_partition_count(row_count)

            try:
                safe_table = validate_sql_identifier(table_name, "table")
                safe_schema = validate_sql_identifier(source_schema, "schema")
            except ValueError as e:
                logger.warning(f"  {source_schema}.{table_name}: {e}, using regular transfer")
                regular_tables.append({**table_info_with_state, "truncate_first": True})
                continue

            if is_composite:
                # Composite PK: use ROW_NUMBER ranges
                rows_per_partition = (row_count + partition_count - 1) // partition_count
                pk_col_names = [c['name'] for c in pk_columns]

                for i in range(partition_count):
                    start_row = i * rows_per_partition + 1
                    end_row = min((i + 1) * rows_per_partition, row_count)

                    partitions.append({
                        **table_info_with_state,
                        "partition_name": f"partition_{i + 1}",
                        "partition_index": i,
                        "use_row_number": True,
                        "order_by_columns": pk_col_names,
                        "start_row": start_row,
                        "end_row": end_row,
                        "pk_column": pk_col_names[0],
                        "estimated_rows": end_row - start_row + 1,
                        "truncate_first": i == 0,
                        "total_partitions": partition_count,
                    })

                logger.info(f"  {source_schema}.{table_name} ({row_count:,} rows) -> {partition_count} partitions (ROW_NUMBER)")
            else:
                # Single PK: use NTILE boundaries
                pk_column = pk_columns[0]['name']
                safe_pk = validate_sql_identifier(pk_column, "pk column")

                boundaries_query = f"""
                WITH numbered AS (
                    SELECT [{safe_pk}],
                           NTILE({partition_count}) OVER (ORDER BY [{safe_pk}]) as partition_id
                    FROM [{safe_schema}].[{safe_table}]
                )
                SELECT partition_id, MIN([{safe_pk}]) as min_pk, MAX([{safe_pk}]) as max_pk, COUNT(*) as cnt
                FROM numbered
                GROUP BY partition_id
                ORDER BY partition_id
                """

                try:
                    boundaries = mssql_hook.get_records(boundaries_query)
                except Exception as e:
                    logger.warning(f"  {source_schema}.{table_name}: NTILE failed ({e}), using regular transfer")
                    regular_tables.append({**table_info_with_state, "truncate_first": True})
                    continue

                if not boundaries:
                    regular_tables.append({**table_info_with_state, "truncate_first": True})
                    continue

                for i, boundary in enumerate(boundaries):
                    _, min_pk, max_pk, part_count = boundary

                    # Format boundary values
                    if isinstance(min_pk, str):
                        min_sql = f"'{min_pk.replace(chr(39), chr(39)+chr(39))}'"
                        max_sql = f"'{max_pk.replace(chr(39), chr(39)+chr(39))}'"
                    elif isinstance(min_pk, (int, float)):
                        min_sql = str(min_pk)
                        max_sql = str(max_pk)
                    else:
                        min_sql = f"'{min_pk}'"
                        max_sql = f"'{max_pk}'"

                    # Build WHERE clause
                    if i == len(boundaries) - 1:
                        where = f"[{safe_pk}] >= {min_sql}"
                    elif i == 0:
                        where = f"[{safe_pk}] <= {max_sql}"
                    else:
                        where = f"[{safe_pk}] >= {min_sql} AND [{safe_pk}] <= {max_sql}"

                    partitions.append({
                        **table_info_with_state,
                        "partition_name": f"partition_{i + 1}",
                        "partition_index": i,
                        "where_clause": where,
                        "pk_column": pk_column,
                        "min_pk": min_pk,
                        "max_pk": max_pk,
                        "estimated_rows": part_count,
                        "truncate_first": i == 0,
                        "total_partitions": len(boundaries),
                    })

                logger.info(f"  {source_schema}.{table_name} ({row_count:,} rows) -> {len(boundaries)} partitions (NTILE)")

        # Split partitions into first (truncate) and remaining (no truncate)
        first_partitions = [p for p in partitions if p.get("truncate_first", False)]
        remaining_partitions = [p for p in partitions if not p.get("truncate_first", False)]

        logger.info(f"Transfer plan: {len(regular_tables)} regular tables, "
                   f"{len(first_partitions)} first partitions, {len(remaining_partitions)} remaining partitions")

        if skipped_tables:
            logger.info(f"Skipped {len(skipped_tables)} tables (already completed): {', '.join(skipped_tables)}")

        return {
            "regular": regular_tables,
            "first_partitions": first_partitions,
            "remaining_partitions": remaining_partitions,
            "skipped": skipped_tables,
            "dag_run_id": dag_run_id,
        }

    @task
    def get_regular_tables(plan: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        """Extract regular tables from plan."""
        return plan.get("regular", [])

    @task
    def get_first_partitions(plan: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        """Extract first partitions from plan."""
        return plan.get("first_partitions", [])

    @task
    def get_remaining_partitions(plan: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        """Extract remaining partitions from plan."""
        return plan.get("remaining_partitions", [])

    @task(max_active_tis_per_dagrun=MAX_PARALLEL_TRANSFERS)
    def transfer_table_data(table_info: Dict[str, Any], **context) -> Dict[str, Any]:
        """Transfer data for a single table (TRUNCATE + COPY)."""
        params = context["params"]
        table_name = table_info["table_name"]
        source_schema = table_info.get("source_schema", "unknown")
        target_schema = table_info.get("target_schema", "unknown")
        dag_run_id = table_info.get("dag_run_id", "unknown")
        row_count = table_info.get("row_count", 0)

        logger.info(f"Transferring {source_schema}.{table_name} ({row_count:,} rows)")

        # Initialize state manager and start sync
        state_mgr = IncrementalStateManager(postgres_conn_id=params["target_conn_id"])
        sync_id = state_mgr.start_sync(
            table_name=table_name,
            source_schema=source_schema,
            target_schema=target_schema,
            source_row_count=row_count,
            migration_type='full',
            dag_run_id=dag_run_id,
        )

        result = data_transfer.transfer_table_data(
            mssql_conn_id=params["source_conn_id"],
            postgres_conn_id=params["target_conn_id"],
            table_info=table_info,
            chunk_size=params["chunk_size"],
            truncate=table_info.get("truncate_first", True),
        )

        result["table_name"] = table_name
        result["source_schema"] = source_schema
        result["target_schema"] = target_schema
        result["sync_id"] = sync_id

        if result["success"]:
            logger.info(f"  {source_schema}.{table_name}: {result['rows_transferred']:,} rows "
                       f"in {result['elapsed_time_seconds']:.1f}s "
                       f"({result['avg_rows_per_second']:,.0f} rows/sec)")
            # Mark as completed
            state_mgr.complete_sync(
                sync_id=sync_id,
                rows_inserted=result.get("rows_transferred", 0),
                rows_updated=0,
                rows_unchanged=0,
                target_row_count=result.get("target_row_count"),
            )
        else:
            errors = result.get('errors', [])
            error_msg = '; '.join(str(e) for e in errors) if errors else 'Unknown error'
            logger.error(f"  {source_schema}.{table_name}: FAILED - {error_msg}")
            # Mark as failed
            state_mgr.fail_sync(sync_id=sync_id, error=error_msg)

        return result

    @task(max_active_tis_per_dagrun=MAX_PARALLEL_TRANSFERS)
    def transfer_partition(partition_info: Dict[str, Any], **context) -> Dict[str, Any]:
        """Transfer a partition of a large table."""
        params = context["params"]
        table_name = partition_info["table_name"]
        source_schema = partition_info.get("source_schema", "unknown")
        target_schema = partition_info.get("target_schema", "unknown")
        dag_run_id = partition_info.get("dag_run_id", "unknown")
        partition_name = partition_info["partition_name"]
        partition_index = partition_info.get("partition_index", 0)
        total_partitions = partition_info.get("total_partitions", 1)
        is_first_partition = partition_info.get("truncate_first", False)
        row_count = partition_info.get("row_count", 0)
        is_resume = partition_info.get("is_resume", False)
        cleanup_required = partition_info.get("cleanup_required", False)
        provided_sync_id = partition_info.get("sync_id")

        logger.info(f"Transferring {source_schema}.{table_name} {partition_name} "
                   f"(~{partition_info.get('estimated_rows', 0):,} rows)"
                   f"{' [RESUME]' if is_resume else ''}"
                   f"{' [CLEANUP]' if cleanup_required else ''}")

        # Initialize state manager
        state_mgr = IncrementalStateManager(postgres_conn_id=params["target_conn_id"])

        # Determine sync_id: use provided one for resume, or create/find
        sync_id = None
        if is_resume and provided_sync_id:
            # Resume mode: use the existing sync_id
            sync_id = provided_sync_id
        elif is_first_partition:
            # First partition of fresh transfer: create state record
            initial_partition_info = {
                "total_partitions": total_partitions,
                "partitions": [
                    {
                        "id": i,
                        "status": "pending",
                        "min_pk": partition_info.get("min_pk") if i == partition_index else None,
                        "max_pk": partition_info.get("max_pk") if i == partition_index else None,
                    }
                    for i in range(total_partitions)
                ]
            }
            sync_id = state_mgr.start_sync(
                table_name=table_name,
                source_schema=source_schema,
                target_schema=target_schema,
                source_row_count=row_count,
                migration_type='full',
                dag_run_id=dag_run_id,
                partition_info=initial_partition_info,
            )
        else:
            # Non-first partition of fresh transfer: get sync_id from state
            conn = state_mgr._hook.get_conn()
            try:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """SELECT id FROM _migration_state
                           WHERE table_name = %s AND source_schema = %s AND target_schema = %s""",
                        (table_name, source_schema, target_schema)
                    )
                    row = cursor.fetchone()
                    if row:
                        sync_id = row[0]
            finally:
                conn.close()

        # Get resume checkpoint if resuming mid-partition
        resume_from_pk = partition_info.get("resume_from_pk")

        # Cleanup logic (Gemini fix for stale checkpoints):
        # 1. Full retry (no checkpoint): delete entire partition range
        # 2. Checkpoint resume: delete rows AFTER checkpoint to handle stale checkpoint case
        #    (worker may have committed rows but crashed before saving checkpoint)
        pk_column = partition_info.get("pk_column")
        min_pk = partition_info.get("min_pk")
        max_pk = partition_info.get("max_pk")

        if pk_column and max_pk is not None:
            from airflow.providers.postgres.hooks.postgres import PostgresHook
            pg_hook = PostgresHook(postgres_conn_id=params["target_conn_id"])

            if cleanup_required and min_pk is not None:
                # Full retry: delete entire partition range
                delete_query = f"""
                    DELETE FROM {target_schema}."{table_name}"
                    WHERE "{pk_column}" >= %s AND "{pk_column}" <= %s
                """
                delete_params = (min_pk, max_pk)
                cleanup_desc = f"PK {min_pk} to {max_pk}"

                conn = None
                try:
                    conn = pg_hook.get_conn()
                    with conn.cursor() as cursor:
                        cursor.execute(delete_query, delete_params)
                        deleted_count = cursor.rowcount
                    conn.commit()
                    logger.info(f"  Cleaned up {deleted_count:,} rows from failed partition ({cleanup_desc})")
                except Exception as e:
                    logger.warning(f"  Cleanup failed (non-fatal): {e}")
                finally:
                    if conn:
                        conn.close()

            elif resume_from_pk is not None:
                # Checkpoint resume: delete rows AFTER checkpoint (handles stale checkpoint)
                delete_query = f"""
                    DELETE FROM {target_schema}."{table_name}"
                    WHERE "{pk_column}" > %s AND "{pk_column}" <= %s
                """
                delete_params = (resume_from_pk, max_pk)
                cleanup_desc = f"PK > {resume_from_pk} to {max_pk}"

                conn = None
                try:
                    conn = pg_hook.get_conn()
                    with conn.cursor() as cursor:
                        cursor.execute(delete_query, delete_params)
                        deleted_count = cursor.rowcount
                    conn.commit()
                    if deleted_count > 0:
                        logger.info(f"  Cleaned up {deleted_count:,} stale rows after checkpoint ({cleanup_desc})")
                    else:
                        logger.info(f"  No stale rows to clean up after checkpoint")
                except Exception as e:
                    logger.warning(f"  Stale checkpoint cleanup failed (non-fatal): {e}")
                finally:
                    if conn:
                        conn.close()

        # Update partition status to 'running' and store PK bounds
        if sync_id:
            state_mgr.update_partition_status(
                sync_id, partition_index, 'running',
                min_pk=partition_info.get("min_pk"),
                max_pk=partition_info.get("max_pk"),
            )

        if resume_from_pk is not None:
            logger.info(f"  Resuming partition from checkpoint: PK > {resume_from_pk}")

        result = data_transfer.transfer_table_data(
            mssql_conn_id=params["source_conn_id"],
            postgres_conn_id=params["target_conn_id"],
            table_info=partition_info,
            chunk_size=params["chunk_size"],
            truncate=partition_info.get("truncate_first", False),
            where_clause=partition_info.get("where_clause"),
            resume_from_pk=resume_from_pk,
        )

        result["table_name"] = table_name
        result["source_schema"] = source_schema
        result["target_schema"] = target_schema
        result["partition_name"] = partition_name
        result["partition_index"] = partition_index
        result["is_partition"] = True
        result["sync_id"] = sync_id
        result["success"] = len(result.get("errors", [])) == 0 and result.get("rows_transferred", 0) > 0

        if result["success"]:
            logger.info(f"  {source_schema}.{table_name} {partition_name}: {result['rows_transferred']:,} rows "
                       f"in {result['elapsed_time_seconds']:.1f}s")
            # Update partition status to 'completed'
            if sync_id:
                state_mgr.update_partition_status(
                    sync_id, partition_index, 'completed',
                    rows_transferred=result.get("rows_transferred", 0)
                )
        else:
            errors = result.get('errors', [])
            error_msg = '; '.join(str(e) for e in errors) if errors else 'Unknown error'
            logger.error(f"  {source_schema}.{table_name} {partition_name}: FAILED - {error_msg}")
            # Update partition status to 'failed' with checkpoint for resume
            last_pk_synced = result.get("last_pk_synced")
            if last_pk_synced is not None:
                logger.info(f"  Saving checkpoint: last_pk_synced = {last_pk_synced}")
            if sync_id:
                state_mgr.update_partition_status(
                    sync_id, partition_index, 'failed',
                    rows_transferred=result.get("rows_transferred", 0),
                    error=error_msg,
                    last_pk_synced=last_pk_synced,
                )

        return result

    @task(trigger_rule="all_done")
    def collect_results(**context) -> List[Dict[str, Any]]:
        """
        Aggregate all transfer results by querying XCom table directly.

        NOTE: Airflow 3.0 changed xcom_pull() behavior for mapped tasks - it returns
        None instead of all mapped task results (see https://github.com/apache/airflow/issues/50982).
        We work around this by querying the XCom table directly via the metadata database.

        Requires AIRFLOW_CONN_AIRFLOW_DB connection to be configured pointing to the
        Airflow metadata database (PostgreSQL).
        """
        import json
        from collections import defaultdict
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from airflow.models.connection import Connection

        dag_run = context["dag_run"]
        dag_id = dag_run.dag_id
        run_id = dag_run.run_id

        # Check if airflow_db connection exists before attempting to use it
        try:
            Connection.get_connection_from_secrets("airflow_db")
        except Exception as e:
            logger.error(
                "airflow_db connection not found. This connection is required for "
                "collect_results to work around Airflow 3.0 XCom bug. "
                "Add AIRFLOW_CONN_AIRFLOW_DB to your environment pointing to the "
                "Airflow metadata database. Error: %s", e
            )
            raise ValueError(
                "Missing required connection 'airflow_db'. See logs for details."
            ) from e

        # Query XCom directly from metadata database to work around Airflow 3.0 bug
        # where xcom_pull returns None for dynamically mapped tasks
        pg_hook = PostgresHook(postgres_conn_id="airflow_db")

        def get_mapped_results(task_id: str) -> List[Dict]:
            """Pull all return_value XComs for a mapped task."""
            query = """
                SELECT value::text
                FROM xcom
                WHERE dag_id = %s
                  AND run_id = %s
                  AND task_id = %s
                  AND key = 'return_value'
                ORDER BY map_index
            """
            with pg_hook.get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(query, [dag_id, run_id, task_id])
                    rows = cur.fetchall()
            results = []
            for row in rows:
                try:
                    results.append(json.loads(row[0]))
                except (json.JSONDecodeError, TypeError) as e:
                    logger.warning(
                        "Failed to decode XCom JSON value for task_id=%s, dag_id=%s, run_id=%s: %s",
                        task_id, dag_id, run_id, e
                    )
            return results

        regular_results = get_mapped_results("transfer_table_data")
        first_results = get_mapped_results("transfer_first_partitions")
        remaining_results = get_mapped_results("transfer_remaining_partitions")

        logger.info(f"Retrieved XCom results: {len(regular_results)} regular, "
                   f"{len(first_results)} first partitions, {len(remaining_results)} remaining partitions")

        all_results = []

        # Process regular tables (non-partitioned)
        for r in regular_results:
            if r and isinstance(r, dict):
                all_results.append(r)

        # Aggregate partitions by table
        table_partitions = defaultdict(list)
        for r in first_results + remaining_results:
            if r and isinstance(r, dict):
                key = (r.get("source_schema", ""), r.get("table_name", "Unknown"))
                table_partitions[key].append(r)

        # Get target_conn_id from params for state updates
        params = context["params"]
        target_conn_id = params["target_conn_id"]
        state_mgr = IncrementalStateManager(postgres_conn_id=target_conn_id)

        for (source_schema, table_name), parts in table_partitions.items():
            total_rows = sum(p.get("rows_transferred", 0) for p in parts)
            success = all(p.get("success", False) for p in parts)
            target_schema = parts[0].get("target_schema", "unknown") if parts else "unknown"

            # Finalize partitioned table state
            sync_id = parts[0].get("sync_id") if parts else None
            if sync_id:
                if success:
                    state_mgr.complete_sync(
                        sync_id=sync_id,
                        rows_inserted=total_rows,
                        rows_updated=0,
                        rows_unchanged=0,
                        target_row_count=total_rows,
                    )
                else:
                    # Find the first failed partition error
                    failed_parts = [p for p in parts if not p.get("success", False)]
                    error_msg = "Partition transfer failed"
                    if failed_parts:
                        errors = failed_parts[0].get("errors", [])
                        if errors:
                            error_msg = '; '.join(str(e) for e in errors)
                    state_mgr.fail_sync(sync_id=sync_id, error=error_msg)

            all_results.append({
                "table_name": table_name,
                "source_schema": source_schema,
                "target_schema": target_schema,
                "rows_transferred": total_rows,
                "success": success,
                "partitions": len(parts),
                "sync_id": sync_id,
            })

        successful = sum(1 for r in all_results if r.get("success"))
        failed = len(all_results) - successful
        total_rows = sum(r.get("rows_transferred", 0) for r in all_results)

        logger.info(f"Transfer complete: {successful} tables succeeded, {failed} failed, "
                   f"{total_rows:,} total rows")

        return all_results

    @task(trigger_rule="all_done")
    def reset_sequences(
        tables: List[Dict[str, Any]],
        results: List[Dict[str, Any]],
        **context
    ) -> str:
        """
        Reset SERIAL/BIGSERIAL sequences to MAX(column) value after bulk data load.

        When using COPY to bulk load data, PostgreSQL sequences don't auto-update.
        This task finds all columns with sequences and resets them to avoid
        duplicate key errors on subsequent INSERTs.

        Only processes tables that:
        1. Have SERIAL/BIGSERIAL columns (detected via column_default LIKE 'nextval%')
        2. Were successfully transferred in this run
        """
        params = context["params"]
        target_conn_id = params["target_conn_id"]

        # Get successful tables with their target schemas
        successful = {
            (r.get("target_schema"), r["table_name"])
            for r in results if r.get("success")
        }
        logger.info(f"Found {len(successful)} successfully transferred tables")

        # Get unique target schemas from tables
        target_schemas = set(t.get("target_schema") for t in tables if t.get("target_schema"))
        logger.info(f"Processing schemas: {sorted(target_schemas)}")

        from airflow.providers.postgres.hooks.postgres import PostgresHook
        pg_hook = PostgresHook(postgres_conn_id=target_conn_id)

        reset_count = 0

        from psycopg2 import sql

        for target_schema in target_schemas:
            # Find columns with sequences (SERIAL/BIGSERIAL) in this schema
            # Only these columns need sequence reset - tables without sequences are skipped
            seq_query = """
                SELECT table_name, column_name
                FROM information_schema.columns
                WHERE table_schema = %s
                  AND column_default LIKE 'nextval%%'
            """
            # NOTE: Using direct cursor instead of pg_hook.get_records() due to
            # Airflow 3.0 bug where get_records() throws IndexError in _run_command
            # when processing parameterized queries. This is a provider compatibility
            # issue with airflow-providers-common-sql.
            with pg_hook.get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(seq_query, [target_schema])
                    seq_columns = cur.fetchall()

            if not seq_columns:
                logger.info(f"Schema {target_schema}: no sequences found, skipping")
                continue

            logger.info(f"Schema {target_schema}: found {len(seq_columns)} columns with sequences")

            with pg_hook.get_conn() as conn:
                with conn.cursor() as cursor:
                    for table_name, col_name in seq_columns:
                        if (target_schema, table_name) not in successful:
                            continue
                        try:
                            # Use sql.Identifier for proper escaping to prevent SQL injection
                            # NOTE: pg_get_serial_sequence requires quoted identifiers to preserve
                            # case sensitivity. Use "schema"."table" format in the literal string.
                            # Escape double quotes within names (replace " with "") per SQL standard.
                            safe_schema = target_schema.replace('"', '""')
                            safe_table = table_name.replace('"', '""')
                            reset_sql = sql.SQL("""
                            SELECT setval(
                                pg_get_serial_sequence({table}, {column}),
                                COALESCE((SELECT MAX({col_id}) FROM {schema}.{table_id}), 1),
                                true
                            )
                            """).format(
                                table=sql.Literal(f'"{safe_schema}"."{safe_table}"'),
                                column=sql.Literal(col_name),
                                col_id=sql.Identifier(col_name),
                                schema=sql.Identifier(target_schema),
                                table_id=sql.Identifier(table_name),
                            )
                            cursor.execute(reset_sql)
                            reset_count += 1
                            logger.info(f"Reset sequence for {target_schema}.{table_name}.{col_name}")
                        except Exception as e:
                            logger.warning(f"Could not reset sequence for {target_schema}.{table_name}.{col_name}: {e}")
                conn.commit()

        logger.info(f"Reset {reset_count} sequences")
        return f"Reset {reset_count} sequences"

    @task(trigger_rule="all_done")
    def generate_summary(results: List[Dict[str, Any]], **context) -> str:
        """Generate migration summary and send notifications."""
        dag_run = context.get("dag_run")

        successful = [r for r in results if r.get("success")]
        failed = [r for r in results if not r.get("success")]
        total_rows = sum(r.get("rows_transferred", 0) for r in successful)

        duration = 0
        if dag_run and dag_run.start_date:
            duration = (pendulum.now("UTC") - dag_run.start_date).total_seconds()

        rps = int(total_rows / duration) if duration > 0 else 0

        stats = {
            "tables_migrated": len(successful),
            "tables_failed": len(failed),
            "total_rows": total_rows,
            "rows_per_second": rps,
            "tables_list": [f"{r.get('source_schema', '')}.{r['table_name']}" for r in successful],
            "failed_tables_list": [f"{r.get('source_schema', '')}.{r['table_name']}" for r in failed],
        }

        if failed:
            logger.warning(f"Migration completed with {len(failed)} failures: "
                          f"{', '.join(stats['failed_tables_list'])}")
        else:
            logger.info(f"Migration completed: {len(successful)} tables, "
                       f"{total_rows:,} rows in {duration:.1f}s ({rps:,} rows/sec)")

        send_success_notification(
            dag_id=dag_run.dag_id if dag_run else "unknown",
            run_id=dag_run.run_id if dag_run else "unknown",
            start_date=dag_run.start_date if dag_run else None,
            duration_seconds=duration,
            stats=stats,
        )

        return f"Migrated {len(successful)} tables, {total_rows:,} rows"

    # =========================================================================
    # Task Flow
    # =========================================================================

    # 1. Branch based on skip_schema_dag, then trigger schema DAG or skip
    # 2. Initialize migration state (restartability)
    migration_state = initialize_migration_state()
    [trigger_schema, skip_schema] >> migration_state

    # 3. Discover tables from target PostgreSQL (using derived schemas)
    tables = discover_target_tables()
    migration_state >> tables

    # 4. Get row counts from source
    tables_with_counts = get_source_row_counts(tables)

    # 5. Prepare transfer plan (filters tables based on state in resume mode)
    plan = prepare_transfer_plan(tables_with_counts, migration_state)

    # 6. Extract from plan
    regular = get_regular_tables(plan)
    first_parts = get_first_partitions(plan)
    remaining_parts = get_remaining_partitions(plan)

    # 7. Transfer data
    regular_results = transfer_table_data.expand(table_info=regular)
    # Use explicit task IDs for partition transfers to avoid relying on auto-generated names
    first_results = transfer_partition.override(task_id="transfer_first_partitions").expand(partition_info=first_parts)
    remaining_results = transfer_partition.override(task_id="transfer_remaining_partitions").expand(partition_info=remaining_parts)

    # First partitions must complete before remaining (prevent truncate race)
    first_results >> remaining_results

    # 8. Collect results (wait for all transfer tasks)
    results = collect_results()
    [regular_results, first_results, remaining_results] >> results

    # 9. Reset sequences
    seq_status = reset_sequences(tables_with_counts, results)

    # 10. Trigger validation
    trigger_validation = TriggerDagRunOperator(
        task_id="trigger_validation_dag",
        trigger_dag_id="validate_migration_env",
        wait_for_completion=True,
        poke_interval=30,
        trigger_rule="all_done",
        conf={
            "source_conn_id": "{{ params.source_conn_id }}",
            "target_conn_id": "{{ params.target_conn_id }}",
            "include_tables": "{{ params.include_tables | tojson }}",
        },
    )

    seq_status >> trigger_validation

    # 11. Generate summary
    summary = generate_summary(results)
    trigger_validation >> summary


# Instantiate
mssql_to_postgres_migration()
