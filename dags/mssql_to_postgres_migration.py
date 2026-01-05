"""
SQL Server to PostgreSQL Migration DAG (Data Transfer Only)

This DAG performs data transfer from SQL Server to PostgreSQL.
It assumes tables already exist in the target (created by schema DAG).

Workflow:
1. Trigger schema DAG (ensures tables exist with PKs)
2. Discover tables from target PostgreSQL
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

logger = logging.getLogger(__name__)

# Configuration from environment
MAX_PARALLEL_TRANSFERS = int(os.environ.get('MAX_PARALLEL_TRANSFERS', '8'))
MAX_ACTIVE_TASKS = int(os.environ.get('MAX_ACTIVE_TASKS', '16'))
DEFAULT_CHUNK_SIZE = int(os.environ.get('DEFAULT_CHUNK_SIZE', '200000'))
LARGE_TABLE_THRESHOLD = 1_000_000


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
        "source_schema": Param(default="dbo", type="string"),
        "target_schema": Param(default="public", type="string"),
        "chunk_size": Param(default=DEFAULT_CHUNK_SIZE, type="integer", minimum=100, maximum=500000),
        "include_tables": Param(default=[], type="array"),
        "exclude_tables": Param(default=[], type="array"),
        "skip_schema_dag": Param(default=False, type="boolean", description="Skip schema DAG trigger"),
    },
    tags=["migration", "mssql", "postgres", "etl", "full-refresh"],
)
def mssql_to_postgres_migration():
    """Migration DAG: Trigger schema DAG, then transfer data."""

    # Step 1: Trigger schema DAG to ensure tables exist
    trigger_schema = TriggerDagRunOperator(
        task_id="trigger_schema_dag",
        trigger_dag_id="mssql_to_postgres_schema",
        wait_for_completion=True,
        poke_interval=10,
        conf={
            "source_conn_id": "{{ params.source_conn_id }}",
            "target_conn_id": "{{ params.target_conn_id }}",
            "source_schema": "{{ params.source_schema }}",
            "target_schema": "{{ params.target_schema }}",
            "include_tables": "{{ params.include_tables | tojson }}",
            "exclude_tables": "{{ params.exclude_tables | tojson }}",
            "drop_existing": True,
        },
    )

    @task
    def discover_target_tables(**context) -> List[Dict[str, Any]]:
        """
        Discover tables from target PostgreSQL.

        Queries information_schema to find tables, columns, and PK info.
        This ensures we only transfer data for tables that exist.
        """
        params = context["params"]
        target_schema = params["target_schema"]

        from airflow.providers.postgres.hooks.postgres import PostgresHook
        pg_hook = PostgresHook(postgres_conn_id=params["target_conn_id"])

        # Get all tables in target schema
        tables_query = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """
        tables = pg_hook.get_records(tables_query, parameters=[target_schema])
        table_names = [t[0] for t in tables]

        logger.info(f"Found {len(table_names)} tables in {target_schema}")

        # Get columns for each table
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

        # Build table info list
        discovered_tables = []
        for table_name in table_names:
            discovered_tables.append({
                "table_name": table_name,
                "source_schema": params["source_schema"],
                "target_schema": target_schema,
                "columns": table_columns.get(table_name, []),
                "pk_columns": table_pks.get(table_name, []),
            })

        logger.info(f"Discovered {len(discovered_tables)} tables for migration")
        return discovered_tables

    @task
    def get_source_row_counts(tables: List[Dict[str, Any]], **context) -> List[Dict[str, Any]]:
        """
        Get row counts from source SQL Server for partitioning decisions.

        Also fetches PK column info needed for keyset pagination.
        """
        params = context["params"]
        source_schema = params["source_schema"]

        from mssql_pg_migration.odbc_helper import OdbcConnectionHelper
        mssql_hook = OdbcConnectionHelper(odbc_conn_id=params["source_conn_id"])

        tables_with_counts = []

        for table_info in tables:
            table_name = table_info["table_name"]

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

                logger.info(f"  {table_name}: {row_count:,} rows")

            except Exception as e:
                logger.warning(f"Could not get row count for {table_name}: {e}")
                tables_with_counts.append({
                    **table_info,
                    "row_count": 0,
                    "pk_columns_info": {'columns': [], 'is_composite': False},
                })

        total_rows = sum(t["row_count"] for t in tables_with_counts)
        logger.info(f"Total rows to transfer: {total_rows:,}")

        return tables_with_counts

    @task
    def prepare_transfer_plan(tables: List[Dict[str, Any]], **context) -> Dict[str, List[Dict[str, Any]]]:
        """
        Prepare transfer plan: split into regular tables and partitioned tables.

        Large tables (>1M rows) are partitioned for parallel transfer.
        Returns dict with 'regular' and 'partitions' lists.
        """
        params = context["params"]
        source_schema = params["source_schema"]

        regular_tables = []
        partitions = []

        from mssql_pg_migration.odbc_helper import OdbcConnectionHelper
        mssql_hook = OdbcConnectionHelper(odbc_conn_id=params["source_conn_id"])

        for table_info in tables:
            table_name = table_info["table_name"]
            row_count = table_info.get("row_count", 0)
            pk_info = table_info.get("pk_columns_info", {})
            pk_columns = pk_info.get("columns", [])

            if row_count < LARGE_TABLE_THRESHOLD:
                # Small table: regular transfer
                regular_tables.append({
                    **table_info,
                    "truncate_first": True,
                })
                logger.info(f"  {table_name} ({row_count:,} rows) -> regular transfer")
                continue

            # Large table: partition it
            if not pk_columns:
                # No PK: fall back to regular transfer
                logger.warning(f"  {table_name} has no PK, using regular transfer")
                regular_tables.append({
                    **table_info,
                    "truncate_first": True,
                })
                continue

            is_composite = pk_info.get("is_composite", False)
            partition_count = get_partition_count(row_count)

            try:
                safe_table = validate_sql_identifier(table_name, "table")
                safe_schema = validate_sql_identifier(source_schema, "schema")
            except ValueError as e:
                logger.warning(f"  {table_name}: {e}, using regular transfer")
                regular_tables.append({**table_info, "truncate_first": True})
                continue

            if is_composite:
                # Composite PK: use ROW_NUMBER ranges
                rows_per_partition = (row_count + partition_count - 1) // partition_count
                pk_col_names = [c['name'] for c in pk_columns]

                for i in range(partition_count):
                    start_row = i * rows_per_partition + 1
                    end_row = min((i + 1) * rows_per_partition, row_count)

                    partitions.append({
                        **table_info,
                        "partition_name": f"partition_{i + 1}",
                        "partition_index": i,
                        "use_row_number": True,
                        "order_by_columns": pk_col_names,
                        "start_row": start_row,
                        "end_row": end_row,
                        "pk_column": pk_col_names[0],
                        "estimated_rows": end_row - start_row + 1,
                        "truncate_first": i == 0,
                    })

                logger.info(f"  {table_name} ({row_count:,} rows) -> {partition_count} partitions (ROW_NUMBER)")
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
                    logger.warning(f"  {table_name}: NTILE failed ({e}), using regular transfer")
                    regular_tables.append({**table_info, "truncate_first": True})
                    continue

                if not boundaries:
                    regular_tables.append({**table_info, "truncate_first": True})
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
                        **table_info,
                        "partition_name": f"partition_{i + 1}",
                        "partition_index": i,
                        "where_clause": where,
                        "pk_column": pk_column,
                        "estimated_rows": part_count,
                        "truncate_first": i == 0,
                    })

                logger.info(f"  {table_name} ({row_count:,} rows) -> {len(boundaries)} partitions (NTILE)")

        # Split partitions into first (truncate) and remaining (no truncate)
        first_partitions = [p for p in partitions if p.get("truncate_first", False)]
        remaining_partitions = [p for p in partitions if not p.get("truncate_first", False)]

        logger.info(f"Transfer plan: {len(regular_tables)} regular tables, "
                   f"{len(first_partitions)} first partitions, {len(remaining_partitions)} remaining partitions")

        return {
            "regular": regular_tables,
            "first_partitions": first_partitions,
            "remaining_partitions": remaining_partitions,
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

        logger.info(f"Transferring {table_name} ({table_info.get('row_count', 0):,} rows)")

        result = data_transfer.transfer_table_data(
            mssql_conn_id=params["source_conn_id"],
            postgres_conn_id=params["target_conn_id"],
            table_info=table_info,
            chunk_size=params["chunk_size"],
            truncate=table_info.get("truncate_first", True),
        )

        result["table_name"] = table_name

        if result["success"]:
            logger.info(f"  {table_name}: {result['rows_transferred']:,} rows "
                       f"in {result['elapsed_time_seconds']:.1f}s "
                       f"({result['avg_rows_per_second']:,.0f} rows/sec)")
        else:
            logger.error(f"  {table_name}: FAILED - {result.get('errors', [])}")

        return result

    @task(max_active_tis_per_dagrun=MAX_PARALLEL_TRANSFERS)
    def transfer_partition(partition_info: Dict[str, Any], **context) -> Dict[str, Any]:
        """Transfer a partition of a large table."""
        params = context["params"]
        table_name = partition_info["table_name"]
        partition_name = partition_info["partition_name"]

        logger.info(f"Transferring {table_name} {partition_name} "
                   f"(~{partition_info.get('estimated_rows', 0):,} rows)")

        result = data_transfer.transfer_table_data(
            mssql_conn_id=params["source_conn_id"],
            postgres_conn_id=params["target_conn_id"],
            table_info=partition_info,
            chunk_size=params["chunk_size"],
            truncate=partition_info.get("truncate_first", False),
            where_clause=partition_info.get("where_clause"),
        )

        result["table_name"] = table_name
        result["partition_name"] = partition_name
        result["is_partition"] = True
        result["success"] = len(result.get("errors", [])) == 0 and result.get("rows_transferred", 0) > 0

        if result["success"]:
            logger.info(f"  {table_name} {partition_name}: {result['rows_transferred']:,} rows "
                       f"in {result['elapsed_time_seconds']:.1f}s")
        else:
            logger.error(f"  {table_name} {partition_name}: FAILED")

        return result

    @task(trigger_rule="all_done")
    def collect_results(**context) -> List[Dict[str, Any]]:
        """
        Aggregate all transfer results by pulling XCom manually.

        This works around Airflow 3.0's buggy automatic XCom resolution
        for dynamically mapped tasks.
        """
        from collections import defaultdict

        ti = context["ti"]
        all_results = []

        # Manually pull XCom from each transfer task
        # Use default=[] to handle cases where no tasks ran
        regular_results = ti.xcom_pull(
            task_ids="transfer_table_data",
            key="return_value",
            default=[]
        )
        first_results = ti.xcom_pull(
            task_ids="transfer_partition",
            key="return_value",
            default=[]
        )
        remaining_results = ti.xcom_pull(
            task_ids="transfer_partition__1",
            key="return_value",
            default=[]
        )

        # Normalize to lists (xcom_pull returns single value if only one result)
        if regular_results and not isinstance(regular_results, list):
            regular_results = [regular_results]
        if first_results and not isinstance(first_results, list):
            first_results = [first_results]
        if remaining_results and not isinstance(remaining_results, list):
            remaining_results = [remaining_results]

        # Process regular tables
        for r in (regular_results or []):
            if r and isinstance(r, dict):
                all_results.append(r)

        # Aggregate partitions by table
        table_partitions = defaultdict(list)
        for r in (first_results or []) + (remaining_results or []):
            if r and isinstance(r, dict):
                table_partitions[r.get("table_name", "Unknown")].append(r)

        for table_name, parts in table_partitions.items():
            total_rows = sum(p.get("rows_transferred", 0) for p in parts)
            success = all(p.get("success", False) for p in parts)
            all_results.append({
                "table_name": table_name,
                "rows_transferred": total_rows,
                "success": success,
                "partitions": len(parts),
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
        """Reset SERIAL sequences to MAX(column) value."""
        from concurrent.futures import ThreadPoolExecutor, as_completed
        from mssql_pg_migration import ddl_generator
        from mssql_pg_migration.type_mapping import map_table_schema

        params = context["params"]
        target_schema = params["target_schema"]
        target_conn_id = params["target_conn_id"]

        # Get successful tables
        successful = {r["table_name"] for r in results if r.get("success")}

        # We need original schema info for column types
        # Query PostgreSQL to find SERIAL columns
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        pg_hook = PostgresHook(postgres_conn_id=target_conn_id)

        # Find columns with sequences (SERIAL/BIGSERIAL)
        seq_query = """
            SELECT table_name, column_name
            FROM information_schema.columns
            WHERE table_schema = %s
              AND column_default LIKE 'nextval%%'
        """
        seq_columns = pg_hook.get_records(seq_query, parameters=[target_schema])

        reset_count = 0
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                for table_name, col_name in seq_columns:
                    if table_name not in successful:
                        continue
                    try:
                        quoted_table = f'"{target_schema}"."{table_name}"'
                        reset_sql = f"""
                        SELECT setval(
                            pg_get_serial_sequence('{quoted_table}', '{col_name}'),
                            COALESCE((SELECT MAX("{col_name}") FROM "{target_schema}"."{table_name}"), 1),
                            true
                        )
                        """
                        cursor.execute(reset_sql)
                        reset_count += 1
                        logger.info(f"Reset sequence for {table_name}.{col_name}")
                    except Exception as e:
                        logger.warning(f"Could not reset sequence for {table_name}.{col_name}: {e}")
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
            "tables_list": [r["table_name"] for r in successful],
            "failed_tables_list": [r["table_name"] for r in failed],
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

    # 1. Trigger schema DAG first
    # 2. Discover tables from target PostgreSQL
    tables = discover_target_tables()
    trigger_schema >> tables

    # 3. Get row counts from source
    tables_with_counts = get_source_row_counts(tables)

    # 4. Prepare transfer plan
    plan = prepare_transfer_plan(tables_with_counts)

    # 5. Extract from plan
    regular = get_regular_tables(plan)
    first_parts = get_first_partitions(plan)
    remaining_parts = get_remaining_partitions(plan)

    # 6. Transfer data
    regular_results = transfer_table_data.expand(table_info=regular)
    first_results = transfer_partition.expand(partition_info=first_parts)
    remaining_results = transfer_partition.expand(partition_info=remaining_parts)

    # First partitions must complete before remaining (prevent truncate race)
    first_results >> remaining_results

    # 7. Collect results (pulls XCom manually to work around Airflow 3.0 bug)
    results = collect_results()
    # Set dependencies: collect_results waits for all transfers to complete
    [regular_results, remaining_results] >> results

    # 8. Reset sequences
    seq_status = reset_sequences(tables_with_counts, results)

    # 9. Trigger validation
    trigger_validation = TriggerDagRunOperator(
        task_id="trigger_validation_dag",
        trigger_dag_id="validate_migration_env",
        wait_for_completion=True,
        poke_interval=30,
        trigger_rule="all_done",
        conf={
            "source_schema": "{{ params.source_schema }}",
            "target_schema": "{{ params.target_schema }}",
        },
    )

    seq_status >> trigger_validation

    # 10. Generate summary
    summary = generate_summary(results)
    trigger_validation >> summary


# Instantiate
mssql_to_postgres_migration()
