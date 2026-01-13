# Date-Based Incremental Loading Implementation Guide

This document provides a complete blueprint for implementing date-based incremental data loading from SQL Server to PostgreSQL. It is designed to be followed by another AI or developer to replicate this pattern in a new pipeline.

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Core Components](#core-components)
3. [State Management](#state-management)
4. [Date Column Detection](#date-column-detection)
5. [Incremental Sync Algorithm](#incremental-sync-algorithm)
6. [Staging Table Pattern](#staging-table-pattern)
7. [Partitioning for Large Tables](#partitioning-for-large-tables)
8. [Checkpoint & Resume Mechanism](#checkpoint--resume-mechanism)
9. [Configuration](#configuration)
10. [Implementation Checklist](#implementation-checklist)

---

## Architecture Overview

### The Watermark Approach

Date-based incremental loading uses a **watermark** (timestamp) to track synchronization progress. Each sync run:

1. Reads the last successful sync timestamp from state storage
2. Queries source data WHERE `date_column > last_sync_timestamp`
3. Upserts changed rows into the target database
4. Records the new sync timestamp for the next run

```
┌─────────────────┐      ┌──────────────────┐      ┌─────────────────┐
│   SQL Server    │      │   Sync Process   │      │   PostgreSQL    │
│    (Source)     │─────▶│                  │─────▶│    (Target)     │
└─────────────────┘      │  - Read state    │      └─────────────────┘
                         │  - Filter by date│              │
                         │  - Stage & upsert│              │
                         │  - Update state  │              ▼
                         └──────────────────┘      ┌─────────────────┐
                                                   │  _migration     │
                                                   │  ._migration_   │
                                                   │    state        │
                                                   └─────────────────┘
```

### Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| **Watermark over CDC** | Simpler, no SQL Server Agent dependency, works with any edition |
| **Staging tables** | Enables atomic upserts, change detection, and rollback capability |
| **IS DISTINCT FROM** | Proper NULL handling, avoids expensive hash comparisons |
| **UNLOGGED staging** | 2-3x faster writes (acceptable since staging is temporary) |
| **Keyset pagination** | O(1) performance vs O(n) for OFFSET-based paging |

---

## Core Components

### Required Modules

You need these components to implement date-based incremental loading:

```
plugins/
├── mssql_pg_migration/
│   ├── __init__.py
│   ├── incremental_state.py   # State management
│   ├── data_transfer.py       # Transfer logic with date filtering
│   ├── type_mapping.py        # MSSQL to PostgreSQL type conversion
│   └── table_config.py        # Table discovery and configuration

config/
├── {database}_include_tables.txt   # Tables to sync
└── hostname_alias.txt              # Instance name mapping
```

### Database Requirements

**Source (SQL Server):**
- Tables must have a date/datetime column for change tracking
- Single-column primary key recommended (composite PKs supported but slower)
- `READ COMMITTED` or `WITH (NOLOCK)` for non-blocking reads

**Target (PostgreSQL):**
- Primary key constraints must exist (for ON CONFLICT)
- `_migration` schema for state tracking
- Sufficient disk space for staging tables

---

## State Management

### State Table Schema

Create a dedicated schema and table for tracking sync state:

```sql
-- Create isolated schema
CREATE SCHEMA IF NOT EXISTS _migration;
REVOKE ALL ON SCHEMA _migration FROM PUBLIC;

-- State tracking table
CREATE TABLE IF NOT EXISTS _migration._migration_state (
    id SERIAL PRIMARY KEY,

    -- Table identity
    table_name VARCHAR(255) NOT NULL,
    source_schema VARCHAR(255) NOT NULL,
    target_schema VARCHAR(255) NOT NULL,

    -- Sync status
    sync_status VARCHAR(50) DEFAULT 'pending',  -- pending|running|completed|failed
    last_sync_start TIMESTAMP WITH TIME ZONE,
    last_sync_end TIMESTAMP WITH TIME ZONE,
    sync_duration_seconds INTEGER,

    -- Row metrics
    rows_inserted INTEGER DEFAULT 0,
    rows_updated INTEGER DEFAULT 0,
    rows_unchanged INTEGER DEFAULT 0,

    -- CRITICAL: Watermark for date-based incremental
    last_sync_timestamp TIMESTAMP WITH TIME ZONE,

    -- Resume checkpoint (for interrupted syncs)
    last_pk_synced JSONB,

    -- Partition tracking (for large tables)
    partition_info JSONB,

    -- Metadata
    migration_type VARCHAR(50) DEFAULT 'incremental',
    dag_run_id VARCHAR(255),
    error_message TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    -- Unique constraint for table identity
    CONSTRAINT uq_migration_state_table
        UNIQUE (table_name, source_schema, target_schema)
);

-- Index for fast lookups
CREATE INDEX IF NOT EXISTS idx_migration_state_lookup
    ON _migration._migration_state(source_schema, table_name);
```

### State Manager Class

```python
class IncrementalStateManager:
    """Manages sync state for date-based incremental loading."""

    def __init__(self, postgres_hook):
        self.hook = postgres_hook
        self.ensure_state_table()

    def get_last_sync_timestamp(
        self,
        table_name: str,
        source_schema: str,
        target_schema: str
    ) -> datetime | None:
        """
        Get the last successful sync timestamp (watermark).
        Returns None if table has never been synced.
        """
        query = """
            SELECT last_sync_timestamp
            FROM _migration._migration_state
            WHERE table_name = %s
              AND source_schema = %s
              AND target_schema = %s
              AND sync_status = 'completed'
        """
        result = self.hook.get_first(query, (table_name, source_schema, target_schema))
        return result[0] if result else None

    def update_sync_timestamp(
        self,
        sync_id: int,
        sync_timestamp: datetime
    ) -> None:
        """
        Record the sync timestamp for next incremental run.
        IMPORTANT: Use the timestamp from BEFORE data was queried,
        not the current time, to avoid missing concurrent changes.
        """
        query = """
            UPDATE _migration._migration_state
            SET last_sync_timestamp = %s
            WHERE id = %s
        """
        self.hook.run(query, (sync_timestamp, sync_id))

    def start_sync(
        self,
        table_name: str,
        source_schema: str,
        target_schema: str,
        dag_run_id: str = None
    ) -> int:
        """
        Initialize or reset state for a new sync run.
        Returns sync_id for tracking.
        """
        query = """
            INSERT INTO _migration._migration_state
                (table_name, source_schema, target_schema, sync_status,
                 last_sync_start, dag_run_id, rows_inserted, rows_updated, rows_unchanged)
            VALUES (%s, %s, %s, 'running', NOW(), %s, 0, 0, 0)
            ON CONFLICT (table_name, source_schema, target_schema)
            DO UPDATE SET
                sync_status = 'running',
                last_sync_start = NOW(),
                dag_run_id = EXCLUDED.dag_run_id,
                rows_inserted = 0,
                rows_updated = 0,
                rows_unchanged = 0,
                error_message = NULL
            RETURNING id
        """
        result = self.hook.get_first(query, (table_name, source_schema, target_schema, dag_run_id))
        return result[0]

    def complete_sync(
        self,
        sync_id: int,
        rows_inserted: int,
        rows_updated: int,
        rows_unchanged: int
    ) -> None:
        """Mark sync as successfully completed."""
        query = """
            UPDATE _migration._migration_state
            SET sync_status = 'completed',
                last_sync_end = NOW(),
                sync_duration_seconds = EXTRACT(EPOCH FROM (NOW() - last_sync_start))::INTEGER,
                rows_inserted = %s,
                rows_updated = %s,
                rows_unchanged = %s
            WHERE id = %s
        """
        self.hook.run(query, (rows_inserted, rows_updated, rows_unchanged, sync_id))

    def fail_sync(self, sync_id: int, error_message: str) -> None:
        """Mark sync as failed with error details."""
        query = """
            UPDATE _migration._migration_state
            SET sync_status = 'failed',
                last_sync_end = NOW(),
                error_message = %s
            WHERE id = %s
        """
        self.hook.run(query, (error_message[:4000], sync_id))  # Truncate long errors
```

---

## Date Column Detection

### Valid Temporal Types

Only these SQL Server types are valid for date-based incremental loading:

```python
VALID_TEMPORAL_TYPES = frozenset([
    'datetime',       # Legacy, 3.33ms precision
    'datetime2',      # Modern, up to 100ns precision
    'smalldatetime',  # 1-minute precision
    'date',           # Date only, no time
    'datetimeoffset', # Timezone-aware
])
```

### Detection Algorithm

```python
def get_date_column_info(
    mssql_conn,
    schema: str,
    table: str,
    candidate_columns: list[str]
) -> dict | None:
    """
    Detect a valid date column for incremental loading.

    Args:
        mssql_conn: Active SQL Server connection
        schema: Source table schema
        table: Source table name
        candidate_columns: Ordered list of column names to try
                          (e.g., ['ModifiedDate', 'UpdatedAt', 'LastChanged'])

    Returns:
        {'column_name': 'ActualName', 'data_type': 'datetime2'} or None
    """
    cursor = mssql_conn.cursor()

    for candidate in candidate_columns:
        # Case-sensitive match using SQL Server collation
        query = """
            SELECT COLUMN_NAME, DATA_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = ?
              AND TABLE_NAME = ?
              AND COLUMN_NAME = ? COLLATE Latin1_General_CS_AS
        """
        cursor.execute(query, (schema, table, candidate))
        result = cursor.fetchone()

        if result:
            column_name, data_type = result
            if data_type.lower() in VALID_TEMPORAL_TYPES:
                return {
                    'column_name': column_name,
                    'data_type': data_type
                }

    return None  # No valid date column found
```

### Configuration

Provide candidate column names via environment variable or config:

```bash
# Environment variable (comma-separated, order matters)
DATE_UPDATED_FIELD=LastEditedWhen,ModifiedDate,UpdatedAt,updated_at,LastModified
```

The algorithm tries each name in order and uses the first valid match. This allows the same config to work across tables with different naming conventions.

---

## Incremental Sync Algorithm

### Core Transfer Function

```python
def transfer_incremental_staging(
    mssql_hook,
    postgres_hook,
    source_schema: str,
    source_table: str,
    target_schema: str,
    target_table: str,
    date_column_info: dict | None,
    state_manager: IncrementalStateManager,
    batch_size: int = 100_000,
) -> dict:
    """
    Transfer data using staging table pattern with date-based filtering.

    Returns:
        {'inserted': int, 'updated': int, 'unchanged': int, 'status': str}
    """

    # 1. Capture sync start time BEFORE querying data
    #    This becomes the watermark for the next run
    sync_start_time = datetime.now(timezone.utc)

    # 2. Initialize state tracking
    sync_id = state_manager.start_sync(
        source_table, source_schema, target_schema
    )

    try:
        # 3. Get last sync timestamp (watermark)
        last_sync_timestamp = state_manager.get_last_sync_timestamp(
            source_table, source_schema, target_schema
        )

        # 4. Build date filter clause
        where_clause = None
        where_params = []

        if date_column_info and last_sync_timestamp:
            date_col = date_column_info['column_name']
            # Include NULL dates to catch rows without timestamps
            where_clause = f"([{date_col}] > ? OR [{date_col}] IS NULL)"
            where_params = [last_sync_timestamp]

        # 5. Create staging table
        staging_table = create_staging_table(
            postgres_hook, target_schema, target_table
        )

        # 6. Stream data from source to staging with date filter
        with mssql_hook.get_conn() as mssql_conn:
            with postgres_hook.get_conn() as pg_conn:
                total_rows = stream_to_staging(
                    mssql_conn,
                    pg_conn,
                    source_schema,
                    source_table,
                    target_schema,
                    staging_table,
                    where_clause,
                    where_params,
                    batch_size
                )

        # 7. Upsert from staging to target with change detection
        inserted, updated, unchanged = upsert_from_staging(
            postgres_hook,
            target_schema,
            staging_table,
            target_table
        )

        # 8. Cleanup staging table
        drop_staging_table(postgres_hook, target_schema, staging_table)

        # 9. Record sync timestamp for next run
        state_manager.update_sync_timestamp(sync_id, sync_start_time)

        # 10. Mark sync complete
        state_manager.complete_sync(sync_id, inserted, updated, unchanged)

        return {
            'inserted': inserted,
            'updated': updated,
            'unchanged': unchanged,
            'status': 'success'
        }

    except Exception as e:
        state_manager.fail_sync(sync_id, str(e))
        raise
```

### Critical Timing Note

```python
# CORRECT: Capture time BEFORE querying data
sync_start_time = datetime.now(timezone.utc)
# ... query and transfer data ...
state_manager.update_sync_timestamp(sync_id, sync_start_time)

# WRONG: Using current time AFTER transfer
# This would miss changes that occurred during the transfer
```

---

## Staging Table Pattern

### Why Use Staging Tables?

1. **Atomic operations** - Upsert entire batch or nothing
2. **Change detection** - Compare staging vs target using `IS DISTINCT FROM`
3. **Rollback capability** - Drop staging table if anything fails
4. **Isolation** - No locks on target table during data load

### Creating the Staging Table

```python
def create_staging_table(
    postgres_hook,
    target_schema: str,
    target_table: str,
    use_unlogged: bool = True
) -> str:
    """
    Create an empty staging table with same structure as target.

    Returns staging table name.
    """
    import uuid

    staging_name = f"_staging_{target_table}_{uuid.uuid4().hex[:8]}"

    # UNLOGGED is faster but not crash-safe (acceptable for temp tables)
    logged = "UNLOGGED" if use_unlogged else ""

    query = f"""
        CREATE {logged} TABLE {target_schema}.{staging_name}
        AS SELECT * FROM {target_schema}.{target_table}
        LIMIT 0
    """
    postgres_hook.run(query)

    return staging_name
```

### Upserting with Change Detection

```python
def upsert_from_staging(
    postgres_hook,
    target_schema: str,
    staging_table: str,
    target_table: str,
    pk_column: str,
    columns: list[str]
) -> tuple[int, int, int]:
    """
    Upsert rows from staging to target using IS DISTINCT FROM.

    Returns:
        (rows_inserted, rows_updated, rows_unchanged)
    """

    # Build column lists
    non_pk_columns = [c for c in columns if c != pk_column]

    # SET clause for updates
    set_clause = ", ".join([
        f"{col} = EXCLUDED.{col}"
        for col in non_pk_columns
    ])

    # WHERE clause using IS DISTINCT FROM (handles NULLs correctly)
    change_check = " OR ".join([
        f"({target_schema}.{target_table}.{col} IS DISTINCT FROM EXCLUDED.{col})"
        for col in non_pk_columns
    ])

    # Single upsert query with change detection
    upsert_query = f"""
        WITH upsert_result AS (
            INSERT INTO {target_schema}.{target_table} ({", ".join(columns)})
            SELECT {", ".join(columns)}
            FROM {target_schema}.{staging_table}
            ON CONFLICT ({pk_column}) DO UPDATE
            SET {set_clause}
            WHERE {change_check}
            RETURNING
                CASE
                    WHEN xmax = 0 THEN 'inserted'
                    ELSE 'updated'
                END AS operation
        )
        SELECT
            COUNT(*) FILTER (WHERE operation = 'inserted') AS inserted,
            COUNT(*) FILTER (WHERE operation = 'updated') AS updated
        FROM upsert_result
    """

    result = postgres_hook.get_first(upsert_query)
    inserted, updated = result

    # Calculate unchanged (total in staging minus changed)
    total_staged = postgres_hook.get_first(
        f"SELECT COUNT(*) FROM {target_schema}.{staging_table}"
    )[0]
    unchanged = total_staged - inserted - updated

    return inserted, updated, unchanged
```

### Why `IS DISTINCT FROM`?

Standard `!=` comparison:
```sql
-- NULL != NULL returns NULL (not TRUE), so this row won't update
WHERE old_value != new_value  -- WRONG for NULLs
```

`IS DISTINCT FROM` comparison:
```sql
-- NULL IS DISTINCT FROM NULL returns FALSE
-- NULL IS DISTINCT FROM 'value' returns TRUE
WHERE old_value IS DISTINCT FROM new_value  -- CORRECT
```

---

## Partitioning for Large Tables

### When to Partition

Partition tables that exceed a threshold (e.g., 1 million rows) to enable:
- Parallel processing across partitions
- Smaller staging tables
- Faster checkpoint intervals

### Partition Calculation

```python
def calculate_partitions(
    mssql_hook,
    source_schema: str,
    source_table: str,
    pk_column: str,
    max_partitions: int = 6,
    min_rows_per_partition: int = 100_000
) -> list[dict]:
    """
    Calculate partition ranges based on primary key distribution.

    Returns list of partition configs:
    [{'id': 0, 'pk_start': 1, 'pk_end': 500000}, ...]
    """

    # Get PK bounds
    query = f"""
        SELECT MIN([{pk_column}]), MAX([{pk_column}]), COUNT(*)
        FROM [{source_schema}].[{source_table}]
    """
    pk_min, pk_max, row_count = mssql_hook.get_first(query)

    if pk_min is None or pk_max is None:
        return []  # Empty table

    # Calculate optimal partition count
    optimal_partitions = min(
        max_partitions,
        max(1, row_count // min_rows_per_partition)
    )

    pk_range = pk_max - pk_min + 1
    partition_size = pk_range // optimal_partitions

    partitions = []
    for i in range(optimal_partitions):
        pk_start = pk_min + (i * partition_size)
        pk_end = pk_min + ((i + 1) * partition_size) - 1

        # Last partition gets remaining range
        if i == optimal_partitions - 1:
            pk_end = pk_max

        partitions.append({
            'id': i,
            'pk_start': pk_start,
            'pk_end': pk_end,
            'status': 'pending'
        })

    return partitions
```

### Partition Transfer with Date Filter

```python
def transfer_partition(
    partition: dict,
    date_column_info: dict | None,
    last_sync_timestamp: datetime | None,
    pk_column: str,
    # ... other params
):
    """Transfer a single partition with combined PK and date filters."""

    # Build combined WHERE clause
    pk_filter = f"[{pk_column}] >= ? AND [{pk_column}] <= ?"
    pk_params = [partition['pk_start'], partition['pk_end']]

    if date_column_info and last_sync_timestamp:
        date_col = date_column_info['column_name']
        date_filter = f"([{date_col}] > ? OR [{date_col}] IS NULL)"

        where_clause = f"({pk_filter}) AND ({date_filter})"
        where_params = pk_params + [last_sync_timestamp]
    else:
        where_clause = pk_filter
        where_params = pk_params

    # ... proceed with transfer using combined filter
```

---

## Checkpoint & Resume Mechanism

### Saving Checkpoints

During data transfer, periodically save progress:

```python
def save_checkpoint(
    state_manager: IncrementalStateManager,
    sync_id: int,
    last_pk_value: Any,
    rows_processed: int
) -> None:
    """
    Save checkpoint for resume capability.
    Call this every N batches (e.g., every 10,000 rows).
    """
    query = """
        UPDATE _migration._migration_state
        SET last_pk_synced = %s,
            rows_inserted = rows_inserted + %s
        WHERE id = %s
    """
    # Store as JSON for type flexibility
    pk_json = json.dumps({'pk': last_pk_value})
    state_manager.hook.run(query, (pk_json, rows_processed, sync_id))
```

### Resuming from Checkpoint

```python
def get_resume_point(
    state_manager: IncrementalStateManager,
    table_name: str,
    source_schema: str,
    target_schema: str
) -> Any | None:
    """
    Get the last checkpoint if previous sync was interrupted.
    Returns None if no checkpoint or previous sync completed.
    """
    query = """
        SELECT last_pk_synced
        FROM _migration._migration_state
        WHERE table_name = %s
          AND source_schema = %s
          AND target_schema = %s
          AND sync_status IN ('running', 'failed')
          AND last_pk_synced IS NOT NULL
    """
    result = state_manager.hook.get_first(
        query, (table_name, source_schema, target_schema)
    )

    if result and result[0]:
        return json.loads(result[0])['pk']
    return None
```

### Partition State Tracking

For partitioned tables, track each partition's status:

```python
partition_info = {
    "total_partitions": 4,
    "partitions": [
        {
            "id": 0,
            "status": "completed",
            "pk_start": 1,
            "pk_end": 250000,
            "rows_synced": 125000,
            "last_pk_synced": 250000
        },
        {
            "id": 1,
            "status": "running",
            "pk_start": 250001,
            "pk_end": 500000,
            "rows_synced": 50000,
            "last_pk_synced": 300000  # Resume from here
        },
        {
            "id": 2,
            "status": "pending",
            "pk_start": 500001,
            "pk_end": 750000
        },
        {
            "id": 3,
            "status": "pending",
            "pk_start": 750001,
            "pk_end": 1000000
        }
    ]
}
```

---

## Configuration

### Environment Variables

```bash
# Date column detection (comma-separated candidates)
DATE_UPDATED_FIELD=LastEditedWhen,ModifiedDate,UpdatedAt,updated_at

# Performance tuning
DEFAULT_INCREMENTAL_BATCH_SIZE=100000   # Rows per batch
MAX_PARALLEL_TRANSFERS=8                 # Concurrent table syncs
PARTITION_THRESHOLD=1000000              # Rows to trigger partitioning
MAX_PARTITIONS_PER_TABLE=6               # Max partitions per table

# Staging options
USE_UNLOGGED_STAGING=true               # Faster but not crash-safe

# Consistency (disable NOLOCK for strict reads)
STRICT_CONSISTENCY=false
```

### Table Configuration Files

Create `config/{database}_include_tables.txt`:

```
# Tables to sync (one per line, schema.table format)
dbo.Users
dbo.Posts
dbo.Comments
sales.Orders
sales.OrderDetails

# Comments starting with # are ignored
# Empty lines are ignored
```

### Target Schema Naming

Target schemas are derived from source metadata:

```
{instance_alias}__{database}__{schema}

Example:
- Source: sqlprod01/StackOverflow2010.dbo.Users
- Instance alias: prod (from hostname_alias.txt)
- Target schema: prod__stackoverflow2010__dbo
- Target table: prod__stackoverflow2010__dbo.users
```

Hostname aliases (`config/hostname_alias.txt`):

```
# hostname = alias
mssql-server = dev
sqlprod01 = prod
sqltest02 = test
```

---

## Implementation Checklist

Use this checklist when implementing date-based incremental loading:

### Phase 1: Infrastructure

- [ ] Create `_migration` schema in PostgreSQL
- [ ] Create `_migration_state` table with all required columns
- [ ] Implement `IncrementalStateManager` class
- [ ] Add indexes for fast state lookups
- [ ] Set up configuration files and environment variables

### Phase 2: Core Transfer Logic

- [ ] Implement date column detection with case-sensitive matching
- [ ] Build WHERE clause generator for date filtering
- [ ] Implement staging table creation (UNLOGGED option)
- [ ] Implement keyset pagination for source reads
- [ ] Implement upsert with `IS DISTINCT FROM` change detection
- [ ] Add staging table cleanup in finally block

### Phase 3: State Management

- [ ] Capture `sync_start_time` BEFORE querying data
- [ ] Track sync status transitions (pending -> running -> completed/failed)
- [ ] Record sync timestamps for watermark tracking
- [ ] Implement row counters (inserted/updated/unchanged)

### Phase 4: Resilience

- [ ] Implement checkpoint saving every N batches
- [ ] Implement resume from checkpoint on restart
- [ ] Add error handling with state update on failure
- [ ] Test interrupted sync and resume scenarios

### Phase 5: Scale (Optional)

- [ ] Implement partition calculation for large tables
- [ ] Track partition state in JSONB column
- [ ] Enable parallel partition processing
- [ ] Test with tables > 1M rows

### Phase 6: Testing

- [ ] Test first sync (no prior state)
- [ ] Test incremental sync (with prior state)
- [ ] Test with NULL date values
- [ ] Test resume after failure
- [ ] Test partitioned table sync
- [ ] Verify row counts match expectations

---

## Example: End-to-End Flow

### First Sync (No Prior State)

```
1. discover_tables() finds "dbo.Users" with column "ModifiedDate"
2. State table has no record for this table
3. get_last_sync_timestamp() returns None
4. transfer_incremental_staging() runs WITHOUT date filter (full load)
5. Syncs all 100,000 rows
6. Records sync_start_time "2025-01-13 10:30:00" as watermark
7. State: status='completed', last_sync_timestamp='2025-01-13 10:30:00'
```

### Second Sync (1 Hour Later)

```
1. discover_tables() finds same table
2. get_last_sync_timestamp() returns "2025-01-13 10:30:00"
3. transfer_incremental_staging() filters:
   WHERE ModifiedDate > '2025-01-13 10:30:00' OR ModifiedDate IS NULL
4. Only 500 rows match (modified in last hour + NULL dates)
5. Upsert finds: 50 new, 400 updated, 50 unchanged
6. Records new sync_start_time "2025-01-13 11:30:00"
7. State updated for next run
```

### Interrupted Sync with Resume

```
1. Sync starts, processes 50,000 of 100,000 rows
2. Saves checkpoint: last_pk_synced = 50000
3. Process crashes
4. New process starts, checks state
5. Finds status='running', last_pk_synced=50000
6. Resumes from pk > 50000 with same date filter
7. Completes remaining 50,000 rows
8. Marks complete, updates watermark
```

---

## Summary

The key insights for successful date-based incremental loading:

1. **Watermark timing**: Capture sync_start_time BEFORE querying to avoid missing concurrent changes
2. **NULL handling**: Include NULL dates in WHERE clause (`date_col > ? OR date_col IS NULL`)
3. **Staging pattern**: Use staging tables for atomic upserts and change detection
4. **IS DISTINCT FROM**: Use this operator for NULL-safe change detection
5. **Checkpoints**: Save progress regularly for resume capability
6. **State isolation**: Use dedicated schema for state table
7. **Partitioning**: Split large tables for parallel processing and faster checkpoints

This approach provides reliable, resumable, and efficient incremental data synchronization.
