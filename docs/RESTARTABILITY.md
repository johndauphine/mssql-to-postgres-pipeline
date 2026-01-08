# Migration DAG Restartability Guide

This guide explains how the `mssql_to_postgres_migration` DAG handles failures and resumes interrupted migrations automatically.

## Overview

When migrating large datasets, failures can occur due to:
- Network timeouts
- Memory pressure
- Database locks
- Airflow scheduler crashes

The migration DAG automatically handles these scenarios with **smart defaults** - no manual parameters required. Simply re-trigger the DAG and it will do the right thing.

## Quick Start

### After a Failure - Just Re-trigger

```bash
# The DAG automatically detects failures and resumes
airflow dags trigger mssql_to_postgres_migration
```

That's it. The DAG will:
- Skip tables that completed successfully
- Retry tables that failed
- Resume partitions from checkpoints when possible

### Start Fresh (Manual Reset)

If you need to force a complete re-migration, manually clear the state:

```sql
-- Connect to target PostgreSQL database
UPDATE _migration_state
SET sync_status = 'pending', partition_info = NULL
WHERE migration_type = 'full';
```

Then trigger the DAG normally.

## How It Works

### Automatic Mode Detection

The DAG automatically determines the appropriate mode at the start of each run:

```
                    ┌─────────────────────────────────────┐
                    │   Check _migration_state table      │
                    │   for previous 'failed' status      │
                    └──────────────┬──────────────────────┘
                                   │
                    ┌──────────────┴──────────────┐
                    │                             │
               Has Failures                  No Failures
                    │                             │
                    ▼                             ▼
            ┌───────────────┐            ┌───────────────┐
            │ RECOVERY MODE │            │  FRESH MODE   │
            │               │            │               │
            │ - Skip done   │            │ - Reset all   │
            │ - Retry failed│            │ - Full reload │
            └───────────────┘            └───────────────┘
```

**RECOVERY MODE** (triggered when failures exist):
- Skips tables marked `completed`
- Retries tables marked `failed` or `running`
- Resumes partitions from checkpoints

**FRESH MODE** (triggered when no failures):
- Resets all state to `pending`
- Performs full data reload for all tables
- This is the default for clean re-runs

### State Tracking

The migration DAG tracks state in a `_migration_state` table in the target PostgreSQL database:

| Column | Description |
|--------|-------------|
| `table_name` | Table being migrated |
| `sync_status` | `pending`, `running`, `completed`, or `failed` |
| `dag_run_id` | Which DAG run created/updated this state |
| `partition_info` | JSONB tracking partition completion for large tables |
| `error_message` | Error details for failed tables |

### State Transitions

```
                    ┌─────────────┐
                    │   PENDING   │ (initial state)
                    └──────┬──────┘
                           │ transfer starts
                           ▼
                    ┌─────────────┐
        ┌───────────│   RUNNING   │───────────┐
        │           └─────────────┘           │
        │ success                      failure │
        ▼                                      ▼
 ┌─────────────┐                       ┌─────────────┐
 │  COMPLETED  │                       │   FAILED    │
 └─────────────┘                       └─────────────┘
        │                                     │
        │ fresh run (no failures)             │ re-trigger DAG
        │                                     │
        ▼                                     ▼
 ┌─────────────┐                       ┌─────────────┐
 │   PENDING   │ (reset for reload)    │   RUNNING   │ (retry)
 └─────────────┘                       └─────────────┘
```

### Zombie State Handling

If a DAG run crashes (OOM, node failure, scheduler restart), tasks may be left in `running` state. The DAG handles this automatically:

1. At startup, checks for `running` states from **previous** DAG runs
2. Marks these "zombie" states as `failed` (so they'll be retried)
3. Current run's `running` states are not affected

This check happens **before** the failure detection, so zombie states don't accidentally trigger RECOVERY mode when a fresh run is intended.

## Partition-Level Resume

For large tables (>1M rows), the DAG splits transfers into partitions. State tracking includes checkpoint information for each partition.

### Partition State Structure

```json
{
  "total_partitions": 4,
  "partitions": [
    {"id": 0, "status": "completed", "rows": 500000, "min_pk": 1, "max_pk": 500000},
    {"id": 1, "status": "completed", "rows": 500000, "min_pk": 500001, "max_pk": 1000000},
    {"id": 2, "status": "failed", "rows": 300000, "min_pk": 1000001, "max_pk": 1500000, "last_pk_synced": 1300000, "error": "timeout"},
    {"id": 3, "status": "pending", "min_pk": 1500001}
  ]
}
```

### Resume Behavior

On re-trigger after a partition failure:

| Partition Status | Action |
|-----------------|--------|
| `completed` | Skipped entirely |
| `failed` with checkpoint | Resume from `last_pk_synced`, cleanup stale rows |
| `failed` without checkpoint | Full partition retry with cleanup |
| `pending` | Run normally |
| `running` (zombie) | Treated as failed, retry |

### Mid-Partition Checkpoint Resume

When a partition fails mid-transfer, the DAG saves a checkpoint (`last_pk_synced`) indicating the last successfully committed row. On resume:

```
Partition fails at row 900K of 1M:
  ├─ last_pk_synced = 900000 saved to state
  └─ On resume:
      ├─ DELETE rows WHERE pk > 900000 AND pk <= max_pk (cleanup stale)
      ├─ Transfer continues from pk > 900000
      └─ Only ~100K rows transferred (not 1M)
```

**Checkpoint Limitations**:
- Only works for **keyset pagination** (single primary key)
- **Composite PKs** use ROW_NUMBER pagination which doesn't support mid-partition resume
- **Parallel readers** (`PARALLEL_READERS > 1`) use sequential reads for checkpoint accuracy

**Stale Checkpoint Handling**: If the worker commits rows but crashes before saving the checkpoint, those rows would be re-transferred on resume. To prevent duplicate key errors, the DAG always deletes rows after the checkpoint before resuming.

## Usage Scenarios

### Scenario 1: Network Timeout During Migration

1. First run starts, 5 of 9 tables complete, table 6 times out
2. Tables 1-5 marked `completed`, table 6 marked `failed`
3. Re-trigger the DAG
4. DAG detects failures → RECOVERY MODE
5. Second run skips tables 1-5, retries table 6, continues with 7-9

### Scenario 2: Partition Fails Mid-Transfer

1. Large table (Posts, 3.7M rows) split into 4 partitions
2. Partition 2 fails after transferring 700K of 900K rows
3. State shows: `"last_pk_synced": 2500000` in partition_info
4. Re-trigger the DAG
5. Partition 2 continues from pk > 2500000
6. Logs show: `Resuming partition from checkpoint: PK > 2500000`

### Scenario 3: Scheduler Crash Leaves Zombie States

1. DAG run crashes mid-execution
2. Tables left in `running` state (zombies)
3. Re-trigger the DAG
4. DAG detects zombies from previous run_id
5. Marks zombies as `failed`, then determines mode based on real failures
6. If no pre-existing failures: FRESH MODE (zombies alone don't trigger recovery)

### Scenario 4: Re-run After Successful Migration

1. Previous migration completed successfully (all tables `completed`)
2. Source data updated, need full refresh
3. Trigger the DAG
4. DAG detects no failures → FRESH MODE
5. Resets all state to `pending`, performs full reload

### Scenario 5: Force Specific Table Re-migration

1. Need to re-migrate just the `Users` table
2. Manually reset that table's state:
   ```sql
   UPDATE _migration_state
   SET sync_status = 'failed'
   WHERE table_name = 'users' AND migration_type = 'full';
   ```
3. Trigger the DAG
4. DAG detects failure → RECOVERY MODE
5. Skips other completed tables, re-migrates Users

## Viewing Migration State

Query the state table directly to monitor progress:

```sql
-- View all migration state
SELECT table_name, sync_status, rows_inserted, last_sync_end
FROM _migration_state
WHERE migration_type = 'full'
ORDER BY table_name;

-- View failed tables
SELECT table_name, error_message, retry_count
FROM _migration_state
WHERE sync_status = 'failed' AND migration_type = 'full';

-- View partition status for a table
SELECT table_name, partition_info
FROM _migration_state
WHERE table_name = 'posts' AND migration_type = 'full';

-- View checkpoint for failed partitions
SELECT table_name,
       p->>'id' as partition_id,
       p->>'status' as status,
       p->>'last_pk_synced' as checkpoint
FROM _migration_state,
     jsonb_array_elements(partition_info->'partitions') as p
WHERE p->>'status' = 'failed';

-- Count tables by status
SELECT sync_status, COUNT(*), ARRAY_AGG(table_name)
FROM _migration_state
WHERE migration_type = 'full'
GROUP BY sync_status;
```

## Logs

The `initialize_migration_state` task logs a summary at the start of each run:

**RECOVERY MODE example:**
```
============================================================
[PLAN] RECOVERY MODE - Resuming from failure
============================================================
  Completed: 5 tables (will SKIP)
  Failed:    2 tables (will RETRY)
  Running:   0 tables (will RETRY)
============================================================
```

**FRESH MODE example:**
```
============================================================
[PLAN] FRESH RUN - Resetting state for full reload
============================================================
  Reset 7 tables to pending
  Tables to migrate: 9
============================================================
```

## Troubleshooting

### Tables Not Being Skipped

If completed tables are being re-migrated:

1. Check that state exists: `SELECT * FROM _migration_state WHERE migration_type = 'full'`
2. Verify status is `completed` (not `running` or `pending`)
3. Confirm there are failed tables (otherwise FRESH MODE resets everything)

### Partition Resume Not Working

1. Check partition_info has `last_pk_synced`:
   ```sql
   SELECT partition_info FROM _migration_state WHERE table_name = 'your_table'
   ```
2. Verify table uses single PK (composite PKs don't support checkpoint resume)
3. Check logs for "Resuming partition from checkpoint" message

### Unexpected FRESH MODE

If the DAG resets everything when you expected recovery:

1. No `failed` status tables exist (zombies alone don't count)
2. All tables are `completed` → fresh run is appropriate
3. To force recovery mode, manually set a table to `failed`

### State Table Missing

If `_migration_state` doesn't exist:

1. The table is auto-created on first DAG run
2. Check for database connection issues
3. Verify the target PostgreSQL connection has CREATE TABLE permissions

## Best Practices

1. **Trust the automatic behavior** - The DAG chooses the right mode based on state
2. **Don't manually reset state** unless you specifically need a full re-migration
3. **Monitor partition progress** for large tables in the Airflow logs
4. **Check state table** after failures to understand what will be retried
5. **Use `skip_schema_dag=True`** when re-running after a data transfer failure (schema already exists)

## DAG Parameters

The migration DAG no longer has explicit resume/restart parameters. Instead, behavior is automatic:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `source_conn_id` | `mssql_source` | Airflow connection ID for SQL Server |
| `target_conn_id` | `postgres_target` | Airflow connection ID for PostgreSQL |
| `include_tables` | from config | Tables to migrate in `schema.table` format |
| `chunk_size` | `200000` | Rows per batch during transfer |
| `skip_schema_dag` | `false` | Skip schema creation (use for data-only retries) |

---

*Last updated: January 2026*
