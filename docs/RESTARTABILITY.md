# Migration DAG Restartability Guide

This guide explains how to use the restartability features of the `mssql_to_postgres_migration` DAG to resume failed or partially completed migrations without restarting from scratch.

## Overview

When migrating large datasets, failures can occur due to:
- Network timeouts
- Memory pressure
- Database locks
- Airflow scheduler issues

The restartability feature allows you to:
- Skip tables that already completed successfully
- Retry only failed tables
- Resume partitioned tables from the last completed partition
- Force re-run specific tables without resetting everything

## Quick Start

### Resume a Failed Migration

If your migration failed partway through:

```bash
# Resume - skip completed tables, retry failed ones
airflow dags trigger mssql_to_postgres_migration \
  --conf '{"resume_mode": true}'
```

### Start Fresh (Clear Previous State)

If you want to start over completely:

```bash
# Clear state and run all tables
airflow dags trigger mssql_to_postgres_migration \
  --conf '{"reset_state": true}'
```

### Retry Only Failed Tables

If you fixed an issue and want to retry only the tables that failed:

```bash
# Only retry tables that failed (skip new/pending tables)
airflow dags trigger mssql_to_postgres_migration \
  --conf '{"resume_mode": true, "retry_failed_only": true}'
```

### Force Re-run Specific Tables

If you need to re-migrate specific tables that already succeeded:

```bash
# Re-run Users and Posts, skip other completed tables
airflow dags trigger mssql_to_postgres_migration \
  --conf '{"resume_mode": true, "force_refresh_tables": ["Users", "Posts"]}'
```

## Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `resume_mode` | boolean | `false` | Skip tables that completed in previous runs. Retry failed/running tables. |
| `reset_state` | boolean | `false` | Clear all migration state before starting (fresh start). |
| `retry_failed_only` | boolean | `false` | Only retry tables that failed (skip pending tables). Requires `resume_mode=true`. |
| `force_refresh_tables` | array | `[]` | Force re-run specific tables even if completed (e.g., `["Users", "Posts"]`). |

## How It Works

### State Tracking

The migration DAG tracks state in a `_migration_state` table in the target PostgreSQL database. For each table, it records:

- **sync_status**: `pending`, `running`, `completed`, or `failed`
- **dag_run_id**: Which DAG run created/updated this state
- **partition_info**: For large tables, tracks each partition's status

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
        │ reset_state=true                    │ resume_mode=true
        └──────────────┬──────────────────────┘
                       │
                       ▼
                ┌─────────────┐
                │   PENDING   │ (retry)
                └─────────────┘
```

### Zombie State Handling

If a DAG run crashes (OOM, node failure), tasks may be left in `running` state. On the next run with `resume_mode=true`:

1. States from previous DAG runs that are still `running` are treated as `failed`
2. They are automatically marked for retry
3. The current run's `running` states are not affected

### Partition Handling

For large tables (>1M rows), the DAG splits transfers into partitions. State tracking includes:

```json
{
  "sync_status": "running",
  "partition_info": {
    "total_partitions": 4,
    "partitions": [
      {"id": 0, "status": "completed", "rows": 500000},
      {"id": 1, "status": "completed", "rows": 500000},
      {"id": 2, "status": "failed", "rows": 0, "error": "timeout"},
      {"id": 3, "status": "pending"}
    ]
  }
}
```

On resume:
- Completed partitions (0, 1) are skipped
- Failed partition (2) is retried with cleanup
- Pending partitions (3) run normally

**Important**: Before retrying a failed partition, its PK range is deleted to prevent duplicates.

## Usage Scenarios

### Scenario 1: Network Timeout During Migration

1. First run starts, 5 of 9 tables complete, then network timeout on table 6
2. Tables 1-5 are marked `completed`, table 6 is `failed`
3. Resume with: `{"resume_mode": true}`
4. Second run skips tables 1-5, retries table 6, continues with 7-9

### Scenario 2: Schema Change Requires Re-migration

1. Migration completed successfully
2. Discovered a column mapping issue in the `Users` table
3. Fix the mapping in schema DAG
4. Re-run only Users: `{"resume_mode": true, "force_refresh_tables": ["Users"]}`

### Scenario 3: Partition 3 of 6 Keeps Failing

1. Large table split into 6 partitions
2. Partitions 1-2 complete, partition 3 fails with timeout
3. Increase `chunk_size` or check for blocking queries
4. Resume: `{"resume_mode": true}`
5. Only partition 3 onwards are retried

### Scenario 4: Need to Re-migrate Everything

1. Source data was refreshed, need full re-migration
2. Clear state and start over: `{"reset_state": true}`
3. All tables are transferred from scratch

## Viewing Migration State

You can query the state table directly:

```sql
-- View all migration state
SELECT table_name, sync_status, rows_inserted, last_sync_end
FROM _migration_state
WHERE migration_type = 'full'
ORDER BY table_name;

-- View failed tables
SELECT table_name, error_message, retry_count
FROM _migration_state
WHERE sync_status = 'failed';

-- View partition status for a table
SELECT table_name, partition_info
FROM _migration_state
WHERE table_name = 'Posts';
```

## Logs

The `initialize_migration_state` task logs a summary at the start of each run:

```
============================================================
[PLAN] Migration State Summary
============================================================
  resume_mode: True
  reset_state: False
  retry_failed_only: False
  force_refresh_tables: []
------------------------------------------------------------
  Completed: 5 tables
  Failed:    2 tables
  Running:   0 tables
  Pending:   2 tables
============================================================
[PLAN] Skipping: 5 completed tables
[PLAN] Retrying: 2 failed/running tables
[PLAN] Completed tables: Users, Posts, Comments, Badges, Votes
[PLAN] Failed tables: LinkTypes, PostTypes
```

## Troubleshooting

### Tables Not Being Skipped in Resume Mode

1. Check that the state table exists: `SELECT COUNT(*) FROM _migration_state`
2. Verify table has `sync_status = 'completed'`
3. Ensure `migration_type = 'full'` (not `incremental`)

### Partitioned Table Stuck in Running

1. Check partition status: `SELECT partition_info FROM _migration_state WHERE table_name = 'YourTable'`
2. If a partition is stuck in `running` from a previous DAG run, it will be auto-reset on next resume
3. You can manually reset: `UPDATE _migration_state SET sync_status = 'pending' WHERE table_name = 'YourTable'`

### State Table Missing Columns

The state table schema is automatically migrated when the DAG runs. If you see errors about missing columns:

1. Run the migration DAG once to auto-migrate the schema
2. Or manually add columns:
   ```sql
   ALTER TABLE _migration_state ADD COLUMN IF NOT EXISTS migration_type VARCHAR(20) DEFAULT 'incremental';
   ALTER TABLE _migration_state ADD COLUMN IF NOT EXISTS dag_run_id VARCHAR(255);
   ALTER TABLE _migration_state ADD COLUMN IF NOT EXISTS partition_info JSONB;
   ```

## Best Practices

1. **Always use `resume_mode`** after a failure rather than starting fresh
2. **Don't mix incremental and full migrations** - they use separate state (`migration_type`)
3. **Monitor partition progress** for large tables in the Airflow logs
4. **Use `force_refresh_tables`** sparingly - prefer fixing the root cause
5. **Clear state periodically** after successful migrations to prevent state table bloat
