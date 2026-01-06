# Incremental Sync DAG - A Guide for Junior Data Engineers

This document explains how the incremental synchronization pipeline works, step-by-step. Unlike the full migration DAG that copies all data, the incremental DAG only syncs **new and changed** rows.

## Table of Contents

1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [How Incremental Sync Differs from Full Migration](#how-incremental-sync-differs-from-full-migration)
4. [DAG Task Flow](#dag-task-flow)
5. [Deep Dive: The Staging Table Pattern](#deep-dive-the-staging-table-pattern)
6. [Deep Dive: Change Detection with IS DISTINCT FROM](#deep-dive-change-detection-with-is-distinct-from)
7. [State Management](#state-management)
8. [Partitioning Large Tables](#partitioning-large-tables)
9. [Performance Characteristics](#performance-characteristics)
10. [Common Questions](#common-questions)

---

## Overview

### What Does the Incremental DAG Do?

The incremental DAG (`mssql_to_postgres_incremental`) keeps your PostgreSQL tables in sync with SQL Server by:

1. Finding rows that are **new** in the source (INSERT)
2. Finding rows that have **changed** in the source (UPDATE)
3. Applying only those changes to the target

### When to Use Incremental vs Full Migration

| Scenario | Use This DAG |
|----------|--------------|
| First-time migration | Full migration |
| Daily/hourly sync after initial load | **Incremental** |
| Table structure changed | Full migration |
| Data corruption, need to rebuild | Full migration |
| Regular ongoing sync | **Incremental** |

---

## Prerequisites

Before running the incremental DAG, ensure:

1. **Tables exist in target** - Run full migration first
2. **Tables have primary keys** - Required for change detection
3. **Target has data** - Incremental skips empty target tables

```
Full Migration (run once)          Incremental (run repeatedly)
        │                                    │
        ▼                                    ▼
┌───────────────────┐              ┌───────────────────┐
│  Creates tables   │              │  Tables must      │
│  Copies all data  │              │  already exist    │
│  Sets up PKs      │              │  with data        │
└───────────────────┘              └───────────────────┘
```

---

## How Incremental Sync Differs from Full Migration

### Full Migration Approach
```
Source (10M rows) ─────COPY ALL────> Target (10M rows)
                                     [Truncate first]
```
- Truncates target table
- Copies every row
- Takes longer but simpler

### Incremental Approach
```
Source (10M rows) ─────COPY TO STAGING────> Staging Table
                                                  │
                                                  ▼
                                            UPSERT (compare)
                                                  │
                                     ┌────────────┴────────────┐
                                     │                         │
                                Changed rows              Unchanged rows
                                (UPDATE)                   (SKIP)
                                     │
                                     ▼
                              Target (10M rows)
                              [Only changes applied]
```
- Compares source vs target
- Only modifies changed rows
- Much faster for small change sets

---

## DAG Task Flow

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    INCREMENTAL DAG TASK FLOW                             │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  1. initialize_state                                                    │
│     │   Creates _migration_state table if not exists                    │
│     │                                                                   │
│     ▼                                                                   │
│  2. discover_tables                                                     │
│     │   Finds tables with:                                              │
│     │   - Primary keys (required for upsert)                            │
│     │   - Existing data in target (not empty)                           │
│     │                                                                   │
│     ▼                                                                   │
│  3. create_sync_tasks                                                   │
│     │   Partitions large tables (>1M rows) for parallelism              │
│     │                                                                   │
│     ▼                                                                   │
│  4. sync_table (parallel, max 8 concurrent)                             │
│     │   ┌──────────────────────────────────────────────────────────┐   │
│     │   │  For each table/partition:                                │   │
│     │   │  a. Create staging table                                  │   │
│     │   │  b. COPY source data to staging                           │   │
│     │   │  c. UPSERT from staging to target                         │   │
│     │   │  d. Drop staging table                                    │   │
│     │   └──────────────────────────────────────────────────────────┘   │
│     │                                                                   │
│     ▼                                                                   │
│  5. collect_results                                                     │
│        Summarizes: inserted, updated, unchanged counts                  │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Step 1: Initialize State (`initialize_state`)

**Purpose**: Creates a table to track sync progress and history.

```sql
CREATE TABLE IF NOT EXISTS _migration_state (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(255) NOT NULL,
    source_schema VARCHAR(128) NOT NULL,
    target_schema VARCHAR(128) NOT NULL,

    sync_status VARCHAR(20),        -- 'running', 'completed', 'failed'
    last_sync_start TIMESTAMP,
    last_sync_end TIMESTAMP,

    rows_inserted BIGINT,
    rows_updated BIGINT,
    rows_unchanged BIGINT,

    error_message TEXT,
    retry_count INTEGER
);
```

**Why track state?**
- Know which tables synced successfully
- Track how many rows changed
- Debug failures
- Enable resumability (future feature)

### Step 2: Discover Tables (`discover_tables`)

**Purpose**: Find tables eligible for incremental sync.

**Filters applied**:
```
All tables in include_tables
        │
        ▼
┌───────────────────────┐
│ Has primary key?      │──No──> Skip (can't detect changes)
└───────────┬───────────┘
           Yes
            │
            ▼
┌───────────────────────┐
│ Target table exists   │──No──> Skip (run full migration first)
│ with data?            │
└───────────┬───────────┘
           Yes
            │
            ▼
    Ready for sync
```

**Example log output**:
```
Found 9 tables in dbo
Skipping dbo.AuditLog: no primary key
Skipping dbo.NewTable: target table is empty
Prepared 7 tables for incremental sync
```

### Step 3: Create Sync Tasks (`create_sync_tasks`)

**Purpose**: Optionally split large tables into partitions for parallel processing.

**Partitioning logic**:
```
Table row count?
       │
       ├──< 1M rows ──> Single task (no partitioning)
       │
       └──≥ 1M rows ──> Calculate partitions
                              │
                              ▼
                        ┌─────────────────────────────┐
                        │ Partitions = min(           │
                        │   MAX_PARTITIONS (6),       │
                        │   row_count / 500K          │
                        │ )                           │
                        └─────────────────────────────┘
```

**Example**: 3M row table
```
Total: 3,000,000 rows
Partitions: min(6, 3M/500K) = 6 partitions
Each partition: ~500,000 rows

Partition 0: PK 1 - 500,000
Partition 1: PK 500,001 - 1,000,000
Partition 2: PK 1,000,001 - 1,500,000
...
```

### Step 4: Sync Table (`sync_table`)

**Purpose**: The core sync logic - this is where the magic happens.

See [Deep Dive: The Staging Table Pattern](#deep-dive-the-staging-table-pattern) below.

### Step 5: Collect Results (`collect_results`)

**Purpose**: Aggregate results from all sync tasks.

**Example output**:
```
Incremental sync complete: 7 tables synced, 0 failed, 2 had no changes.
Total: 1,234 inserted, 567 updated
```

---

## Deep Dive: The Staging Table Pattern

This is the most important concept to understand. Here's exactly what happens for each table:

### Step-by-Step Visualization

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     STAGING TABLE PATTERN                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  STEP 1: Create Staging Table                                               │
│  ─────────────────────────────                                              │
│                                                                             │
│  CREATE UNLOGGED TABLE _staging_Users_abc123                                │
│  (LIKE stackoverflow2010__dbo.Users INCLUDING ALL);                         │
│                                                                             │
│  Result:                                                                    │
│  ┌─────────────────────────────────┐                                        │
│  │ _staging_Users_abc123 (empty)   │                                        │
│  └─────────────────────────────────┘                                        │
│                                                                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  STEP 2: COPY Source Data to Staging                                        │
│  ───────────────────────────────────                                        │
│                                                                             │
│  SQL Server                          PostgreSQL                             │
│  ┌──────────────────┐               ┌─────────────────────────────────┐    │
│  │ dbo.Users        │──COPY──────>  │ _staging_Users_abc123           │    │
│  │ (299,398 rows)   │  (binary)     │ (299,398 rows)                  │    │
│  └──────────────────┘               └─────────────────────────────────┘    │
│                                                                             │
│  COPY ... FROM STDIN WITH (FORMAT BINARY)                                   │
│                                                                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  STEP 3: UPSERT from Staging to Target                                      │
│  ─────────────────────────────────────                                      │
│                                                                             │
│  ┌─────────────────────────────────┐    ┌─────────────────────────────────┐│
│  │ _staging_Users_abc123           │    │ stackoverflow2010__dbo.Users    ││
│  │                                 │    │                                 ││
│  │ Id=1, Name='Jon', Rep=100       │    │ Id=1, Name='Jon', Rep=50  (OLD) ││
│  │ Id=2, Name='Jane', Rep=200      │    │ Id=2, Name='Jane', Rep=200      ││
│  │ Id=3, Name='NEW USER', Rep=1    │    │ (doesn't exist)                 ││
│  └───────────────┬─────────────────┘    └───────────────▲─────────────────┘│
│                  │                                      │                  │
│                  │    INSERT ... ON CONFLICT DO UPDATE  │                  │
│                  │    WHERE IS DISTINCT FROM            │                  │
│                  └──────────────────────────────────────┘                  │
│                                                                             │
│  Result:                                                                    │
│  - Id=1: Rep changed 50→100 (UPDATE)                                        │
│  - Id=2: No change (SKIP - not updated)                                     │
│  - Id=3: New row (INSERT)                                                   │
│                                                                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  STEP 4: Drop Staging Table                                                 │
│  ──────────────────────────                                                 │
│                                                                             │
│  DROP TABLE _staging_Users_abc123;                                          │
│                                                                             │
│  Cleanup - staging table is temporary                                       │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Why Use a Staging Table?

**Alternative approaches and why they're slower**:

| Approach | Problem |
|----------|---------|
| Row-by-row comparison | N queries to source, N queries to target - extremely slow |
| Hash-based comparison | Must compute hash for every row - CPU intensive |
| Merge join on source | Requires cross-database join - not possible |

**Staging table advantages**:
- Single bulk COPY from source (fast)
- All comparison happens in PostgreSQL (single database)
- PostgreSQL optimizer can use indexes efficiently
- UNLOGGED tables skip WAL for faster writes

### What is an UNLOGGED Table?

```
Regular Table                    UNLOGGED Table
┌─────────────────────┐         ┌─────────────────────┐
│ Write data          │         │ Write data          │
│        │            │         │        │            │
│        ▼            │         │        ▼            │
│ Write to WAL        │         │ (skip WAL)          │
│ (Write-Ahead Log)   │         │                     │
│        │            │         │                     │
│        ▼            │         │                     │
│ Write to disk       │         │ Write to disk       │
└─────────────────────┘         └─────────────────────┘

Crash recovery: YES              Crash recovery: NO
Speed: Normal                    Speed: 2-3x faster
```

**Why it's safe for staging**: The staging table is temporary. If the server crashes, we just re-run the sync.

---

## Deep Dive: Change Detection with IS DISTINCT FROM

The key to efficient incremental sync is the `IS DISTINCT FROM` clause.

### The SQL Statement

```sql
INSERT INTO target_schema.Users (Id, Name, Reputation, CreationDate)
SELECT Id, Name, Reputation, CreationDate
FROM staging_schema._staging_Users
ON CONFLICT (Id) DO UPDATE SET
    Name = EXCLUDED.Name,
    Reputation = EXCLUDED.Reputation,
    CreationDate = EXCLUDED.CreationDate
WHERE
    Users.Name IS DISTINCT FROM EXCLUDED.Name
    OR Users.Reputation IS DISTINCT FROM EXCLUDED.Reputation
    OR Users.CreationDate IS DISTINCT FROM EXCLUDED.CreationDate
RETURNING (xmax = 0) AS inserted
```

### Breaking Down the Magic

#### Part 1: `INSERT ... ON CONFLICT`
```sql
INSERT INTO target_schema.Users (...)
SELECT ... FROM staging_schema._staging_Users
ON CONFLICT (Id) DO UPDATE SET ...
```

This is PostgreSQL's "UPSERT" syntax:
- If the PK doesn't exist → INSERT new row
- If the PK exists → UPDATE existing row

#### Part 2: `IS DISTINCT FROM` (The Secret Sauce)

**Why not use `!=` or `<>`?**

```sql
-- Problem with regular comparison:
NULL != NULL  →  NULL (unknown, not true!)
NULL <> 'abc' →  NULL (unknown, not true!)

-- IS DISTINCT FROM handles NULLs correctly:
NULL IS DISTINCT FROM NULL  →  FALSE (same)
NULL IS DISTINCT FROM 'abc' →  TRUE (different)
'abc' IS DISTINCT FROM 'abc' →  FALSE (same)
```

**Truth table**:

| Value A | Value B | A != B | A IS DISTINCT FROM B |
|---------|---------|--------|----------------------|
| 'foo' | 'foo' | FALSE | FALSE |
| 'foo' | 'bar' | TRUE | TRUE |
| 'foo' | NULL | NULL | TRUE |
| NULL | 'bar' | NULL | TRUE |
| NULL | NULL | NULL | **FALSE** |

The last row is key - `NULL IS DISTINCT FROM NULL` returns FALSE, meaning "these are the same."

#### Part 3: The WHERE Clause

```sql
WHERE
    Users.Name IS DISTINCT FROM EXCLUDED.Name
    OR Users.Reputation IS DISTINCT FROM EXCLUDED.Reputation
    OR Users.CreationDate IS DISTINCT FROM EXCLUDED.CreationDate
```

This WHERE clause means: **only update if at least one column changed**.

**If no columns changed**: The WHERE clause is FALSE, so no UPDATE happens. This saves:
- Disk I/O (no write)
- WAL writes (no logging)
- Index updates (no changes)
- Trigger executions (nothing fires)

#### Part 4: Counting Inserts vs Updates

```sql
RETURNING (xmax = 0) AS inserted
```

PostgreSQL's `xmax` system column tells us:
- `xmax = 0` → Row was just inserted (new)
- `xmax > 0` → Row was updated (existing)

This lets us count exactly how many inserts vs updates occurred.

### Visual Example

```
Staging Table                    Target Table (before)
┌────┬────────┬─────┐            ┌────┬────────┬─────┐
│ Id │ Name   │ Rep │            │ Id │ Name   │ Rep │
├────┼────────┼─────┤            ├────┼────────┼─────┤
│ 1  │ Alice  │ 100 │            │ 1  │ Alice  │ 100 │  ← Same (skip)
│ 2  │ Bob    │ 250 │            │ 2  │ Bob    │ 200 │  ← Changed (update)
│ 3  │ Carol  │ 50  │            │    │        │     │  ← New (insert)
└────┴────────┴─────┘            └────┴────────┴─────┘

                    UPSERT with IS DISTINCT FROM
                              │
                              ▼

Target Table (after)
┌────┬────────┬─────┐
│ Id │ Name   │ Rep │
├────┼────────┼─────┤
│ 1  │ Alice  │ 100 │  ← Unchanged (0 disk writes)
│ 2  │ Bob    │ 250 │  ← Updated (1 disk write)
│ 3  │ Carol  │ 50  │  ← Inserted (1 disk write)
└────┴────────┴─────┘

Result: 1 inserted, 1 updated, 1 unchanged
```

---

## State Management

### The `_migration_state` Table

Every sync operation is tracked:

```
┌────┬────────────┬─────────────┬─────────────────────────┬───────────┬──────────┬──────────┬───────────┐
│ id │ table_name │ sync_status │ last_sync_end           │ inserted  │ updated  │ unchanged │ retry_cnt │
├────┼────────────┼─────────────┼─────────────────────────┼───────────┼──────────┼───────────┼───────────┤
│ 1  │ Users      │ completed   │ 2026-01-06 10:30:00     │ 12        │ 45       │ 299,341   │ 0         │
│ 2  │ Posts      │ completed   │ 2026-01-06 10:31:15     │ 1,234     │ 567      │ 3,727,394 │ 0         │
│ 3  │ Comments   │ failed      │ 2026-01-06 10:32:00     │ 0         │ 0        │ 0         │ 2         │
└────┴────────────┴─────────────┴─────────────────────────┴───────────┴──────────┴───────────┴───────────┘
```

### State Transitions

```
┌─────────────┐
│   (new)     │
└──────┬──────┘
       │ start_sync()
       ▼
┌─────────────┐
│   running   │
└──────┬──────┘
       │
       ├─────────────────────┐
       │ complete_sync()     │ fail_sync()
       ▼                     ▼
┌─────────────┐       ┌─────────────┐
│  completed  │       │   failed    │
└─────────────┘       └──────┬──────┘
                             │ retry
                             ▼
                      ┌─────────────┐
                      │   running   │
                      └─────────────┘
```

---

## Partitioning Large Tables

### Why Partition?

For a 10M row table:
- **Single task**: 1 CPU, ~5 minutes
- **6 partitions**: 6 CPUs, ~1 minute (5x faster)

### How Partitions are Calculated

```python
# Example: 3M row table with PK range 1-3,000,000

num_partitions = min(
    MAX_PARTITIONS_PER_TABLE,  # 6 (hard limit)
    row_count // 500_000       # 3M / 500K = 6
)
# Result: 6 partitions

partition_size = pk_range // num_partitions
# 3M / 6 = 500,000 per partition

# Partition boundaries:
# Partition 0: PK >= 1 AND PK <= 500,000
# Partition 1: PK >= 500,001 AND PK <= 1,000,000
# ... etc
```

### Partition Processing

Each partition:
1. Creates its own staging table: `_staging_Users_p0_abc123`
2. COPYs only its PK range from source
3. Upserts to the target table (same target, different source rows)
4. Drops its staging table

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    PARALLEL PARTITION PROCESSING                         │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  Partition 0 ─────┬───> _staging_p0 ───> UPSERT ───> Target            │
│  (PK 1-500K)      │                                     │               │
│                   │                                     │               │
│  Partition 1 ─────┼───> _staging_p1 ───> UPSERT ───────>│               │
│  (PK 500K-1M)     │                                     │               │
│                   │                                     │               │
│  Partition 2 ─────┼───> _staging_p2 ───> UPSERT ───────>│               │
│  (PK 1M-1.5M)     │                        ...         ...              │
│                   │                                                     │
│  (parallel)       │                                                     │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Performance Characteristics

### Typical Performance Numbers

| Scenario | Source Rows | Changed Rows | Time | Throughput |
|----------|-------------|--------------|------|------------|
| Mostly unchanged | 10M | 1,000 | ~2 min | 80K rows/sec |
| 10% changed | 10M | 1M | ~5 min | 33K rows/sec |
| 50% changed | 10M | 5M | ~8 min | 20K rows/sec |
| All new (first sync) | 10M | 10M | ~12 min | 14K rows/sec |

### Where Time is Spent

```
Typical incremental sync (10M rows, 0.1% changed):

┌────────────────────────────────────────────────────────────────┐
│ COPY to staging: ████████████████████░░░░░░░░░░░░░░░░  45%    │
│ UPSERT compare:  ██████████████░░░░░░░░░░░░░░░░░░░░░░  35%    │
│ Actual updates:  ██░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░   5%    │
│ Overhead:        ██████░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  15%    │
└────────────────────────────────────────────────────────────────┘

Most time is spent copying and comparing, not updating.
Actual disk writes are minimal when few rows changed.
```

### Optimization Tips

1. **Schedule during low-change periods**: Fewer changes = faster sync
2. **Partition threshold**: Lower for wide tables, higher for narrow tables
3. **Batch size**: Increase for narrow tables, decrease for wide tables
4. **Run frequency**: More frequent = fewer changes per run = faster

---

## Common Questions

### Q: Why does incremental copy ALL rows to staging?

**A**: It's actually faster than alternatives:

| Alternative | Problem |
|-------------|---------|
| Query changed rows from source | How do you know which changed? Need to compare anyway. |
| Use timestamps (modified_date) | Not all tables have reliable timestamps |
| Change Data Capture (CDC) | Requires SQL Server configuration, complex setup |

The staging approach is "copy everything, compare locally" - simple and robust.

### Q: What happens if incremental sync fails mid-way?

**A**: Each table/partition is independent. If partition 3 fails:
- Partitions 0, 1, 2: Already committed, data is safe
- Partition 3: State shows "failed", can retry
- Partitions 4, 5: May or may not have run depending on when failure occurred

On retry, the staging pattern is **idempotent** - running it again produces the same result.

### Q: Can I run incremental on a table that's never been migrated?

**A**: No. The DAG checks `target_table_has_data()` and skips empty/missing tables. Run full migration first.

### Q: How do I handle deletes?

**A**: The current incremental DAG does NOT handle deletes. It only syncs:
- New rows (INSERT)
- Changed rows (UPDATE)

To handle deletes, you would need to:
1. Query source for all PKs
2. Compare with target PKs
3. Delete PKs in target but not in source

This is not currently implemented.

### Q: Why use UNLOGGED staging tables?

**A**: UNLOGGED tables don't write to the WAL (Write-Ahead Log), making them 2-3x faster. The trade-off is no crash recovery, but staging tables are temporary anyway - if the server crashes, we just re-run.

---

## Environment Variables Reference

| Variable | Default | Description |
|----------|---------|-------------|
| `INCLUDE_TABLES` | (required) | Comma-separated list of tables |
| `DEFAULT_INCREMENTAL_BATCH_SIZE` | 100000 | Rows per COPY chunk |
| `PARTITION_THRESHOLD` | 1000000 | Rows before partitioning kicks in |
| `MAX_PARTITIONS_PER_TABLE` | 6 | Max partitions per table |
| `MAX_PARALLEL_TRANSFERS` | 8 | Max concurrent sync tasks |
| `USE_BINARY_COPY` | true | Use binary COPY format |
| `USE_UNLOGGED_STAGING` | true | Use UNLOGGED staging tables |

---

*Last updated: January 2026*
