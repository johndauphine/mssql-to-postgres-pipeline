# MSSQL Connection Pool Implementation

## Overview

This document summarizes the changes implementing a global SQL Server connection pool to fix performance issues discovered during SO2013 benchmarking.

## Problem Statement

**Before**: Each `ParallelReader` instance created its own pyodbc connections per thread.
- Total connections = `MAX_PARALLEL_TRANSFERS × PARALLEL_READERS`
- Example: 12 partitions × 2 readers = 24 concurrent SQL Server connections

**Benchmark Results (SO2013, 106M rows)**:
| PARALLEL_READERS | Migration Time | vs Baseline | SQL Connections |
|-----------------|----------------|-------------|-----------------|
| **1 (optimal)** | **19 min 3 sec** | **baseline** | 12 |
| 2 | 22 min 49 sec | 20% slower | 24 |
| 3 | 26 min 44 sec | 40% slower | 36 |

**Root cause**: Connection overhead dominates at scale. More connections = worse performance.

## Solution

Implement a global MSSQL connection pool with fixed max size, similar to the existing PostgreSQL pool.

## Architecture

```
BEFORE (per-partition connections):
┌─────────────────────────────────────────────────────┐
│ Partition 1: Reader1[conn] Reader2[conn]            │
│ Partition 2: Reader1[conn] Reader2[conn]            │
│ ...                                                 │
│ (12 partitions × 2 readers = 24 connections)        │
└─────────────────────────────────────────────────────┘

AFTER (global pool):
┌─────────────────────────────────────────────────────┐
│          MssqlConnectionPool (max=12)               │
│  [conn1] [conn2] [conn3] ... [conn12]               │
└───────────────────┬─────────────────────────────────┘
                    │ acquire() / release()
┌───────────────────┼─────────────────────────────────┐
│ Partition 1: Reader1 → pool.acquire()               │
│ Partition 2: Reader1 → pool.acquire() (waits if full)│
│ ...                                                 │
│ (Fixed 12 connections regardless of partition count)│
└─────────────────────────────────────────────────────┘
```

## Files Changed

### 1. `plugins/mssql_pg_migration/data_transfer.py`

#### New Class: `MssqlConnectionPool` (lines 33-209)

Thread-safe connection pool for SQL Server pyodbc connections:

```python
class MssqlConnectionPool:
    def __init__(self, mssql_config, min_conn=2, max_conn=12, acquire_timeout=120.0)
    def acquire() -> pyodbc.Connection  # Get connection, blocks if pool exhausted
    def release(conn)                    # Return connection to pool
    def close()                          # Close all connections
    def stats -> Dict[str, int]          # Pool statistics
```

Key features:
- Semaphore-based limiting (hard cap on concurrent connections)
- Connection validation before reuse
- Automatic stale connection replacement
- Pre-warming with minimum connections at startup
- Thread-safe via locks and semaphores

#### Modified Class: `DataTransfer`

Added class-level pool management:
```python
class DataTransfer:
    # PostgreSQL connection pools (class-level, shared across instances)
    _postgres_pools: Dict[str, pg_pool.ThreadedConnectionPool] = {}
    _pg_pool_lock = threading.Lock()

    # NEW: MSSQL connection pools (class-level, shared across instances)
    _mssql_pools: Dict[str, MssqlConnectionPool] = {}
    _mssql_pool_lock = threading.Lock()
```

Added methods:
- `_init_mssql_pool()` - Initializes pool for connection ID (double-check locking pattern)
- `_get_mssql_pool()` - Returns pool for current connection ID

Modified `__init__`:
- Stores `self._mssql_conn_id` for pool lookup
- Calls `self._init_mssql_pool()` to initialize pool

#### Modified Class: `ParallelReader`

Changed constructor signature:
```python
# BEFORE
def __init__(self, mssql_config: Dict[str, str], num_readers, queue_size)

# AFTER
def __init__(self, mssql_pool: MssqlConnectionPool, num_readers, queue_size)
```

Removed method:
- `_create_mssql_connection()` - No longer needed

Updated methods:
- `_reader_thread()` - Uses `self.mssql_pool.acquire()` / `self.mssql_pool.release(conn)`
- `_keyset_reader_thread()` - Same pattern

#### Modified Method: `transfer_table()`

Updated ParallelReader instantiation (2 places):
```python
# ROW_NUMBER pagination branch
parallel_reader = ParallelReader(
    mssql_pool=self._get_mssql_pool(),  # CHANGED from mssql_config
    num_readers=num_readers,
    queue_size=queue_size,
)

# Keyset pagination branch
parallel_reader = ParallelReader(
    mssql_pool=self._get_mssql_pool(),  # CHANGED from mssql_config
    num_readers=len(pk_boundaries),
    queue_size=queue_size,
)
```

### 2. `docker-compose.yml`

Added environment variable passthrough:
```yaml
MAX_MSSQL_CONNECTIONS: ${MAX_MSSQL_CONNECTIONS:-12}
```

### 3. `docs/PARALLEL_PARTITIONING.md`

Added MSSQL Connection Pool documentation section describing:
- Configuration via `MAX_MSSQL_CONNECTIONS` environment variable
- Pool behavior (pre-warming, validation, blocking)
- Recommended settings for different workloads

## Configuration

```bash
# .env file
MAX_MSSQL_CONNECTIONS=12  # Fixed pool size (default: 12)
```

| Workload | Recommended Setting |
|----------|---------------------|
| Small (<50M rows) | 8-12 |
| Large (>50M rows) | 12-16 |

## Expected Outcome

With global pool, `PARALLEL_READERS` can be set higher without connection explosion:

| Setting | Connections (Before) | Connections (After) |
|---------|---------------------|---------------------|
| 12 partitions × 1 reader | 12 | 12 (capped) |
| 12 partitions × 2 readers | 24 | 12 (capped) |
| 12 partitions × 3 readers | 36 | 12 (capped) |

Readers will wait for available connections, providing backpressure instead of overwhelming SQL Server.

## Testing Checklist

- [ ] Verify DAG parses without errors: `docker exec airflow-scheduler airflow dags list`
- [ ] Run with SO2010 dataset and verify completion
- [ ] Check logs for pool initialization: "Created MSSQL pool for mssql_source: max=12"
- [ ] Verify connections are reused (pool size stays constant in logs)
- [ ] Test with PARALLEL_READERS=2 to verify backpressure works

## Implementation Notes (Reviewed)

These notes clarify actual behavior vs. documented examples:

- **Process-local pool:** The pool is shared within a Python process, not cluster-wide. Each Airflow worker process creates its own pool. With LocalExecutor (single process), this works as expected. With CeleryExecutor/KubernetesExecutor (multiple workers), aggregate connections = `MAX_MSSQL_CONNECTIONS × worker_count`.

- **All MSSQL connections use pool (FIXED):** The `_mssql_connection()` context manager and `OdbcConnectionHelper` now use the pool. All MSSQL operations in `DataTransfer` go through the pool.

- **Validation overhead:** Connection validation runs `SELECT 1` on every acquire. This adds ~1ms latency but ensures stale connections are replaced. No env flag to disable.

- **Prewarm size:** `min_conn = max(1, max_conn // 4)`, not always 2. For MAX_MSSQL_CONNECTIONS=12, min_conn=3.

- **Timeouts:** Acquire timeout is fixed at 120s. Consider adding `MAX_MSSQL_ACQUIRE_TIMEOUT` env var if needed.

- **Shutdown:** Pool `close()` exists but Airflow doesn't call it. Connections are closed on process exit. For long-running workers, connections may accumulate if many pools are created for different conn_ids.

- **Postgres pool sizing (FIXED):** PostgreSQL pool is now configurable via `MAX_PG_CONNECTIONS` env var (default: 8). Both pools use consistent `min_conn = max(1, max_conn // 4)` sizing.

## Additional Concerns (Correctness / Ops)

- **Parallel keyset correctness:** Parallel keyset readers rely on NTILE min/max with inclusive bounds and no uniqueness guard. If the ordering column is not unique, readers can overlap or skip rows. Consider restricting parallel keyset to truly unique keys or using ROW_NUMBER with a deterministic, unique ORDER BY.
- **Boundary queries and NOLOCK:** NTILE boundary discovery uses NOLOCK unless strict mode is enabled. On a mutable source, boundaries can shift between readers, producing missing/duplicate rows. For correctness-first runs, disable NOLOCK for boundary discovery or require a quiesced source.
- **Pool scope:** Pools are per-process. With multiple Airflow worker processes, aggregate MSSQL connections can exceed `MAX_MSSQL_CONNECTIONS`. Factor executor/worker counts into sizing.
- **Non-pooled connections (RESOLVED):** `OdbcConnectionHelper` now accepts an optional pool via `set_pool()` method. `DataTransfer` shares its pool with `mssql_hook`, so all helper queries also use pooled connections.
- **Config drift (RESOLVED):** Postgres pool now reads `MAX_PG_CONNECTIONS` env var (commit 4157827). Both MSSQL and PG pools are configurable.

### Remaining items to harden

- Enforce uniqueness for parallel keyset: only enable parallel keyset readers when the ordering column(s) are guaranteed unique (true PK/unique index). Otherwise fall back to ROW_NUMBER or a deterministic unique ORDER BY to avoid overlaps/skips.
- Strict mode for boundaries: when parallel readers are enabled, force strict consistency (no NOLOCK) for NTILE boundary discovery to prevent shifting partitions on a mutable source.

## Review Notes for AI

### Thread Safety
- `MssqlConnectionPool` uses `threading.Semaphore` for connection limiting
- `threading.Lock` protects `_all_connections` list
- `queue.Queue` is inherently thread-safe
- Double-check locking pattern used for pool initialization

### Resource Management
- Connections are returned to pool in `finally` blocks (guaranteed cleanup)
- Pool `close()` method properly drains and closes all connections
- `release(None)` is safe (no-op)

### Potential Issues to Watch
1. Pool exhaustion under heavy load - 120s timeout may need tuning
2. Connection validation adds overhead - consider disabling for trusted networks
3. Pre-warming connections at startup adds initialization time

### Backward Compatibility
- No API changes to `transfer_table_data()` or DAG interface
- Only internal implementation changed
- Default `MAX_MSSQL_CONNECTIONS=12` matches previous behavior with 12 partitions
