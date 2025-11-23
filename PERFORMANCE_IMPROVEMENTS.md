# Performance Improvement Plan for MSSQL to PostgreSQL Migration

## Executive Summary
This document outlines critical performance improvements needed for the MSSQL to PostgreSQL migration pipeline. Current implementation has several bottlenecks that significantly impact transfer speeds, especially for large tables like Posts (3.7M+ rows).

## Critical Performance Issues

### 1. Row-by-Row Insertion (CRITICAL)
**Location:** `fix_posts_migration_dag.py:145-150`
**Impact:** 100-1000x slower than bulk operations
**Current Code:**
```python
for row_data in processed_rows:
    postgres_cursor.execute(insert_query, row_data)  # VERY SLOW!
```

### 2. Inefficient Pandas Memory Usage
**Location:** `data_transfer.py:256`
**Impact:** High memory consumption, potential OOM errors
**Issue:** Loading entire chunks into pandas DataFrames

### 3. Suboptimal COPY Implementation
**Location:** `data_transfer.py:338-376`
**Impact:** 2-5x slower than optimized COPY
**Issues:**
- Using CSV format instead of binary
- Processing through pandas unnecessarily
- Creating intermediate StringIO buffers

### 4. Missing Connection Pooling
**Impact:** Connection overhead on each operation
**Issue:** Creating new connections for each task

### 5. No Parallel Processing
**Location:** `mssql_to_postgres_migration.py:403`
**Impact:** Single-threaded bottleneck for large tables
**Issue:** Tables processed sequentially when they could be parallel

## Recommended Optimizations

### Priority 1: Replace Row-by-Row with Bulk Operations

#### Option A: Use executemany() (5-10x faster)
```python
# Instead of loop, use:
postgres_cursor.executemany(insert_query, processed_rows)
```

#### Option B: Use COPY (50-100x faster)
```python
# Use pg8000's native COPY support
import io
import csv

buffer = io.StringIO()
writer = csv.writer(buffer, delimiter='\t')
for row in processed_rows:
    writer.writerow(row)
buffer.seek(0)

postgres_cursor.execute(
    f'COPY public."posts" FROM STDIN WITH (FORMAT CSV, DELIMITER E\'\\t\')'
)
postgres_cursor.execute(buffer.read())
```

### Priority 2: Optimize Data Transfer Pipeline

#### Remove Pandas Dependency for Large Tables
```python
def transfer_without_pandas(self, source_schema, source_table, target_schema, target_table):
    """Direct cursor-to-cursor transfer without pandas overhead"""
    # Use server-side cursor for memory efficiency
    mssql_cursor = self.mssql_hook.get_cursor(server_side=True)

    # Stream directly to PostgreSQL COPY
    with self.postgres_hook.get_conn() as pg_conn:
        with pg_conn.cursor() as pg_cursor:
            # Start COPY
            copy_sql = f"COPY {target_schema}.{target_table} FROM STDIN WITH (FORMAT BINARY)"
            pg_cursor.copy_expert(copy_sql, data_stream)
```

#### Use Binary COPY Format (2-3x faster)
```python
# Binary format is much faster than CSV
copy_sql = "COPY table FROM STDIN WITH (FORMAT BINARY)"
```

### Priority 3: Implement Parallel Processing

#### For Large Tables (>1M rows)
```python
def parallel_transfer_large_table(table_info):
    """Split large tables into parallel chunks"""
    # Get min/max IDs
    min_id, max_id = get_id_range(table_info)

    # Create ID ranges for parallel processing
    chunk_ranges = create_id_ranges(min_id, max_id, num_workers=4)

    # Transfer chunks in parallel
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [
            executor.submit(transfer_chunk_by_id_range, range_info)
            for range_info in chunk_ranges
        ]
```

#### Dynamic Parallelism Based on Table Size
```python
def determine_parallelism(row_count):
    if row_count < 10_000:
        return 1  # Serial
    elif row_count < 100_000:
        return 2  # 2 parallel
    elif row_count < 1_000_000:
        return 4  # 4 parallel
    else:
        return 8  # 8 parallel
```

### Priority 4: Connection Optimization

#### Implement Connection Pooling
```python
from psycopg2 import pool

class ConnectionPool:
    def __init__(self, min_conn=2, max_conn=10):
        self.pool = psycopg2.pool.ThreadedConnectionPool(
            min_conn, max_conn,
            host=config['host'],
            database=config['database'],
            user=config['user'],
            password=config['password']
        )
```

#### Reuse Connections Across Transfers
```python
def transfer_with_connection_reuse(self, tables, connection):
    """Reuse single connection for multiple tables"""
    for table in tables:
        # Use same connection
        self.transfer_table(table, connection)
```

### Priority 5: Query Optimization

#### Add Index Hints for ORDER BY
```python
# Instead of:
ORDER BY [Id]  # May not use index

# Use:
ORDER BY [Id] WITH (INDEX(PK_Posts_Id))  # Force index usage
```

#### Use NOLOCK for Read Operations
```python
SELECT * FROM [dbo].[Posts] WITH (NOLOCK)  # Avoid lock contention
```

### Priority 6: Chunk Size Optimization

#### Dynamic Chunk Sizing
```python
def calculate_optimal_chunk_size(table_info):
    """Calculate chunk size based on row size and count"""
    avg_row_size = table_info.get('avg_row_size', 100)
    total_rows = table_info.get('row_count', 0)

    # Target ~10MB chunks
    target_bytes = 10 * 1024 * 1024
    optimal_chunk = target_bytes // avg_row_size

    # Bounds
    return max(1000, min(50000, optimal_chunk))
```

### Priority 7: Memory Optimization

#### Stream Processing Instead of Batch Loading
```python
def stream_transfer(self):
    """Stream data without loading into memory"""
    # Use server-side cursor
    with self.mssql_hook.get_server_cursor() as cursor:
        cursor.execute(query)

        # Stream to PostgreSQL
        for row in cursor:
            # Process and write immediately
            write_row_to_postgres(row)
```

#### Use Generators for Data Processing
```python
def process_rows_generator(cursor):
    """Generator to process rows without loading all into memory"""
    for row in cursor:
        yield process_row(row)
```

## Performance Benchmarks (Expected)

| Optimization | Current Speed | Expected Speed | Improvement |
|-------------|---------------|----------------|-------------|
| Row-by-row → Bulk insert | 100 rows/sec | 5,000 rows/sec | 50x |
| CSV COPY → Binary COPY | 10,000 rows/sec | 25,000 rows/sec | 2.5x |
| Serial → Parallel (4 workers) | 10,000 rows/sec | 35,000 rows/sec | 3.5x |
| With connection pooling | - | +20% | 1.2x |
| Optimized chunk sizes | - | +15% | 1.15x |
| **Combined optimizations** | **100 rows/sec** | **50,000+ rows/sec** | **500x+** |

## Implementation Priority

1. **Immediate (Critical):**
   - Fix row-by-row insertion in `fix_posts_migration_dag.py`
   - Implement bulk operations

2. **High Priority:**
   - Optimize COPY implementation
   - Add parallel processing for large tables

3. **Medium Priority:**
   - Implement connection pooling
   - Dynamic chunk sizing
   - Query optimization

4. **Low Priority:**
   - Memory optimizations
   - Monitoring and metrics

## Testing Strategy

1. **Performance Testing:**
   ```bash
   # Test with small dataset first
   python test_performance.py --rows 10000

   # Gradually increase
   python test_performance.py --rows 100000
   python test_performance.py --rows 1000000
   ```

2. **Memory Profiling:**
   ```python
   import memory_profiler

   @profile
   def transfer_data():
       # Monitor memory usage during transfer
   ```

3. **Benchmark Comparisons:**
   - Create benchmark script comparing old vs new methods
   - Track metrics: rows/sec, memory usage, CPU usage

## Monitoring and Metrics

### Key Metrics to Track:
- Rows per second transfer rate
- Memory usage (peak and average)
- Connection pool utilization
- Query execution times
- Network throughput
- Error rates and retries

### Implementation Example:
```python
class PerformanceMonitor:
    def __init__(self):
        self.metrics = {
            'rows_per_second': [],
            'memory_usage': [],
            'active_connections': 0
        }

    def log_transfer_rate(self, rows, duration):
        rate = rows / duration
        self.metrics['rows_per_second'].append(rate)
        logger.info(f"Transfer rate: {rate:.0f} rows/sec")
```

## Configuration Recommendations

### Airflow Configuration:
```python
# Increase parallelism for DAG
default_args = {
    'pool': 'data_transfer_pool',  # Create dedicated pool
    'max_active_tis_per_dag': 16,  # Allow more parallel tasks
}
```

### PostgreSQL Tuning:
```sql
-- Increase work_mem for COPY operations
SET work_mem = '256MB';

-- Disable triggers during bulk load
ALTER TABLE posts DISABLE TRIGGER ALL;

-- Re-enable after load
ALTER TABLE posts ENABLE TRIGGER ALL;
```

### SQL Server Tuning:
```sql
-- Add NOLOCK hint for reads
-- Ensure statistics are updated
UPDATE STATISTICS dbo.Posts;
```

## Next Steps

1. Create a new branch: `feature/performance-optimization`
2. Implement Priority 1 fixes first (row-by-row issue)
3. Benchmark improvements
4. Gradually implement other optimizations
5. Run full regression tests
6. Monitor production performance

## Notes for Future Sessions

- Start with fixing `fix_posts_migration_dag.py` line 145-150
- The Posts table (3.7M rows) is the best test case
- Current transfer takes hours, should be <10 minutes with optimizations
- Consider using `psycopg2` instead of `pg8000` for better COPY support
- Test with Docker resource limits to simulate production constraints

---

**Document Created:** 2025-11-22
**Status:** Ready for implementation
**Estimated Performance Gain:** 50-500x depending on table size