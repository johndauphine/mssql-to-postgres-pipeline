# Parallel Table Partitioning for Large Tables

## Overview

This document describes the automatic partitioning feature that parallelizes the transfer of large tables (>5M rows) by splitting them into multiple partitions. The system uses **NTILE-based partitioning** which works with ALL primary key types.

## Supported Primary Key Types

| PK Type | Strategy | Example |
|---------|----------|---------|
| Integer (INT, BIGINT) | NTILE boundaries | `WHERE Id >= 1000 AND Id < 2000` |
| GUID/UUID | NTILE boundaries | `WHERE OrderId >= 'a1b2...' AND OrderId < 'c3d4...'` |
| String (VARCHAR, CHAR) | NTILE boundaries | `WHERE ProductCode >= 'PRD-001' AND ProductCode < 'PRD-500'` |
| Composite (multi-column) | ROW_NUMBER ranges | `WHERE _rn BETWEEN 1 AND 500000` |

## How It Works

### 1. Automatic Detection
Tables are automatically identified for partitioning based on row count:

```python
LARGE_TABLE_THRESHOLD = 5_000_000  # 5 million rows
MAX_PARTITIONS = int(os.environ.get('MAX_PARTITIONS', '8'))
```

Any table exceeding the threshold is automatically partitioned.

### 2. Primary Key Analysis
The system analyzes the primary key to determine the partitioning strategy:

```python
pk_info = {
    'columns': [{'name': 'OrderId', 'data_type': 'uniqueidentifier'}],
    'is_composite': False,
    'is_all_integer': False
}
```

### 3. NTILE Boundary Detection (Single-Column PKs)
For single-column primary keys of ANY type, NTILE divides rows into equal groups:

```sql
WITH numbered AS (
    SELECT [OrderId],
           NTILE(8) OVER (ORDER BY [OrderId]) as partition_id
    FROM [dbo].[GuidOrders]
)
SELECT partition_id,
       MIN([OrderId]) as min_pk,
       MAX([OrderId]) as max_pk,
       COUNT(*) as row_count
FROM numbered
GROUP BY partition_id
ORDER BY partition_id
```

This produces balanced partitions regardless of PK type or gaps:
- **Partition 1**: `OrderId >= 'a1b2...' AND OrderId <= 'b2c3...'`
- **Partition 2**: `OrderId >= 'b2c4...' AND OrderId <= 'c3d4...'`
- etc.

### 4. ROW_NUMBER Pagination (Composite PKs)
For composite primary keys, we use ROW_NUMBER-based pagination:

```sql
SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (ORDER BY tenant_id, order_id) as _rn
    FROM [dbo].[OrderDetails]
) sub
WHERE _rn BETWEEN 1 AND 500000
```

### 5. Parallel Execution with Truncate Safety
Partitions are executed in two phases to prevent race conditions:

```
Phase 1 (First partitions - do TRUNCATE):
transfer_partition[table1_p1] ──┐
transfer_partition[table2_p1] ──┼──► (wait for all)
transfer_partition[table3_p1] ──┘
                                │
                                ▼
Phase 2 (Remaining partitions - append only):
transfer_partition__1[table1_p2] ──┐
transfer_partition__1[table1_p3] ──┤
transfer_partition__1[table2_p2] ──┼──► collect_all_results
transfer_partition__1[table2_p3] ──┤
...                                ──┘
```

## Why NTILE Instead of Arithmetic?

The previous approach used arithmetic division:
```python
# Old approach (problematic with gaps)
chunk_size = (max_id - min_id) // partition_count
partition_1 = f"Id >= {min_id} AND Id < {min_id + chunk_size}"
```

This fails with sparse IDs:
- IDs: 1-1000, 10001-11000, 20001-21000 (gaps from deletions)
- Arithmetic: Partition 1 gets 99% of rows, others nearly empty

**NTILE divides by row count**, guaranteeing balanced partitions:
```sql
NTILE(4) OVER (ORDER BY Id)
-- Each partition gets exactly 25% of rows
```

## Parallel Readers (Advanced)

For additional throughput, you can enable parallel SQL Server readers within each partition. This overlaps read and write I/O operations for better utilization.

### How Parallel Readers Work

```
Default (PARALLEL_READERS=1):
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│ Read Chunk  │ ──► │ Write Chunk │ ──► │   Commit    │ ──► (repeat)
│ (MSSQL)     │     │ (PG COPY)   │     │             │
└─────────────┘     └─────────────┘     └─────────────┘
     ~50%                ~50%
   (blocked)          (blocked)

With PARALLEL_READERS=2:
┌──────────────┐
│ Reader 1     │──┐
│ (rows 1-25K) │  │
└──────────────┘  │     ┌─────────────┐     ┌─────────────┐
                  ├───► │   Queue     │ ──► │   Writer    │
┌──────────────┐  │     │ (bounded)   │     │ (PG COPY)   │
│ Reader 2     │──┘     └─────────────┘     └─────────────┘
│ (rows 25K+)  │
└──────────────┘
```

Each reader has its own SQL Server connection and reads a disjoint row range. The bounded queue provides backpressure to prevent memory exhaustion.

### Enabling Parallel Readers

```bash
# .env file
PARALLEL_READERS=2      # Number of concurrent SQL Server connections (default: 1)
READER_QUEUE_SIZE=5     # Max chunks buffered in queue (default: 5)
```

**Recommendations:**
- `PARALLEL_READERS=1`: Default, sequential reads (current behavior)
- `PARALLEL_READERS=2`: Recommended for most workloads
- `PARALLEL_READERS=3-4`: For high-bandwidth networks only

### When Parallel Readers Are Used

Parallel readers are active when:
1. `PARALLEL_READERS > 1` is set in environment
2. The row count is at least 2x the chunk_size (default: 400K+ rows)

**Supported pagination modes:**
- **Keyset pagination** (single-column PKs): Uses NTILE to compute balanced PK boundaries
- **ROW_NUMBER pagination** (composite PKs): Divides row ranges among readers

For small tables, sequential mode is always used.

### Parallel Readers Benchmark

**Hardware:** Windows/WSL2, 32GB RAM, 16 cores

#### Small Dataset: StackOverflow 2010 (19.3M rows, 10 tables)

| PARALLEL_READERS | Migration Time | vs Baseline |
|-----------------|----------------|-------------|
| 1 (sequential) | 3 min 57 sec | baseline |
| **2 (recommended)** | **3 min 11 sec** | **20% faster** |
| 3 | 3 min 31 sec | 11% faster |

#### Large Dataset: StackOverflow 2013 (106M rows, 9 tables)

| PARALLEL_READERS | Migration Time | vs Baseline | SQL Connections |
|-----------------|----------------|-------------|-----------------|
| **1 (optimal)** | **19 min 3 sec** | **baseline** | 12 |
| 2 | 22 min 49 sec | 20% slower | 24 |
| 3 | 26 min 44 sec | 40% slower | 36 |

**Key findings:**
- For **small datasets (<50M rows)**: 2 parallel readers provides ~20% improvement
- For **large datasets (>50M rows)**: Sequential reads (PARALLEL_READERS=1) is optimal
- The difference is due to:
  - More concurrent partitions at larger scale (12 vs 4-8)
  - Total SQL connections = PARALLEL_READERS × concurrent_partitions
  - Connection overhead and contention dominate at higher connection counts
  - **Solution**: Global MSSQL connection pool limits total connections regardless of partition count

## Configuration

### Environment Variables

```bash
# .env file
MAX_PARTITIONS=8           # Maximum partitions per table (default: 8)
PARALLEL_READERS=1         # SQL Server reader threads per partition (default: 1)
READER_QUEUE_SIZE=5        # Max chunks buffered between readers and writer (default: 5)
MAX_MSSQL_CONNECTIONS=12   # Maximum SQL Server connections in pool (default: 12)
MAX_PG_CONNECTIONS=8       # Maximum PostgreSQL connections in pool (default: 8)
```

### MSSQL Connection Pool

The pipeline uses a global connection pool for SQL Server to limit total concurrent connections regardless of partition count or parallel reader settings.

| Setting | Description | Default |
|---------|-------------|---------|
| `MAX_MSSQL_CONNECTIONS` | Hard limit on concurrent pyodbc connections | 12 |

The pool automatically manages connection lifecycle:
- Pre-warms minimum connections at startup (25% of max)
- Validates connections before reuse
- Replaces stale connections transparently
- Blocks callers when pool is exhausted (with 120s timeout)

**Recommended settings:**
| Workload | MAX_MSSQL_CONNECTIONS |
|----------|----------------------|
| Small (<50M rows) | 8-12 |
| Large (>50M rows) | 12-16 |

### PostgreSQL Connection Pool

The pipeline also uses a connection pool for PostgreSQL writes.

| Setting | Description | Default |
|---------|-------------|---------|
| `MAX_PG_CONNECTIONS` | Hard limit on concurrent psycopg2 connections | 8 |

**Recommended settings:**
| Workload | MAX_PG_CONNECTIONS |
|----------|-------------------|
| Default | 8 |
| High parallelism (12+ partitions) | 12-16 |

### DAG Parameters

```python
# Threshold for partitioning large tables (rows)
LARGE_TABLE_THRESHOLD = 5_000_000

# Number of partitions (capped by MAX_PARTITIONS env var)
partition_count = min(calculated_count, MAX_PARTITIONS)
```

### Partition Count Recommendations
| Table Size | Recommended Partitions |
|------------|----------------------|
| 5-10M rows | 4 partitions |
| 10-50M rows | 8 partitions |
| 50M+ rows | 8-12 partitions (I/O bound) |

Note: Beyond 8 partitions, gains diminish due to I/O bottlenecks.

## Performance Results

### Test: StackOverflow 2013 (106M rows)
- **Hardware**: WSL2, 32GB RAM
- **Throughput**: 270K rows/sec
- **Partitions**: 8 (optimal for this workload)

### Test: PartitionTest (7M rows, 4 tables)
| Table | PK Type | Rows | Time | Rate |
|-------|---------|------|------|------|
| CompositeOrderDetails | INT, INT | 2M | ~8s | 250K/s |
| GuidOrders | UNIQUEIDENTIFIER | 2M | ~9s | 220K/s |
| SparseIntTable | INT (gaps) | 1.5M | ~5s | 300K/s |
| StringProducts | VARCHAR(20) | 1.5M | ~6s | 250K/s |

## Task Flow

```
extract_source_schema
        │
        ▼
create_target_tables
        │
        ├──────────────────────┐
        ▼                      ▼
prepare_regular_tables    prepare_large_table_partitions
        │                      │
        ▼                      ├──► get_first_partitions
transfer_table_data[...]       ├──► get_remaining_partitions
        │                      │
        │              ┌───────┴───────┐
        │              ▼               ▼
        │    transfer_partition   transfer_partition__1
        │    (first partitions)   (remaining partitions)
        │              │               │
        │              └───────┬───────┘
        └──────────────────────┤
                               ▼
                     collect_all_results
                               │
                               ▼
                       reset_sequences
                               │
                               ▼
                     create_primary_keys
                               │
                               ▼
                     create_indexes
                               │
                               ▼
                     trigger_validation_dag
```

## Troubleshooting

### Partition Task Naming
In Airflow 3.x, multiple `.expand()` calls create separate task IDs:
- `transfer_partition` - First partitions (with TRUNCATE)
- `transfer_partition__1` - Remaining partitions (append)

### Composite PK Performance
ROW_NUMBER pagination is slower than keyset pagination due to the subquery. For very large tables with composite PKs, consider:
1. Adding a surrogate integer PK column
2. Reducing partition count to minimize overhead

### Memory Issues
If partitions are still too large:
1. Decrease `chunk_size` parameter (default: 200,000)
2. Increase partition count (up to MAX_PARTITIONS)

### Verifying Partition Balance
Check the partition info in Airflow logs:
```
Partition 1: 500,000 rows (min: 'a1b2...', max: 'c3d4...')
Partition 2: 500,000 rows (min: 'c3d5...', max: 'e5f6...')
```

All partitions should have approximately equal row counts.

## Implementation Details

### Key Files
- `dags/mssql_to_postgres_migration.py`: NTILE queries, partition logic
- `plugins/mssql_pg_migration/data_transfer.py`: ROW_NUMBER pagination, parallel readers
- `plugins/mssql_pg_migration/schema_extractor.py`: PK column detection

### NTILE Boundary Query
```python
boundaries_query = f"""
WITH numbered AS (
    SELECT [{pk_column}],
           NTILE({partition_count}) OVER (ORDER BY [{pk_column}]) as partition_id
    FROM [{schema}].[{table}]
)
SELECT partition_id,
       MIN([{pk_column}]) as min_pk,
       MAX([{pk_column}]) as max_pk,
       COUNT(*) as row_count
FROM numbered
GROUP BY partition_id
ORDER BY partition_id
"""
```

### ROW_NUMBER Read Method
```python
def _read_chunk_row_number(self, conn, schema, table, columns,
                           order_by_columns, start_row, end_row):
    query = f"""
    SELECT {columns} FROM (
        SELECT *, ROW_NUMBER() OVER (ORDER BY {order_by}) as _rn
        FROM [{schema}].[{table}] WITH (NOLOCK)
    ) sub
    WHERE _rn BETWEEN %s AND %s
    """
```
