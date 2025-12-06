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

## Configuration

### Environment Variables

```bash
# .env file
MAX_PARTITIONS=8        # Maximum partitions per table (default: 8)
```

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
                     convert_tables_to_logged
                               │
                               ▼
                     create_primary_keys
                               │
                               ▼
                     create_indexes
                               │
                               ▼
                     create_foreign_keys
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
1. Decrease `chunk_size` parameter (default: 100,000)
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
- `include/mssql_pg_migration/data_transfer.py`: ROW_NUMBER pagination
- `include/mssql_pg_migration/schema_extractor.py`: PK column detection

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
