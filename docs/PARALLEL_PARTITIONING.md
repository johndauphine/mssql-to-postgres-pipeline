# Parallel Table Partitioning for Large Tables

## Overview

This document describes the automatic partitioning feature that parallelizes the transfer of large tables (>5M rows) by splitting them into multiple partitions based on primary key ranges.

## How It Works

### 1. Automatic Detection
Tables are automatically identified for partitioning based on row count:

```python
LARGE_TABLE_THRESHOLD = 5_000_000  # 5 million rows
PARTITION_COUNT = 4                 # Number of parallel partitions
```

Any table exceeding the threshold is automatically partitioned.

### 2. Primary Key Discovery
The system automatically discovers the primary key column:

```sql
SELECT COLUMN_NAME
FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + CONSTRAINT_NAME), 'IsPrimaryKey') = 1
AND TABLE_SCHEMA = 'dbo'
AND TABLE_NAME = 'TableName'
```

### 3. Range Calculation
Partition boundaries are calculated by querying the min/max PK values:

```sql
SELECT MIN([Id]), MAX([Id]) FROM [dbo].[TableName]
```

The ID range is then divided into equal chunks:
- **Partition 1**: `Id >= 1 AND Id <= 2,500,000`
- **Partition 2**: `Id >= 2,500,001 AND Id <= 5,000,000`
- **Partition 3**: `Id >= 5,000,001 AND Id <= 7,500,000`
- **Partition 4**: `Id >= 7,500,001` (catches all remaining)

### 4. Parallel Execution
Each partition runs as a separate Airflow task, executing in parallel:

```
transfer_partition[0] ──┐
transfer_partition[1] ──┼──► collect_all_results
transfer_partition[2] ──┤
transfer_partition[3] ──┘
```

## Configuration

### Adjusting Thresholds
Edit the constants in `dags/mssql_to_postgres_migration.py`:

```python
# Threshold for partitioning large tables (rows)
LARGE_TABLE_THRESHOLD = 5_000_000

# Number of partitions per large table
PARTITION_COUNT = 4
```

### Partition Count Recommendations
| Table Size | Recommended Partitions |
|------------|----------------------|
| 5-10M rows | 4 partitions |
| 10-50M rows | 8 partitions |
| 50M+ rows | 16 partitions |

## Performance Results

### Before Optimization
- Votes table (10.1M rows): ~115 seconds
- Total migration: ~3 minutes

### After Optimization
- Votes table (10.1M rows): ~50 seconds (4 partitions)
- Total migration: ~2.5 minutes
- **Improvement: ~25% faster overall**

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
        ▼                      ▼
transfer_table_data[0-7]  transfer_partition[0-3]
        │                      │
        └──────────┬───────────┘
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

### Partition Imbalance
If partitions have very unequal row counts (due to gaps in PK sequence), consider:
1. Using a different column for partitioning
2. Querying actual percentile values instead of min/max

### Memory Issues
If partitions are still too large:
1. Increase `PARTITION_COUNT`
2. Decrease `chunk_size` parameter

### Missing Primary Key
Tables without a primary key will be skipped for partitioning. Ensure all large tables have a suitable integer PK column.

## Future Improvements

1. **Adaptive partitioning**: Query actual row distribution for balanced partitions
2. **Configurable partition column**: Allow specifying partition column per table
3. **Dynamic partition count**: Adjust based on table size and available resources
