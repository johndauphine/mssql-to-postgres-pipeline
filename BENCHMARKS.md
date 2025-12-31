# Performance Benchmarks

## Test Environment

- **Source:** MSSQL Server 2022 (Docker container)
- **Target:** PostgreSQL 15 (Docker container)
- **Dataset:** Stack Overflow 2010 (~19.3M rows, 9 tables)
- **Hardware:** Apple M3 Max MacBook Pro (36GB RAM, 14 cores), localhost to localhost
- **Test Date:** December 2025

## Dataset Details

| Table | Rows | Description |
|-------|------|-------------|
| Votes | 10,143,364 | Largest table |
| Comments | 3,905,844 | Text-heavy |
| Posts | 3,685,627 | Mixed types |
| Badges | 1,102,020 | Datetime columns |
| Users | 299,398 | Various types |
| PostLinks | 161,519 | Foreign keys |
| VoteTypes | 15 | Small lookup |
| PostTypes | 10 | Small lookup |
| LinkTypes | 2 | Small lookup |
| **Total** | **19,297,799** | |

## Results

### Full Migration (drop_recreate mode)

| Implementation | Duration | Throughput | vs Airflow |
|----------------|----------|------------|------------|
| Rust | 58.6s | 329,309 rows/sec | 1.74x faster |
| Go | 74.6s | 259,307 rows/sec | 1.37x faster |
| **Airflow (baseline)** | 102s | 189,320 rows/sec | - |

### Incremental/Upsert Mode

| Implementation | Duration | Throughput | vs Airflow Optimized |
|----------------|----------|------------|----------------------|
| **Airflow (optimized)** | 84s | 229,736 rows/sec | - |
| Rust | 96s | 200,657 rows/sec | 14% slower |
| Go | 142s | 135,717 rows/sec | 69% slower |
| Airflow (baseline) | 199s | 97,039 rows/sec | 137% slower |

## Optimizations Applied

These optimizations are applied to **both DAGs**:
- `mssql_to_postgres_migration` - Full migration DAG (drop_recreate/truncate modes)
- `mssql_to_postgres_incremental` - Incremental sync DAG (upsert mode)

The optimized Airflow pipeline achieves **58% improvement** over baseline (199s → 84s) through:

### 1. Binary COPY Format (`USE_BINARY_COPY=true`)

PostgreSQL binary protocol instead of CSV text format:
- ~20-30% throughput improvement
- Reduced serialization overhead
- More efficient type encoding

```bash
USE_BINARY_COPY=true docker compose up -d
```

### 2. Intra-Table Partitioning

Large tables (>1M rows) are split into parallel partitions:
- 6-way partitioning for tables exceeding threshold
- Each partition uses separate staging table
- Reduces lock contention during MERGE operations

```bash
PARTITION_THRESHOLD=1000000
MAX_PARTITIONS_PER_TABLE=6
```

### 3. Connection Pool Tuning

Optimized connection pools for parallel operations:

```bash
PARALLEL_READERS=4
MAX_MSSQL_CONNECTIONS=24
MAX_PG_CONNECTIONS=16
PG_POOL_MAXCONN=16
```

### 4. Batch Size Auto-Tuning

Dynamic chunk sizing based on table width:
- Narrow tables (≤5 columns): 2x base chunk size
- Medium tables (6-20 columns): 1-1.5x base chunk size
- Wide tables (40+ columns): 0.25x base chunk size

```bash
AUTO_TUNE_CHUNK_SIZE=true
```

## Key Findings

1. **Airflow beats Rust for incremental loads** - 14% faster due to parallel staging tables and 6-way intra-table partitioning

2. **Rust is fastest for full migrations** - 1.74x faster than Airflow baseline due to lower runtime overhead

3. **Binary COPY is essential** - Both optimized Airflow and Rust use PostgreSQL's binary COPY protocol

4. **Parallelism reduces lock contention** - Airflow's partition-based approach outperforms single-table upsert paths

## Running Benchmarks

Both DAGs use the same optimizations when enabled via environment variables.

### Full Migration (Optimized)

```bash
# Run with all optimizations enabled
USE_BINARY_COPY=true \
PARALLEL_READERS=4 \
MAX_MSSQL_CONNECTIONS=24 \
docker compose up -d

# Trigger mssql_to_postgres_migration DAG via Airflow UI
```

### Incremental Migration (Optimized)

```bash
# Run with all optimizations enabled (includes partitioning for large tables)
USE_BINARY_COPY=true \
PARTITION_THRESHOLD=1000000 \
MAX_PARTITIONS_PER_TABLE=6 \
PARALLEL_READERS=4 \
MAX_MSSQL_CONNECTIONS=24 \
docker compose up -d

# Trigger mssql_to_postgres_incremental DAG via Airflow UI
```

## Configuration Reference

| Variable | Default | Description |
|----------|---------|-------------|
| `USE_BINARY_COPY` | `true` | Use PostgreSQL binary COPY format |
| `PARTITION_THRESHOLD` | `1000000` | Row count threshold for partitioning |
| `MAX_PARTITIONS_PER_TABLE` | `6` | Maximum partitions per large table |
| `PARALLEL_READERS` | `4` | Concurrent MSSQL readers per table |
| `MAX_MSSQL_CONNECTIONS` | `24` | MSSQL connection pool size |
| `MAX_PG_CONNECTIONS` | `16` | PostgreSQL connection pool size |
| `AUTO_TUNE_CHUNK_SIZE` | `true` | Dynamic batch sizing |
| `READER_QUEUE_SIZE` | `8` | Read-ahead queue depth |
