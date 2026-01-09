# Auto-Tuning Process

The pipeline implements automatic tuning at multiple levels to optimize performance based on table characteristics without manual configuration.

---

## 1. Dynamic Partition Count Selection

**Location**: `dags/mssql_to_postgres_migration.py:71-79`

```python
def get_partition_count(row_count: int) -> int:
    """Calculate optimal partition count based on table size."""
    max_partitions = int(os.environ.get('MAX_PARTITIONS', '8'))

    if row_count < 2_000_000:
        return min(2, max_partitions)      # 2 partitions
    elif row_count < 5_000_000:
        return min(4, max_partitions)      # 4 partitions
    else:
        return max_partitions              # 8 partitions (default max)
```

**Decision Logic**:
```
┌─────────────────────────────────────────────────────────────┐
│              Partition Count Selection                       │
├─────────────────┬───────────────┬───────────────────────────┤
│ Row Count       │ Partitions    │ Rationale                 │
├─────────────────┼───────────────┼───────────────────────────┤
│ < 1M rows       │ 1 (no split)  │ Overhead > benefit        │
│ 1M - 2M rows    │ 2             │ Minimal parallelism       │
│ 2M - 5M rows    │ 4             │ Moderate parallelism      │
│ > 5M rows       │ 8 (max)       │ Full parallelism          │
└─────────────────┴───────────────┴───────────────────────────┘
```

---

## 2. Chunk Size Auto-Tuning

**Location**: `plugins/mssql_pg_migration/data_transfer.py:1759-1773`

```python
def _calculate_optimal_chunk_size(self, row_count: int, requested_chunk: int) -> int:
    """Determine an appropriate chunk size based on table volume."""
    if row_count <= 0:
        return requested_chunk

    if row_count < 100_000:
        target = min(requested_chunk, 10_000)      # Small tables: small chunks
    elif row_count < 1_000_000:
        target = max(requested_chunk, 20_000)      # Medium tables: medium chunks
    elif row_count < 5_000_000:
        target = max(requested_chunk, 50_000)      # Large tables: larger chunks
    else:
        target = max(requested_chunk, 100_000)     # Very large: max chunks

    return min(max(target, 5_000), 200_000)        # Clamp to 5K-200K range
```

**Decision Logic**:
```
┌─────────────────────────────────────────────────────────────┐
│               Chunk Size Selection                           │
├─────────────────┬───────────────┬───────────────────────────┤
│ Row Count       │ Chunk Size    │ Rationale                 │
├─────────────────┼───────────────┼───────────────────────────┤
│ < 100K rows     │ <= 10,000     │ Avoid over-buffering      │
│ 100K - 1M rows  │ >= 20,000     │ Reduce round trips        │
│ 1M - 5M rows    │ >= 50,000     │ Batch efficiency          │
│ > 5M rows       │ >= 100,000    │ Maximum throughput        │
├─────────────────┴───────────────┴───────────────────────────┤
│ Hard limits: 5,000 minimum, 200,000 maximum                 │
└─────────────────────────────────────────────────────────────┘
```

---

## 3. Column-Width Based Chunk Tuning (Optional)

**Location**: `plugins/mssql_pg_migration/data_transfer.py:53-112`

When `AUTO_TUNE_CHUNK_SIZE=true`:

```python
def calculate_optimal_chunk_size(
    num_columns: int,
    avg_row_width_bytes: Optional[int] = None,
    base_chunk_size: int = 200000,
) -> int:
    """Adjust chunk size based on table width (column count)."""

    # Heuristic based on column count
    if num_columns <= 5:
        multiplier = 2.0      # Narrow table: 2x chunks
    elif num_columns <= 10:
        multiplier = 1.5      # Lean table: 1.5x chunks
    elif num_columns <= 20:
        multiplier = 1.0      # Medium table: base chunks
    elif num_columns <= 40:
        multiplier = 0.5      # Wide table: 0.5x chunks
    else:
        multiplier = 0.25     # Very wide: 0.25x chunks

    # If row width known, blend with width-based estimate
    if avg_row_width_bytes:
        target_chunk_bytes = 50 * 1024 * 1024  # Target 50MB per chunk
        width_based_chunk = target_chunk_bytes // avg_row_width_bytes
        optimal = int((base_chunk_size * multiplier + width_based_chunk) / 2)
    else:
        optimal = int(base_chunk_size * multiplier)

    return max(10000, min(500000, optimal))  # Clamp to bounds
```

**Decision Logic**:
```
┌─────────────────────────────────────────────────────────────┐
│           Column-Width Chunk Adjustment                      │
├─────────────────┬───────────────┬───────────────────────────┤
│ Column Count    │ Multiplier    │ Effective Chunk Size      │
├─────────────────┼───────────────┼───────────────────────────┤
│ <= 5 columns    │ 2.0x          │ 400,000 rows              │
│ 6-10 columns    │ 1.5x          │ 300,000 rows              │
│ 11-20 columns   │ 1.0x          │ 200,000 rows (base)       │
│ 21-40 columns   │ 0.5x          │ 100,000 rows              │
│ > 40 columns    │ 0.25x         │ 50,000 rows               │
└─────────────────┴───────────────┴───────────────────────────┘
```

---

## 4. Parallel Reader Auto-Selection

**Location**: `plugins/mssql_pg_migration/data_transfer.py:1131-1157`

```python
# Check if parallel readers are enabled
num_readers, queue_size = _get_parallel_reader_config()

if num_readers > 1 and total_expected >= chunk_size * 2:
    # Use parallel readers - enough rows to benefit
    logger.info(f"Using {num_readers} parallel readers")
    parallel_reader = ParallelReader(...)
else:
    # Sequential mode - too few rows for parallel overhead
    if num_readers > 1:
        logger.info(f"Row count {total_expected:,} too small for parallel readers")
```

**Decision Logic**:
```
┌─────────────────────────────────────────────────────────────┐
│           Parallel Reader Selection                          │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  PARALLEL_READERS > 1  AND  row_count >= chunk_size * 2     │
│           │                          │                       │
│           ▼                          ▼                       │
│     ┌──────────┐              ┌──────────┐                  │
│     │   YES    │              │    NO    │                  │
│     └────┬─────┘              └────┬─────┘                  │
│          │                         │                        │
│          ▼                         ▼                        │
│   Parallel Mode              Sequential Mode                │
│   (multi-threaded)           (single thread)                │
│                                                              │
│   Example: PARALLEL_READERS=4, chunk_size=200K              │
│   Threshold: 400K rows minimum for parallel mode            │
└─────────────────────────────────────────────────────────────┘
```

---

## 5. Pagination Mode Auto-Detection

**Location**: `plugins/mssql_pg_migration/data_transfer.py:1044-1061`

```python
# Auto-detect composite PKs and switch to ROW_NUMBER mode
pk_columns = self._get_primary_key_columns(source_schema, source_table, columns)
is_composite_pk = len(pk_columns) > 1

if is_composite_pk and not use_row_number:
    # Composite PK detected - must use ROW_NUMBER pagination
    use_row_number = True
    order_by_columns = pk_columns
    logger.info(f"Detected composite PK ({', '.join(pk_columns)}) - switching to ROW_NUMBER")
```

**Decision Logic**:
```
┌─────────────────────────────────────────────────────────────┐
│           Pagination Mode Selection                          │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│             Query Primary Key Columns                        │
│                       │                                      │
│           ┌───────────┴───────────┐                         │
│           ▼                       ▼                         │
│   Single Column PK         Composite PK                     │
│   (e.g., Id)               (e.g., tenant_id, order_id)     │
│           │                       │                         │
│           ▼                       ▼                         │
│   ┌──────────────┐        ┌──────────────┐                 │
│   │   Keyset     │        │  ROW_NUMBER  │                 │
│   │  Pagination  │        │  Pagination  │                 │
│   └──────────────┘        └──────────────┘                 │
│                                                              │
│   WHERE Id > @last     vs  WHERE _rn BETWEEN @s AND @e     │
│   (O(log n) per chunk)     (O(n) full scan required)       │
└─────────────────────────────────────────────────────────────┘
```

---

## 6. Summary: Auto-Tuning Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        Auto-Tuning Decision Tree                             │
│                                                                              │
│  ┌──────────────────┐                                                       │
│  │   Table Info     │                                                       │
│  │  - row_count     │                                                       │
│  │  - pk_columns    │                                                       │
│  │  - num_columns   │                                                       │
│  └────────┬─────────┘                                                       │
│           │                                                                  │
│           ▼                                                                  │
│  ┌──────────────────┐    row_count < 1M?    ┌──────────────────┐           │
│  │ Partitioning     │──────── YES ─────────►│  No Partitioning │           │
│  │ Decision         │                       │  (single task)   │           │
│  └────────┬─────────┘                       └──────────────────┘           │
│           │ NO                                                              │
│           ▼                                                                  │
│  ┌──────────────────┐                                                       │
│  │ Partition Count  │    2M-5M → 4 parts                                   │
│  │ Selection        │    >5M → 8 parts                                     │
│  └────────┬─────────┘                                                       │
│           │                                                                  │
│           ▼                                                                  │
│  ┌──────────────────┐    composite PK?      ┌──────────────────┐           │
│  │ Pagination Mode  │──────── YES ─────────►│  ROW_NUMBER      │           │
│  │ Selection        │                       │  Pagination      │           │
│  └────────┬─────────┘                       └──────────────────┘           │
│           │ NO                                                              │
│           ▼                                                                  │
│  ┌──────────────────┐                                                       │
│  │ Keyset Pagination│                                                       │
│  │ + NTILE bounds   │                                                       │
│  └────────┬─────────┘                                                       │
│           │                                                                  │
│           ▼                                                                  │
│  ┌──────────────────┐    rows > chunk*2?    ┌──────────────────┐           │
│  │ Parallel Reader  │──────── YES ─────────►│  Multi-threaded  │           │
│  │ Selection        │                       │  Readers         │           │
│  └────────┬─────────┘                       └──────────────────┘           │
│           │ NO                                                              │
│           ▼                                                                  │
│  ┌──────────────────┐                                                       │
│  │ Sequential Mode  │                                                       │
│  └────────┬─────────┘                                                       │
│           │                                                                  │
│           ▼                                                                  │
│  ┌──────────────────┐                                                       │
│  │ Chunk Size       │    <100K → 10K chunks                                │
│  │ Optimization     │    100K-1M → 20K chunks                              │
│  │                  │    1M-5M → 50K chunks                                │
│  │                  │    >5M → 100K chunks                                 │
│  └──────────────────┘                                                       │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Configuration Override Points

| Auto-Tuned Parameter | Environment Variable | Default | Effect |
|---------------------|---------------------|---------|--------|
| Max Partitions | `MAX_PARTITIONS` | 8 | Caps partition count |
| Parallel Readers | `PARALLEL_READERS` | 1 | Enables multi-threaded reads |
| Base Chunk Size | `DEFAULT_CHUNK_SIZE` | 200000 | Starting point for tuning |
| Large Table Threshold | (hardcoded) | 1,000,000 | Partition trigger point |
| Column-Width Tuning | `AUTO_TUNE_CHUNK_SIZE` | false | Enables width-based adjustment |
