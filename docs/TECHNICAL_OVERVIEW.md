# SQL Server to PostgreSQL Migration Pipeline
## Technical Deep Dive for Technical Audiences

---

## 1. System Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           Apache Airflow 3.0 Orchestration                       │
│  ┌────────────────┐  ┌────────────────┐  ┌────────────────┐  ┌────────────────┐ │
│  │   Webserver    │  │   Scheduler    │  │  DAG Processor │  │   Triggerer    │ │
│  └────────────────┘  └────────────────┘  └────────────────┘  └────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────────┘
                                        │
                    ┌───────────────────┼───────────────────┐
                    ▼                   ▼                   ▼
          ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
          │   Schema DAG    │  │  Migration DAG  │  │ Validation DAG  │
          │ (DDL Creation)  │  │ (Data Transfer) │  │ (Row Counts)    │
          └────────┬────────┘  └────────┬────────┘  └────────┬────────┘
                   │                    │                    │
                   └────────────────────┼────────────────────┘
                                        │
                    ┌───────────────────┴───────────────────┐
                    ▼                                       ▼
          ┌─────────────────┐                     ┌─────────────────┐
          │   SQL Server    │                     │   PostgreSQL    │
          │    (Source)     │                     │    (Target)     │
          │  ODBC Driver 18 │                     │    psycopg2     │
          └─────────────────┘                     └─────────────────┘
```

---

## 2. DAG Execution Flow

### Main Migration DAG (`mssql_to_postgres_migration`)

```
┌─────────────────────┐
│  check_skip_schema  │ ─── Branch decision
└──────────┬──────────┘
           │
     ┌─────┴─────┐
     ▼           ▼
┌─────────┐  ┌─────────┐
│ trigger │  │  skip   │
│ schema  │  │ schema  │
│   DAG   │  │  task   │
└────┬────┘  └────┬────┘
     └─────┬──────┘
           ▼
┌─────────────────────────┐
│ initialize_migration    │ ─── Creates _migration_state table
│        state            │     Handles recovery mode
└───────────┬─────────────┘
            ▼
┌─────────────────────────┐
│  discover_target_tables │ ─── Queries PostgreSQL information_schema
└───────────┬─────────────┘     Gets columns, PKs from target
            ▼
┌─────────────────────────┐
│  get_source_row_counts  │ ─── Queries sys.partitions for counts
└───────────┬─────────────┘     Gets PK column info for partitioning
            ▼
┌─────────────────────────┐
│   prepare_transfer_plan │ ─── NTILE boundaries for large tables
└───────────┬─────────────┘     ROW_NUMBER ranges for composite PKs
            │
     ┌──────┼──────┐
     ▼      ▼      ▼
┌────────┐ ┌────────────────────┐ ┌─────────────────────────┐
│regular │ │  first_partitions  │ │  remaining_partitions   │
│ tables │ │   (TRUNCATE=true)  │ │    (TRUNCATE=false)     │
└───┬────┘ └─────────┬──────────┘ └───────────┬─────────────┘
    │                │                        │
    │                │──────────────────────► │
    │                │    (dependency)        │
    └────────────────┼────────────────────────┘
                     ▼
          ┌─────────────────────┐
          │   collect_results   │ ─── Aggregates via XCom table
          └──────────┬──────────┘
                     ▼
          ┌─────────────────────┐
          │   reset_sequences   │ ─── setval(pg_get_serial_sequence())
          └──────────┬──────────┘
                     ▼
          ┌─────────────────────┐
          │ trigger_validation  │ ─── Triggers validate_migration_env
          └──────────┬──────────┘
                     ▼
          ┌─────────────────────┐
          │  generate_summary   │ ─── Logs stats, sends notifications
          └─────────────────────┘
```

---

## 3. Core Components

### 3.1 Schema Extraction (`schema_extractor.py`)

**Purpose**: Extract complete metadata from SQL Server system catalogs

**Key System Views Used**:
- `sys.tables` - Table metadata
- `sys.schemas` - Schema information
- `sys.columns` - Column definitions
- `sys.types` - Data type mappings
- `sys.indexes` / `sys.index_columns` - PK and index info
- `sys.partitions` - Row count estimates

**Schema Discovery Flow**:
```
┌───────────────────┐      ┌───────────────────┐
│  sys.tables +     │      │   sys.columns +   │
│  sys.schemas      │ ──►  │   sys.types       │
│                   │      │                   │
│  (table_name,     │      │  (column_name,    │
│   object_id,      │      │   data_type,      │
│   row_count)      │      │   max_length,     │
│                   │      │   precision,      │
│                   │      │   scale,          │
│                   │      │   is_nullable,    │
│                   │      │   is_identity)    │
└───────────────────┘      └───────────────────┘
           │                        │
           └──────────┬─────────────┘
                      ▼
              ┌───────────────────┐
              │  Complete Table   │
              │     Schema        │
              │                   │
              │  + primary_key    │
              │  + indexes        │
              │  + constraints    │
              └───────────────────┘
```

### 3.2 Type Mapping (`type_mapping.py`)

**30+ SQL Server Types Mapped**:

| Category | SQL Server | PostgreSQL |
|----------|-----------|------------|
| **Integer** | `bit` | `BOOLEAN` |
| | `tinyint`, `smallint` | `SMALLINT` |
| | `int` | `INTEGER` |
| | `bigint` | `BIGINT` |
| **Decimal** | `decimal(p,s)`, `numeric(p,s)` | `DECIMAL(p,s)`, `NUMERIC(p,s)` |
| | `money` | `NUMERIC(19,4)` |
| **Float** | `float` | `DOUBLE PRECISION` |
| | `real` | `REAL` |
| **String** | `char(n)`, `varchar(n)` | `CHAR(n)`, `VARCHAR(n)` |
| | `nchar(n)`, `nvarchar(n)` | `CHAR(n/2)`, `VARCHAR(n/2)` |
| | `text`, `ntext`, `varchar(max)` | `TEXT` |
| **Binary** | `binary`, `varbinary`, `image` | `BYTEA` |
| **DateTime** | `date` | `DATE` |
| | `time(p)` | `TIME(p)` |
| | `datetime`, `datetime2(p)` | `TIMESTAMP(p)` |
| | `datetimeoffset(p)` | `TIMESTAMP(p) WITH TIME ZONE` |
| **Special** | `uniqueidentifier` | `UUID` |
| | `xml` | `XML` |
| | `geography`, `geometry` | `GEOGRAPHY`, `GEOMETRY` |

**Identity Column Mapping**:
- `int IDENTITY` → `SERIAL`
- `bigint IDENTITY` → `BIGSERIAL`

### 3.3 Identifier Sanitization

```python
def sanitize_identifier(name: str) -> str:
    """
    SQL Server 'User Name'   → PostgreSQL 'user_name'
    SQL Server 'OrderID'     → PostgreSQL 'orderid'
    SQL Server 'First-Name'  → PostgreSQL 'first_name'
    SQL Server '123Column'   → PostgreSQL 'col_123column'
    """
```

**Target Schema Naming Convention**:
```
{alias}__{database}__{schema}

Examples:
  mssql-server → dev     + StackOverflow2010 + dbo → dev__stackoverflow2010__dbo
  sqlprod01    → prod    + SalesDB           + orders → prod__salesdb__orders
```

**Hostname Alias Configuration** (`config/hostname_alias.txt`):
```
# Format: hostname = alias
mssql-server = dev
sqlprod01 = prod_sales
sqlprod01.company.com = prod_sales
```

---

## 4. Data Transfer Architecture

### 4.1 Connection Pooling

```
┌─────────────────────────────────────────────────────────────────────┐
│                     DataTransfer Class                               │
│  ┌─────────────────────────────────────────────────────────────────┐│
│  │              Class-Level Shared Pools                            ││
│  │  ┌───────────────────────┐  ┌───────────────────────────────┐  ││
│  │  │  MssqlConnectionPool  │  │ PostgreSQL ThreadedConnPool   │  ││
│  │  │  (pyodbc)             │  │ (psycopg2)                    │  ││
│  │  │                       │  │                               │  ││
│  │  │  max_conn=12          │  │  max_conn=8                   │  ││
│  │  │  min_conn=3           │  │  min_conn=2                   │  ││
│  │  │  timeout=120s         │  │                               │  ││
│  │  │                       │  │                               │  ││
│  │  │  • acquire()          │  │  • getconn()                  │  ││
│  │  │  • release()          │  │  • putconn()                  │  ││
│  │  │  • validate()         │  │                               │  ││
│  │  └───────────────────────┘  └───────────────────────────────┘  ││
│  └─────────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────┘
```

### 4.2 Pagination Strategies

#### Keyset Pagination (Single-Column PKs)

```sql
-- Efficient: Uses index scan, O(log n) per chunk
SELECT [Id], [Name], [Email], [CreatedDate]
FROM [dbo].[Users] WITH (NOLOCK)
WHERE [Id] > @last_pk_value
ORDER BY [Id]
OFFSET 0 ROWS FETCH NEXT 200000 ROWS ONLY
```

**Advantages**:
- Constant time per chunk regardless of table position
- Uses clustered index efficiently
- No memory overhead from ROW_NUMBER

#### ROW_NUMBER Pagination (Composite PKs)

```sql
-- Required for composite PKs: tenant_id, order_id
SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (ORDER BY tenant_id, order_id) as _rn
    FROM [dbo].[Orders] WITH (NOLOCK)
) sub
WHERE _rn BETWEEN @start_row AND @end_row
```

**Trade-off**: Slower than keyset (requires full scan for ROW_NUMBER), but handles composite PKs

### 4.3 Large Table Partitioning (NTILE-Based)

**Problem**: Tables >5M rows need parallel transfer for acceptable performance

**Solution**: Use SQL Server's `NTILE()` to compute balanced partition boundaries

```sql
WITH numbered AS (
    SELECT [Id],
           NTILE(8) OVER (ORDER BY [Id]) as partition_id
    FROM [dbo].[Votes]
)
SELECT partition_id,
       MIN([Id]) as min_pk,
       MAX([Id]) as max_pk,
       COUNT(*) as row_count
FROM numbered
GROUP BY partition_id
ORDER BY partition_id
```

**Result**: 8 partitions with ~equal row counts:
```
Partition 1: min_pk=1,       max_pk=1268000,  rows=1,267,920
Partition 2: min_pk=1268001, max_pk=2535000,  rows=1,267,920
...
Partition 8: min_pk=8870001, max_pk=10143364, rows=1,267,923
```

**Why NTILE vs Arithmetic Division**:
```
Table with gaps: IDs 1-1000, 10001-11000, 20001-21000

Arithmetic (pk_range / n):   NTILE (row_count / n):
├─ Part 1: 1-5000 (99%)     ├─ Part 1: 1-1000 (33%)
├─ Part 2: 5001-10000 (0%)  ├─ Part 2: 10001-10500 (33%)
├─ Part 3: 10001-15000 (1%) ├─ Part 3: 10501-11000 (17%)
└─ Part 4: 15001-21000 (0%) └─ Part 4: 20001-21000 (17%)
         UNBALANCED                    BALANCED
```

### 4.4 Parallel Reader Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                    Parallel Reader Pipeline                          │
│                                                                      │
│  ┌────────────────┐  ┌────────────────┐  ┌────────────────┐         │
│  │ Reader Thread 1│  │ Reader Thread 2│  │ Reader Thread 3│         │
│  │ (MSSQL Pool)   │  │ (MSSQL Pool)   │  │ (MSSQL Pool)   │         │
│  │                │  │                │  │                │         │
│  │ pk: 1-250K     │  │ pk: 250K-500K  │  │ pk: 500K-750K  │         │
│  └───────┬────────┘  └───────┬────────┘  └───────┬────────┘         │
│          │                   │                   │                   │
│          └───────────────────┼───────────────────┘                   │
│                              ▼                                       │
│                   ┌──────────────────────┐                          │
│                   │   Bounded Queue      │                          │
│                   │   (READER_QUEUE_SIZE)│ ◄── Backpressure         │
│                   │                      │                          │
│                   │   [chunk][chunk]...  │                          │
│                   └──────────┬───────────┘                          │
│                              │                                       │
│                              ▼                                       │
│                   ┌──────────────────────┐                          │
│                   │   Writer Thread      │                          │
│                   │   (PostgreSQL Pool)  │                          │
│                   │                      │                          │
│                   │   COPY Protocol      │                          │
│                   └──────────────────────┘                          │
└─────────────────────────────────────────────────────────────────────┘
```

**Configuration**:
```bash
PARALLEL_READERS=4        # Reader threads per table transfer
READER_QUEUE_SIZE=5       # Max chunks buffered (backpressure)
MAX_MSSQL_CONNECTIONS=12  # Total SQL Server connections
MAX_PG_CONNECTIONS=8      # Total PostgreSQL connections
```

### 4.5 PostgreSQL COPY Protocol

```python
def _write_chunk(self, rows, target_schema, target_table, columns, conn):
    """Bulk load using PostgreSQL COPY protocol."""

    # Build CSV in-memory
    buffer = StringIO()
    writer = csv.writer(buffer, quoting=csv.QUOTE_MINIMAL)

    for row in rows:
        # Convert Python types to PostgreSQL-compatible strings
        converted = [self._convert_value(v) for v in row]
        writer.writerow(converted)

    buffer.seek(0)

    # COPY FROM STDIN - bypasses INSERT overhead
    with conn.cursor() as cursor:
        cursor.copy_expert(
            f"COPY {target_schema}.{target_table} ({columns}) FROM STDIN WITH CSV",
            buffer
        )
```

**Why COPY vs INSERT**:
- `INSERT`: ~10,000 rows/sec (per-row overhead, WAL per row)
- `COPY`: ~200,000+ rows/sec (bulk protocol, batched WAL)

---

## 5. Parallelism Configuration

### Environment Variables Explained

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        Airflow Scheduler                                         │
│                    PARALLELISM=64 (global cap)                                   │
│  ┌───────────────────────────────────────────────────────────────────────────┐  │
│  │                       Migration DAG Run                                    │  │
│  │                  MAX_PARALLEL_TRANSFERS=8                                  │  │
│  │  ┌───────────┐ ┌───────────┐ ┌───────────┐         ┌───────────┐         │  │
│  │  │  Table 1  │ │  Table 2  │ │  Table 3  │   ...   │  Table 8  │         │  │
│  │  │   Task    │ │   Task    │ │   Task    │         │   Task    │         │  │
│  │  │┌─────────┐│ │┌─────────┐│ │┌─────────┐│         │┌─────────┐│         │  │
│  │  ││Reader 1 ││ ││Reader 1 ││ ││Reader 1 ││         ││Reader 1 ││         │  │
│  │  ││Reader 2 ││ ││Reader 2 ││ ││Reader 2 ││         ││Reader 2 ││         │  │
│  │  ││Reader 3 ││ ││Reader 3 ││ ││Reader 3 ││         ││Reader 3 ││         │  │
│  │  ││Reader 4 ││ ││Reader 4 ││ ││Reader 4 ││         ││Reader 4 ││         │  │
│  │  │└─────────┘│ │└─────────┘│ │└─────────┘│         │└─────────┘│         │  │
│  │  │PARALLEL_  │ │PARALLEL_  │ │PARALLEL_  │         │PARALLEL_  │         │  │
│  │  │READERS=4  │ │READERS=4  │ │READERS=4  │         │READERS=4  │         │  │
│  │  └───────────┘ └───────────┘ └───────────┘         └───────────┘         │  │
│  └───────────────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────────┘

Total SQL Server Connections: MAX_PARALLEL_TRANSFERS × PARALLEL_READERS = 8 × 4 = 32
Total PostgreSQL Connections: MAX_PARALLEL_TRANSFERS × 1 = 8
```

| Variable | Scope | Description |
|----------|-------|-------------|
| `AIRFLOW__CORE__PARALLELISM` | Global | Max concurrent Airflow tasks across ALL DAGs |
| `MAX_PARALLEL_TRANSFERS` | DAG | Max concurrent table transfers in migration DAG |
| `PARALLEL_READERS` | Task | Reader threads per table transfer task |
| `MAX_MSSQL_CONNECTIONS` | System | SQL Server connection pool ceiling |
| `MAX_PG_CONNECTIONS` | System | PostgreSQL connection pool ceiling |

---

## 6. State Management & Restartability

### Migration State Table (`_migration_state`)

```sql
CREATE TABLE _migration_state (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(255),
    source_schema VARCHAR(255),
    target_schema VARCHAR(255),
    status VARCHAR(50),          -- pending, running, completed, failed
    migration_type VARCHAR(50),  -- full, incremental
    dag_run_id VARCHAR(255),
    source_row_count BIGINT,
    rows_inserted BIGINT,
    rows_updated BIGINT,
    partition_info JSONB,        -- For partitioned tables
    error_message TEXT,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    last_pk_synced TEXT          -- Checkpoint for mid-partition resume
);
```

### Recovery Mode Logic

```python
# Check for REAL failures BEFORE zombie cleanup
pre_cleanup_summary = state_mgr.get_migration_summary(migration_type='full')
has_real_failures = pre_cleanup_summary['failed']['count'] > 0

# Reset zombie 'running' states from crashed DAG runs
zombies = state_mgr.reset_stale_running_states(
    current_dag_run_id=dag_run_id,
    migration_type='full'
)

if has_real_failures:
    # RECOVERY MODE: Keep completed tables, retry failed
    logger.info("RECOVERY MODE - Resuming from failure")
    # completed tables → SKIP
    # failed tables → RETRY
else:
    # FRESH RUN: Reset all state for full reload
    state_mgr.reset_all_state(migration_type='full')
```

### Partition-Level Checkpoints

```json
{
  "total_partitions": 4,
  "partitions": [
    {"id": 0, "status": "completed", "rows": 500000, "min_pk": 1, "max_pk": 500000},
    {"id": 1, "status": "completed", "rows": 500000, "min_pk": 500001, "max_pk": 1000000},
    {"id": 2, "status": "failed", "rows": 250000, "min_pk": 1000001, "max_pk": 1500000, "last_pk_synced": 1250000},
    {"id": 3, "status": "pending", "min_pk": 1500001, "max_pk": 2000000}
  ]
}
```

On restart:
- Partitions 0, 1: SKIP (completed)
- Partition 2: RESUME from `last_pk_synced=1250000`
- Partition 3: START from beginning

---

## 7. Incremental Sync (Staging Table Pattern)

```
┌───────────────────────────────────────────────────────────────────────────────┐
│                       Incremental Sync Flow                                    │
│                                                                                │
│  Step 1: COPY to UNLOGGED Staging Table                                       │
│  ┌─────────────────┐                      ┌─────────────────────────────────┐ │
│  │   SQL Server    │  ──COPY FROM STDIN──►│   staging_users (UNLOGGED)      │ │
│  │   [Users]       │                      │   - No WAL overhead             │ │
│  │   2.4M rows     │                      │   - Fast bulk insert            │ │
│  └─────────────────┘                      └─────────────────────────────────┘ │
│                                                                                │
│  Step 2: Smart Upsert with IS DISTINCT FROM                                   │
│  ┌─────────────────────────────────────────────────────────────────────────┐  │
│  │  INSERT INTO users (id, name, email, ...)                                │  │
│  │  SELECT id, name, email, ... FROM staging_users                          │  │
│  │  ON CONFLICT (id) DO UPDATE SET                                          │  │
│  │      name = EXCLUDED.name,                                               │  │
│  │      email = EXCLUDED.email,                                             │  │
│  │      ...                                                                 │  │
│  │  WHERE users.name IS DISTINCT FROM EXCLUDED.name                         │  │
│  │     OR users.email IS DISTINCT FROM EXCLUDED.email                       │  │
│  │     OR ...                                                               │  │
│  └─────────────────────────────────────────────────────────────────────────┘  │
│                                                                                │
│  Step 3: Drop Staging Table                                                   │
│  DROP TABLE staging_users;                                                    │
│                                                                                │
└───────────────────────────────────────────────────────────────────────────────┘
```

**Why `IS DISTINCT FROM`**:
- NULL-safe comparison (unlike `!=`)
- PostgreSQL evaluates lazily - skips UPDATE if no columns changed
- No pre-hashing required on application side

**Performance**: ~65,000 rows/sec (vs ~10,000 rows/sec for hash-based comparison)

---

## 8. Performance Benchmarks

### Full Migration (StackOverflow 2010 - 19.3M rows)

| Metric | Value |
|--------|-------|
| Total Rows | 19,338,498 |
| Tables | 9 |
| Migration Time | ~2.5 minutes |
| Throughput | ~125,000 rows/sec |
| Validation | 100% (9/9 tables) |

### Table Breakdown

| Table | Rows | Time | Rate |
|-------|------|------|------|
| Votes | 10,143,364 | ~45s | 225K/s |
| Comments | 3,875,183 | ~20s | 194K/s |
| Posts | 3,729,195 | ~20s | 186K/s |
| Badges | 1,102,019 | ~5s | 220K/s |
| Users | 299,398 | ~2s | 150K/s |

### Parallel Readers Impact

| PARALLEL_READERS | Time | vs Baseline |
|-----------------|------|-------------|
| 1 (sequential) | 3:57 | baseline |
| 2 (recommended) | 3:11 | **20% faster** |
| 3 | 3:31 | 11% faster |

---

## 9. Error Handling & Safety

### SQL Injection Prevention

```python
def validate_sql_identifier(identifier: str, identifier_type: str = "identifier") -> str:
    """Validate SQL identifiers to prevent injection."""
    if not identifier:
        raise ValueError(f"Invalid {identifier_type}: cannot be empty")
    if len(identifier) > 128:
        raise ValueError(f"Invalid {identifier_type}: exceeds maximum length")
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', identifier):
        raise ValueError(f"Invalid {identifier_type} '{identifier}'")
    return identifier
```

### Consistency Mode

```bash
# Default: NOLOCK hints for faster reads (may have phantom reads under concurrent writes)
STRICT_CONSISTENCY=false

# Production: Disable NOLOCK for guaranteed consistency (slower)
STRICT_CONSISTENCY=true
```

### Idempotent Retries

- Partition transfers delete existing data before re-inserting
- State table prevents duplicate completed work
- Checkpoint resume handles mid-transfer failures

---

## 10. Deployment Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                          Docker Compose Stack                                    │
│                                                                                  │
│  ┌────────────────────────────────────────────────────────────────────────────┐ │
│  │                        Airflow Services                                     │ │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐   │ │
│  │  │  webserver   │  │  scheduler   │  │dag-processor │  │  triggerer   │   │ │
│  │  │  :8080       │  │              │  │              │  │              │   │ │
│  │  └──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘   │ │
│  └────────────────────────────────────────────────────────────────────────────┘ │
│                                          │                                       │
│  ┌────────────────────────────────────────────────────────────────────────────┐ │
│  │                        Database Services                                    │ │
│  │  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────────────┐ │ │
│  │  │ postgres-metadata│  │   mssql-server   │  │    postgres-target       │ │ │
│  │  │ (Airflow DB)     │  │   (Source)       │  │    (Migration Target)    │ │ │
│  │  │ :5432            │  │   :1433          │  │    :5433                 │ │ │
│  │  └──────────────────┘  └──────────────────┘  └──────────────────────────┘ │ │
│  └────────────────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 11. Key Takeaways

1. **NTILE-based partitioning** works with ANY primary key type and guarantees balanced partitions regardless of ID gaps

2. **Keyset pagination** provides O(log n) chunk reads vs O(n) for OFFSET/FETCH

3. **PostgreSQL COPY protocol** achieves 20x throughput vs individual INSERTs

4. **Connection pooling** prevents connection exhaustion under high parallelism

5. **State tracking** enables restartable migrations with partition-level checkpoints

6. **Hostname aliases** provide meaningful, consistent schema names across environments

7. **Identifier sanitization** ensures case-insensitive PostgreSQL compatibility
