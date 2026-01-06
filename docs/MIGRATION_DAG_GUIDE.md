# SQL Server to PostgreSQL Migration DAGs - A Guide for Junior Data Engineers

This document explains how the migration pipeline works, step-by-step. If you're new to data engineering or Apache Airflow, this guide will help you understand what happens when you trigger a migration.

## Table of Contents

1. [Overview](#overview)
2. [The Three DAGs](#the-three-dags)
3. [Schema DAG Deep Dive](#schema-dag-deep-dive)
4. [Migration DAG Deep Dive](#migration-dag-deep-dive)
5. [Validation DAG Deep Dive](#validation-dag-deep-dive)
6. [Key Concepts for Beginners](#key-concepts-for-beginners)
7. [Common Issues and Troubleshooting](#common-issues-and-troubleshooting)

---

## Overview

### What Does This Pipeline Do?

This pipeline moves data from **Microsoft SQL Server** to **PostgreSQL**. Think of it as a sophisticated copy operation that:

1. Reads the structure of tables in SQL Server (schema)
2. Creates matching tables in PostgreSQL
3. Copies all the data over
4. Verifies everything transferred correctly

### Why Use Airflow for This?

Airflow provides:
- **Orchestration**: Runs tasks in the correct order
- **Parallelism**: Transfers multiple tables simultaneously
- **Monitoring**: Visual progress tracking in the UI
- **Retry Logic**: Automatic retries if something fails
- **Logging**: Detailed logs for debugging

---

## The Three DAGs

The pipeline consists of three DAGs (Directed Acyclic Graphs) that work together:

```
┌─────────────────────────────────────────────────────────────────────┐
│                    MIGRATION DAG (Main Orchestrator)                 │
│                                                                      │
│  ┌──────────────┐     ┌──────────────┐     ┌──────────────────────┐ │
│  │  Trigger     │────>│   Transfer   │────>│  Trigger             │ │
│  │  Schema DAG  │     │   Data       │     │  Validation DAG      │ │
│  └──────────────┘     └──────────────┘     └──────────────────────┘ │
│         │                                            │               │
│         v                                            v               │
│  ┌──────────────┐                          ┌──────────────────────┐ │
│  │ SCHEMA DAG   │                          │  VALIDATION DAG      │ │
│  │ (Creates     │                          │  (Compares row       │ │
│  │  tables)     │                          │   counts)            │ │
│  └──────────────┘                          └──────────────────────┘ │
└─────────────────────────────────────────────────────────────────────┘
```

| DAG | Purpose | When It Runs |
|-----|---------|--------------|
| `mssql_to_postgres_schema` | Creates tables in PostgreSQL | Before data transfer |
| `mssql_to_postgres_migration` | Transfers all data | Main DAG you trigger |
| `validate_migration_env` | Verifies data integrity | After transfer completes |

---

## Schema DAG Deep Dive

**DAG ID**: `mssql_to_postgres_schema`

### Purpose
Creates the "container" (tables) in PostgreSQL before data is poured in.

### Step-by-Step Breakdown

#### Step 1: Extract Source Schema (`extract_source_schema`)

**What it does**: Reads the structure of tables from SQL Server.

```
SQL Server                          What We Extract
┌─────────────────────┐            ┌─────────────────────────────┐
│  dbo.Users          │     -->    │  - Column names             │
│  ├─ Id (int, PK)    │            │  - Data types               │
│  ├─ Name (varchar)  │            │  - Which column is the PK   │
│  └─ Email (varchar) │            │  - Nullable or not          │
└─────────────────────┘            └─────────────────────────────┘
```

**How it works**:
1. Parses the `include_tables` parameter (e.g., `["dbo.Users", "dbo.Posts"]`)
2. Queries SQL Server's `sys.tables`, `sys.columns`, `sys.indexes` system tables
3. Returns a list of table definitions with all column metadata

**Code location**: `plugins/mssql_pg_migration/schema_extractor.py`

#### Step 2: Create Target Schemas (`create_target_schemas`)

**What it does**: Creates PostgreSQL schemas (like folders for tables).

**Schema Naming Convention**:
```
Source: StackOverflow2010.dbo.Users
        ─────────────── ─── ─────
        Database        Schema  Table

Target: stackoverflow2010__dbo.Users
        ────────────────────── ─────
        PostgreSQL Schema      Table
```

The target schema name is: `{database}__{schema}` (lowercase, double underscore separator)

**Why lowercase?** PostgreSQL treats unquoted identifiers as lowercase. Using lowercase avoids quoting issues.

#### Step 3: Create Tables with PKs (`create_tables_with_pks`)

**What it does**: Generates and executes `CREATE TABLE` statements.

**Type Mapping** - SQL Server types are converted to PostgreSQL equivalents:

| SQL Server | PostgreSQL | Notes |
|------------|------------|-------|
| `INT` | `INTEGER` | Direct equivalent |
| `BIGINT` | `BIGINT` | Direct equivalent |
| `VARCHAR(50)` | `VARCHAR(50)` | Direct equivalent |
| `NVARCHAR(100)` | `VARCHAR(50)` | Unicode = 2 bytes, so length is halved |
| `DATETIME` | `TIMESTAMP(3)` | 3 = millisecond precision |
| `BIT` | `BOOLEAN` | 0/1 becomes true/false |
| `MONEY` | `NUMERIC(19,4)` | Preserves precision |
| `UNIQUEIDENTIFIER` | `UUID` | GUIDs |
| `INT IDENTITY` | `SERIAL` | Auto-increment |

**Code location**: `plugins/mssql_pg_migration/type_mapping.py`

**Generated DDL Example**:
```sql
DROP TABLE IF EXISTS "stackoverflow2010__dbo"."Users" CASCADE;

CREATE TABLE "stackoverflow2010__dbo"."Users" (
    "Id" SERIAL,
    "Reputation" INTEGER,
    "DisplayName" VARCHAR(40),
    "EmailHash" VARCHAR(40),
    "CreationDate" TIMESTAMP(3),
    CONSTRAINT "pk_Users" PRIMARY KEY ("Id")
);
```

---

## Migration DAG Deep Dive

**DAG ID**: `mssql_to_postgres_migration`

### The Complete Flow

```
┌────────────────────────────────────────────────────────────────────────────┐
│                         MIGRATION DAG TASK FLOW                             │
├────────────────────────────────────────────────────────────────────────────┤
│                                                                            │
│  1. check_skip_schema ──┬──> trigger_schema_dag ──┬                        │
│                         │                          │                        │
│                         └──> skip_schema_dag_task ─┘                        │
│                                      │                                      │
│                                      v                                      │
│  2. discover_target_tables                                                  │
│                                      │                                      │
│                                      v                                      │
│  3. get_source_row_counts                                                   │
│                                      │                                      │
│                                      v                                      │
│  4. prepare_transfer_plan                                                   │
│            │                                                                │
│            ├──────────────┬──────────────────┐                              │
│            v              v                  v                              │
│     get_regular    get_first_parts    get_remaining_parts                  │
│            │              │                  │                              │
│            v              v                  v                              │
│  5. transfer_table_data  transfer_first    transfer_remaining              │
│     (parallel x8)        (parallel x8)     (parallel x8)                   │
│            │              │                  │                              │
│            └──────────────┴──────────────────┘                              │
│                           │                                                 │
│                           v                                                 │
│  6. collect_results                                                         │
│                           │                                                 │
│                           v                                                 │
│  7. reset_sequences                                                         │
│                           │                                                 │
│                           v                                                 │
│  8. trigger_validation_dag                                                  │
│                           │                                                 │
│                           v                                                 │
│  9. generate_summary                                                        │
│                                                                            │
└────────────────────────────────────────────────────────────────────────────┘
```

### Detailed Step Breakdown

#### Step 1: Branch - Schema DAG (`check_skip_schema`)

**Purpose**: Decides whether to create tables or assume they exist.

- `skip_schema_dag=False` (default): Triggers schema DAG to create/recreate tables
- `skip_schema_dag=True`: Skips schema creation (useful for re-running failed transfers)

#### Step 2: Discover Target Tables (`discover_target_tables`)

**Purpose**: Queries PostgreSQL to find which tables exist and their structure.

**Why query the target?** The schema DAG just ran, so we query PostgreSQL to:
- Confirm tables were created
- Get the exact column list (in case of any transformations)
- Get primary key information for partitioning decisions

#### Step 3: Get Source Row Counts (`get_source_row_counts`)

**Purpose**: Counts rows in SQL Server to plan the transfer strategy.

**Why this matters**: Large tables (>1M rows) are handled differently than small tables.

```sql
-- This query gets row counts efficiently from SQL Server metadata
SELECT SUM(p.rows) as row_count
FROM sys.partitions p
INNER JOIN sys.tables t ON p.object_id = t.object_id
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE s.name = 'dbo' AND t.name = 'Users' AND p.index_id IN (0, 1)
```

#### Step 4: Prepare Transfer Plan (`prepare_transfer_plan`)

**Purpose**: Decides how to transfer each table based on its size.

**The Decision Tree**:

```
                    Table Size?
                        │
           ┌────────────┴────────────┐
           │                         │
      < 1M rows                  >= 1M rows
           │                         │
           v                         v
    Regular Transfer          Has Primary Key?
    (single task)                    │
                        ┌────────────┴────────────┐
                        │                         │
                       Yes                        No
                        │                         │
                        v                         v
               Partitioned Transfer        Regular Transfer
               (multiple parallel tasks)   (fallback)
```

**Partitioning Strategies**:

1. **Single Integer PK (NTILE)**: Divides the PK range into equal chunks
   ```sql
   -- Partition 1: WHERE Id >= 1 AND Id <= 500000
   -- Partition 2: WHERE Id >= 500001 AND Id <= 1000000
   -- etc.
   ```

2. **Composite PK (ROW_NUMBER)**: Uses row numbering for ranges
   ```sql
   -- Uses ROW_NUMBER() OVER (ORDER BY pk_columns)
   -- Then filters by row number ranges
   ```

**Partition count by table size**:
| Row Count | Partitions |
|-----------|------------|
| < 2M | 2 |
| 2M - 5M | 4 |
| > 5M | 8 (max) |

#### Step 5: Transfer Data (Parallel Tasks)

**Three task types run in parallel**:

1. **`transfer_table_data`**: Small tables (< 1M rows)
2. **`transfer_first_partitions`**: First partition of large tables (does TRUNCATE)
3. **`transfer_remaining_partitions`**: Other partitions (no TRUNCATE, just INSERT)

**Why separate first partition?**
- First partition TRUNCATEs the target table (clears old data)
- Remaining partitions just append
- This prevents data duplication if you re-run

**The Transfer Process**:

```
┌───────────────────────────────────────────────────────────────────────┐
│                    DATA TRANSFER PROCESS                               │
├───────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  SQL Server                           PostgreSQL                      │
│  ┌─────────────┐                      ┌─────────────┐                 │
│  │   Table     │                      │   Table     │                 │
│  │  (Source)   │                      │  (Target)   │                 │
│  └──────┬──────┘                      └──────▲──────┘                 │
│         │                                    │                        │
│         │  SELECT with keyset pagination     │  COPY FROM STDIN       │
│         │  (chunk_size rows at a time)       │  (binary format)       │
│         │                                    │                        │
│         v                                    │                        │
│  ┌─────────────────────────────────────────────────────────┐          │
│  │                   In-Memory Buffer                       │          │
│  │                                                         │          │
│  │   Row 1  │  Row 2  │  Row 3  │  ...  │  Row N          │          │
│  │                                                         │          │
│  │   Parallel Readers ──> Queue ──> PostgreSQL Writer      │          │
│  └─────────────────────────────────────────────────────────┘          │
│                                                                       │
└───────────────────────────────────────────────────────────────────────┘
```

**Key optimizations**:
- **Keyset pagination**: Uses `WHERE pk > last_pk` instead of `OFFSET` (faster)
- **Binary COPY**: Uses PostgreSQL's binary format (~20-30% faster than text)
- **Parallel readers**: Multiple threads read from SQL Server concurrently
- **UNLOGGED staging**: Temporary tables skip WAL logging for speed

#### Step 6: Collect Results (`collect_results`)

**Purpose**: Gathers results from all parallel transfer tasks.

**Technical Note (Airflow 3.0 Bug)**:
Normal `xcom_pull()` doesn't work for dynamically mapped tasks in Airflow 3.0.
We work around this by querying the XCom table directly:

```python
# Instead of: ti.xcom_pull(task_ids="transfer_table_data")
# We query: SELECT value FROM xcom WHERE task_id = 'transfer_table_data'
```

#### Step 7: Reset Sequences (`reset_sequences`)

**Purpose**: Fixes auto-increment counters after bulk loading.

**The Problem**:
```
-- If we bulk-loaded Users with max Id = 500000
-- But the sequence thinks the next value is 1
INSERT INTO Users (Name) VALUES ('New User');
-- ERROR: duplicate key value violates unique constraint
```

**The Solution**:
```sql
SELECT setval(
    pg_get_serial_sequence('"stackoverflow2010__dbo"."Users"', 'Id'),
    (SELECT MAX("Id") FROM "stackoverflow2010__dbo"."Users"),
    true  -- next nextval() returns MAX + 1
);
```

#### Step 8: Trigger Validation (`trigger_validation_dag`)

**Purpose**: Kicks off the validation DAG to verify data integrity.

#### Step 9: Generate Summary (`generate_summary`)

**Purpose**: Logs final statistics and sends notifications.

```
Migration completed: 9 tables, 19,310,706 rows in 118.5s (162,958 rows/sec)
```

---

## Validation DAG Deep Dive

**DAG ID**: `validate_migration_env`

### Purpose
Compares row counts between source and target to verify all data transferred.

### How It Works

```
┌─────────────────────────────────────────────────────────────────────┐
│                      VALIDATION PROCESS                              │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  For each table:                                                    │
│                                                                     │
│  SQL Server                        PostgreSQL                       │
│  ┌──────────────┐                 ┌──────────────┐                  │
│  │ SELECT COUNT │                 │ SELECT COUNT │                  │
│  │ FROM dbo.Users│                │ FROM schema.Users│              │
│  └──────┬───────┘                 └──────┬───────┘                  │
│         │                                │                          │
│         v                                v                          │
│    1,102,020                        1,102,020                       │
│         │                                │                          │
│         └───────────────┬────────────────┘                          │
│                         │                                           │
│                         v                                           │
│                   Compare Counts                                    │
│                         │                                           │
│              ┌──────────┴──────────┐                                │
│              │                     │                                │
│           Match                 Mismatch                            │
│              │                     │                                │
│              v                     v                                │
│            PASS                  FAIL                               │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### Sample Output

```
PASS dbo.Badges          | Source:  1,102,020 | Target:  1,102,020 | Diff:    +0
PASS dbo.Comments        | Source:  3,875,183 | Target:  3,875,183 | Diff:    +0
PASS dbo.Posts           | Source:  3,729,195 | Target:  3,729,195 | Diff:    +0
...
============================================================
VALIDATION SUMMARY
============================================================
Tables Checked: 9
Passed: 9
Failed: 0
Success Rate: 100.0%
============================================================
```

---

## Key Concepts for Beginners

### What is a Schema?

In database terms, "schema" has two meanings:

1. **Schema (structure)**: The definition of tables, columns, and their types
2. **Schema (namespace)**: A container/folder for tables (like `dbo` in SQL Server)

This pipeline uses both meanings:
- We extract the *structure* from SQL Server
- We create a *namespace* in PostgreSQL (`stackoverflow2010__dbo`)

### What is XCom?

XCom (cross-communication) is how Airflow tasks share data:

```python
# Task 1 pushes data
context["ti"].xcom_push(key="table_count", value=9)

# Task 2 pulls data
count = context["ti"].xcom_pull(task_ids="task1", key="table_count")
```

### What is Dynamic Task Mapping?

Instead of creating 9 separate tasks for 9 tables:

```python
# Without mapping (bad - hardcoded)
transfer_users = transfer_table(table="Users")
transfer_posts = transfer_table(table="Posts")
# ... 7 more

# With mapping (good - dynamic)
transfer_all = transfer_table.expand(table_info=get_tables())
# Creates N tasks based on how many tables exist
```

### What is COPY vs INSERT?

PostgreSQL's `COPY` command is optimized for bulk loading:

| Method | Speed | Use Case |
|--------|-------|----------|
| `INSERT INTO ... VALUES` | Slow | Small amounts of data |
| `COPY FROM STDIN` | Fast | Bulk loading millions of rows |

COPY bypasses many checks that INSERT performs, making it 10-100x faster.

---

## Common Issues and Troubleshooting

### Issue: "include_tables parameter is required"

**Cause**: No tables specified for migration.

**Fix**: Set the `INCLUDE_TABLES` environment variable:
```bash
INCLUDE_TABLES=dbo.Badges,dbo.Comments,dbo.Posts
```

### Issue: Tasks stuck in "queued" state

**Cause**: Airflow scheduler needs restart (Airflow 3.0 issue).

**Fix**:
```bash
docker-compose restart airflow-scheduler
```

### Issue: "relation does not exist" in reset_sequences

**Cause**: Case sensitivity in PostgreSQL identifiers.

**Explanation**: PostgreSQL lowercases unquoted identifiers:
- `Users` (unquoted) = `users` (lowercase)
- `"Users"` (quoted) = `Users` (preserved case)

Our tables are created with quoted names, so queries must also quote them.

### Issue: collect_results returns 0 tables

**Cause**: Airflow 3.0 bug with `xcom_pull()` for mapped tasks.

**Fix**: We query the XCom table directly (already implemented in the code).

### Issue: Sequence not reset (duplicate key errors)

**Cause**: `pg_get_serial_sequence()` requires exact case match.

**Fix**: Use fully quoted identifier format:
```sql
pg_get_serial_sequence('"schema"."table"', 'column')
```

---

## Environment Variables Reference

| Variable | Default | Description |
|----------|---------|-------------|
| `INCLUDE_TABLES` | (required) | Comma-separated list of tables to migrate |
| `MAX_PARTITIONS` | 8 | Maximum partitions for large tables |
| `MAX_PARALLEL_TRANSFERS` | 8 | Max concurrent transfer tasks |
| `DEFAULT_CHUNK_SIZE` | 200000 | Rows per chunk during transfer |
| `USE_BINARY_COPY` | true | Use PostgreSQL binary COPY format |
| `USE_UNLOGGED_STAGING` | true | Use UNLOGGED tables for staging |
| `PARALLEL_READERS` | 4 | Concurrent SQL Server reader threads |

---

## Further Reading

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [PostgreSQL COPY Command](https://www.postgresql.org/docs/current/sql-copy.html)
- [SQL Server System Tables](https://docs.microsoft.com/en-us/sql/relational-databases/system-catalog-views/sys-tables-transact-sql)

---

*Last updated: January 2026*
