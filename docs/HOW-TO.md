# MSSQL to PostgreSQL Migration - Quick Reference

## Setup

```bash
# 1. Clone and configure
git clone <repo-url> && cd mssql-to-postgres-pipeline
cp .env.example .env

# 2. Edit .env with your connection strings
AIRFLOW_CONN_MSSQL_SOURCE='mssql://user:pass@host:1433/database'
AIRFLOW_CONN_POSTGRES_TARGET='postgresql://user:pass@host:5432/database'

# 3. Start stack
docker-compose up -d

# 4. Access UI: http://localhost:8080 (airflow/airflow)
```

## Run Migration

```bash
# Default (all tables in dbo schema)
docker exec airflow-scheduler airflow dags trigger mssql_to_postgres_migration

# Custom schema
docker exec airflow-scheduler airflow dags trigger mssql_to_postgres_migration \
  --conf '{"source_schema": "sales", "target_schema": "sales"}'

# Exclude tables
docker exec airflow-scheduler airflow dags trigger mssql_to_postgres_migration \
  --conf '{"exclude_tables": ["AuditLog", "Temp*"]}'

# With sample validation (slower, validates actual data)
docker exec airflow-scheduler airflow dags trigger mssql_to_postgres_migration \
  --conf '{"validate_samples": true}'
```

## Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `source_schema` | `dbo` | SQL Server source schema |
| `target_schema` | `public` | PostgreSQL target schema |
| `source_conn_id` | `mssql_source` | Airflow connection ID for SQL Server |
| `target_conn_id` | `postgres_target` | Airflow connection ID for PostgreSQL |
| `chunk_size` | `200000` | Rows per batch (tune for memory) |
| `exclude_tables` | `[]` | Tables to skip (supports wildcards) |
| `validate_samples` | `false` | Compare sample data, not just counts |
| `create_foreign_keys` | `true` | Create FKs after data load |

## Monitor Progress

```bash
# Watch scheduler logs
docker-compose logs airflow-scheduler -f

# Check DAG run status
docker exec airflow-scheduler airflow dags list-runs mssql_to_postgres_migration

# View validation results
docker exec airflow-scheduler airflow dags list-runs validate_migration_env
```

## Performance Tuning

### Environment Variables (.env)

```bash
# Parallelism
MAX_PARTITIONS=8              # Partitions per large table (default: 8)
MAX_PARALLEL_TRANSFERS=4      # Concurrent table transfers

# Chunk size (also configurable via DAG param)
DEFAULT_CHUNK_SIZE=200000     # Rows per batch
```

### Recommendations by Table Size

| Table Size | chunk_size | MAX_PARTITIONS |
|------------|------------|----------------|
| < 1M rows | 50,000 | N/A (no partitioning) |
| 1-5M rows | 100,000 | 2-4 |
| 5-50M rows | 200,000 | 4-8 |
| > 50M rows | 200,000 | 8 |

### Memory Presets (.env)

```bash
# 16GB RAM (conservative)
MAX_PARTITIONS=4
MAX_PARALLEL_TRANSFERS=2

# 32GB RAM (default)
MAX_PARTITIONS=8
MAX_PARALLEL_TRANSFERS=4

# 64GB+ RAM
MAX_PARTITIONS=8
MAX_PARALLEL_TRANSFERS=8
```

## Key Behaviors

### Pagination Strategy
- **Single-column PK**: Keyset pagination (fast, O(1) per page)
- **Composite PK**: ROW_NUMBER pagination (auto-detected)
- **No PK**: Falls back to first column

### Large Table Partitioning (>1M rows)
- Tables automatically partitioned using NTILE
- Works with any PK type (INT, GUID, VARCHAR)
- First partition truncates, remaining append

### Data Type Mappings
| SQL Server | PostgreSQL |
|------------|------------|
| `bit` | `BOOLEAN` |
| `datetime`, `datetime2` | `TIMESTAMP` |
| `uniqueidentifier` | `UUID` |
| `nvarchar(max)` | `TEXT` |
| `varbinary` | `BYTEA` |
| `money` | `DECIMAL(19,4)` |

## Troubleshooting

### Common Issues

```bash
# DAG stuck in "queued"
docker-compose restart airflow-scheduler airflow-dag-processor

# Connection errors
docker exec airflow-scheduler airflow connections get mssql_source
docker exec airflow-scheduler airflow connections get postgres_target

# Check for import errors
docker exec airflow-scheduler airflow dags list-import-errors

# View task logs
docker-compose logs airflow-scheduler -f | grep -i error
```

### Table Transfer Failed

1. Check task logs in Airflow UI
2. Common causes:
   - Composite PK without ROW_NUMBER (fixed in latest)
   - Data type conversion errors
   - NULL in NOT NULL columns
   - Network timeout on large tables

### Re-run Single Table

```bash
# Exclude all except target table
docker exec airflow-scheduler airflow dags trigger mssql_to_postgres_migration \
  --conf '{"exclude_tables": ["Table1", "Table2", "...all others..."]}'
```

## Validation

### Standalone Validation DAG

```bash
# Trigger validation independently
docker exec airflow-scheduler airflow dags trigger validate_migration_env \
  --conf '{"source_schema": "dbo", "target_schema": "public"}'
```

### Manual Row Count Check

```bash
# SQL Server
docker exec mssql-server /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa \
  -P 'YourPassword' -C -d YourDB -Q "SELECT COUNT(*) FROM dbo.YourTable"

# PostgreSQL
docker exec postgres-target psql -U postgres -d yourdb \
  -c "SELECT COUNT(*) FROM public.\"YourTable\""
```

## File Structure

```
dags/
  mssql_to_postgres_migration.py   # Main migration DAG
  validate_migration_env.py        # Standalone validation DAG
include/mssql_pg_migration/
  schema_extractor.py              # SQL Server schema discovery
  type_mapping.py                  # Data type conversion
  ddl_generator.py                 # PostgreSQL DDL generation
  data_transfer.py                 # Streaming data transfer
  validation.py                    # Row count/sample validation
```

## Remote Database Setup

For corporate/cloud databases (no local containers):

```bash
# .env - SQL Server (Kerberos auth)
AIRFLOW_CONN_MSSQL_SOURCE='generic://sqlserver.corp.com:1433/MyDatabase'

# .env - SQL Server (SQL auth)
AIRFLOW_CONN_MSSQL_SOURCE='mssql://user:pass@sqlserver.corp.com:1433/MyDatabase'

# .env - PostgreSQL (AWS RDS example)
AIRFLOW_CONN_POSTGRES_TARGET='postgresql://user:pass@mydb.abc123.rds.amazonaws.com:5432/mydb'
```

Then start only Airflow services:
```bash
docker-compose up -d airflow-webserver airflow-scheduler airflow-dag-processor airflow-triggerer postgres-metadata
```
