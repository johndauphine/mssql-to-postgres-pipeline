# SQL Server to PostgreSQL Migration Pipeline

An Apache Airflow 3.0 pipeline for automated, full-refresh migrations from Microsoft SQL Server to PostgreSQL. Uses Docker Compose with LocalExecutor for reliable orchestration and flexible deployment.

## Features

- **Schema Discovery**: Automatically extract table structures, columns, indexes, and foreign keys from SQL Server
- **Type Mapping**: Convert 30+ SQL Server data types to their PostgreSQL equivalents
- **Streaming Data Transfer**: Move data efficiently using server-side cursors, keyset pagination, and PostgreSQL's COPY protocol
- **Validation**: Standalone validation DAG verifies migration success through row count comparisons
- **Parallelization**: Transfer multiple tables concurrently using Airflow's dynamic task mapping
- **Large Table Partitioning**: Automatically partitions tables >5M rows into parallel chunks by primary key range

## Performance

Tested against the StackOverflow 2010 dataset:

| Metric | Value |
|--------|-------|
| Total Rows Migrated | 19.3 million |
| Tables | 9 |
| Migration Time | ~2.5 minutes |
| Throughput | ~125,000 rows/sec |
| Validation Success | 100% (9/9 tables) |

### Performance Optimizations

- **100k chunk size**: 10x larger batches reduce overhead
- **Parallel partitioning**: Large tables split into 4 parallel partitions by PK range
- **Connection pooling**: Reuses PostgreSQL connections across operations

See [docs/PARALLEL_PARTITIONING.md](docs/PARALLEL_PARTITIONING.md) for details on large table partitioning.

### Tables Migrated

| Table | Rows |
|-------|------|
| Badges | 1,102,019 |
| Comments | 3,875,183 |
| Posts | 3,729,195 |
| Users | 299,398 |
| Votes | 10,143,364 |
| PostTypes | 8 |
| VoteTypes | 15 |
| PostLinks | 149,313 |
| LinkTypes | 3 |

## How It Works

The pipeline executes as a single Airflow DAG with the following stages:

```
Extract Schema -> Create Target Schema -> Create Tables -> Transfer Data (parallel) -> Create Foreign Keys -> Validate -> Report
```

1. **Schema Extraction**: Queries SQL Server system catalogs to discover all tables, columns, data types, indexes, and constraints
2. **DDL Generation**: Converts SQL Server schemas to PostgreSQL-compatible DDL with proper type mappings
3. **Table Creation**: Creates target tables in PostgreSQL (drops existing tables first)
4. **Data Transfer**: Streams data using keyset pagination with direct pymssql connections and PostgreSQL COPY protocol
5. **Foreign Key Creation**: Adds foreign key constraints after all data is loaded
6. **Validation**: Triggers standalone validation DAG that compares source and target row counts

## Architecture

### Data Transfer Approach

The pipeline uses a streaming architecture optimized for large datasets:

- **Keyset Pagination**: Uses primary key ordering instead of OFFSET/FETCH for efficient chunking of large tables
- **Direct Database Connections**: Uses pymssql and psycopg2 directly to avoid Airflow hook limitations with large datasets
- **PostgreSQL COPY Protocol**: Bulk loads data for maximum throughput
- **Server-Side Cursors**: Streams rows without loading entire result sets into memory

### Validation DAG

A standalone `validate_migration_env` DAG handles validation separately to avoid XCom serialization issues with large result sets. This DAG:
- Uses direct database connections (psycopg2, pymssql)
- Compares row counts between source and target for all tables
- Can be triggered independently for ad-hoc validation

## Supported Data Types

| Category | SQL Server Types | PostgreSQL Mapping |
|----------|-----------------|-------------------|
| Integer | `bit`, `tinyint`, `smallint`, `int`, `bigint` | `BOOLEAN`, `SMALLINT`, `INTEGER`, `BIGINT` |
| Decimal | `decimal`, `numeric`, `money`, `smallmoney` | `DECIMAL`, `NUMERIC` |
| Float | `float`, `real` | `DOUBLE PRECISION`, `REAL` |
| String | `char`, `varchar`, `text`, `nchar`, `nvarchar`, `ntext` | `CHAR`, `VARCHAR`, `TEXT` |
| Binary | `binary`, `varbinary`, `image` | `BYTEA` |
| Date/Time | `date`, `time`, `datetime`, `datetime2`, `smalldatetime`, `datetimeoffset` | `DATE`, `TIME`, `TIMESTAMP` |
| Other | `uniqueidentifier`, `xml`, `geography`, `geometry` | `UUID`, `XML`, `GEOGRAPHY`, `GEOMETRY` |

## Quick Start

Migrate any SQL Server database to PostgreSQL in 3 steps:

```bash
# 1. Configure your databases
cp .env.example .env
# Edit .env to set connection strings (AIRFLOW_CONN_MSSQL_SOURCE, AIRFLOW_CONN_POSTGRES_TARGET)

# 2. Start the stack
docker-compose up -d

# 3. Run migration (UI: http://localhost:8080, login: airflow/airflow)
docker exec airflow-scheduler airflow dags trigger mssql_to_postgres_migration
```

See [Configuration](#configuration) for details on connecting to remote databases.

## Getting Started

### Prerequisites

- Docker Desktop (16GB+ RAM recommended for full stack)
- Docker Compose (included with Docker Desktop)
- Access to source SQL Server and target PostgreSQL databases

### Installation

1. Clone this repository:
   ```bash
   git clone <repository-url>
   cd mssql-to-postgres-pipeline
   ```

2. Configure your environment:
   ```bash
   cp .env.example .env
   # Edit .env with:
   #   - Resource settings for your machine (RAM presets: 16GB/32GB/64GB)
   #   - Database connection strings (AIRFLOW_CONN_MSSQL_SOURCE, AIRFLOW_CONN_POSTGRES_TARGET)
   ```

3. Start the full stack (Airflow + databases):
   ```bash
   docker-compose up -d
   ```

   This starts 7 containers:
   - `airflow-webserver` - Web UI and API server
   - `airflow-scheduler` - Task orchestration
   - `airflow-dag-processor` - DAG parsing
   - `airflow-triggerer` - Deferrable operators
   - `postgres-metadata` - Airflow metadata database
   - `mssql-server` - SQL Server 2022 (source - optional for testing)
   - `postgres-target` - PostgreSQL 16 (target - optional for testing)

   > **Note**: For remote databases (Azure SQL, AWS RDS, on-prem), configure connection strings in `.env` and skip local database containers.

4. Access the Airflow UI at http://localhost:8080 (username: `airflow`, password: `airflow`)

## Configuration

Configuration is managed through the `.env` file which contains:
- Resource settings (memory limits, parallelism)
- Database connection strings

### Database Connections

Edit `.env` to configure your source and target databases using Airflow connection URIs:

```bash
# SOURCE: SQL Server
AIRFLOW_CONN_MSSQL_SOURCE='mssql://sa:YourStrong@Passw0rd@mssql-server:1433/StackOverflow2010'
# Examples for remote databases:
# AIRFLOW_CONN_MSSQL_SOURCE='mssql://user:pass@192.168.1.100:1433/MyDatabase'
# AIRFLOW_CONN_MSSQL_SOURCE='mssql://user:pass@myserver.database.windows.net:1433/MyDatabase'

# TARGET: PostgreSQL
AIRFLOW_CONN_POSTGRES_TARGET='postgresql://postgres:PostgresPassword123@postgres-target:5432/stackoverflow'
# Examples for remote databases:
# AIRFLOW_CONN_POSTGRES_TARGET='postgresql://user:pass@mydb.abc123.rds.amazonaws.com:5432/mydb'
```

Connection string format: `<type>://<user>:<password>@<host>:<port>/<database>`

Connections are automatically loaded from environment variables when Airflow starts.

### Resource Settings

The `.env` file contains resource presets for different machine sizes:

| RAM | Preset |
|-----|--------|
| 16GB | Conservative settings for laptops |
| 32GB | Default balanced settings |
| 64GB+ | High performance settings |

Uncomment the appropriate preset section in `.env` for your machine.

### Applying Configuration Changes

After editing `.env`, restart Airflow services to reload configuration:

```bash
docker-compose restart airflow-scheduler airflow-webserver
```

### Loading Test Data (Local SQL Server)

To restore a SQL Server backup file (`.bak`) into the local container:

```bash
# 1. Copy backup file to container
docker cp /path/to/YourDatabase.bak mssql-server:/tmp/

# 2. Restore the database
docker exec -it mssql-server /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P 'YourStrong@Passw0rd' -C \
  -Q "RESTORE DATABASE YourDatabase FROM DISK='/tmp/YourDatabase.bak' \
      WITH MOVE 'YourDatabase' TO '/var/opt/mssql/data/YourDatabase.mdf', \
           MOVE 'YourDatabase_log' TO '/var/opt/mssql/data/YourDatabase_log.ldf'"

# 3. Update .env with the database name in connection string
# AIRFLOW_CONN_MSSQL_SOURCE='mssql://sa:YourStrong@Passw0rd@mssql-server:1433/YourDatabase'
```

> **Note**: Logical file names in the MOVE clause vary by backup. Use `RESTORE FILELISTONLY` to discover them.

## Usage

### Running the Migration

1. Open the Airflow UI at http://localhost:8080
2. Find the `mssql_to_postgres_migration` DAG
3. Click the play button to trigger with default parameters, or use "Trigger DAG w/ config" to customize

### Configuration Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `source_conn_id` | `mssql_source` | Airflow connection ID for SQL Server |
| `target_conn_id` | `postgres_target` | Airflow connection ID for PostgreSQL |
| `source_schema` | `dbo` | Schema to migrate from SQL Server |
| `target_schema` | `public` | Target schema in PostgreSQL |
| `chunk_size` | `200000` | Rows per batch during transfer (100-500,000) |
| `exclude_tables` | `[]` | Table patterns to skip (supports wildcards) |
| `validate_samples` | `false` | Enable sample data validation (slower) |
| `create_foreign_keys` | `true` | Create foreign key constraints after transfer |

### Example: Trigger via CLI

```bash
docker exec airflow-scheduler airflow dags trigger mssql_to_postgres_migration
```

Or with custom configuration:
```bash
docker exec airflow-scheduler airflow dags trigger mssql_to_postgres_migration \
  --conf '{"source_schema": "sales", "target_schema": "sales", "chunk_size": 50000}'
```

### Monitoring

```bash
# View scheduler logs
docker-compose logs airflow-scheduler -f

# List DAG runs
docker exec airflow-scheduler airflow dags list-runs mssql_to_postgres_migration

# Check validation results
docker exec airflow-scheduler airflow dags list-runs validate_migration_env
```

## Project Structure

```
mssql-to-postgres-pipeline/
├── dags/
│   ├── mssql_to_postgres_migration.py   # Main migration DAG
│   └── validate_migration_env.py        # Standalone validation DAG
├── include/
│   └── mssql_pg_migration/
│       ├── schema_extractor.py          # SQL Server schema discovery
│       ├── type_mapping.py              # Data type conversion logic
│       ├── ddl_generator.py             # PostgreSQL DDL generation
│       ├── data_transfer.py             # Streaming data transfer with keyset pagination
│       └── validation.py                # Migration validation
├── tests/
│   └── dags/
│       └── test_dag_example.py          # DAG validation tests
├── docs/
│   ├── HANDOFF_NOTES.md                 # Session handoff documentation
│   └── *.md                             # Additional technical documentation
├── docker-compose.yml                   # Full stack (Airflow + databases)
├── Dockerfile                           # Apache Airflow 3.0 image
├── requirements.txt                     # Python dependencies
└── .env.example                         # Configuration template
```

## Development

### Validate DAGs

```bash
docker exec airflow-scheduler airflow dags list
```

### Run Tests

```bash
# Run tests inside scheduler container
docker exec airflow-scheduler pytest /opt/airflow/tests/
```

### View Logs

```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs airflow-scheduler -f
```

### Stop Airflow

```bash
docker-compose down
```

## Known Issues and Workarounds

### Airflow 3.0 Callback Limitation

Airflow 3.0 does not yet support task-level and DAG-level failure callbacks ([GitHub Issue #44354](https://github.com/apache/airflow/issues/44354)). As a temporary workaround:

- Failure callbacks are currently disabled in the migration DAG
- Notification system (Slack/email) is temporarily unavailable
- Will be re-enabled once Airflow implements callback support

### TEXT Column NULL Handling

SQL Server databases may have NULL values in columns marked as NOT NULL (data integrity issues). The DDL generator skips NOT NULL constraints for TEXT columns to handle this:

```python
# Skip NOT NULL for TEXT columns as source data may have integrity issues
if not column.get('is_nullable', True) and column['data_type'].upper() != 'TEXT':
    parts.append('NOT NULL')
```

### XCom Serialization

Large validation results can cause XCom serialization issues. The pipeline uses a separate validation DAG with direct database connections to avoid this.

## Dependencies

- Apache Airflow 3.0.0 (vanilla)
- apache-airflow-providers-microsoft-mssql >= 3.8.0
- apache-airflow-providers-postgres >= 5.12.0
- pymssql >= 2.2.0
- psycopg2-binary >= 2.9.9
- FreeTDS development libraries (for SQL Server connectivity)

## License

See LICENSE file for details.
