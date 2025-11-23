# SQL Server to PostgreSQL Migration Pipeline

An Apache Airflow pipeline for automated, full-refresh migrations from Microsoft SQL Server to PostgreSQL. Built with the Astronomer framework for reliable orchestration and easy deployment.

## Features

- **Schema Discovery**: Automatically extract table structures, columns, indexes, and foreign keys from SQL Server
- **Type Mapping**: Convert 30+ SQL Server data types to their PostgreSQL equivalents
- **Streaming Data Transfer**: Move data efficiently using server-side cursors, keyset pagination, and PostgreSQL's COPY protocol
- **Validation**: Standalone validation DAG verifies migration success through row count comparisons
- **Parallelization**: Transfer multiple tables concurrently using Airflow's dynamic task mapping

## Performance

Tested against the StackOverflow 2010 dataset:

| Metric | Value |
|--------|-------|
| Total Rows Migrated | 19.3 million |
| Tables | 9 |
| Migration Time | ~3 minutes |
| Validation Success | 100% (9/9 tables) |

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
- **Direct Database Connections**: Uses pymssql and pg8000 directly to avoid Airflow hook limitations with large datasets
- **PostgreSQL COPY Protocol**: Bulk loads data for maximum throughput
- **Server-Side Cursors**: Streams rows without loading entire result sets into memory

### Validation DAG

A standalone `validate_migration_env` DAG handles validation separately to avoid XCom serialization issues with large result sets. This DAG:
- Uses direct database connections (pg8000, pymssql)
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

## Getting Started

### Prerequisites

- Docker Desktop (4GB+ RAM recommended per database container)
- [Astronomer CLI](https://www.astronomer.io/docs/astro/cli/install-cli)
- Access to source SQL Server and target PostgreSQL databases

### Installation

1. Clone this repository:
   ```bash
   git clone <repository-url>
   cd mssql-to-postgres-pipeline
   ```

2. Start Airflow locally:
   ```bash
   astro dev start
   ```

3. Access the Airflow UI at http://localhost:8080

### Configure Database Connections

Add connections via the Airflow CLI or UI:

```bash
# SQL Server source
docker exec <scheduler-container> airflow connections add mssql_source \
  --conn-type mssql \
  --conn-host your-sqlserver-host \
  --conn-schema your_database \
  --conn-login username \
  --conn-password 'password' \
  --conn-port 1433

# PostgreSQL target
docker exec <scheduler-container> airflow connections add postgres_target \
  --conn-type postgres \
  --conn-host your-postgres-host \
  --conn-schema target_database \
  --conn-login username \
  --conn-password 'password' \
  --conn-port 5432
```

Or configure in `airflow_settings.yaml`:

```yaml
connections:
  - conn_id: mssql_source
    conn_type: mssql
    conn_host: your-sqlserver-host
    conn_schema: your_database
    conn_login: username
    conn_password: password
    conn_port: 1433

  - conn_id: postgres_target
    conn_type: postgres
    conn_host: your-postgres-host
    conn_schema: target_database
    conn_login: username
    conn_password: password
    conn_port: 5432
```

Then restart Airflow to load the connections:
```bash
astro dev restart
```

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
| `chunk_size` | `10000` | Rows per batch during transfer (100-100,000) |
| `exclude_tables` | `[]` | Table patterns to skip (supports wildcards) |
| `validate_samples` | `false` | Enable sample data validation (slower) |
| `create_foreign_keys` | `true` | Create foreign key constraints after transfer |

### Example: Trigger via CLI

```bash
astro dev run dags trigger mssql_to_postgres_migration
```

Or with custom configuration:
```bash
astro dev run dags trigger mssql_to_postgres_migration \
  --conf '{"source_schema": "sales", "target_schema": "sales", "chunk_size": 50000}'
```

### Monitoring

```bash
# View scheduler logs
astro dev logs -s -f

# List DAG runs
astro dev run dags list-runs mssql_to_postgres_migration

# Check validation results
astro dev run dags list-runs validate_migration_env
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
├── docker-compose.yml                   # Database containers (SQL Server, PostgreSQL)
├── Dockerfile                           # Astronomer Runtime image
├── requirements.txt                     # Python dependencies
└── airflow_settings.yaml                # Local connections/variables
```

## Development

### Validate DAGs

```bash
astro dev parse
```

### Run Tests

```bash
# Initialize Airflow DB first (required for DagBag tests)
astro dev run airflow db init

# Run tests
astro dev pytest tests/
```

### View Logs

```bash
astro dev logs
```

### Stop Airflow

```bash
astro dev stop
```

## Known Issues and Workarounds

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

- Astronomer Runtime 3.1
- apache-airflow-providers-microsoft-mssql >= 3.8.0
- apache-airflow-providers-postgres >= 5.12.0
- pymssql >= 2.2.0
- pg8000 >= 1.30.0

## License

See LICENSE file for details.
