# SQL Server to PostgreSQL Migration Pipeline

An Apache Airflow pipeline for automated, full-refresh migrations from Microsoft SQL Server to PostgreSQL. Built with the Astronomer framework for reliable orchestration and easy deployment.

## Goals

This project automates the complete migration of database schemas and data from SQL Server to PostgreSQL, handling:

- **Schema Discovery**: Automatically extract table structures, columns, indexes, and foreign keys from SQL Server
- **Type Mapping**: Convert 30+ SQL Server data types to their PostgreSQL equivalents
- **Data Transfer**: Move data efficiently using chunked transfers and PostgreSQL's COPY protocol
- **Validation**: Verify migration success through row count comparisons and optional data sampling
- **Parallelization**: Transfer multiple tables concurrently using Airflow's dynamic task mapping

## How It Works

The pipeline executes as a single Airflow DAG with the following stages:

```
Extract Schema ─→ Create Target Schema ─→ Create Tables ─→ Transfer Data (parallel) ─→ Create Foreign Keys ─→ Validate ─→ Report
```

1. **Schema Extraction**: Queries SQL Server system catalogs to discover all tables, columns, data types, indexes, and constraints
2. **DDL Generation**: Converts SQL Server schemas to PostgreSQL-compatible DDL with proper type mappings
3. **Table Creation**: Creates target tables in PostgreSQL (drops existing tables first)
4. **Data Transfer**: Transfers data in configurable chunks using the COPY protocol for performance
5. **Foreign Key Creation**: Adds foreign key constraints after all data is loaded
6. **Validation**: Compares source and target row counts, generates a migration report

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

- Docker Desktop
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

Add connections in `airflow_settings.yaml` or through the Airflow UI:

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
airflow dags trigger mssql_to_postgres_migration \
  --conf '{"source_schema": "sales", "target_schema": "sales", "chunk_size": 50000}'
```

## Project Structure

```
mssql-to-postgres-pipeline/
├── dags/
│   └── mssql_to_postgres_migration.py   # Main migration DAG
├── include/
│   └── mssql_pg_migration/
│       ├── schema_extractor.py          # SQL Server schema discovery
│       ├── type_mapping.py              # Data type conversion logic
│       ├── ddl_generator.py             # PostgreSQL DDL generation
│       ├── data_transfer.py             # Chunked data transfer
│       └── validation.py                # Migration validation
├── tests/
│   └── dags/
│       └── test_dag_example.py          # DAG validation tests
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

## Dependencies

- Astronomer Runtime 3.1
- apache-airflow-providers-microsoft-mssql >= 3.8.0
- apache-airflow-providers-postgres >= 5.12.0
- pandas >= 2.0.0
- pyodbc >= 5.1.0
- pg8000 >= 1.30.0

## License

See LICENSE file for details.
