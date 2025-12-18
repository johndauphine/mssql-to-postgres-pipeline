# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is an Apache Airflow 3.0 project for orchestrating data pipelines, specifically designed for ETL processes from Microsoft SQL Server to PostgreSQL. The project uses vanilla Airflow with Docker Compose and LocalExecutor for flexible local development and deployment.

## Common Development Commands

### Starting and Managing Airflow

```bash
# Start Airflow locally (spins up 7 Docker containers)
docker-compose up -d

# Stop all containers
docker-compose down

# Restart specific services
docker-compose restart airflow-scheduler airflow-webserver

# View container logs
docker-compose logs -f                    # All services
docker-compose logs airflow-scheduler -f  # Specific service

# Parse and validate DAGs
docker exec airflow-scheduler airflow dags list

# Run pytest tests
docker exec airflow-scheduler pytest /opt/airflow/tests/
```

### Running Tests

```bash
# Run all tests
docker exec airflow-scheduler pytest /opt/airflow/tests/

# Run specific test file
docker exec airflow-scheduler pytest /opt/airflow/tests/dags/test_dag_example.py

# Run DAG integrity check (validates imports)
docker exec airflow-scheduler airflow dags list
```

## Architecture and Code Structure

### DAG Development Pattern

This project uses Airflow 3.0's TaskFlow API (@task decorator) for writing DAGs. Key patterns:

1. **DAG Definition**: Use the @dag decorator with proper configuration:
```python
from airflow.decorators import dag, task
from airflow.datasets import Dataset
from pendulum import datetime

@dag(
    start_date=datetime(2025, 4, 22),
    schedule="@daily",
    default_args={"owner": "data-team", "retries": 3},
    tags=["example"],  # Required by tests
)
def my_dag():
    # DAG logic here
```

2. **Task Creation**: Use @task decorator for Python functions:
```python
@task
def extract_data(**context) -> dict:
    # Task implementation
    return data
```

3. **Dynamic Task Mapping**: For parallel processing of variable-sized data:
```python
process_task.partial(static_param="value").expand(
    dynamic_param=get_data_list()
)
```

4. **Data Assets**: Define outlets for downstream dependencies:
```python
@task(outlets=[Dataset("table_name")])
def update_table():
    # Task that updates a data asset
```

### Testing Requirements

All DAGs must meet these criteria (enforced by tests/dags/test_dag_example.py):

1. **Tags Required**: Every DAG must have at least one tag
2. **Retry Policy**: default_args must include `retries >= 2`
3. **Import Validation**: DAG files must import without errors
4. **Naming Convention**: Place all DAG files in the `dags/` directory

### Docker and Deployment

The project uses Apache Airflow 3.0.0 vanilla image (Dockerfile). When modifying dependencies:

1. **Python packages**: Add to `requirements.txt`
2. **System packages**: Add RUN commands to Dockerfile (e.g., Microsoft ODBC Driver for SQL Server)
3. **Environment variables**: Configure in `.env` file
4. **Connections**: Use `AIRFLOW_CONN_*` environment variables in `.env`
5. **Shared modules**: Place in `plugins/` directory (auto-added to Python path by Airflow)

### Local Development Environment

When `docker-compose up -d` is running:
- Airflow UI: http://localhost:8080 (username: `airflow`, password: `airflow`)
- SQL Server: localhost:1433 (user: sa, pass: YourStrong@Passw0rd)
- PostgreSQL Target: localhost:5433 (user: postgres, pass: PostgresPassword123)
- Seven containers run: webserver, scheduler, dag-processor, triggerer, postgres-metadata, mssql-server, postgres-target

### Connection and Variable Management

For local development, define connections in `.env` using Airflow connection URI format:
```bash
# SQL Server source
AIRFLOW_CONN_MSSQL_SOURCE='mssql://sa:YourStrong@Passw0rd@mssql-server:1433/StackOverflow2010'

# PostgreSQL target
AIRFLOW_CONN_POSTGRES_TARGET='postgresql://postgres:PostgresPassword123@postgres-target:5432/stackoverflow'

# Format: <type>://<user>:<password>@<host>:<port>/<database>
```

After changing `.env`, restart services:
```bash
docker-compose restart airflow-scheduler airflow-webserver
```

### DAG File Structure

Place DAGs in `/dags` directory. Use this structure:
```python
"""
Docstring explaining DAG purpose and workflow
"""

from airflow.decorators import dag, task
from pendulum import datetime

@dag(
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    default_args={"owner": "data-team", "retries": 3},
    tags=["etl", "postgres"],
)
def pipeline_name():
    @task
    def extract():
        # Extract logic
        pass

    @task
    def transform(data):
        # Transform logic
        pass

    @task
    def load(data):
        # Load logic
        pass

    # Define task dependencies
    load(transform(extract()))

# Instantiate DAG
pipeline_name()
```

### Project-Specific Considerations

1. **MSSQL to PostgreSQL Migration**: When implementing ETL pipelines:
   - Uses pyodbc with Microsoft ODBC Driver 18 for SQL Server (not Airflow's mssql provider)
   - Uses apache-airflow-providers-postgres for PostgreSQL connections
   - Shared migration modules in `plugins/mssql_pg_migration/` handle type mapping, schema extraction, DDL generation, and data transfer
   - Consider data type mappings between MSSQL and PostgreSQL (see `plugins/mssql_pg_migration/type_mapping.py`)

2. **Error Handling**: Tasks should include proper exception handling and use Airflow's retry mechanism

3. **XCom Usage**: For passing data between tasks, use return values or context["ti"].xcom_push()

4. **Testing DAGs**: Always validate new DAGs with `docker exec airflow-scheduler airflow dags list` before committing

5. **Airflow 3.0 Limitations**:
   - Task/DAG failure callbacks not yet implemented ([GitHub Issue #44354](https://github.com/apache/airflow/issues/44354))
   - Callbacks are temporarily disabled in migration DAG - notifications unavailable until Airflow adds support