# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is an Apache Airflow project built using the Astronomer framework for orchestrating data pipelines, specifically designed for ETL processes from Microsoft SQL Server to PostgreSQL. The project uses Astronomer Runtime (Docker-based) for local development and deployment.

## Common Development Commands

### Starting and Managing Airflow

```bash
# Start Airflow locally (spins up 5 Docker containers)
astro dev start

# Stop all Airflow containers
astro dev stop

# Restart Airflow containers
astro dev restart

# View container logs
astro dev logs

# Parse and validate DAGs without starting Airflow
astro dev parse

# Run pytest tests
astro dev pytest tests/
```

### Running Tests

```bash
# Run all DAG validation tests
astro dev pytest tests/dags/test_dag_example.py

# Run specific test
astro dev pytest tests/dags/test_dag_example.py::test_file_imports

# Run DAG integrity check (validates imports)
astro dev parse
```

## Architecture and Code Structure

### DAG Development Pattern

This project uses Airflow's TaskFlow API (@task decorator) for writing DAGs. Key patterns:

1. **DAG Definition**: Use the @dag decorator with proper configuration:
```python
from airflow.sdk import Asset, dag, task
from pendulum import datetime

@dag(
    start_date=datetime(2025, 4, 22),
    schedule="@daily",
    default_args={"owner": "Astro", "retries": 3},
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
@task(outlets=[Asset("table_name")])
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

The project uses Astronomer Runtime v3.1-5 (Dockerfile). When modifying dependencies:

1. **Python packages**: Add to `requirements.txt`
2. **OS packages**: Add to `packages.txt`
3. **Environment variables**: Use `.env` file (local only)
4. **Connections/Variables**: Configure in `airflow_settings.yaml` (local only)

### Local Development Environment

When `astro dev start` is running:
- Airflow UI: http://localhost:8080/ (no auth required locally)
- PostgreSQL: localhost:5432/postgres (user: postgres, pass: postgres)
- Five containers run: postgres, scheduler, dag-processor, webserver, triggerer

### Connection and Variable Management

For local development, define connections and variables in `airflow_settings.yaml`:
```yaml
connections:
  - conn_id: mssql_connection
    conn_type: mssql
    conn_host: your-host
    conn_schema: database_name
    conn_login: username
    conn_password: password
    conn_port: 1433

variables:
  - variable_name: target_schema
    variable_value: public
```

### DAG File Structure

Place DAGs in `/dags` directory. Use this structure:
```python
"""
Docstring explaining DAG purpose and workflow
"""

from airflow.sdk import dag, task
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
   - Use appropriate provider packages (apache-airflow-providers-microsoft-mssql, apache-airflow-providers-postgres)
   - Consider data type mappings between MSSQL and PostgreSQL
   - Handle schema differences and naming conventions

2. **Error Handling**: Tasks should include proper exception handling and use Airflow's retry mechanism

3. **XCom Usage**: For passing data between tasks, use return values or context["ti"].xcom_push()

4. **Testing DAGs**: Always validate new DAGs with `astro dev parse` before committing