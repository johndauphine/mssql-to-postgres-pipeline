"""
SQL Server to PostgreSQL Schema DAG

This DAG handles schema operations only:
1. Extract schema from SQL Server (tables, columns, primary keys)
2. Create target schema in PostgreSQL
3. Create tables with primary keys

This DAG is triggered by the migration DAG before data transfer.
It can also be run standalone to set up or refresh the target schema.

Note: This DAG does NOT create foreign keys or secondary indexes.
"""

from airflow.decorators import dag, task
from airflow.models.param import Param
from pendulum import datetime
from datetime import timedelta
from typing import List, Dict, Any
import logging
import re
import os

from mssql_pg_migration import schema_extractor, ddl_generator

logger = logging.getLogger(__name__)


def validate_sql_identifier(identifier: str, identifier_type: str = "identifier") -> str:
    """Validate and sanitize SQL identifiers to prevent SQL injection."""
    if not identifier:
        raise ValueError(f"Invalid {identifier_type}: cannot be empty")
    if len(identifier) > 128:
        raise ValueError(f"Invalid {identifier_type}: exceeds maximum length")
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', identifier):
        raise ValueError(f"Invalid {identifier_type} '{identifier}'")
    return identifier


@dag(
    dag_id="mssql_to_postgres_schema",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    doc_md=__doc__,
    default_args={
        "owner": "data-team",
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
    },
    params={
        "source_conn_id": Param(
            default="mssql_source",
            type="string",
            description="SQL Server connection ID"
        ),
        "target_conn_id": Param(
            default="postgres_target",
            type="string",
            description="PostgreSQL connection ID"
        ),
        "source_schema": Param(
            default="dbo",
            type="string",
            description="Source schema in SQL Server"
        ),
        "target_schema": Param(
            default="public",
            type="string",
            description="Target schema in PostgreSQL"
        ),
        "include_tables": Param(
            default=[],
            description="Tables to include (empty = all)"
        ),
        "exclude_tables": Param(
            default=[],
            description="Tables to exclude"
        ),
        "drop_existing": Param(
            default=True,
            type="boolean",
            description="Drop existing tables before creating"
        ),
    },
    tags=["schema", "mssql", "postgres", "ddl"],
)
def mssql_to_postgres_schema():
    """Schema DAG: Extract from MSSQL, create tables with PKs in PostgreSQL."""

    @task
    def extract_source_schema(**context) -> List[Dict[str, Any]]:
        """Extract schema information from SQL Server."""
        params = context["params"]

        # Parse include_tables parameter
        include_tables_raw = params.get("include_tables", [])
        if isinstance(include_tables_raw, str):
            import json
            try:
                include_tables = json.loads(include_tables_raw)
            except json.JSONDecodeError:
                include_tables = [t.strip() for t in include_tables_raw.split(',') if t.strip()]
        else:
            include_tables = include_tables_raw

        # Expand comma-separated items
        if include_tables and isinstance(include_tables, list):
            expanded = []
            for item in include_tables:
                if isinstance(item, str) and ',' in item:
                    expanded.extend([t.strip() for t in item.split(',') if t.strip()])
                elif isinstance(item, str) and item.strip():
                    expanded.append(item.strip())
            include_tables = expanded

        logger.info(f"Extracting schema from {params['source_schema']}")

        tables = schema_extractor.extract_schema_info(
            mssql_conn_id=params["source_conn_id"],
            schema_name=params["source_schema"],
            exclude_tables=params.get("exclude_tables", []),
            include_tables=include_tables or None
        )

        logger.info(f"Extracted schema for {len(tables)} tables")

        # Log table summary
        for t in tables:
            pk_info = t.get('primary_key', {})
            pk_cols = pk_info.get('columns', []) if isinstance(pk_info, dict) else []
            logger.info(f"  {t['table_name']}: {len(t.get('columns', []))} columns, PK: {pk_cols}")

        context["ti"].xcom_push(key="table_count", value=len(tables))
        context["ti"].xcom_push(key="table_names", value=[t["table_name"] for t in tables])

        return tables

    @task
    def create_target_schema(**context) -> str:
        """Create target schema in PostgreSQL if it doesn't exist."""
        params = context["params"]
        schema_name = params["target_schema"]

        from airflow.providers.postgres.hooks.postgres import PostgresHook
        postgres_hook = PostgresHook(postgres_conn_id=params["target_conn_id"])

        create_sql = f'CREATE SCHEMA IF NOT EXISTS "{schema_name}"'
        postgres_hook.run(create_sql)

        logger.info(f"Ensured schema '{schema_name}' exists")
        return f"Schema {schema_name} ready"

    @task
    def create_tables_with_pks(
        tables_schema: List[Dict[str, Any]],
        schema_status: str,
        **context
    ) -> List[str]:
        """
        Create all tables in PostgreSQL WITH primary keys.

        No foreign keys or secondary indexes are created.
        """
        params = context["params"]
        target_schema = params["target_schema"]
        drop_existing = params.get("drop_existing", True)

        generator = ddl_generator.DDLGenerator(params["target_conn_id"])
        created_tables = []

        for table_schema in tables_schema:
            table_name = table_schema["table_name"]

            try:
                ddl_statements = []

                # Drop existing table if requested
                if drop_existing:
                    ddl_statements.append(
                        generator.generate_drop_table(table_name, target_schema, cascade=True)
                    )

                # Create table WITH primary key (include_constraints=True)
                ddl_statements.append(
                    generator.generate_create_table(
                        table_schema,
                        target_schema,
                        include_constraints=True  # Include PK in CREATE TABLE
                    )
                )

                # Execute DDL
                generator.execute_ddl(ddl_statements, transaction=False)
                created_tables.append(table_name)

                logger.info(f"Created table {target_schema}.{table_name} with PK")

            except Exception as e:
                logger.error(f"Failed to create table {table_name}: {e}")
                raise

        logger.info(f"Successfully created {len(created_tables)} tables with primary keys")
        return created_tables

    @task
    def log_schema_summary(created_tables: List[str], **context) -> str:
        """Log summary of schema creation."""
        summary = f"Schema DAG complete: {len(created_tables)} tables created with PKs"
        logger.info(summary)
        logger.info(f"Tables: {', '.join(created_tables)}")
        return summary

    # Task flow
    schema_data = extract_source_schema()
    schema_status = create_target_schema()
    created_tables = create_tables_with_pks(schema_data, schema_status)
    summary = log_schema_summary(created_tables)


# Instantiate
mssql_to_postgres_schema()
