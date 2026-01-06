"""
SQL Server to PostgreSQL Schema DAG

This DAG handles schema operations only:
1. Extract schema from SQL Server (tables, columns, primary keys)
2. Create target schema(s) in PostgreSQL (derived from source db/schema)
3. Create tables with primary keys

Tables must be explicitly specified in 'schema.table' format in include_tables.
Target PostgreSQL schema is derived as: {sourcedb}__{sourceschema} (lowercase)

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

from mssql_pg_migration import schema_extractor
from mssql_pg_migration.ddl_generator import DDLGenerator
from mssql_pg_migration.table_config import (
    expand_include_tables_param,
    validate_include_tables,
    parse_include_tables,
    get_source_database,
    derive_target_schema,
    load_include_tables_from_config,
)

logger = logging.getLogger(__name__)


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
        "include_tables": Param(
            default=load_include_tables_from_config("mssql_source"),
            description="Tables to include in 'schema.table' format (e.g., ['dbo.Users', 'dbo.Posts']). "
                        "Defaults from config/{database}_include_tables.txt or INCLUDE_TABLES env var."
        ),
    },
    tags=["schema", "mssql", "postgres", "ddl"],
)
def mssql_to_postgres_schema():
    """Schema DAG: Extract from MSSQL, create tables with PKs in PostgreSQL."""

    @task
    def extract_source_schema(**context) -> List[Dict[str, Any]]:
        """
        Extract schema information from SQL Server.

        Parses include_tables in schema.table format, derives target schemas,
        and extracts full schema info for each table.
        """
        params = context["params"]
        source_conn_id = params["source_conn_id"]

        # Parse and expand include_tables parameter
        # Defaults are loaded from config file or env var at DAG parse time
        include_tables_raw = params.get("include_tables", [])
        include_tables = expand_include_tables_param(include_tables_raw)

        # Validate include_tables (will raise ValueError if empty or invalid)
        validate_include_tables(include_tables)

        logger.info(f"Processing {len(include_tables)} tables: {include_tables}")

        # Parse into {schema: [tables]} dict
        schema_tables = parse_include_tables(include_tables)
        logger.info(f"Grouped into schemas: {list(schema_tables.keys())}")

        # Get source database name for deriving target schemas
        source_db = get_source_database(source_conn_id)
        logger.info(f"Source database: {source_db}")

        # Build target schema map: {source_schema: target_schema}
        target_schema_map = {
            src_schema: derive_target_schema(source_db, src_schema)
            for src_schema in schema_tables.keys()
        }
        logger.info(f"Target schemas: {target_schema_map}")

        # Extract schema info using new function that accepts schema_tables dict
        tables = schema_extractor.extract_schema_for_tables(
            mssql_conn_id=source_conn_id,
            schema_tables=schema_tables,
            target_schema_map=target_schema_map
        )

        logger.info(f"Extracted schema for {len(tables)} tables")

        # Log table summary
        for t in tables:
            pk_info = t.get('primary_key', {})
            pk_cols = pk_info.get('columns', []) if isinstance(pk_info, dict) else []
            logger.info(
                f"  {t.get('source_schema', 'unknown')}.{t['table_name']} -> "
                f"{t.get('target_schema', 'unknown')}: {len(t.get('columns', []))} cols, PK: {pk_cols}"
            )

        # Push metadata to XCom
        context["ti"].xcom_push(key="table_count", value=len(tables))
        context["ti"].xcom_push(key="table_names", value=[t["table_name"] for t in tables])
        context["ti"].xcom_push(
            key="target_schemas",
            value=list(set(t.get("target_schema") for t in tables if t.get("target_schema")))
        )

        return tables

    @task
    def create_target_schemas(tables_schema: List[Dict[str, Any]], **context) -> List[str]:
        """
        Create all unique target schemas in PostgreSQL.

        Target schemas are derived from source db/schema and embedded in tables_schema.
        """
        params = context["params"]

        from airflow.providers.postgres.hooks.postgres import PostgresHook
        postgres_hook = PostgresHook(postgres_conn_id=params["target_conn_id"])

        # Get unique target schemas from table info
        target_schemas = list(set(
            t.get("target_schema")
            for t in tables_schema
            if t.get("target_schema")
        ))

        if not target_schemas:
            raise ValueError("No target schemas found in table info")

        created_schemas = []
        for schema_name in sorted(target_schemas):
            # Use quoted identifier for safety
            create_sql = f'CREATE SCHEMA IF NOT EXISTS "{schema_name}"'
            postgres_hook.run(create_sql)
            logger.info(f"Ensured schema '{schema_name}' exists")
            created_schemas.append(schema_name)

        return created_schemas

    @task
    def create_tables_with_pks(
        tables_schema: List[Dict[str, Any]],
        created_schemas: List[str],
        **context
    ) -> List[Dict[str, str]]:
        """
        Create all tables in PostgreSQL WITH primary keys.

        Always drops and recreates tables to ensure clean state.
        Uses target_schema from each table's info dict.
        No foreign keys or secondary indexes are created.
        """
        params = context["params"]

        generator = DDLGenerator(params["target_conn_id"])
        created_tables = []

        for table_schema in tables_schema:
            table_name = table_schema["table_name"]
            target_schema = table_schema.get("target_schema")

            if not target_schema:
                logger.error(f"No target_schema for table {table_name}, skipping")
                continue

            try:
                ddl_statements = []

                # Always drop existing table before recreating
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

                created_tables.append({
                    "table_name": table_name,
                    "source_schema": table_schema.get("source_schema", "unknown"),
                    "target_schema": target_schema,
                })

                logger.info(f"Created table {target_schema}.{table_name} with PK")

            except Exception as e:
                logger.error(f"Failed to create table {target_schema}.{table_name}: {e}")
                raise

        logger.info(f"Successfully created {len(created_tables)} tables with primary keys")
        return created_tables

    @task
    def log_schema_summary(created_tables: List[Dict[str, str]], **context) -> str:
        """Log summary of schema creation."""
        summary = f"Schema DAG complete: {len(created_tables)} tables created with PKs"
        logger.info(summary)

        # Group by target schema for nice summary
        by_schema: Dict[str, List[str]] = {}
        for t in created_tables:
            schema = t.get("target_schema", "unknown")
            if schema not in by_schema:
                by_schema[schema] = []
            by_schema[schema].append(t["table_name"])

        for schema, tables in by_schema.items():
            logger.info(f"  {schema}: {', '.join(tables)}")

        return summary

    # Task flow
    schema_data = extract_source_schema()
    created_schemas = create_target_schemas(schema_data)
    created_tables = create_tables_with_pks(schema_data, created_schemas)
    log_schema_summary(created_tables)


# Instantiate
mssql_to_postgres_schema()
