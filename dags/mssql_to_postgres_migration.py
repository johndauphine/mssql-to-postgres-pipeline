"""
SQL Server to PostgreSQL Migration DAG

This DAG performs a complete schema and data migration from SQL Server to PostgreSQL.
It handles:
1. Schema extraction from SQL Server
2. Data type mapping from SQL Server to PostgreSQL
3. Table creation in PostgreSQL
4. Data transfer with chunking and parallelization
5. Row count validation and reporting

The DAG is designed to be generic and reusable for any SQL Server database migration.
"""

from airflow.sdk import Asset, dag, task
from airflow.models.param import Param
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from pendulum import datetime
from datetime import timedelta
from typing import List, Dict, Any
import logging

# Import our custom migration modules
from include.mssql_pg_migration import (
    schema_extractor,
    ddl_generator,
    data_transfer,
    validation,
)

logger = logging.getLogger(__name__)


@dag(
    start_date=datetime(2025, 1, 1),
    schedule=None,  # Run manually or trigger via API
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    doc_md=__doc__,
    default_args={
        "owner": "data-team",
        "retries": 3,
        "retry_delay": timedelta(seconds=30),
        "retry_exponential_backoff": False,
        "max_retry_delay": timedelta(minutes=30),
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
        "chunk_size": Param(
            default=10000,
            type="integer",
            minimum=100,
            maximum=100000,
            description="Number of rows to transfer per batch"
        ),
        "exclude_tables": Param(
            default=[],
            type="array",
            description="List of table patterns to exclude (supports wildcards)"
        ),
        "validate_samples": Param(
            default=False,
            type="boolean",
            description="Whether to validate sample data (slower)"
        ),
        "create_foreign_keys": Param(
            default=True,
            type="boolean",
            description="Whether to create foreign key constraints"
        ),
        "use_unlogged_tables": Param(
            default=True,
            type="boolean",
            description="Create tables as UNLOGGED during load for faster bulk inserts (converts to LOGGED after)"
        ),
    },
    tags=["migration", "mssql", "postgres", "etl", "full-refresh"],
)
def mssql_to_postgres_migration():
    """
    Main DAG for SQL Server to PostgreSQL migration.
    """

    @task(
        outlets=[Asset("mssql_schema_extracted")]
    )
    def extract_source_schema(**context) -> List[Dict[str, Any]]:
        """
        Extract complete schema information from SQL Server.

        Returns:
            List of table schema dictionaries
        """
        params = context["params"]
        logger.info(f"Extracting schema from {params['source_schema']} in SQL Server")

        # Extract all tables and their schemas
        tables = schema_extractor.extract_schema_info(
            mssql_conn_id=params["source_conn_id"],
            schema_name=params["source_schema"],
            exclude_tables=params.get("exclude_tables", [])
        )

        logger.info(f"Extracted schema for {len(tables)} tables")

        # Push summary to XCom for visibility
        context["ti"].xcom_push(
            key="extracted_tables",
            value=[t["table_name"] for t in tables]
        )
        context["ti"].xcom_push(
            key="total_row_count",
            value=sum(t.get("row_count", 0) for t in tables)
        )

        return tables

    @task
    def create_target_schema(schema_name: str, **context) -> str:
        """
        Create target schema in PostgreSQL if it doesn't exist.

        Args:
            schema_name: Schema name to create

        Returns:
            Schema creation status
        """
        params = context["params"]
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        postgres_hook = PostgresHook(postgres_conn_id=params["target_conn_id"])

        create_schema_sql = f"CREATE SCHEMA IF NOT EXISTS {schema_name}"
        postgres_hook.run(create_schema_sql)

        logger.info(f"Ensured schema {schema_name} exists in PostgreSQL")
        return f"Schema {schema_name} ready"

    @task
    def create_target_tables(
        tables_schema: List[Dict[str, Any]],
        schema_status: str,
        **context
    ) -> List[Dict[str, Any]]:
        """
        Create all tables in PostgreSQL with proper data types.

        Args:
            tables_schema: List of table schemas from SQL Server
            schema_status: Status from schema creation task

        Returns:
            List of created tables with mapping information
        """
        params = context["params"]
        target_schema = params["target_schema"]
        use_unlogged = params.get("use_unlogged_tables", True)

        generator = ddl_generator.DDLGenerator(params["target_conn_id"])
        created_tables = []

        for table_schema in tables_schema:
            table_name = table_schema["table_name"]
            unlogged_msg = " (UNLOGGED)" if use_unlogged else ""
            logger.info(f"Creating table {target_schema}.{table_name}{unlogged_msg}")

            try:
                # Generate DDL statements (optionally as UNLOGGED for faster bulk loads)
                # Skip indexes and PK here - they will be created after data load for better performance
                ddl_statements = generator.generate_complete_ddl(
                    table_schema,
                    target_schema,
                    drop_if_exists=True,
                    create_indexes=False,  # Indexes created after data load for performance
                    create_foreign_keys=False,  # Will be done after all tables are created
                    unlogged=use_unlogged
                )
                # Remove PK constraint from CREATE TABLE - will be added after data load
                # This is done by setting include_constraints=False in generate_create_table
                ddl_statements = [generator.generate_drop_table(table_name, target_schema, cascade=True)]
                ddl_statements.append(generator.generate_create_table(
                    table_schema,
                    target_schema,
                    include_constraints=False,  # Skip PK - added after data load
                    unlogged=use_unlogged
                ))

                # Execute DDL
                generator.execute_ddl(ddl_statements, transaction=False)

                # Prepare table info for data transfer
                table_info = {
                    "table_name": table_name,
                    "source_schema": params["source_schema"],
                    "target_schema": target_schema,
                    "target_table": table_name,
                    "row_count": table_schema.get("row_count", 0),
                    "columns": [col["column_name"] for col in table_schema["columns"]],
                }
                created_tables.append(table_info)

                logger.info(f"✓ Created table {table_name}")

            except Exception as e:
                logger.error(f"✗ Failed to create table {table_name}: {str(e)}")
                raise

        logger.info(f"Successfully created {len(created_tables)} tables")
        return created_tables

    @task
    def transfer_table_data(table_info: Dict[str, Any], **context) -> Dict[str, Any]:
        """
        Transfer data for a single table from SQL Server to PostgreSQL.

        Args:
            table_info: Table information including source and target details

        Returns:
            Transfer result dictionary with statistics
        """
        params = context["params"]

        logger.info(
            f"Starting data transfer for {table_info['table_name']} "
            f"({table_info.get('row_count', 0):,} rows)"
        )

        result = data_transfer.transfer_table_data(
            mssql_conn_id=params["source_conn_id"],
            postgres_conn_id=params["target_conn_id"],
            table_info=table_info,
            chunk_size=params["chunk_size"],
            truncate=True  # Ensure tables are truncated before transfer
        )

        # Add table name to result for tracking
        result["table_name"] = table_info["table_name"]

        if result["success"]:
            logger.info(
                f"✓ {table_info['table_name']}: Transferred {result['rows_transferred']:,} rows "
                f"in {result['elapsed_time_seconds']:.2f}s "
                f"({result['avg_rows_per_second']:,.0f} rows/sec)"
            )
        else:
            logger.error(
                f"✗ {table_info['table_name']}: Transfer failed or incomplete. "
                f"Errors: {result.get('errors', [])}"
            )

        return result

    @task
    def create_foreign_keys(
        tables_schema: List[Dict[str, Any]],
        transfer_results: List[Dict[str, Any]],
        **context
    ) -> str:
        """
        Create foreign key constraints after all data is transferred.

        Args:
            tables_schema: Original table schemas with foreign key definitions
            transfer_results: Results from data transfers

        Returns:
            Status message
        """
        params = context["params"]

        if not params["create_foreign_keys"]:
            logger.info("Skipping foreign key creation (disabled by parameter)")
            return "Foreign keys skipped"

        # Only create foreign keys for successfully transferred tables
        successful_tables = {r["table_name"] for r in transfer_results if r.get("success", False)}

        generator = ddl_generator.DDLGenerator(params["target_conn_id"])
        fk_count = 0

        for table_schema in tables_schema:
            if table_schema["table_name"] not in successful_tables:
                continue

            if table_schema.get("foreign_keys"):
                fk_statements = generator.generate_foreign_keys(
                    table_schema,
                    params["target_schema"]
                )

                for fk_ddl in fk_statements:
                    try:
                        generator.execute_ddl([fk_ddl], transaction=False)
                        fk_count += 1
                        logger.info(f"✓ Created foreign key for {table_schema['table_name']}")
                    except Exception as e:
                        logger.warning(f"Could not create foreign key: {str(e)}")

        logger.info(f"Created {fk_count} foreign key constraints")
        return f"Created {fk_count} foreign keys"

    @task
    def convert_tables_to_logged(
        transfer_results: List[Dict[str, Any]],
        **context
    ) -> str:
        """
        Convert UNLOGGED tables to LOGGED after data transfer.

        This ensures data durability after bulk loading is complete.

        Args:
            transfer_results: Results from data transfers

        Returns:
            Status message
        """
        params = context["params"]

        if not params.get("use_unlogged_tables", True):
            logger.info("Tables were created as LOGGED, no conversion needed")
            return "Tables already logged"

        target_schema = params["target_schema"]
        generator = ddl_generator.DDLGenerator(params["target_conn_id"])

        # Convert successfully transferred tables to LOGGED
        successful_tables = [r["table_name"] for r in transfer_results if r.get("success", False)]
        converted_count = 0

        for table_name in successful_tables:
            try:
                set_logged_ddl = generator.generate_set_logged(table_name, target_schema)
                generator.execute_ddl([set_logged_ddl], transaction=False)
                converted_count += 1
                logger.info(f"✓ Converted {table_name} to LOGGED")
            except Exception as e:
                logger.warning(f"Could not convert {table_name} to LOGGED: {str(e)}")

        logger.info(f"Converted {converted_count} tables to LOGGED for durability")
        return f"Converted {converted_count} tables to LOGGED"

    @task
    def create_indexes(
        tables_schema: List[Dict[str, Any]],
        transfer_results: List[Dict[str, Any]],
        **context
    ) -> str:
        """
        Create indexes after data transfer for better performance.

        Building indexes after bulk data load is much faster than maintaining
        indexes during inserts.

        Args:
            tables_schema: Original table schemas with index definitions
            transfer_results: Results from data transfers

        Returns:
            Status message
        """
        params = context["params"]
        target_schema = params["target_schema"]

        generator = ddl_generator.DDLGenerator(params["target_conn_id"])

        # Only create indexes for successfully transferred tables
        successful_tables = {r["table_name"] for r in transfer_results if r.get("success", False)}
        index_count = 0

        for table_schema in tables_schema:
            table_name = table_schema["table_name"]
            if table_name not in successful_tables:
                continue

            index_statements = generator.generate_indexes(table_schema, target_schema)

            for index_ddl in index_statements:
                try:
                    generator.execute_ddl([index_ddl], transaction=False)
                    index_count += 1
                    logger.info(f"✓ Created index for {table_name}")
                except Exception as e:
                    logger.warning(f"Could not create index: {str(e)}")

        logger.info(f"Created {index_count} indexes")
        return f"Created {index_count} indexes"

    @task
    def create_primary_keys(
        tables_schema: List[Dict[str, Any]],
        transfer_results: List[Dict[str, Any]],
        **context
    ) -> str:
        """
        Create primary key constraints after data transfer for better performance.

        Building PK indexes after bulk data load is much faster than maintaining
        them during inserts.

        Args:
            tables_schema: Original table schemas with PK definitions
            transfer_results: Results from data transfers

        Returns:
            Status message
        """
        params = context["params"]
        target_schema = params["target_schema"]

        generator = ddl_generator.DDLGenerator(params["target_conn_id"])

        # Only create PKs for successfully transferred tables
        successful_tables = {r["table_name"] for r in transfer_results if r.get("success", False)}
        pk_count = 0

        for table_schema in tables_schema:
            table_name = table_schema["table_name"]
            if table_name not in successful_tables:
                continue

            pk_ddl = generator.generate_primary_key(table_schema, target_schema)
            if pk_ddl:
                try:
                    generator.execute_ddl([pk_ddl], transaction=False)
                    pk_count += 1
                    logger.info(f"✓ Created primary key for {table_name}")
                except Exception as e:
                    logger.warning(f"Could not create primary key for {table_name}: {str(e)}")

        logger.info(f"Created {pk_count} primary key constraints")
        return f"Created {pk_count} primary keys"

    """
    # Commented out - replaced with TriggerDagRunOperator to avoid XCom bug
    @task(
        outlets=[Asset("migration_validated")]
    )
    def validate_migration(
        tables_info: List[Dict[str, Any]],
        transfer_results: List[Dict[str, Any]],
        **context
    ) -> Dict[str, Any]:
        '''
        Validate the migration by comparing row counts and optionally sample data.

        Args:
            tables_info: List of table information
            transfer_results: Results from data transfers

        Returns:
            Validation results with report
        '''
        params = context["params"]

        logger.info("Starting migration validation")

        # Validate all tables
        validation_results = validation.validate_migration(
            mssql_conn_id=params["source_conn_id"],
            postgres_conn_id=params["target_conn_id"],
            tables=tables_info,
            validate_samples=params["validate_samples"],
            transfer_results=transfer_results
        )

        # Push summary to XCom
        context["ti"].xcom_push(key="validation_summary", value={
            "total_tables": validation_results["total_tables"],
            "passed_tables": validation_results["passed_count"],
            "failed_tables": validation_results["failed_count"],
            "success_rate": validation_results["success_rate"],
            "overall_success": validation_results["overall_success"],
        })

        # Log the report
        if validation_results.get("report"):
            logger.info(f"\\n{validation_results['report']}")

        # Raise alert if validation failed
        if not validation_results["overall_success"]:
            logger.warning(
                f"Migration validation failed for {validation_results['failed_count']} tables. "
                f"Check the report for details."
            )

        return validation_results
    """

    """
    # Commented out - replaced with simplified version
    @task
    def generate_final_report(validation_results: Dict[str, Any], **context) -> str:
        '''
        Generate and output the final migration report.

        Args:
            validation_results: Validation results from previous task

        Returns:
            Final status message
        '''
        report = validation_results.get("report", "No report generated")

        # Save report to a file if needed
        # This could be extended to send the report via email, Slack, etc.

        if validation_results["overall_success"]:
            status = f"✓ Migration completed successfully for all {validation_results['total_tables']} tables"
            logger.info(status)
        else:
            status = (
                f"⚠ Migration completed with issues: "
                f"{validation_results['failed_count']}/{validation_results['total_tables']} tables failed"
            )
            logger.warning(status)

        # Push final status to XCom
        context["ti"].xcom_push(key="final_status", value=status)

        return status
    """

    # Define the task flow
    schema_data = extract_source_schema()
    schema_status = create_target_schema(schema_name="{{ params.target_schema }}")
    created_tables = create_target_tables(schema_data, schema_status)

    # Use dynamic task mapping for parallel table transfers
    # Each table is transferred independently in parallel
    transfer_results = transfer_table_data.expand(table_info=created_tables)

    # Convert UNLOGGED tables to LOGGED after data transfer (for durability)
    logged_status = convert_tables_to_logged(transfer_results)

    # Create primary keys after data load (much faster than during inserts)
    pk_status = create_primary_keys(schema_data, transfer_results)

    # Create secondary indexes after PKs
    index_status = create_indexes(schema_data, transfer_results)

    # Create foreign keys after indexes are created
    fk_status = create_foreign_keys(schema_data, transfer_results)

    # Task order: convert_to_logged -> create_primary_keys -> create_indexes -> create_foreign_keys
    logged_status >> pk_status >> index_status >> fk_status

    # Trigger validation DAG instead of internal validation (avoids XCom bug)
    trigger_validation = TriggerDagRunOperator(
        task_id="trigger_validation_dag",
        trigger_dag_id="validate_migration_env",
        wait_for_completion=True,
        poke_interval=30,
        conf={
            "source_schema": "{{ params.source_schema }}",
            "target_schema": "{{ params.target_schema }}",
        },
    )

    # Set task dependencies: trigger validation after foreign keys are created
    fk_status >> trigger_validation

    # Generate final report (simplified version without validation results)
    @task
    def generate_migration_summary(**context):
        """Generate a summary of the migration."""
        logger.info("Migration completed successfully!")
        logger.info("Validation DAG has been triggered to verify data integrity.")
        return "Migration complete. Check validation DAG for results."

    final_status = generate_migration_summary()
    trigger_validation >> final_status

    # Define task dependencies
    # The task flow is already defined through function calls above
    # Additional explicit dependencies can be added if needed


# Instantiate the DAG
mssql_to_postgres_migration()