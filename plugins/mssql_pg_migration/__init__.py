"""
SQL Server to PostgreSQL Migration Utilities

This package provides utilities for migrating schemas and data from
Microsoft SQL Server to PostgreSQL databases using Apache Airflow.

Modules:
- schema_extractor: Extract schema information from SQL Server
- type_mapping: Map SQL Server types to PostgreSQL
- ddl_generator: Generate PostgreSQL DDL statements
- data_transfer: Transfer data with chunking and parallelization
- validation: Validate migration results
- incremental_state: Track sync state for incremental loading
- diff_detector: Detect new/changed rows for incremental sync
"""

__version__ = "1.1.0"

# Core modules
from mssql_pg_migration import schema_extractor
from mssql_pg_migration import type_mapping
from mssql_pg_migration import ddl_generator
from mssql_pg_migration import data_transfer
from mssql_pg_migration import validation

# Incremental loading modules
from mssql_pg_migration import incremental_state
from mssql_pg_migration import diff_detector

__all__ = [
    "schema_extractor",
    "type_mapping",
    "ddl_generator",
    "data_transfer",
    "validation",
    "incremental_state",
    "diff_detector",
]