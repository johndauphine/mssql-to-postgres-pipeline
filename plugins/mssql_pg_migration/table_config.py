"""
Table Configuration Utility Module

This module handles parsing of include_tables in 'schema.table' format
and derives target PostgreSQL schema names from source database/schema.
"""

import re
from typing import List, Dict, Tuple, Optional
import logging

logger = logging.getLogger(__name__)


def parse_schema_table(entry: str) -> Tuple[str, str]:
    """
    Parse single 'schema.table' entry.

    Handles:
    - Simple format: "dbo.Users" -> ("dbo", "Users")
    - Bracketed format: "[dbo].[My Table]" -> ("dbo", "My Table")

    Args:
        entry: Schema.table string in either format

    Returns:
        Tuple of (schema, table)

    Raises:
        ValueError: If format is invalid (no dot separator found)
    """
    entry = entry.strip()

    # Handle bracketed format: [schema].[table]
    bracketed_pattern = r'^\[([^\]]+)\]\.\[([^\]]+)\]$'
    match = re.match(bracketed_pattern, entry)
    if match:
        return (match.group(1), match.group(2))

    # Handle simple format: schema.table
    if '.' not in entry:
        raise ValueError(
            f"Invalid table format '{entry}': must be 'schema.table' or '[schema].[table]'"
        )

    # Split on first dot only (table names could theoretically contain dots in brackets)
    parts = entry.split('.', 1)
    if len(parts) != 2 or not parts[0] or not parts[1]:
        raise ValueError(
            f"Invalid table format '{entry}': must be 'schema.table' or '[schema].[table]'"
        )

    return (parts[0].strip(), parts[1].strip())


def parse_include_tables(include_tables: List[str]) -> Dict[str, List[str]]:
    """
    Parse 'schema.table' entries into {schema: [tables]} dict.

    Example:
        ["dbo.Users", "dbo.Posts", "sales.Orders"]
        -> {"dbo": ["Users", "Posts"], "sales": ["Orders"]}

    Args:
        include_tables: List of schema.table strings

    Returns:
        Dictionary mapping schema names to list of table names

    Raises:
        ValueError: If any entry is invalid format
    """
    result: Dict[str, List[str]] = {}

    for entry in include_tables:
        schema, table = parse_schema_table(entry)

        if schema not in result:
            result[schema] = []

        if table not in result[schema]:
            result[schema].append(table)

    return result


def get_source_database(mssql_conn_id: str) -> str:
    """
    Extract database name from Airflow MSSQL connection.

    Uses conn.schema attribute where MSSQL stores database name in Airflow.

    Args:
        mssql_conn_id: Airflow connection ID for SQL Server

    Returns:
        Database name from connection

    Raises:
        ValueError: If database name cannot be extracted
    """
    from airflow.hooks.base import BaseHook

    conn = BaseHook.get_connection(mssql_conn_id)

    # In Airflow, MSSQL connections store database in the 'schema' field
    database = conn.schema

    if not database:
        raise ValueError(
            f"Cannot extract database name from connection '{mssql_conn_id}'. "
            f"Ensure the connection has a database/schema configured."
        )

    return database


def derive_target_schema(source_db: str, source_schema: str) -> str:
    """
    Return sanitized PostgreSQL schema name.

    Format: {sourcedb}__{sourceschema} (lowercase)

    - Lowercase for PostgreSQL compatibility (avoids quoting issues)
    - Replace special chars with underscores
    - Truncate to 63 chars (PostgreSQL identifier limit)

    Note: The double underscore separator could cause ambiguity if source
    schemas contain double underscores (e.g., "db__schema" vs "db" + "__schema").
    Also, different source names may collide after sanitization
    (e.g., "My-DB" and "My_DB" both become "my_db__*").

    Args:
        source_db: Source database name
        source_schema: Source schema name

    Returns:
        Sanitized PostgreSQL schema name

    Examples:
        derive_target_schema("StackOverflow2010", "dbo")
        -> "stackoverflow2010__dbo"

        derive_target_schema("My-DB", "sales")
        -> "my_db__sales"
    """
    raw = f"{source_db}__{source_schema}".lower()
    # Replace any non-alphanumeric/underscore chars with underscore
    sanitized = re.sub(r'[^a-z0-9_]', '_', raw)
    # Truncate to PostgreSQL limit
    return sanitized[:63]


def validate_include_tables(include_tables: Optional[List[str]]) -> None:
    """
    Validate include_tables parameter.

    Args:
        include_tables: List of schema.table entries to validate

    Raises:
        ValueError: If list is empty or any entry is invalid format
    """
    if not include_tables:
        raise ValueError(
            "include_tables parameter is required and cannot be empty. "
            "Specify tables in 'schema.table' format, e.g., ['dbo.Users', 'dbo.Posts']"
        )

    # Validate each entry format
    for entry in include_tables:
        try:
            parse_schema_table(entry)
        except ValueError as e:
            raise ValueError(f"Invalid include_tables entry: {e}")


def expand_include_tables_param(include_tables_raw) -> List[str]:
    """
    Expand and normalize include_tables parameter from various input formats.

    Handles:
    - List of strings: ["dbo.Users", "dbo.Posts"]
    - JSON string: '["dbo.Users", "dbo.Posts"]'
    - Comma-separated string: "dbo.Users,dbo.Posts"
    - List with comma-separated items: ["dbo.Users,dbo.Posts"]

    Args:
        include_tables_raw: Raw parameter value from DAG params

    Returns:
        Normalized list of schema.table strings
    """
    import json

    # Handle string input (JSON or comma-separated)
    if isinstance(include_tables_raw, str):
        include_tables_raw = include_tables_raw.strip()
        if not include_tables_raw:
            return []

        # Try JSON parsing first
        try:
            parsed = json.loads(include_tables_raw)
            if isinstance(parsed, list):
                include_tables_raw = parsed
            else:
                # Single value from JSON
                include_tables_raw = [str(parsed)]
        except json.JSONDecodeError:
            # Treat as comma-separated
            include_tables_raw = [t.strip() for t in include_tables_raw.split(',') if t.strip()]

    # Expand comma-separated items within list
    if isinstance(include_tables_raw, list):
        expanded = []
        for item in include_tables_raw:
            if isinstance(item, str):
                if ',' in item:
                    expanded.extend([t.strip() for t in item.split(',') if t.strip()])
                elif item.strip():
                    expanded.append(item.strip())
        return expanded

    # Unsupported type - log warning to help debug configuration issues
    logger.warning(
        "expand_include_tables_param received unsupported type %s; returning empty list.",
        type(include_tables_raw).__name__,
    )
    return []


def build_table_info_list(
    mssql_conn_id: str,
    include_tables: List[str]
) -> List[Dict[str, str]]:
    """
    Build list of table info dicts with source and target schema.

    Args:
        mssql_conn_id: Airflow connection ID for SQL Server
        include_tables: List of schema.table strings

    Returns:
        List of dicts with keys:
        - table_name: Table name
        - source_schema: Source schema name
        - target_schema: Derived target schema name

    Example:
        build_table_info_list("mssql_source", ["dbo.Users", "dbo.Posts"])
        -> [
            {"table_name": "Users", "source_schema": "dbo", "target_schema": "stackoverflow2010__dbo"},
            {"table_name": "Posts", "source_schema": "dbo", "target_schema": "stackoverflow2010__dbo"},
        ]
    """
    validate_include_tables(include_tables)

    source_db = get_source_database(mssql_conn_id)
    schema_tables = parse_include_tables(include_tables)

    result = []
    for source_schema, tables in schema_tables.items():
        target_schema = derive_target_schema(source_db, source_schema)

        for table_name in tables:
            result.append({
                "table_name": table_name,
                "source_schema": source_schema,
                "target_schema": target_schema,
            })

    return result


def get_unique_target_schemas(
    mssql_conn_id: str,
    include_tables: List[str]
) -> List[str]:
    """
    Get list of unique target schema names.

    Args:
        mssql_conn_id: Airflow connection ID for SQL Server
        include_tables: List of schema.table strings

    Returns:
        List of unique target schema names

    Example:
        get_unique_target_schemas("mssql_source", ["dbo.Users", "sales.Orders"])
        -> ["stackoverflow2010__dbo", "stackoverflow2010__sales"]
    """
    validate_include_tables(include_tables)

    source_db = get_source_database(mssql_conn_id)
    schema_tables = parse_include_tables(include_tables)

    return [
        derive_target_schema(source_db, source_schema)
        for source_schema in schema_tables.keys()
    ]
