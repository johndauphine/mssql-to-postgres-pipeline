"""
Table Configuration Utility Module

This module handles parsing of include_tables in 'schema.table' format
and derives target PostgreSQL schema names from source database/schema.
"""

import os
import re
from pathlib import Path
from typing import List, Dict, Tuple, Optional
import logging

logger = logging.getLogger(__name__)

# Base path for config files (relative to Airflow home or project root)
CONFIG_DIR = Path(os.environ.get("AIRFLOW_HOME", "/opt/airflow")) / "config"

# Hostname alias config file
HOSTNAME_ALIAS_FILE = CONFIG_DIR / "hostname_alias.txt"


def load_hostname_aliases() -> Dict[str, str]:
    """
    Load hostname to alias mappings from config file.

    File: config/hostname_alias.txt
    Format: hostname = alias (one per line, # for comments)

    Returns:
        Dict mapping lowercase hostnames to aliases

    Raises:
        FileNotFoundError: If config file doesn't exist
        ValueError: If config file is empty or has no valid mappings
    """
    if not HOSTNAME_ALIAS_FILE.exists():
        raise FileNotFoundError(
            f"Hostname alias config file not found: {HOSTNAME_ALIAS_FILE}\n"
            f"Create this file with hostname = alias mappings."
        )

    aliases: Dict[str, str] = {}

    with open(HOSTNAME_ALIAS_FILE, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()

            # Skip empty lines and comments
            if not line or line.startswith('#'):
                continue

            # Parse hostname = alias
            if '=' not in line:
                logger.warning(
                    f"Invalid line {line_num} in {HOSTNAME_ALIAS_FILE}: '{line}' "
                    f"(expected 'hostname = alias' format)"
                )
                continue

            parts = line.split('=', 1)
            hostname = parts[0].strip().lower()
            alias = parts[1].strip()

            if not hostname or not alias:
                logger.warning(
                    f"Invalid line {line_num} in {HOSTNAME_ALIAS_FILE}: '{line}' "
                    f"(hostname and alias cannot be empty)"
                )
                continue

            # Sanitize alias (lowercase, replace special chars)
            alias_sanitized = re.sub(r'[^a-z0-9_]', '_', alias.lower()).strip('_')

            if hostname in aliases:
                logger.warning(
                    f"Duplicate hostname '{hostname}' in {HOSTNAME_ALIAS_FILE} "
                    f"(line {line_num}). Using latest mapping: '{alias_sanitized}'"
                )

            aliases[hostname] = alias_sanitized

    if not aliases:
        raise ValueError(
            f"No valid hostname mappings found in {HOSTNAME_ALIAS_FILE}\n"
            f"Add at least one mapping in 'hostname = alias' format."
        )

    logger.debug(f"Loaded {len(aliases)} hostname aliases from {HOSTNAME_ALIAS_FILE}")
    return aliases


def get_alias_for_hostname(hostname: str) -> str:
    """
    Look up alias for hostname from config file.

    Matching is tried in order:
    1. Exact match (case-insensitive)
    2. Without port: 'server,1433' -> 'server'
    3. Without instance: 'server\\instance' -> 'server'

    Args:
        hostname: SQL Server hostname (may include instance/port)

    Returns:
        Alias for the hostname (sanitized)

    Raises:
        ValueError: If hostname not found in mapping file
    """
    aliases = load_hostname_aliases()

    # Normalize hostname for lookup
    hostname_lower = hostname.lower()

    # Try exact match first
    if hostname_lower in aliases:
        return aliases[hostname_lower]

    # Try without port (server,port -> server)
    hostname_no_port = hostname_lower.split(',')[0]
    if hostname_no_port in aliases:
        return aliases[hostname_no_port]

    # Try without instance (server\instance -> server)
    hostname_no_instance = hostname_no_port.split('\\')[0]
    if hostname_no_instance in aliases:
        return aliases[hostname_no_instance]

    # Not found - fail with helpful error
    available = '\n'.join(f"  {h} = {a}" for h, a in sorted(aliases.items()))
    raise ValueError(
        f"Hostname '{hostname}' not found in {HOSTNAME_ALIAS_FILE}\n"
        f"Available mappings:\n{available}\n"
        f"Add a mapping for this hostname and retry."
    )


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


def get_instance_name(mssql_conn_id: str) -> str:
    """
    Get alias for SQL Server hostname from config file.

    Looks up the connection's hostname in config/hostname_alias.txt
    and returns the configured alias. Fails if hostname not found.

    Args:
        mssql_conn_id: Airflow connection ID for SQL Server

    Returns:
        Alias for the hostname (sanitized, from config file)

    Raises:
        ValueError: If hostname not found in hostname_alias.txt
        FileNotFoundError: If hostname_alias.txt doesn't exist

    Examples:
        Connection host 'mssql-server' with alias 'dev' -> 'dev'
        Connection host 'sqlprod01' with alias 'prod_sales' -> 'prod_sales'
    """
    from airflow.hooks.base import BaseHook

    conn = BaseHook.get_connection(mssql_conn_id)
    host = conn.host or ''

    if not host:
        raise ValueError(
            f"Cannot get instance name from connection '{mssql_conn_id}'. "
            f"Connection has no host configured."
        )

    # Look up alias in config file (raises ValueError if not found)
    alias = get_alias_for_hostname(host)

    logger.info(f"Using alias '{alias}' for hostname '{host}'")
    return alias


def derive_target_schema(source_db: str, source_schema: str, instance_name: str) -> str:
    """
    Return sanitized PostgreSQL schema name with instance prefix.

    Format: {instance}__{sourcedb}__{sourceschema} (lowercase)

    - Lowercase for PostgreSQL compatibility (avoids quoting issues)
    - Replace special chars with underscores
    - If > 63 chars, hash only the instance name to fit PostgreSQL limit

    Note: The double underscore separator could cause ambiguity if source
    schemas contain double underscores. Different source names may collide
    after sanitization (e.g., "My-DB" and "My_DB" both become "my_db").

    Args:
        source_db: Source database name
        source_schema: Source schema name
        instance_name: SQL Server instance name (already sanitized)

    Returns:
        Sanitized PostgreSQL schema name

    Examples:
        derive_target_schema("StackOverflow2010", "dbo", "sqlprod01")
        -> "sqlprod01__stackoverflow2010__dbo"

        derive_target_schema("My-DB", "sales", "inst1")
        -> "inst1__my_db__sales"

        derive_target_schema("VeryLongDB", "schema", "verylonginstancename")
        -> "a1b2c3d4__verylongdb__schema" (if > 63 chars)
    """
    import hashlib

    # Sanitize db and schema
    db_sanitized = re.sub(r'[^a-z0-9_]', '_', source_db.lower())
    schema_sanitized = re.sub(r'[^a-z0-9_]', '_', source_schema.lower())

    # Build full schema name
    full_name = f"{instance_name}__{db_sanitized}__{schema_sanitized}"

    if len(full_name) <= 63:
        return full_name

    # Need to truncate - hash only the instance name
    # Calculate how much space we need for db__schema
    db_schema_part = f"{db_sanitized}__{schema_sanitized}"
    # 8 chars for hash + 2 for separator = 10 chars for instance part
    available_for_db_schema = 63 - 10  # 53 chars

    if len(db_schema_part) > available_for_db_schema:
        # Even with hashed instance, db__schema is too long
        # This is an edge case - log warning and truncate db__schema too
        logger.warning(
            f"Schema name '{full_name}' exceeds 63 chars even with hashed instance. "
            f"Truncating db__schema portion."
        )
        db_schema_part = db_schema_part[:available_for_db_schema]

    # Hash the instance name (first 8 chars of MD5)
    instance_hash = hashlib.md5(instance_name.encode()).hexdigest()[:8]

    return f"{instance_hash}__{db_schema_part}"


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


def get_default_include_tables() -> List[str]:
    """
    Get default include_tables from environment variable.

    Safe to call at DAG parse time - no database access.

    Returns:
        List of schema.table strings from INCLUDE_TABLES env var, or empty list
    """
    env_tables = os.environ.get("INCLUDE_TABLES", "")
    if env_tables:
        return expand_include_tables_param(env_tables)
    return []


def load_include_tables_from_config(mssql_conn_id: str) -> List[str]:
    """
    Load include_tables from config file based on source database name.

    IMPORTANT: This function accesses the Airflow metadata database.
    Only call at RUNTIME (inside tasks), not at DAG parse time.

    Looks for: config/{databasename}_include_tables.txt

    File format: One schema.table entry per line, e.g.:
        dbo.Users
        dbo.Posts
        sales.Orders

    Args:
        mssql_conn_id: Airflow connection ID for SQL Server

    Returns:
        List of schema.table strings from config file, or empty list if not found
    """
    from airflow.hooks.base import BaseHook

    conn = BaseHook.get_connection(mssql_conn_id)
    database = conn.schema

    if database:
        # Sanitize database name to prevent path traversal
        safe_database = re.sub(r'[^a-zA-Z0-9_-]', '_', database)
        if safe_database != database:
            logger.warning(f"Database name sanitized: '{database}' -> '{safe_database}'")

        config_file = CONFIG_DIR / f"{safe_database}_include_tables.txt"
        if config_file.exists():
            logger.info(f"Loading include_tables from {config_file}")
            with open(config_file, 'r', encoding='utf-8') as f:
                tables = [
                    line.strip()
                    for line in f
                    if line.strip() and not line.strip().startswith('#')
                ]
            if tables:
                logger.info(f"Loaded {len(tables)} tables from config file")
                return tables
            else:
                logger.warning(f"Config file {config_file} is empty")
        else:
            logger.debug(f"Config file not found: {config_file}")

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
            {"table_name": "Users", "source_schema": "dbo", "target_schema": "sqlprod01__stackoverflow2010__dbo"},
            {"table_name": "Posts", "source_schema": "dbo", "target_schema": "sqlprod01__stackoverflow2010__dbo"},
        ]
    """
    validate_include_tables(include_tables)

    source_db = get_source_database(mssql_conn_id)
    instance_name = get_instance_name(mssql_conn_id)
    schema_tables = parse_include_tables(include_tables)

    result = []
    for source_schema, tables in schema_tables.items():
        target_schema = derive_target_schema(source_db, source_schema, instance_name)

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
        -> ["sqlprod01__stackoverflow2010__dbo", "sqlprod01__stackoverflow2010__sales"]
    """
    validate_include_tables(include_tables)

    source_db = get_source_database(mssql_conn_id)
    instance_name = get_instance_name(mssql_conn_id)
    schema_tables = parse_include_tables(include_tables)

    return [
        derive_target_schema(source_db, source_schema, instance_name)
        for source_schema in schema_tables.keys()
    ]
