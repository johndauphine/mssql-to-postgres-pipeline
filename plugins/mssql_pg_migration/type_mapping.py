"""
SQL Server to PostgreSQL Type Mapping Module

This module provides comprehensive data type mapping from Microsoft SQL Server
to PostgreSQL, handling all common and special data types.
"""

from typing import Optional, Dict, Any
import logging

logger = logging.getLogger(__name__)


# Complete mapping of SQL Server data types to PostgreSQL
TYPE_MAPPING = {
    # Exact Numeric Types
    "bit": "BOOLEAN",
    "tinyint": "SMALLINT",
    "smallint": "SMALLINT",
    "int": "INTEGER",
    "bigint": "BIGINT",
    "decimal": "DECIMAL({precision},{scale})",
    "numeric": "NUMERIC({precision},{scale})",
    "money": "NUMERIC(19,4)",
    "smallmoney": "NUMERIC(10,4)",

    # Approximate Numeric Types
    "float": "DOUBLE PRECISION",
    "real": "REAL",

    # Character String Types
    "char": "CHAR({length})",
    "varchar": "VARCHAR({length})",
    "text": "TEXT",

    # Unicode Character String Types
    "nchar": "CHAR({length})",
    "nvarchar": "VARCHAR({length})",
    "ntext": "TEXT",

    # Binary String Types
    "binary": "BYTEA",
    "varbinary": "BYTEA",
    "image": "BYTEA",

    # Date and Time Types
    "date": "DATE",
    "time": "TIME({precision})",
    "datetime": "TIMESTAMP(3)",
    "datetime2": "TIMESTAMP({precision})",
    "smalldatetime": "TIMESTAMP(0)",
    "datetimeoffset": "TIMESTAMP({precision}) WITH TIME ZONE",

    # Other Data Types
    "uniqueidentifier": "UUID",
    "xml": "XML",
    "sql_variant": "TEXT",  # No direct equivalent, store as TEXT
    "hierarchyid": "VARCHAR(4000)",  # No direct equivalent
    "geography": "GEOGRAPHY",
    "geometry": "GEOMETRY",
    "timestamp": "BYTEA",  # SQL Server timestamp is actually a rowversion
    "rowversion": "BYTEA",
    "sysname": "VARCHAR(128)",
}


def map_type(
    sql_server_type: str,
    max_length: Optional[int] = None,
    precision: Optional[int] = None,
    scale: Optional[int] = None,
    is_max: bool = False
) -> str:
    """
    Map a SQL Server data type to its PostgreSQL equivalent.

    Args:
        sql_server_type: The SQL Server data type name
        max_length: Maximum length for character/binary types
        precision: Precision for numeric/datetime types
        scale: Scale for numeric types
        is_max: Whether the type uses MAX keyword (e.g., VARCHAR(MAX))

    Returns:
        The PostgreSQL equivalent data type
    """
    # Normalize type name to lowercase for comparison
    sql_type = sql_server_type.lower().strip()

    # Handle MAX types
    if is_max or sql_type.endswith('(max)'):
        if 'char' in sql_type:
            return "TEXT"
        elif 'binary' in sql_type:
            return "BYTEA"
        sql_type = sql_type.replace('(max)', '').strip()

    # Get base mapping
    if sql_type not in TYPE_MAPPING:
        logger.warning(f"Unknown SQL Server type '{sql_server_type}', using TEXT as fallback")
        return "TEXT"

    pg_type = TYPE_MAPPING[sql_type]

    # Replace placeholders with actual values
    if "{length}" in pg_type:
        if max_length:
            # For nchar/nvarchar, SQL Server counts bytes, PostgreSQL counts characters
            if sql_type.startswith('n'):
                # Unicode types in SQL Server use 2 bytes per character
                char_length = max_length // 2 if max_length > 0 else 1
                pg_type = pg_type.replace("{length}", str(char_length))
            else:
                pg_type = pg_type.replace("{length}", str(max_length))
        else:
            # Default to reasonable length if not specified
            pg_type = pg_type.replace("({length})", "(255)")

    if "{precision}" in pg_type:
        if precision is not None:
            pg_type = pg_type.replace("{precision}", str(precision))
        else:
            # Default precision values
            if "timestamp" in pg_type.lower():
                pg_type = pg_type.replace("{precision}", "6")
            elif "time" in pg_type.lower():
                pg_type = pg_type.replace("{precision}", "6")
            else:
                pg_type = pg_type.replace("{precision}", "18")

    if "{scale}" in pg_type:
        if scale is not None:
            pg_type = pg_type.replace("{scale}", str(scale))
        else:
            pg_type = pg_type.replace("{scale}", "0")

    return pg_type


def map_column(column_info: Dict[str, Any]) -> Dict[str, Any]:
    """
    Map a complete column definition from SQL Server to PostgreSQL.

    Args:
        column_info: Dictionary containing column metadata
            Expected keys: column_name, data_type, max_length, precision,
                         scale, is_nullable, is_identity, default_value

    Returns:
        Dictionary with PostgreSQL column definition
    """
    result = {
        'column_name': column_info['column_name'],
        'is_nullable': column_info.get('is_nullable', True),
        'default_value': None,
        'is_identity': column_info.get('is_identity', False),
    }

    # Map the data type
    result['data_type'] = map_type(
        column_info['data_type'],
        column_info.get('max_length'),
        column_info.get('precision'),
        column_info.get('scale'),
        column_info.get('is_max', False)
    )

    # Handle identity columns
    if result['is_identity']:
        # Determine identity type based on base data type
        base_type = column_info['data_type'].lower()
        if base_type == 'bigint':
            result['data_type'] = 'BIGSERIAL'
        elif base_type in ('int', 'smallint'):
            result['data_type'] = 'SERIAL'
        else:
            # Use GENERATED AS IDENTITY for other types
            result['data_type'] = f"{result['data_type']} GENERATED ALWAYS AS IDENTITY"

    # Map default values if present
    if column_info.get('default_value'):
        result['default_value'] = map_default_value(column_info['default_value'])

    return result


def map_default_value(sql_server_default: str) -> Optional[str]:
    """
    Map SQL Server default value expressions to PostgreSQL.

    Args:
        sql_server_default: SQL Server default value expression

    Returns:
        PostgreSQL equivalent or None if unmappable
    """
    if not sql_server_default:
        return None

    # Remove outer parentheses if present
    default = sql_server_default.strip()
    if default.startswith('(') and default.endswith(')'):
        default = default[1:-1].strip()

    # Common function mappings
    mappings = {
        'getdate()': 'CURRENT_TIMESTAMP',
        'getutcdate()': 'CURRENT_TIMESTAMP AT TIME ZONE \'UTC\'',
        'newid()': 'gen_random_uuid()',
        'user_name()': 'CURRENT_USER',
        'suser_sname()': 'CURRENT_USER',
        'host_name()': 'inet_client_addr()',
    }

    # Check for direct function mappings
    default_lower = default.lower()
    for sql_func, pg_func in mappings.items():
        if sql_func in default_lower:
            default = default_lower.replace(sql_func, pg_func)
            return default

    # Handle numeric/string literals
    if default.replace('.', '').replace('-', '').isdigit():
        return default  # Numeric literal
    elif default.startswith("'") and default.endswith("'"):
        return default  # String literal
    elif default.upper() in ('NULL', 'TRUE', 'FALSE'):
        return default.upper()

    # If we can't map it, log a warning
    logger.warning(f"Cannot map default value '{sql_server_default}', skipping")
    return None


def map_table_schema(table_schema: Dict[str, Any]) -> Dict[str, Any]:
    """
    Map an entire table schema from SQL Server to PostgreSQL.

    Args:
        table_schema: Complete table schema information including
                     table_name, columns, primary_key, indexes, etc.

    Returns:
        Mapped table schema for PostgreSQL
    """
    result = {
        'table_name': table_schema['table_name'],
        'schema_name': table_schema.get('schema_name', 'public'),
        'columns': [],
        'primary_key': table_schema.get('primary_key', []),
        'indexes': [],
        'foreign_keys': [],
    }

    # Map all columns
    for column in table_schema.get('columns', []):
        result['columns'].append(map_column(column))

    # Map indexes if present
    for index in table_schema.get('indexes', []):
        result['indexes'].append({
            'index_name': index['index_name'],
            'columns': index['columns'],
            'is_unique': index.get('is_unique', False),
            'is_clustered': False,  # PostgreSQL doesn't have clustered indexes like SQL Server
        })

    # Map foreign keys if present
    for fk in table_schema.get('foreign_keys', []):
        result['foreign_keys'].append({
            'constraint_name': fk['constraint_name'],
            'columns': fk['columns'],
            'referenced_table': fk['referenced_table'],
            'referenced_columns': fk['referenced_columns'],
            'on_delete': fk.get('on_delete', 'NO ACTION'),
            'on_update': fk.get('on_update', 'NO ACTION'),
        })

    return result


def validate_type_mapping(sql_server_type: str) -> bool:
    """
    Check if a SQL Server type has a known mapping.

    Args:
        sql_server_type: The SQL Server data type to check

    Returns:
        True if the type has a mapping, False otherwise
    """
    normalized = sql_server_type.lower().strip()
    # Remove (max) suffix if present
    if normalized.endswith('(max)'):
        normalized = normalized[:-5].strip()
    return normalized in TYPE_MAPPING


def get_supported_types() -> list:
    """
    Get a list of all supported SQL Server data types.

    Returns:
        List of supported SQL Server data type names
    """
    return list(TYPE_MAPPING.keys())