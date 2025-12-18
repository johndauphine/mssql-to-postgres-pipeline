"""
Utility functions for data migration pipeline.

This module provides common utility functions including input validation,
SQL identifier sanitization, and data transformation helpers.
"""

import re
from typing import Any


def validate_sql_identifier(identifier: str, identifier_type: str = "identifier") -> str:
    """
    Validate and sanitize SQL identifiers to prevent SQL injection.

    This function ensures that identifiers (table names, schema names, column names)
    follow PostgreSQL naming rules and contain only safe characters.

    Args:
        identifier: The identifier to validate
        identifier_type: Type description for error messages (e.g., "table name", "schema")

    Returns:
        The validated identifier (unchanged if valid)

    Raises:
        ValueError: If the identifier is invalid

    Rules:
        - Non-empty
        - Max 128 characters (PostgreSQL limit: 63, SQL Server limit: 128)
        - Must start with letter or underscore
        - Can contain only alphanumeric characters and underscores
        - Pattern: [a-zA-Z_][a-zA-Z0-9_]*

    Examples:
        >>> validate_sql_identifier("users")
        'users'
        >>> validate_sql_identifier("table_2023")
        'table_2023'
        >>> validate_sql_identifier("_temp")
        '_temp'
        >>> validate_sql_identifier("2_invalid")  # doctest: +SKIP
        ValueError: Invalid identifier '2_invalid': must start with letter or underscore
        >>> validate_sql_identifier("drop; --")  # doctest: +SKIP
        ValueError: Invalid identifier 'drop; --': must contain only alphanumeric characters
    """
    if not identifier:
        raise ValueError(f"Invalid {identifier_type}: cannot be empty")

    if len(identifier) > 128:
        raise ValueError(
            f"Invalid {identifier_type}: exceeds maximum length of 128 characters "
            f"(got {len(identifier)} characters)"
        )

    # Check if identifier matches the safe pattern
    # Must start with letter or underscore, followed by alphanumeric or underscore
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', identifier):
        raise ValueError(
            f"Invalid {identifier_type} '{identifier}': must start with letter or underscore "
            "and contain only alphanumeric characters and underscores"
        )

    return identifier


def quote_sql_literal(value: Any) -> str:
    """
    Quote a value for safe use in SQL WHERE clauses.

    Handles different data types appropriately:
    - Integers: returned as-is (no quoting)
    - Strings/UUIDs: single-quoted with escaped quotes

    Args:
        value: The value to quote

    Returns:
        Quoted string ready for SQL insertion

    Examples:
        >>> quote_sql_literal(123)
        '123'
        >>> quote_sql_literal("admin")
        "'admin'"
        >>> quote_sql_literal("O'Brien")
        "'O''Brien'"
        >>> quote_sql_literal("test'; DROP TABLE users--")
        "'test''; DROP TABLE users--'"

    Note:
        This is for VALUES in WHERE clauses, NOT identifiers.
        For identifiers (table/column names), use psycopg2.sql.Identifier().
    """
    if isinstance(value, int):
        return str(value)

    # Convert to string and escape single quotes by doubling them (SQL standard)
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"


def validate_where_clause_template(clause_sql: str) -> None:
    """
    Validate WHERE clause templates to prevent SQL injection.

    This function ensures that WHERE clause templates used in data transfer
    contain only safe patterns with parameterized values.

    Args:
        clause_sql: The WHERE clause template to validate

    Raises:
        ValueError: If the clause contains unsafe patterns

    Valid patterns:
        - "column_name" <= %s
        - "column_name" > %s
        - "column_name" > %s AND "column_name" <= %s
        - ("col1", "col2") > (%s, %s)

    Examples:
        >>> validate_where_clause_template('"id" > %s')
        >>> validate_where_clause_template('"id" > %s AND "id" <= %s')
        >>> validate_where_clause_template('DROP TABLE users')  # doctest: +SKIP
        ValueError: Invalid WHERE clause template
    """
    # Pattern for safe WHERE clause templates
    # Allows:
    #   - Quoted identifiers: "column_name"
    #   - Comparison operators: <, <=, >, >=, =, !=
    #   - Logical operators: AND, OR
    #   - Parameterized values: %s
    #   - Tuple notation: ("col1", "col2") > (%s, %s)
    #   - Parentheses for grouping
    #
    # This is intentionally strict - only allows known-safe patterns
    safe_pattern = re.compile(
        r'^'
        r'[\s(]*'  # Leading whitespace and parens
        r'('  # Start of repeating group
        r'"[a-zA-Z_][a-zA-Z0-9_]*"'  # Quoted identifier
        r'[\s]*'  # Whitespace
        r'(<|<=|>|>=|=|!=)'  # Comparison operator
        r'[\s]*'  # Whitespace
        r'%s'  # Parameterized value
        r'|'  # OR
        r'\('  # Tuple start
        r'("[a-zA-Z_][a-zA-Z0-9_]*"[\s]*,[\s]*)*'  # Tuple of quoted identifiers
        r'"[a-zA-Z_][a-zA-Z0-9_]*"'  # Last identifier in tuple
        r'\)'  # Tuple end
        r'[\s]*'  # Whitespace
        r'(<|<=|>|>=|=|!=)'  # Comparison operator
        r'[\s]*'  # Whitespace
        r'\('  # Values tuple start
        r'(%s[\s]*,[\s]*)*'  # Tuple of params
        r'%s'  # Last param
        r'\)'  # Values tuple end
        r')'  # End of repeating group
        r'('  # Optional AND/OR connector
        r'[\s]+(AND|OR)[\s]+'
        r'[\s(]*'  # Whitespace and parens
        r'('  # Repeat the pattern
        r'"[a-zA-Z_][a-zA-Z0-9_]*"'
        r'[\s]*'
        r'(<|<=|>|>=|=|!=)'
        r'[\s]*'
        r'%s'
        r'|'
        r'\('
        r'("[a-zA-Z_][a-zA-Z0-9_]*"[\s]*,[\s]*)*'
        r'"[a-zA-Z_][a-zA-Z0-9_]*"'
        r'\)'
        r'[\s]*'
        r'(<|<=|>|>=|=|!=)'
        r'[\s]*'
        r'\('
        r'(%s[\s]*,[\s]*)*'
        r'%s'
        r'\)'
        r')'
        r'[\s)]*'  # Trailing whitespace and parens
        r')*'  # End optional connector
        r'[\s)]*'  # Trailing whitespace and parens
        r'$',
        re.IGNORECASE
    )

    if not safe_pattern.match(clause_sql.strip()):
        raise ValueError(
            f"Invalid WHERE clause template. Only quoted identifier comparisons with "
            f"parameterized values are allowed. Examples:\n"
            f'  - "id" > %s\n'
            f'  - "id" > %s AND "id" <= %s\n'
            f'  - ("user_id", "post_id") > (%s, %s)\n'
            f"Got: {clause_sql[:200]}"
        )


def sanitize_table_name(table_name: str) -> str:
    """
    Sanitize a table name for safe logging and display.

    This does NOT make the name safe for SQL queries - use psycopg2.sql.Identifier() for that.
    This is purely for logging/display purposes.

    Args:
        table_name: The table name to sanitize

    Returns:
        Sanitized table name safe for logging

    Examples:
        >>> sanitize_table_name("users")
        'users'
        >>> sanitize_table_name("users_2023")
        'users_2023'
        >>> sanitize_table_name("table'; DROP TABLE--")
        'table_DROP_TABLE'
    """
    # Remove or replace unsafe characters
    # Keep only alphanumeric, underscore, and hyphen
    sanitized = re.sub(r'[^a-zA-Z0-9_-]', '_', table_name)

    # Limit length for logging
    if len(sanitized) > 100:
        sanitized = sanitized[:97] + "..."

    return sanitized


def format_bytes(num_bytes: int) -> str:
    """
    Format bytes into human-readable format.

    Args:
        num_bytes: Number of bytes

    Returns:
        Formatted string (e.g., "1.5 GB")

    Examples:
        >>> format_bytes(1024)
        '1.0 KB'
        >>> format_bytes(1536)
        '1.5 KB'
        >>> format_bytes(1048576)
        '1.0 MB'
        >>> format_bytes(1073741824)
        '1.0 GB'
    """
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if num_bytes < 1024.0 or unit == 'TB':
            return f"{num_bytes:.1f} {unit}"
        num_bytes /= 1024.0
    return f"{num_bytes:.1f} PB"


def truncate_string(s: str, max_length: int = 100, suffix: str = "...") -> str:
    """
    Truncate a string to a maximum length.

    Args:
        s: String to truncate
        max_length: Maximum length (including suffix)
        suffix: Suffix to append if truncated

    Returns:
        Truncated string

    Examples:
        >>> truncate_string("short")
        'short'
        >>> truncate_string("a" * 150, max_length=20)
        'aaaaaaaaaaaaaaaaa...'
    """
    if len(s) <= max_length:
        return s
    return s[:max_length - len(suffix)] + suffix
