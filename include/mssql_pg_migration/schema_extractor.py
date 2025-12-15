"""
SQL Server Schema Extraction Module

This module provides functions to extract schema metadata from SQL Server databases
using system tables and information schema views.
"""

from typing import List, Dict, Any, Optional
from include.mssql_pg_migration.odbc_helper import OdbcConnectionHelper
import logging

logger = logging.getLogger(__name__)


class SchemaExtractor:
    """Extract schema information from SQL Server databases."""

    def __init__(self, mssql_conn_id: str):
        """
        Initialize the schema extractor.

        Args:
            mssql_conn_id: Airflow connection ID for SQL Server
        """
        self.mssql_hook = OdbcConnectionHelper(odbc_conn_id=mssql_conn_id)

    def get_tables(self, schema_name: str = 'dbo', exclude_patterns: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        Get all tables from a specific schema.

        Args:
            schema_name: Schema name to extract tables from
            exclude_patterns: List of table name patterns to exclude (supports wildcards)

        Returns:
            List of table information dictionaries
        """
        query = """
        SELECT
            s.name AS schema_name,
            t.name AS table_name,
            t.object_id,
            t.create_date,
            t.modify_date,
            (SELECT SUM(p.rows)
             FROM sys.partitions p
             WHERE p.object_id = t.object_id
               AND p.index_id IN (0, 1)) AS row_count
        FROM sys.tables t
        INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name = %s
          AND t.is_ms_shipped = 0  -- Exclude system tables
        ORDER BY t.name
        """

        tables = self.mssql_hook.get_records(query, parameters=[schema_name])

        result = []
        for table in tables:
            table_dict = {
                'schema_name': table[0],
                'table_name': table[1],
                'object_id': table[2],
                'create_date': table[3],
                'modify_date': table[4],
                'row_count': table[5] or 0,
            }

            # Apply exclusion patterns
            if exclude_patterns:
                skip = False
                for pattern in exclude_patterns:
                    if self._matches_pattern(table_dict['table_name'], pattern):
                        logger.info(f"Excluding table {table_dict['table_name']} (matches pattern '{pattern}')")
                        skip = True
                        break
                if skip:
                    continue

            result.append(table_dict)

        logger.info(f"Found {len(result)} tables in schema '{schema_name}'")
        return result

    def get_columns(self, table_object_id: int) -> List[Dict[str, Any]]:
        """
        Get all columns for a specific table.

        Args:
            table_object_id: SQL Server object_id of the table

        Returns:
            List of column information dictionaries
        """
        query = """
        SELECT
            c.column_id,
            c.name AS column_name,
            t.name AS data_type,
            c.max_length,
            c.precision,
            c.scale,
            c.is_nullable,
            c.is_identity,
            c.is_computed,
            ic.seed_value,
            ic.increment_value,
            dc.definition AS default_value,
            cc.definition AS computed_definition,
            CASE
                WHEN t.name IN ('varchar', 'nvarchar', 'varbinary') AND c.max_length = -1
                THEN 1
                ELSE 0
            END AS is_max
        FROM sys.columns c
        INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
        LEFT JOIN sys.identity_columns ic ON c.object_id = ic.object_id AND c.column_id = ic.column_id
        LEFT JOIN sys.default_constraints dc ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
        LEFT JOIN sys.computed_columns cc ON c.object_id = cc.object_id AND c.column_id = cc.column_id
        WHERE c.object_id = %s
        ORDER BY c.column_id
        """

        columns = self.mssql_hook.get_records(query, parameters=[table_object_id])

        result = []
        for col in columns:
            column_dict = {
                'column_id': col[0],
                'column_name': col[1],
                'data_type': col[2],
                'max_length': col[3],
                'precision': col[4],
                'scale': col[5],
                'is_nullable': bool(col[6]),
                'is_identity': bool(col[7]),
                'is_computed': bool(col[8]),
                'seed_value': col[9],
                'increment_value': col[10],
                'default_value': col[11],
                'computed_definition': col[12],
                'is_max': bool(col[13]),
            }
            result.append(column_dict)

        return result

    def get_primary_key(self, table_object_id: int) -> Optional[Dict[str, Any]]:
        """
        Get primary key information for a table.

        Args:
            table_object_id: SQL Server object_id of the table

        Returns:
            Primary key information or None if no primary key exists
        """
        query = """
        SELECT
            i.name AS constraint_name,
            STUFF((
                SELECT ', ' + c2.name
                FROM sys.index_columns ic2
                INNER JOIN sys.columns c2 ON ic2.object_id = c2.object_id AND ic2.column_id = c2.column_id
                WHERE ic2.object_id = i.object_id AND ic2.index_id = i.index_id
                ORDER BY ic2.key_ordinal
                FOR XML PATH('')
            ), 1, 2, '') AS columns
        FROM sys.indexes i
        WHERE i.is_primary_key = 1
          AND i.object_id = %s
        """

        result = self.mssql_hook.get_first(query, parameters=[table_object_id])

        if result:
            # Handle bytes from FOR XML PATH
            columns_str = result[1].decode('utf-8') if isinstance(result[1], bytes) else result[1]
            return {
                'constraint_name': result[0],
                'columns': [col.strip() for col in columns_str.split(',')]
            }
        return None

    def get_primary_key_columns(self, table_object_id: int) -> Optional[Dict[str, Any]]:
        """
        Get detailed primary key column information including data types.

        Args:
            table_object_id: SQL Server object_id of the table

        Returns:
            Dict with pk_columns list (name, data_type, ordinal) and metadata,
            or None if no primary key exists
        """
        query = """
        SELECT
            c.name AS column_name,
            t.name AS data_type,
            ic.key_ordinal
        FROM sys.indexes i
        INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
        INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
        WHERE i.is_primary_key = 1
          AND i.object_id = %s
        ORDER BY ic.key_ordinal
        """
        results = self.mssql_hook.get_records(query, parameters=[table_object_id])

        if not results:
            return None

        integer_types = {'int', 'bigint', 'smallint', 'tinyint'}
        columns = [
            {'name': row[0], 'data_type': row[1], 'ordinal': row[2]}
            for row in results
        ]

        return {
            'columns': columns,
            'is_composite': len(results) > 1,
            'is_all_integer': all(col['data_type'].lower() in integer_types for col in columns)
        }

    def get_indexes(self, table_object_id: int) -> List[Dict[str, Any]]:
        """
        Get all indexes for a table (excluding primary key).

        Args:
            table_object_id: SQL Server object_id of the table

        Returns:
            List of index information dictionaries
        """
        query = """
        SELECT
            i.name AS index_name,
            i.is_unique,
            i.type_desc,
            STUFF((
                SELECT ', ' + c2.name
                FROM sys.index_columns ic2
                INNER JOIN sys.columns c2 ON ic2.object_id = c2.object_id AND ic2.column_id = c2.column_id
                WHERE ic2.object_id = i.object_id AND ic2.index_id = i.index_id AND ic2.is_included_column = 0
                ORDER BY ic2.key_ordinal
                FOR XML PATH('')
            ), 1, 2, '') AS columns,
            STUFF((
                SELECT ', ' + c2.name + CASE ic2.is_descending_key WHEN 1 THEN ' DESC' ELSE ' ASC' END
                FROM sys.index_columns ic2
                INNER JOIN sys.columns c2 ON ic2.object_id = c2.object_id AND ic2.column_id = c2.column_id
                WHERE ic2.object_id = i.object_id AND ic2.index_id = i.index_id AND ic2.is_included_column = 0
                ORDER BY ic2.key_ordinal
                FOR XML PATH('')
            ), 1, 2, '') AS columns_with_order
        FROM sys.indexes i
        WHERE i.object_id = %s
          AND i.is_primary_key = 0  -- Exclude primary key
          AND i.type > 0  -- Exclude heap
        """

        indexes = self.mssql_hook.get_records(query, parameters=[table_object_id])

        result = []
        for idx in indexes:
            # Handle bytes from FOR XML PATH
            columns_str = idx[3].decode('utf-8') if isinstance(idx[3], bytes) else idx[3]
            columns_order_str = idx[4].decode('utf-8') if isinstance(idx[4], bytes) else idx[4]
            index_dict = {
                'index_name': idx[0],
                'is_unique': bool(idx[1]),
                'type_desc': idx[2],
                'columns': [col.strip() for col in columns_str.split(',')] if columns_str else [],
                'columns_with_order': columns_order_str if columns_order_str else '',
            }
            result.append(index_dict)

        return result

    def get_foreign_keys(self, table_object_id: int) -> List[Dict[str, Any]]:
        """
        Get all foreign keys for a table.

        Args:
            table_object_id: SQL Server object_id of the table

        Returns:
            List of foreign key information dictionaries
        """
        query = """
        SELECT
            fk.name AS constraint_name,
            STUFF((
                SELECT ', ' + c1.name
                FROM sys.foreign_key_columns fkc2
                INNER JOIN sys.columns c1 ON fkc2.parent_object_id = c1.object_id AND fkc2.parent_column_id = c1.column_id
                WHERE fkc2.constraint_object_id = fk.object_id
                ORDER BY fkc2.constraint_column_id
                FOR XML PATH('')
            ), 1, 2, '') AS columns,
            OBJECT_SCHEMA_NAME(fk.referenced_object_id) AS referenced_schema,
            OBJECT_NAME(fk.referenced_object_id) AS referenced_table,
            STUFF((
                SELECT ', ' + c2.name
                FROM sys.foreign_key_columns fkc2
                INNER JOIN sys.columns c2 ON fkc2.referenced_object_id = c2.object_id AND fkc2.referenced_column_id = c2.column_id
                WHERE fkc2.constraint_object_id = fk.object_id
                ORDER BY fkc2.constraint_column_id
                FOR XML PATH('')
            ), 1, 2, '') AS referenced_columns,
            fk.delete_referential_action_desc AS on_delete,
            fk.update_referential_action_desc AS on_update
        FROM sys.foreign_keys fk
        WHERE fk.parent_object_id = %s
        """

        foreign_keys = self.mssql_hook.get_records(query, parameters=[table_object_id])

        result = []
        for fk in foreign_keys:
            # Handle bytes from FOR XML PATH
            columns_str = fk[1].decode('utf-8') if isinstance(fk[1], bytes) else fk[1]
            ref_columns_str = fk[4].decode('utf-8') if isinstance(fk[4], bytes) else fk[4]
            fk_dict = {
                'constraint_name': fk[0],
                'columns': [col.strip() for col in columns_str.split(',')] if columns_str else [],
                'referenced_schema': fk[2],
                'referenced_table': fk[3],
                'referenced_columns': [col.strip() for col in ref_columns_str.split(',')] if ref_columns_str else [],
                'on_delete': fk[5].replace('_', ' ') if fk[5] else 'NO ACTION',  # Convert NO_ACTION to NO ACTION
                'on_update': fk[6].replace('_', ' ') if fk[6] else 'NO ACTION',
            }
            result.append(fk_dict)

        return result

    def get_check_constraints(self, table_object_id: int) -> List[Dict[str, Any]]:
        """
        Get all check constraints for a table.

        Args:
            table_object_id: SQL Server object_id of the table

        Returns:
            List of check constraint information dictionaries
        """
        query = """
        SELECT
            cc.name AS constraint_name,
            cc.definition,
            cc.is_disabled
        FROM sys.check_constraints cc
        WHERE cc.parent_object_id = %s
        """

        constraints = self.mssql_hook.get_records(query, parameters=[table_object_id])

        result = []
        for con in constraints:
            constraint_dict = {
                'constraint_name': con[0],
                'definition': con[1],
                'is_disabled': bool(con[2]),
            }
            result.append(constraint_dict)

        return result

    def get_table_schema(self, schema_name: str, table_name: str) -> Dict[str, Any]:
        """
        Get complete schema information for a single table.

        Args:
            schema_name: Schema name
            table_name: Table name

        Returns:
            Complete table schema information
        """
        # Get table object_id
        query = """
        SELECT object_id
        FROM sys.tables t
        INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name = %s AND t.name = %s
        """

        result = self.mssql_hook.get_first(query, parameters=[schema_name, table_name])

        if not result:
            raise ValueError(f"Table {schema_name}.{table_name} not found")

        object_id = result[0]

        # Get all schema components
        schema_info = {
            'schema_name': schema_name,
            'table_name': table_name,
            'object_id': object_id,
            'columns': self.get_columns(object_id),
            'primary_key': self.get_primary_key(object_id),
            'indexes': self.get_indexes(object_id),
            'foreign_keys': self.get_foreign_keys(object_id),
            'check_constraints': self.get_check_constraints(object_id),
        }

        return schema_info

    def get_all_tables_schema(self, schema_name: str = 'dbo', exclude_patterns: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        Get complete schema information for all tables in a schema.

        Args:
            schema_name: Schema name to extract from
            exclude_patterns: List of table name patterns to exclude

        Returns:
            List of complete table schema information
        """
        tables = self.get_tables(schema_name, exclude_patterns)
        result = []

        for table in tables:
            logger.info(f"Extracting schema for table {table['table_name']}")
            table_schema = {
                'schema_name': table['schema_name'],
                'table_name': table['table_name'],
                'object_id': table['object_id'],
                'row_count': table['row_count'],
                'columns': self.get_columns(table['object_id']),
                'primary_key': self.get_primary_key(table['object_id']),
                'pk_columns': self.get_primary_key_columns(table['object_id']),
                'indexes': self.get_indexes(table['object_id']),
                'foreign_keys': self.get_foreign_keys(table['object_id']),
                'check_constraints': self.get_check_constraints(table['object_id']),
            }
            result.append(table_schema)

        logger.info(f"Extracted schema for {len(result)} tables")
        # Clean any remaining bytes from the result
        return self._clean_bytes_recursive(result)

    def _clean_bytes_recursive(self, obj):
        """Recursively convert any bytes objects to strings and sanitize for PostgreSQL JSON storage."""
        import re
        import struct
        if isinstance(obj, bytes):
            # Try to interpret as little-endian integer first (common for SQL Server identity columns)
            if len(obj) == 4:
                try:
                    # Convert 4-byte little-endian integer
                    return str(struct.unpack('<i', obj)[0])
                except Exception:
                    pass
            elif len(obj) == 8:
                try:
                    # Convert 8-byte little-endian integer
                    return str(struct.unpack('<q', obj)[0])
                except Exception:
                    pass
            # Try UTF-8 decoding for regular strings
            try:
                decoded = obj.decode('utf-8')
                # Clean any control characters that made it through
                cleaned = ''.join(c if ord(c) >= 32 or c in '\n\r\t' else '' for c in decoded)
                return cleaned
            except Exception:
                # Fall back to hex representation for binary data
                return obj.hex()
        elif isinstance(obj, str):
            # Replace any control characters (including null bytes)
            cleaned = ''.join(c if ord(c) >= 32 or c in '\n\r\t' else '' for c in obj)
            return cleaned
        elif isinstance(obj, dict):
            return {k: self._clean_bytes_recursive(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._clean_bytes_recursive(item) for item in obj]
        elif isinstance(obj, tuple):
            return tuple(self._clean_bytes_recursive(item) for item in obj)
        else:
            return obj

    def _matches_pattern(self, name: str, pattern: str) -> bool:
        """
        Check if a name matches a pattern with wildcard support.

        Args:
            name: Name to check
            pattern: Pattern with optional wildcards (*)

        Returns:
            True if name matches pattern
        """
        import fnmatch
        return fnmatch.fnmatch(name.lower(), pattern.lower())


def extract_schema_info(
    mssql_conn_id: str,
    schema_name: str = 'dbo',
    exclude_tables: Optional[List[str]] = None
) -> List[Dict[str, Any]]:
    """
    Convenience function to extract complete schema information.

    Args:
        mssql_conn_id: Airflow connection ID for SQL Server
        schema_name: Schema name to extract from
        exclude_tables: List of table patterns to exclude

    Returns:
        List of complete table schema information
    """
    extractor = SchemaExtractor(mssql_conn_id)
    return extractor.get_all_tables_schema(schema_name, exclude_tables)