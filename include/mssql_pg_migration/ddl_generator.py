"""
PostgreSQL DDL Generation Module

This module generates PostgreSQL DDL statements from SQL Server schema metadata,
handling table creation, constraints, and indexes.
"""

from typing import Dict, Any, List, Optional
from airflow.providers.postgres.hooks.postgres import PostgresHook
from .type_mapping import map_column, map_table_schema
import logging

logger = logging.getLogger(__name__)


class DDLGenerator:
    """Generate PostgreSQL DDL statements from schema metadata."""

    def __init__(self, postgres_conn_id: str):
        """
        Initialize the DDL generator.

        Args:
            postgres_conn_id: Airflow connection ID for PostgreSQL
        """
        self.postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)

    def generate_create_table(
        self,
        table_schema: Dict[str, Any],
        target_schema: str = 'public',
        include_constraints: bool = True,
        unlogged: bool = False
    ) -> str:
        """
        Generate CREATE TABLE statement for PostgreSQL.

        Args:
            table_schema: Table schema information from SQL Server
            target_schema: Target PostgreSQL schema name
            include_constraints: Whether to include constraints in the DDL
            unlogged: Whether to create table as UNLOGGED (faster bulk loads, no WAL)

        Returns:
            CREATE TABLE DDL statement
        """
        # Map the table schema to PostgreSQL
        mapped_schema = map_table_schema(table_schema)

        # Start building the CREATE TABLE statement
        table_name = mapped_schema['table_name']
        qualified_name = f"{target_schema}.{self._quote_identifier(table_name)}"

        unlogged_clause = "UNLOGGED " if unlogged else ""
        ddl_parts = [f"CREATE {unlogged_clause}TABLE {qualified_name} ("]
        column_definitions = []

        # Add column definitions
        for column in mapped_schema['columns']:
            col_def = self._generate_column_definition(column)
            column_definitions.append(col_def)

        # Add primary key constraint if exists and include_constraints is True
        if include_constraints and mapped_schema.get('primary_key'):
            pk = mapped_schema['primary_key']
            # Handle both dict format (from schema extractor) and list format
            if isinstance(pk, dict):
                pk_column_list = pk.get('columns', [])
            else:
                pk_column_list = pk
            pk_columns = ', '.join([self._quote_identifier(col) for col in pk_column_list])
            pk_constraint = f"CONSTRAINT {self._quote_identifier(f'pk_{table_name}')} PRIMARY KEY ({pk_columns})"
            column_definitions.append(pk_constraint)

        # Join all definitions
        ddl_parts.append(',\n    '.join(column_definitions))
        ddl_parts.append(')')

        return '\n'.join(ddl_parts)

    def generate_drop_table(self, table_name: str, schema_name: str = 'public', cascade: bool = True) -> str:
        """
        Generate DROP TABLE statement.

        Args:
            table_name: Table name to drop
            schema_name: Schema name
            cascade: Whether to use CASCADE option

        Returns:
            DROP TABLE DDL statement
        """
        qualified_name = f"{schema_name}.{self._quote_identifier(table_name)}"
        cascade_clause = " CASCADE" if cascade else ""
        return f"DROP TABLE IF EXISTS {qualified_name}{cascade_clause}"

    def generate_truncate_table(self, table_name: str, schema_name: str = 'public', cascade: bool = True) -> str:
        """
        Generate TRUNCATE TABLE statement.

        Args:
            table_name: Table name to truncate
            schema_name: Schema name
            cascade: Whether to use CASCADE option

        Returns:
            TRUNCATE TABLE DDL statement
        """
        qualified_name = f"{schema_name}.{self._quote_identifier(table_name)}"
        cascade_clause = " CASCADE" if cascade else ""
        return f"TRUNCATE TABLE {qualified_name}{cascade_clause}"

    def generate_indexes(self, table_schema: Dict[str, Any], target_schema: str = 'public') -> List[str]:
        """
        Generate CREATE INDEX statements for a table.

        Args:
            table_schema: Table schema information
            target_schema: Target PostgreSQL schema name

        Returns:
            List of CREATE INDEX DDL statements
        """
        mapped_schema = map_table_schema(table_schema)
        table_name = mapped_schema['table_name']
        index_statements = []

        for index in mapped_schema.get('indexes', []):
            index_name = index['index_name']
            columns = ', '.join([self._quote_identifier(col) for col in index['columns']])
            unique_clause = "UNIQUE " if index.get('is_unique') else ""

            ddl = f"CREATE {unique_clause}INDEX {self._quote_identifier(index_name)} " \
                  f"ON {target_schema}.{self._quote_identifier(table_name)} ({columns})"
            index_statements.append(ddl)

        return index_statements

    def generate_foreign_keys(self, table_schema: Dict[str, Any], target_schema: str = 'public') -> List[str]:
        """
        Generate ALTER TABLE ADD CONSTRAINT statements for foreign keys.

        Args:
            table_schema: Table schema information
            target_schema: Target PostgreSQL schema name

        Returns:
            List of ALTER TABLE DDL statements for foreign keys
        """
        mapped_schema = map_table_schema(table_schema)
        table_name = mapped_schema['table_name']
        fk_statements = []

        for fk in mapped_schema.get('foreign_keys', []):
            constraint_name = fk['constraint_name']
            columns = ', '.join([self._quote_identifier(col) for col in fk['columns']])
            ref_table = fk['referenced_table']
            ref_columns = ', '.join([self._quote_identifier(col) for col in fk['referenced_columns']])

            # Map referential actions
            on_delete = fk.get('on_delete', 'NO ACTION')
            on_update = fk.get('on_update', 'NO ACTION')

            ddl = f"ALTER TABLE {target_schema}.{self._quote_identifier(table_name)} " \
                  f"ADD CONSTRAINT {self._quote_identifier(constraint_name)} " \
                  f"FOREIGN KEY ({columns}) " \
                  f"REFERENCES {target_schema}.{self._quote_identifier(ref_table)} ({ref_columns}) " \
                  f"ON DELETE {on_delete} ON UPDATE {on_update}"
            fk_statements.append(ddl)

        return fk_statements

    def generate_check_constraints(self, table_schema: Dict[str, Any], target_schema: str = 'public') -> List[str]:
        """
        Generate ALTER TABLE ADD CONSTRAINT statements for check constraints.

        Args:
            table_schema: Table schema information
            target_schema: Target PostgreSQL schema name

        Returns:
            List of ALTER TABLE DDL statements for check constraints
        """
        table_name = table_schema['table_name']
        check_statements = []

        for check in table_schema.get('check_constraints', []):
            if check.get('is_disabled'):
                continue  # Skip disabled constraints

            constraint_name = check['constraint_name']
            definition = check['definition']

            # Try to convert SQL Server check constraint syntax to PostgreSQL
            # This is a simplified conversion - complex constraints may need manual review
            pg_definition = self._convert_check_constraint(definition)

            if pg_definition:
                ddl = f"ALTER TABLE {target_schema}.{self._quote_identifier(table_name)} " \
                      f"ADD CONSTRAINT {self._quote_identifier(constraint_name)} " \
                      f"CHECK {pg_definition}"
                check_statements.append(ddl)
            else:
                logger.warning(f"Could not convert check constraint {constraint_name}: {definition}")

        return check_statements

    def generate_set_logged(self, table_name: str, schema_name: str = 'public') -> str:
        """
        Generate ALTER TABLE SET LOGGED statement.

        Use this after bulk loading data into UNLOGGED tables to convert them
        back to regular logged tables for durability.

        Args:
            table_name: Table name to convert
            schema_name: Schema name

        Returns:
            ALTER TABLE SET LOGGED DDL statement
        """
        qualified_name = f"{schema_name}.{self._quote_identifier(table_name)}"
        return f"ALTER TABLE {qualified_name} SET LOGGED"

    def generate_primary_key(self, table_schema: Dict[str, Any], target_schema: str = 'public') -> Optional[str]:
        """
        Generate ALTER TABLE ADD PRIMARY KEY statement.

        Use this after bulk loading data to add the primary key constraint.
        Building PK index after data load is faster than maintaining it during inserts.

        Args:
            table_schema: Table schema information
            target_schema: Target PostgreSQL schema name

        Returns:
            ALTER TABLE ADD PRIMARY KEY DDL statement, or None if no PK defined
        """
        mapped_schema = map_table_schema(table_schema)
        table_name = mapped_schema['table_name']
        qualified_name = f"{target_schema}.{self._quote_identifier(table_name)}"

        pk = mapped_schema.get('primary_key')
        if not pk:
            return None

        # Handle both dict format (from schema extractor) and list format
        if isinstance(pk, dict):
            pk_column_list = pk.get('columns', [])
        else:
            pk_column_list = pk

        if not pk_column_list:
            return None

        pk_columns = ', '.join([self._quote_identifier(col) for col in pk_column_list])
        return f"ALTER TABLE {qualified_name} ADD CONSTRAINT {self._quote_identifier(f'pk_{table_name}')} PRIMARY KEY ({pk_columns})"

    def generate_complete_ddl(
        self,
        table_schema: Dict[str, Any],
        target_schema: str = 'public',
        drop_if_exists: bool = True,
        create_indexes: bool = True,
        create_foreign_keys: bool = False,  # Usually done after all tables are created
        unlogged: bool = False  # Create as UNLOGGED for faster bulk loads
    ) -> List[str]:
        """
        Generate complete DDL for a table including all objects.

        Args:
            table_schema: Table schema information
            target_schema: Target PostgreSQL schema name
            drop_if_exists: Whether to include DROP TABLE statement
            create_indexes: Whether to include CREATE INDEX statements
            create_foreign_keys: Whether to include foreign key constraints
            unlogged: Whether to create table as UNLOGGED (faster bulk loads)

        Returns:
            List of DDL statements in execution order
        """
        ddl_statements = []

        # Drop table if requested
        if drop_if_exists:
            ddl_statements.append(self.generate_drop_table(
                table_schema['table_name'],
                target_schema,
                cascade=True
            ))

        # Create table (optionally as UNLOGGED for faster bulk loads)
        ddl_statements.append(self.generate_create_table(
            table_schema,
            target_schema,
            include_constraints=True,  # Include primary key
            unlogged=unlogged
        ))

        # Create indexes
        if create_indexes:
            ddl_statements.extend(self.generate_indexes(table_schema, target_schema))

        # Create check constraints
        ddl_statements.extend(self.generate_check_constraints(table_schema, target_schema))

        # Create foreign keys (if requested)
        if create_foreign_keys:
            ddl_statements.extend(self.generate_foreign_keys(table_schema, target_schema))

        return ddl_statements

    def execute_ddl(self, ddl_statements: List[str], transaction: bool = True) -> None:
        """
        Execute DDL statements in PostgreSQL.

        Args:
            ddl_statements: List of DDL statements to execute
            transaction: Whether to execute in a single transaction
        """
        if transaction:
            # Execute all statements in a single transaction
            with self.postgres_hook.get_conn() as conn:
                with conn.cursor() as cursor:
                    for ddl in ddl_statements:
                        logger.info(f"Executing DDL: {ddl[:100]}...")
                        cursor.execute(ddl)
                conn.commit()
        else:
            # Execute each statement separately with autocommit
            for ddl in ddl_statements:
                logger.info(f"Executing DDL: {ddl[:100]}...")
                self.postgres_hook.run(ddl, autocommit=True)

    def _generate_column_definition(self, column: Dict[str, Any]) -> str:
        """
        Generate a column definition for CREATE TABLE.

        Args:
            column: Mapped column information

        Returns:
            Column definition string
        """
        parts = [
            self._quote_identifier(column['column_name']),
            column['data_type']
        ]

        # Add NOT NULL constraint
        # Skip NOT NULL for TEXT columns as source data may have integrity issues
        # (e.g., SQL Server nvarchar(max) columns with NULL values despite NOT NULL constraint)
        if not column.get('is_nullable', True) and column['data_type'].upper() != 'TEXT':
            parts.append('NOT NULL')

        # Add default value if present
        if column.get('default_value'):
            parts.append(f"DEFAULT {column['default_value']}")

        return '    ' + ' '.join(parts)

    def _quote_identifier(self, identifier: str) -> str:
        """
        Quote a PostgreSQL identifier if necessary.

        Args:
            identifier: Identifier to quote

        Returns:
            Quoted identifier
        """
        # List of PostgreSQL reserved words (simplified)
        reserved_words = {
            'user', 'order', 'group', 'table', 'column', 'select', 'insert',
            'update', 'delete', 'where', 'from', 'join', 'left', 'right',
            'inner', 'outer', 'cross', 'natural', 'using', 'on', 'as', 'case',
            'when', 'then', 'else', 'end', 'null', 'not', 'and', 'or', 'in',
            'exists', 'like', 'between', 'distinct', 'having', 'limit', 'offset'
        }

        # Quote if reserved word, contains special characters, or starts with number
        if (identifier.lower() in reserved_words or
            not identifier.replace('_', '').isalnum() or
            identifier[0].isdigit()):
            return f'"{identifier}"'
        return identifier

    def _convert_check_constraint(self, sql_server_definition: str) -> Optional[str]:
        """
        Convert SQL Server check constraint to PostgreSQL syntax.

        Args:
            sql_server_definition: SQL Server check constraint definition

        Returns:
            PostgreSQL check constraint definition or None if cannot convert
        """
        # Remove outer parentheses if present
        definition = sql_server_definition.strip()
        if definition.startswith('(') and definition.endswith(')'):
            definition = definition[1:-1].strip()

        # Basic conversions (this is simplified - complex constraints need more work)
        conversions = {
            'getdate()': 'CURRENT_TIMESTAMP',
            'len(': 'length(',
            'isnull(': 'coalesce(',
            ' N\'': ' \'',  # Remove N prefix from strings
        }

        for old, new in conversions.items():
            definition = definition.replace(old, new)

        # Wrap column names in the definition with quotes if needed
        # This is a simplified approach - a proper parser would be better
        # For now, return the definition as-is and let PostgreSQL validate it
        return f"({definition})"


def create_tables_ddl(
    tables_schema: List[Dict[str, Any]],
    target_schema: str = 'public',
    drop_if_exists: bool = True
) -> Dict[str, List[str]]:
    """
    Generate DDL for multiple tables, organizing by dependency order.

    Args:
        tables_schema: List of table schema information
        target_schema: Target PostgreSQL schema name
        drop_if_exists: Whether to include DROP statements

    Returns:
        Dictionary with 'drops', 'creates', 'indexes', 'foreign_keys' lists
    """
    result = {
        'drops': [],
        'creates': [],
        'indexes': [],
        'foreign_keys': [],
    }

    # Temporary generator instance (connection not used for generation)
    generator = DDLGenerator.__new__(DDLGenerator)

    for table_schema in tables_schema:
        table_name = table_schema['table_name']

        # Generate DROP statements (in reverse order for dependencies)
        if drop_if_exists:
            result['drops'].insert(0, generator.generate_drop_table(table_name, target_schema, cascade=True))

        # Generate CREATE TABLE statements
        result['creates'].append(generator.generate_create_table(
            table_schema,
            target_schema,
            include_constraints=True  # Primary keys only
        ))

        # Generate indexes
        result['indexes'].extend(generator.generate_indexes(table_schema, target_schema))

        # Generate foreign keys (to be added after all tables are created)
        result['foreign_keys'].extend(generator.generate_foreign_keys(table_schema, target_schema))

    return result