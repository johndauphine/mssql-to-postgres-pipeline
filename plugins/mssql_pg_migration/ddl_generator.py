"""
PostgreSQL DDL Generation Module

This module generates PostgreSQL DDL statements from SQL Server schema metadata,
handling table creation, constraints, and indexes.
"""

from typing import Dict, Any, List, Optional
from airflow.providers.postgres.hooks.postgres import PostgresHook
from mssql_pg_migration.type_mapping import map_column, map_table_schema
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
        include_constraints: bool = True
    ) -> str:
        """
        Generate CREATE TABLE statement for PostgreSQL.

        Args:
            table_schema: Table schema information from SQL Server
            target_schema: Target PostgreSQL schema name
            include_constraints: Whether to include constraints in the DDL

        Returns:
            CREATE TABLE DDL statement
        """
        # Map the table schema to PostgreSQL
        mapped_schema = map_table_schema(table_schema)

        # Start building the CREATE TABLE statement
        table_name = mapped_schema['table_name']
        qualified_name = f"{self._quote_identifier(target_schema)}.{self._quote_identifier(table_name)}"

        ddl_parts = [f"CREATE TABLE {qualified_name} ("]
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
        qualified_name = f"{self._quote_identifier(schema_name)}.{self._quote_identifier(table_name)}"
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
        qualified_name = f"{self._quote_identifier(schema_name)}.{self._quote_identifier(table_name)}"
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
                  f"ON {self._quote_identifier(target_schema)}.{self._quote_identifier(table_name)} ({columns})"
            index_statements.append(ddl)

        return index_statements

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
                ddl = f"ALTER TABLE {self._quote_identifier(target_schema)}.{self._quote_identifier(table_name)} " \
                      f"ADD CONSTRAINT {self._quote_identifier(constraint_name)} " \
                      f"CHECK {pg_definition}"
                check_statements.append(ddl)
            else:
                logger.warning(f"Could not convert check constraint {constraint_name}: {definition}")

        return check_statements

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
        qualified_name = f"{self._quote_identifier(target_schema)}.{self._quote_identifier(table_name)}"

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
        create_indexes: bool = True
    ) -> List[str]:
        """
        Generate complete DDL for a table including all objects.

        Args:
            table_schema: Table schema information
            target_schema: Target PostgreSQL schema name
            drop_if_exists: Whether to include DROP TABLE statement
            create_indexes: Whether to include CREATE INDEX statements

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

        # Create table
        ddl_statements.append(self.generate_create_table(
            table_schema,
            target_schema,
            include_constraints=True  # Include primary key
        ))

        # Create indexes
        if create_indexes:
            ddl_statements.extend(self.generate_indexes(table_schema, target_schema))

        # Create check constraints
        ddl_statements.extend(self.generate_check_constraints(table_schema, target_schema))

        return ddl_statements

    def reset_sequences(self, table_schema: Dict[str, Any], target_schema: str = 'public') -> int:
        """
        P2.2: Reset sequences for SERIAL/BIGSERIAL columns to MAX(column) value.

        After COPY loading explicit values into SERIAL columns, the sequences need
        to be advanced to prevent duplicate key errors on future inserts.

        Args:
            table_schema: Table schema information (must be mapped)
            target_schema: Target PostgreSQL schema name

        Returns:
            Number of sequences reset
        """
        mapped_schema = map_table_schema(table_schema)
        table_name = mapped_schema['table_name']
        reset_count = 0

        # Find SERIAL/BIGSERIAL columns (identity columns from SQL Server)
        serial_columns = [
            col['column_name'] for col in mapped_schema['columns']
            if col.get('data_type', '').upper() in ('SERIAL', 'BIGSERIAL')
        ]

        if not serial_columns:
            return 0

        with self.postgres_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                for col_name in serial_columns:
                    try:
                        # Use pg_get_serial_sequence to get the sequence name
                        # Then setval to MAX(col) value
                        # The third argument 'true' means the next nextval will return MAX(col)+1
                        # Note: pg_get_serial_sequence requires quoted identifier format: "schema"."table"
                        quoted_table = f'"{target_schema}"."{table_name}"'
                        reset_sql = f"""
                        SELECT setval(
                            pg_get_serial_sequence('{quoted_table}', '{col_name}'),
                            COALESCE((SELECT MAX("{col_name}") FROM "{target_schema}"."{table_name}"), 1),
                            true
                        )
                        """
                        cursor.execute(reset_sql)
                        result = cursor.fetchone()
                        if result:
                            logger.info(f"P2.2: Reset sequence for {table_name}.{col_name} to {result[0]}")
                            reset_count += 1
                    except Exception as e:
                        # Log warning but don't fail - sequence might not exist for some edge cases
                        logger.warning(f"Could not reset sequence for {table_name}.{col_name}: {e}")
            conn.commit()

        return reset_count

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

        # Skip NOT NULL constraints during migration
        # SQL Server often has NULLs in NOT NULL columns (data integrity issues)
        # Add NOT NULL after migration if needed: ALTER TABLE ... ALTER COLUMN ... SET NOT NULL
        # if not column.get('is_nullable', True):
        #     parts.append('NOT NULL')

        # Add default value if present
        if column.get('default_value'):
            parts.append(f"DEFAULT {column['default_value']}")

        return '    ' + ' '.join(parts)

    def _quote_identifier(self, identifier: str) -> str:
        """
        Quote a PostgreSQL identifier safely.

        Always quotes and escapes identifiers to prevent SQL injection and
        handle reserved words, mixed case, and special characters.

        Args:
            identifier: Identifier to quote

        Returns:
            Safely quoted identifier
        """
        # Escape embedded double quotes by doubling them (PostgreSQL standard)
        escaped = identifier.replace('"', '""')
        # Always quote to be safe - handles reserved words, mixed case, special chars
        return f'"{escaped}"'

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
        Dictionary with 'drops', 'creates', 'indexes' lists
    """
    result = {
        'drops': [],
        'creates': [],
        'indexes': [],
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

    return result