"""
ODBC Connection Helper

This module provides a lightweight replacement for MsSqlHook that works with
just pyodbc and BaseHook, eliminating the need for apache-airflow-providers-microsoft-mssql.

This is useful for corporate environments where the MSSQL provider may not be available.
"""

from typing import Any, List, Optional, Tuple
from airflow.hooks.base import BaseHook
import pyodbc
import logging

logger = logging.getLogger(__name__)


class OdbcConnectionHelper:
    """
    Helper class for ODBC connections that mimics MsSqlHook interface.

    This class provides the same methods as MsSqlHook (get_records, get_first, run)
    but uses pyodbc directly instead of requiring the MSSQL provider.

    Optionally uses a connection pool for better resource management.
    """

    def __init__(self, odbc_conn_id: str, pool=None):
        """
        Initialize the ODBC connection helper.

        Args:
            odbc_conn_id: Airflow connection ID for the database
            pool: Optional MssqlConnectionPool for connection reuse
        """
        self.conn_id = odbc_conn_id
        self._conn_config = None
        self._pool = pool

    def _get_connection_config(self) -> dict:
        """
        Get connection configuration from Airflow connection.

        Returns:
            Dictionary with ODBC connection parameters
        """
        if self._conn_config is None:
            conn = BaseHook.get_connection(self.conn_id)

            # Build ODBC connection string parameters
            port = conn.port or 1433
            server = f"{conn.host},{port}" if port != 1433 else conn.host

            self._conn_config = {
                'DRIVER': '{ODBC Driver 18 for SQL Server}',
                'SERVER': server,
                'DATABASE': conn.schema,
                'TrustServerCertificate': 'yes',
            }

            # Add authentication - support both SQL Auth and Windows Auth
            if conn.login:
                # SQL Server Authentication
                self._conn_config['UID'] = conn.login
                self._conn_config['PWD'] = conn.password or ''
                self._conn_config['Trusted_Connection'] = 'no'
            else:
                # Windows Authentication (Kerberos)
                self._conn_config['Trusted_Connection'] = 'yes'

        return self._conn_config

    def _build_connection_string(self) -> str:
        """
        Build ODBC connection string from configuration.

        Returns:
            ODBC connection string
        """
        config = self._get_connection_config()
        return ';'.join([f"{k}={v}" for k, v in config.items() if v])

    def set_pool(self, pool) -> None:
        """
        Set the connection pool for this helper.

        Args:
            pool: MssqlConnectionPool instance
        """
        self._pool = pool

    def get_conn(self) -> pyodbc.Connection:
        """
        Get a pyodbc connection to the database.

        If a pool is configured, acquires from pool. Otherwise creates a new connection.

        Returns:
            pyodbc Connection object
        """
        if self._pool:
            return self._pool.acquire()
        conn_str = self._build_connection_string()
        return pyodbc.connect(conn_str)

    def release_conn(self, conn: pyodbc.Connection) -> None:
        """
        Release a connection back to the pool or close it.

        Args:
            conn: Connection to release
        """
        if conn is None:
            return
        if self._pool:
            self._pool.release(conn)
        else:
            conn.close()

    def get_records(
        self,
        sql: str,
        parameters: Optional[List[Any]] = None
    ) -> List[Tuple[Any, ...]]:
        """
        Execute a query and return all rows as a list of tuples.

        This mimics the MsSqlHook.get_records() method.

        Args:
            sql: SQL query to execute
            parameters: Optional list of parameters for the query

        Returns:
            List of tuples, one per row
        """
        conn = None
        try:
            conn = self.get_conn()
            cursor = conn.cursor()

            if parameters:
                cursor.execute(sql, parameters)
            else:
                cursor.execute(sql)

            rows = cursor.fetchall()
            return rows
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            logger.error(f"Query: {sql}")
            if parameters:
                logger.error(f"Parameters: {parameters}")
            raise
        finally:
            self.release_conn(conn)

    def get_first(
        self,
        sql: str,
        parameters: Optional[List[Any]] = None
    ) -> Optional[Tuple[Any, ...]]:
        """
        Execute a query and return the first row as a tuple.

        This mimics the MsSqlHook.get_first() method.

        Args:
            sql: SQL query to execute
            parameters: Optional list of parameters for the query

        Returns:
            First row as a tuple, or None if no rows
        """
        conn = None
        try:
            conn = self.get_conn()
            cursor = conn.cursor()

            if parameters:
                cursor.execute(sql, parameters)
            else:
                cursor.execute(sql)

            row = cursor.fetchone()
            return row
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            logger.error(f"Query: {sql}")
            if parameters:
                logger.error(f"Parameters: {parameters}")
            raise
        finally:
            self.release_conn(conn)

    def run(
        self,
        sql: str,
        parameters: Optional[List[Any]] = None,
        autocommit: bool = False
    ) -> None:
        """
        Execute a SQL statement (typically DDL or DML).

        This mimics the MsSqlHook.run() method.

        Args:
            sql: SQL statement to execute
            parameters: Optional list of parameters for the query
            autocommit: Whether to commit automatically
        """
        conn = None
        try:
            conn = self.get_conn()
            if autocommit:
                conn.autocommit = True

            cursor = conn.cursor()

            if parameters:
                cursor.execute(sql, parameters)
            else:
                cursor.execute(sql)

            if not autocommit:
                conn.commit()
        except Exception as e:
            logger.error(f"Error executing statement: {e}")
            logger.error(f"SQL: {sql}")
            if parameters:
                logger.error(f"Parameters: {parameters}")
            if conn and not autocommit:
                conn.rollback()
            raise
        finally:
            self.release_conn(conn)

    def get_connection(self, conn_id: str):
        """
        Get Airflow connection object.

        This is a compatibility method for code that uses hook.get_connection().

        Args:
            conn_id: Connection ID

        Returns:
            Airflow Connection object
        """
        return BaseHook.get_connection(conn_id)
