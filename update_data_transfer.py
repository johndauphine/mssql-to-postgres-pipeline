#!/usr/bin/env python3
"""
Script to update data_transfer.py from pymssql to pyodbc
Run this inside the Airflow container
"""

file_path = 'include/mssql_pg_migration/data_transfer.py'

print(f"Reading {file_path}...")
with open(file_path, 'r') as f:
    content = f.read()

# Backup
print("Creating backup...")
with open(f"{file_path}.backup", 'w') as f:
    f.write(content)

# 1. Replace import
print("1. Updating import statement...")
content = content.replace('import pymssql', 'import pyodbc')

# 2. Replace docstring reference
print("2. Updating docstring...")
content = content.replace(
    'Uses direct pymssql connections',
    'Uses direct pyodbc connections'
)

# 3. Replace _mssql_config definition
print("3. Updating _mssql_config...")
old_config = """        self._mssql_config = {
            'server': mssql_conn.host,
            'port': mssql_conn.port or 1433,
            'database': mssql_conn.schema,
            'user': mssql_conn.login,
            'password': mssql_conn.password,
        }"""

new_config = """        # Build ODBC connection parameters
        port = mssql_conn.port or 1433
        server = f"{mssql_conn.host},{port}" if port != 1433 else mssql_conn.host

        self._mssql_config = {
            'DRIVER': '{ODBC Driver 18 for SQL Server}',
            'SERVER': server,
            'DATABASE': mssql_conn.schema,
            'UID': mssql_conn.login if mssql_conn.login else '',
            'PWD': mssql_conn.password if mssql_conn.password else '',
            'TrustServerCertificate': 'yes',
            'Trusted_Connection': 'yes' if not mssql_conn.login else 'no',
        }"""

if old_config in content:
    content = content.replace(old_config, new_config)
    print("   ✓ Config updated")
else:
    print("   ⚠ Warning: Original config pattern not found, skipping...")

# 4. Replace _mssql_connection method
print("4. Updating _mssql_connection method...")
old_method = """    @contextlib.contextmanager
    def _mssql_connection(self):
        conn = pymssql.connect(**self._mssql_config)
        try:
            yield conn
        finally:
            conn.close()"""

new_method = """    @contextlib.contextmanager
    def _mssql_connection(self):
        # Build ODBC connection string from config dict
        conn_str = ';'.join([f"{k}={v}" for k, v in self._mssql_config.items() if v])
        conn = pyodbc.connect(conn_str)
        try:
            yield conn
        finally:
            conn.close()"""

if old_method in content:
    content = content.replace(old_method, new_method)
    print("   ✓ Method updated")
else:
    print("   ⚠ Warning: Original method pattern not found, skipping...")

# Write back
print(f"Writing updated {file_path}...")
with open(file_path, 'w') as f:
    f.write(content)

print("\n✅ data_transfer.py updated successfully!")
print(f"   Backup saved as: {file_path}.backup")
