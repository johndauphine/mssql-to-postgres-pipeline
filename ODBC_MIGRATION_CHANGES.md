# ODBC Migration Changes Guide

This document details all changes needed to switch from pymssql/FreeTDS to pyodbc/Microsoft ODBC Driver with Kerberos support.

## Files Modified

### 1. Dockerfile ✅ COMPLETED
- Replaced FreeTDS with Microsoft ODBC Driver 18
- Added Kerberos packages (krb5-user, libkrb5-dev)

### 2. requirements.txt ✅ COMPLETED
- Changed `pymssql>=2.2.0` to `pyodbc>=5.0.0`

### 3. data_transfer.py (Manual Changes Required)

File: `include/mssql_pg_migration/data_transfer.py`

#### Change 1: Update import statement (Line 23)
```python
# OLD:
import pymssql

# NEW:
import pyodbc
```

#### Change 2: Update module docstring (Lines 6-7)
```python
# OLD:
Uses direct pymssql connections for keyset pagination to avoid issues with
Airflow MSSQL hook's get_pandas_df method on large datasets.

# NEW:
Uses direct pyodbc connections for keyset pagination to avoid issues with
Airflow MSSQL hook's get_pandas_df method on large datasets.
```

#### Change 3: Update _mssql_config dictionary (Lines 51-57)
```python
# OLD:
self._mssql_config = {
    'server': mssql_conn.host,
    'port': mssql_conn.port or 1433,
    'database': mssql_conn.schema,
    'user': mssql_conn.login,
    'password': mssql_conn.password,
}

# NEW:
# Build ODBC connection parameters
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
}
```

#### Change 4: Update _mssql_connection method (Lines 102-108)
```python
# OLD:
@contextlib.contextmanager
def _mssql_connection(self):
    conn = pymssql.connect(**self._mssql_config)
    try:
        yield conn
    finally:
        conn.close()

# NEW:
@contextlib.contextmanager
def _mssql_connection(self):
    # Build ODBC connection string from config dict
    conn_str = ';'.join([f"{k}={v}" for k, v in self._mssql_config.items() if v])
    conn = pyodbc.connect(conn_str)
    try:
        yield conn
    finally:
        conn.close()
```

## Apply Changes to data_transfer.py

Run these commands to apply the changes:

```bash
# Edit through the Docker container (which has proper permissions)
docker exec -it airflow-scheduler bash -c "
cd /opt/airflow/include/mssql_pg_migration

# Backup original
cp data_transfer.py data_transfer.py.backup

# Update import
sed -i 's/import pymssql/import pyodbc/' data_transfer.py

# Update docstring
sed -i 's/Uses direct pymssql connections/Uses direct pyodbc connections/' data_transfer.py
"
```

Then manually update the connection configuration sections using a text editor or create a Python script to do it.

## Alternative: Complete Python Replacement Script

Create and run this script to make all changes:

```python
#!/usr/bin/env python3
import re

file_path = '/opt/airflow/include/mssql_pg_migration/data_transfer.py'

with open(file_path, 'r') as f:
    content = f.read()

# 1. Replace import
content = content.replace('import pymssql', 'import pyodbc')

# 2. Replace docstring reference
content = content.replace(
    'Uses direct pymssql connections',
    'Uses direct pyodbc connections'
)

# 3. Replace _mssql_config definition
old_config = '''        self._mssql_config = {
            'server': mssql_conn.host,
            'port': mssql_conn.port or 1433,
            'database': mssql_conn.schema,
            'user': mssql_conn.login,
            'password': mssql_conn.password,
        }'''

new_config = '''        # Build ODBC connection parameters
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
        }'''

content = content.replace(old_config, new_config)

# 4. Replace _mssql_connection method
old_method = '''    @contextlib.contextmanager
    def _mssql_connection(self):
        conn = pymssql.connect(**self._mssql_config)
        try:
            yield conn
        finally:
            conn.close()'''

new_method = '''    @contextlib.contextmanager
    def _mssql_connection(self):
        # Build ODBC connection string from config dict
        conn_str = ';'.join([f"{k}={v}" for k, v in self._mssql_config.items() if v])
        conn = pyodbc.connect(conn_str)
        try:
            yield conn
        finally:
            conn.close()'''

content = content.replace(old_method, new_method)

# Write back
with open(file_path, 'w') as f:
    f.write(content)

print("✅ data_transfer.py updated successfully")
```

Save as `update_data_transfer.py` and run inside container:
```bash
docker exec -it airflow-scheduler python3 /opt/airflow/update_data_transfer.py
```
