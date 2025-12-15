# ODBC Provider Replacement - Removing MSSQL Provider Dependency

## Overview

This change removes the dependency on `apache-airflow-providers-microsoft-mssql` to support corporate Airflow environments where this provider may not be available.

## Motivation

Corporate Airflow environments often have restricted package installations. This change makes the codebase compatible with environments that have:
- ✅ `pyodbc` (Microsoft ODBC Driver connectivity)
- ✅ `apache-airflow-providers-postgres` (PostgreSQL connectivity)
- ✅ `apache-airflow-providers-odbc` (optional, not used)
- ✅ `apache-airflow-providers-common-sql` (optional, not used)
- ❌ `apache-airflow-providers-microsoft-mssql` (NOT required)

## Changes Made

### New File: `odbc_helper.py`

Created `include/mssql_pg_migration/odbc_helper.py` with the `OdbcConnectionHelper` class that:
- Replaces `MsSqlHook` functionality
- Uses `BaseHook.get_connection()` to retrieve Airflow connection details
- Uses `pyodbc` directly for all SQL Server queries
- Provides the same interface methods:
  - `get_records(sql, parameters)` - Execute query, return all rows
  - `get_first(sql, parameters)` - Execute query, return first row
  - `run(sql, parameters)` - Execute statement (DDL/DML)
  - `get_conn()` - Get raw pyodbc connection
  - `get_connection(conn_id)` - Get Airflow connection object

### Updated Files

**1. `include/mssql_pg_migration/schema_extractor.py`**
```python
# Before
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
self.mssql_hook = MsSqlHook(mssql_conn_id=mssql_conn_id)

# After
from include.mssql_pg_migration.odbc_helper import OdbcConnectionHelper
self.mssql_hook = OdbcConnectionHelper(odbc_conn_id=mssql_conn_id)
```

**2. `include/mssql_pg_migration/validation.py`**
```python
# Before
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
self.mssql_hook = MsSqlHook(mssql_conn_id=mssql_conn_id)

# After
from include.mssql_pg_migration.odbc_helper import OdbcConnectionHelper
self.mssql_hook = OdbcConnectionHelper(odbc_conn_id=mssql_conn_id)
```

**3. `include/mssql_pg_migration/data_transfer.py`**
```python
# Before
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
self.mssql_hook = MsSqlHook(mssql_conn_id=mssql_conn_id)

# After
from include.mssql_pg_migration.odbc_helper import OdbcConnectionHelper
self.mssql_hook = OdbcConnectionHelper(odbc_conn_id=mssql_conn_id)
```

**4. `dags/mssql_to_postgres_migration.py`**
```python
# Before
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
mssql_hook = MsSqlHook(mssql_conn_id=params["source_conn_id"])

# After
from include.mssql_pg_migration.odbc_helper import OdbcConnectionHelper
mssql_hook = OdbcConnectionHelper(odbc_conn_id=params["source_conn_id"])
```

**5. `requirements.txt`**
```python
# Removed
apache-airflow-providers-microsoft-mssql>=3.8.0

# Retained
apache-airflow-providers-postgres>=5.12.0
pyodbc>=5.0.0
psycopg2-binary>=2.9.9
```

## Benefits

1. **Corporate Environment Compatible** - Works with restricted package environments
2. **Simpler Dependencies** - One less provider package to install/manage
3. **Same Functionality** - All existing code works without changes
4. **Direct ODBC Control** - More control over connection parameters
5. **Kerberos Support** - Full Windows Authentication support maintained

## Connection Configuration

The OdbcConnectionHelper automatically detects authentication method:

**SQL Server Authentication:**
```bash
MSSQL_USERNAME=sa
MSSQL_PASSWORD=YourPassword
```

**Windows Authentication (Kerberos):**
```bash
MSSQL_USERNAME=
MSSQL_PASSWORD=
```

## Testing

All existing functionality remains unchanged:
- ✅ Schema extraction queries
- ✅ Data transfer operations
- ✅ Row count validation
- ✅ Primary key detection
- ✅ Partition boundary queries
- ✅ Kerberos authentication

## Migration Path

For existing deployments:
1. Update code from this branch
2. Remove `apache-airflow-providers-microsoft-mssql` from environment (optional)
3. Ensure `pyodbc` is installed
4. Restart Airflow workers
5. Test with validation DAG

No connection configuration changes required - OdbcConnectionHelper reads the same Airflow connections as MsSqlHook.

## Backward Compatibility

⚠️ This change is **fully backward compatible** at the configuration level:
- Same Airflow connection format
- Same environment variables
- Same DAG parameters
- Same query syntax

The only change is the internal implementation - MsSqlHook is replaced with OdbcConnectionHelper.

## Technical Details

### OdbcConnectionHelper Implementation

The helper class:
1. Accepts an Airflow connection ID
2. Uses `BaseHook.get_connection()` to retrieve connection details
3. Builds ODBC connection string from connection properties
4. Executes queries using pyodbc
5. Returns results in the same format as MsSqlHook

### Query Compatibility

All SQL queries use ODBC parameter placeholders (`?`) which are compatible with pyodbc.

### Connection Pooling

The OdbcConnectionHelper creates connections on-demand and closes them after each query, matching the MsSqlHook behavior. Data transfer operations still use the existing connection pooling in `data_transfer.py`.

## Files Changed

```
modified:   dags/mssql_to_postgres_migration.py
modified:   include/mssql_pg_migration/data_transfer.py
modified:   include/mssql_pg_migration/schema_extractor.py
modified:   include/mssql_pg_migration/validation.py
modified:   requirements.txt
new file:   include/mssql_pg_migration/odbc_helper.py
new file:   ODBC_PROVIDER_REPLACEMENT.md
```

## Support

This change has been tested with:
- SQL Server 2019+
- Microsoft ODBC Driver 18 for SQL Server
- Python 3.11
- Apache Airflow 3.0.0
- Both SQL Server Authentication and Windows Authentication (Kerberos)
