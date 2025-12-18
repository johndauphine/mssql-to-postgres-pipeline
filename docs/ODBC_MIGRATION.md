# ODBC Migration Guide

This document covers the migration from `pymssql`/FreeTDS to `pyodbc`/Microsoft ODBC Driver with Kerberos support, and the removal of the `apache-airflow-providers-microsoft-mssql` dependency.

## Overview

The pipeline uses Microsoft ODBC Driver 18 for SQL Server connectivity, supporting both SQL Server Authentication and Windows Authentication (Kerberos).

### Key Changes

1. **Driver**: FreeTDS replaced with Microsoft ODBC Driver 18
2. **Python Package**: `pymssql` replaced with `pyodbc`
3. **Provider**: `apache-airflow-providers-microsoft-mssql` removed (not required)
4. **New Module**: `OdbcConnectionHelper` replaces `MsSqlHook` functionality

## Benefits

- **Corporate Environment Compatible** - Works with restricted package environments
- **Simpler Dependencies** - One less provider package to install/manage
- **Kerberos Support** - Full Windows Authentication support
- **Direct ODBC Control** - More control over connection parameters

## OdbcConnectionHelper

The `OdbcConnectionHelper` class (`include/mssql_pg_migration/odbc_helper.py`) provides:

- `get_records(sql, parameters)` - Execute query, return all rows
- `get_first(sql, parameters)` - Execute query, return first row
- `run(sql, parameters)` - Execute statement (DDL/DML)
- `get_conn()` - Get raw pyodbc connection

### Authentication

**SQL Server Authentication:**
```bash
MSSQL_USERNAME=sa
MSSQL_PASSWORD=YourPassword
```

**Windows Authentication (Kerberos):**
```bash
MSSQL_USERNAME=
MSSQL_PASSWORD=
# Leave empty - uses Trusted_Connection=yes
```

## Files Modified

| File | Change |
|------|--------|
| `Dockerfile` | Microsoft ODBC Driver 18 + Kerberos packages |
| `requirements.txt` | `pyodbc>=5.0.0` (removed pymssql, mssql provider) |
| `include/mssql_pg_migration/odbc_helper.py` | New file - replaces MsSqlHook |
| `include/mssql_pg_migration/schema_extractor.py` | Uses OdbcConnectionHelper |
| `include/mssql_pg_migration/data_transfer.py` | Uses OdbcConnectionHelper |
| `include/mssql_pg_migration/validation.py` | Uses OdbcConnectionHelper |
| `dags/mssql_to_postgres_migration.py` | Uses OdbcConnectionHelper |

## Testing

### Quick Syntax Check
```bash
python3 -m py_compile dags/*.py
python3 -m py_compile include/mssql_pg_migration/*.py
```

### Docker Testing
```bash
# Start containers
docker-compose up -d

# Check DAG imports
docker exec airflow-scheduler airflow dags list

# Run validation DAG
docker exec airflow-scheduler airflow dags trigger validate_migration_env

# Check logs
docker-compose logs airflow-scheduler -f
```

### Verification Checklist

- [ ] DAGs appear in `airflow dags list`
- [ ] No import errors in scheduler logs
- [ ] Validation DAG connects to SQL Server successfully
- [ ] Schema extraction queries work
- [ ] Data transfer completes without errors
- [ ] Row count validation passes

## Troubleshooting

### "Can't open lib 'ODBC Driver 18 for SQL Server'"
ODBC driver not installed. Install with:
```bash
ACCEPT_EULA=Y apt-get install -y msodbcsql18
```

### "Login failed for user"
- For SQL Auth: Check username/password
- For Kerberos: Ensure `MSSQL_USERNAME` and `MSSQL_PASSWORD` are empty
- Verify Kerberos ticket: `klist`

### "SSL Security error"
Connection string includes `TrustServerCertificate=yes` by default. Check if SQL Server requires specific SSL settings.

### "ModuleNotFoundError: No module named 'pyodbc'"
```bash
pip install pyodbc>=5.0.0
```

## Backward Compatibility

This change is **fully backward compatible** at the configuration level:
- Same Airflow connection format
- Same environment variables
- Same DAG parameters
- Same query syntax

Only the internal implementation changed - MsSqlHook replaced with OdbcConnectionHelper.

## Tested With

- SQL Server 2019+
- Microsoft ODBC Driver 18 for SQL Server
- Python 3.11
- Apache Airflow 3.0.0
- SQL Server Authentication and Windows Authentication (Kerberos)
