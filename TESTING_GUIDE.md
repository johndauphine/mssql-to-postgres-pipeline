# Testing Guide - ODBC Provider Replacement

This guide provides multiple options for testing the changes that removed the `apache-airflow-providers-microsoft-mssql` dependency.

## Testing Options

### Option 1: Docker Desktop (Local Testing - Recommended)

**Prerequisites:**
- Docker Desktop installed on Windows
- WSL2 integration enabled in Docker Desktop

**Enable WSL2 Integration:**
1. Open Docker Desktop
2. Go to Settings → Resources → WSL Integration
3. Enable integration for your WSL distro (Ubuntu/Debian)
4. Click "Apply & Restart"

**Run Tests:**
```bash
# Start all containers
docker-compose up -d

# Wait for services to be healthy (30-60 seconds)
docker-compose ps

# Check for DAG import errors
docker exec airflow-scheduler airflow dags list

# Verify DAGs are visible
docker exec airflow-scheduler airflow dags list | grep mssql

# Run validation DAG (tests connections)
docker exec airflow-scheduler airflow dags trigger validate_migration_env

# Check task logs
docker-compose logs airflow-scheduler -f

# Stop containers when done
docker-compose down
```

### Option 2: Corporate Airflow Environment (Real-World Testing)

**Deploy to your corporate Airflow:**

1. **Copy files to corporate Airflow DAGs directory:**
   ```bash
   # From this branch
   git checkout feature/remove-mssql-provider-dependency

   # Copy to your corporate Airflow
   cp -r dags/ /path/to/corporate/airflow/dags/
   cp -r include/ /path/to/corporate/airflow/
   ```

2. **Configure connections:**
   ```bash
   # Create .env from template
   cp .env.corporate .env

   # Edit with your database details
   nano .env
   ```

3. **Test DAG parsing:**
   ```bash
   airflow dags list | grep mssql
   ```

4. **Trigger validation DAG:**
   ```bash
   airflow dags trigger validate_migration_env \
     --conf '{"source_schema": "deltek", "target_schema": "deltek"}'
   ```

5. **Monitor execution:**
   ```bash
   airflow dags list-runs -d validate_migration_env
   ```

### Option 3: Syntax Validation (Quick Check)

**Static analysis without running Airflow:**

```bash
# Check Python syntax
python3 -m py_compile dags/*.py
python3 -m py_compile include/mssql_pg_migration/*.py

# Expected: No output means syntax is valid
```

**Check for import issues in DAG files:**
```bash
# List imports
grep -n "^from\|^import" dags/mssql_to_postgres_migration.py | grep -v "#"

# Should NOT see: airflow.providers.microsoft.mssql
# Should see: include.mssql_pg_migration.odbc_helper
```

## What to Test

### 1. DAG Import Test
**Goal:** Verify DAGs import without errors

```bash
# In Docker
docker exec airflow-scheduler airflow dags list

# In Corporate Airflow
airflow dags list

# Expected output:
# mssql_to_postgres_migration
# validate_migration_env
```

### 2. Connection Test
**Goal:** Verify OdbcConnectionHelper can connect to SQL Server

```bash
# Trigger validation DAG
docker exec airflow-scheduler airflow dags trigger validate_migration_env

# Check logs for connection success
docker-compose logs airflow-scheduler | grep "Connected to SQL Server"

# Expected: "✓ Connected to SQL Server: <your-host>"
```

### 3. Schema Extraction Test
**Goal:** Verify SchemaExtractor works with OdbcConnectionHelper

```bash
# Trigger migration DAG (will extract schema)
docker exec airflow-scheduler airflow dags trigger mssql_to_postgres_migration \
  --conf '{"source_schema": "dbo", "target_schema": "test"}'

# Watch logs
docker-compose logs airflow-scheduler -f

# Expected: "Extracted schema for N tables"
```

### 4. Full Migration Test
**Goal:** Complete migration on test data

```bash
# Trigger migration on small dataset
docker exec airflow-scheduler airflow dags trigger mssql_to_postgres_migration \
  --conf '{
    "source_schema": "dbo",
    "target_schema": "test",
    "chunk_size": 10000,
    "exclude_tables": []
  }'

# Monitor progress
docker exec airflow-scheduler airflow dags list-runs -d mssql_to_postgres_migration

# Check for success
docker-compose logs airflow-scheduler | grep -E "(Successfully transferred|Success: All)"
```

## Verification Checklist

After testing, verify:

- [ ] DAGs appear in `airflow dags list`
- [ ] No import errors in scheduler logs
- [ ] Validation DAG connects to SQL Server successfully
- [ ] Schema extraction queries work (no ODBC errors)
- [ ] Data transfer completes without errors
- [ ] Row count validation passes
- [ ] No references to `MsSqlHook` in logs
- [ ] OdbcConnectionHelper is being used (check logs)

## Troubleshooting

### Docker Issues

**Problem: Docker command not found in WSL**
```bash
# Solution: Enable WSL integration in Docker Desktop
# Settings → Resources → WSL Integration → Enable for your distro
```

**Problem: Containers won't start**
```bash
# Check container status
docker-compose ps

# View logs
docker-compose logs

# Restart containers
docker-compose restart
```

### Import Errors

**Problem: `ModuleNotFoundError: No module named 'airflow'`**
- This is expected outside Docker containers
- Run tests inside the container using `docker exec`

**Problem: `ModuleNotFoundError: No module named 'pyodbc'`**
```bash
# Inside container, check if pyodbc is installed
docker exec airflow-scheduler pip list | grep pyodbc

# If missing, rebuild containers
docker-compose build --no-cache
docker-compose up -d
```

### Connection Errors

**Problem: "Can't open lib 'ODBC Driver 18 for SQL Server'"**
```bash
# Verify ODBC driver is installed in container
docker exec airflow-scheduler odbcinst -q -d

# Should show: [ODBC Driver 18 for SQL Server]
```

**Problem: "Login failed for user"**
- Check connection configuration in `.env`
- Verify SQL Server allows connections from Docker network
- For Kerberos: Ensure `MSSQL_USERNAME` and `MSSQL_PASSWORD` are empty

**Problem: "SSL Security error"**
- Connection string includes `TrustServerCertificate=yes` by default
- Check if SQL Server requires specific SSL settings

### OdbcConnectionHelper Issues

**Problem: Queries fail with parameter errors**
```bash
# Verify queries use ? placeholders (not %s)
grep -n "WHERE.*\?" include/mssql_pg_migration/*.py

# All should use ? for pyodbc compatibility
```

## Test Results Documentation

After testing, document results:

```markdown
## Test Results - ODBC Provider Replacement

**Environment:** [Local Docker / Corporate Airflow]
**Date:** YYYY-MM-DD
**Tester:** Your Name

### Import Test
- [ ] PASS - DAGs visible in `airflow dags list`
- [ ] PASS - No import errors in logs

### Connection Test
- [ ] PASS - SQL Server connection successful
- [ ] PASS - PostgreSQL connection successful

### Migration Test
- [ ] PASS - Schema extraction completed
- [ ] PASS - Table creation successful
- [ ] PASS - Data transfer completed
- [ ] PASS - Validation passed (row counts match)

### Issues Found
- None / [List any issues]

### Notes
- [Any observations or recommendations]
```

## Next Steps

**If all tests pass:**
1. Update TESTING_GUIDE.md with your results
2. Create pull request to merge changes
3. Deploy to production corporate environment

**If tests fail:**
1. Document the error messages
2. Check troubleshooting section
3. Review logs for specific error details
4. Report issues for investigation
