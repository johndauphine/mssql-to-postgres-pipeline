# Corporate Deployment - Quick Checklist

Use this checklist when deploying to your corporate Airflow environment.

## Files to Copy

### ✅ Required Files

Copy these to your Airflow DAGs directory:

```
your-airflow-dags/
├── dags/
│   ├── mssql_to_postgres_migration.py
│   └── validate_migration_env.py
│
└── include/
    └── mssql_pg_migration/
        ├── __init__.py
        ├── schema_extractor.py
        ├── type_mapping.py
        ├── ddl_generator.py
        ├── data_transfer.py
        ├── validation.py
        ├── notifications.py
        └── utils.py
```

### 📋 Configuration File (Choose One)

**Option A: Use .env file**
```bash
cp .env.corporate .env
# Edit with your database details
nano .env
# Copy to your DAGs directory
```

**Option B: Use Airflow Connections**
```bash
# Set up connections via Airflow CLI or UI
# See CORPORATE_DEPLOYMENT.md for connection setup
```

### ❌ Files NOT Needed

These are only for local Docker development - don't copy them:
- ❌ `Dockerfile`
- ❌ `docker-compose.yml`
- ❌ `airflow_settings.yaml`
- ❌ `requirements.txt`
- ❌ `.env.example`
- ❌ `KERBEROS_SETUP.md`
- ❌ `ODBC_*.md`

## Pre-Deployment Checks

### Environment Requirements

- [ ] Airflow 3.0+ (or 2.5+) installed
- [ ] Microsoft ODBC Driver 18 for SQL Server installed
- [ ] Kerberos configured and working
- [ ] Python packages installed:
  - [ ] `pyodbc>=5.0.0`
  - [ ] `psycopg2-binary>=2.9.9`
  - [ ] `apache-airflow-providers-microsoft-mssql>=3.8.0`
  - [ ] `apache-airflow-providers-postgres>=5.12.0`

### Configuration

- [ ] `.env` file created with your database details
- [ ] `MSSQL_HOST` set to your SQL Server hostname
- [ ] `MSSQL_DATABASE` set to your source database
- [ ] `MSSQL_USERNAME` and `MSSQL_PASSWORD` empty (for Kerberos)
- [ ] `POSTGRES_HOST` set to your PostgreSQL hostname
- [ ] `POSTGRES_DATABASE` set to your target database
- [ ] `POSTGRES_USERNAME` and `POSTGRES_PASSWORD` set

### Permissions

- [ ] Service account has READ permissions on source SQL Server
- [ ] Service account has WRITE permissions on target PostgreSQL
- [ ] Valid Kerberos ticket exists (`klist`)

## Deployment Steps

### 1. Copy Files

```bash
# From this repository root
cp -r dags/ /path/to/your/airflow/dags/
cp -r include/ /path/to/your/airflow/
```

### 2. Configure Connections

**Using .env file:**
```bash
cp .env.corporate .env
nano .env  # Edit with your settings
cp .env /path/to/your/airflow/
```

**Using Airflow Connections:**
```bash
# See CORPORATE_DEPLOYMENT.md for connection setup commands
```

### 3. Verify DAGs Load

```bash
airflow dags list | grep mssql
# Should show:
#   - mssql_to_postgres_migration
#   - validate_migration_env
```

### 4. Test Connection

```bash
airflow dags trigger validate_migration_env
# Check logs for successful connections
```

### 5. Run Test Migration

```bash
airflow dags trigger mssql_to_postgres_migration \
  --conf '{"source_schema": "dbo", "target_schema": "test", "exclude_tables": []}'
```

## Configuration Template

Minimal `.env` for corporate deployment:

```bash
# SQL Server (Kerberos Auth)
MSSQL_HOST=your-sqlserver.corp.com
MSSQL_PORT=1433
MSSQL_DATABASE=YourSourceDB
MSSQL_USERNAME=
MSSQL_PASSWORD=

# PostgreSQL
POSTGRES_HOST=your-postgres.corp.com
POSTGRES_PORT=5432
POSTGRES_DATABASE=YourTargetDB
POSTGRES_USERNAME=etl_user
POSTGRES_PASSWORD=your_password

# Performance
MAX_PARTITIONS=8
```

## Quick Test

After deployment, run this quick test:

```bash
# 1. Test validation DAG
airflow dags trigger validate_migration_env

# 2. Check the logs
airflow dags list-runs -d validate_migration_env --state running

# 3. If validation passes, try a small migration
airflow dags trigger mssql_to_postgres_migration \
  --conf '{
    "source_schema": "dbo",
    "target_schema": "test",
    "chunk_size": 10000
  }'
```

## Troubleshooting

**DAGs don't appear**
- Check `include/` is in the right location
- Verify `PYTHONPATH` includes the DAGs directory
- Check for import errors: `airflow dags list-import-errors`

**Connection fails**
- Verify Kerberos ticket: `klist`
- Check `.env` file location
- Verify ODBC driver installed: `odbcinst -q -d`

**pyodbc import error**
- Install pyodbc: `pip install pyodbc>=5.0.0`
- Check Python environment matches Airflow

## Next Steps

After successful deployment:

1. **Schedule Regular Migrations** - Set up DAG schedule if needed
2. **Enable Notifications** - Configure Slack/email alerts
3. **Monitor Performance** - Tune `MAX_PARTITIONS` and `chunk_size`
4. **Document Schema Mappings** - Track which schemas go where
5. **Set Up Validation** - Schedule validation DAG runs

## Support

- Full documentation: [CORPORATE_DEPLOYMENT.md](CORPORATE_DEPLOYMENT.md)
- Kerberos setup: [KERBEROS_SETUP.md](KERBEROS_SETUP.md)
- Architecture details: [README.md](README.md)
