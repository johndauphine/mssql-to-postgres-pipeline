# Corporate Airflow Deployment Guide

This guide explains how to deploy the MSSQL-to-PostgreSQL migration DAGs to an existing corporate Airflow environment that already has Kerberos authentication configured.

## Prerequisites

Your corporate Airflow environment must have:

✅ **Microsoft ODBC Driver 18 for SQL Server** installed
✅ **Kerberos** configured and working for Windows Authentication
✅ **Python packages**: `pyodbc>=5.0.0`, `psycopg2-binary>=2.9.9`
✅ **Apache Airflow** 3.0+ (or 2.5+ with compatible syntax)

## Quick Start

### 1. Copy Files to Your Airflow Environment

Copy these files/directories to your Airflow DAGs directory:

```
your-airflow-dags/
├── dags/
│   ├── mssql_to_postgres_migration.py
│   └── validate_migration_env.py
├── include/
│   └── mssql_pg_migration/
│       ├── __init__.py
│       ├── schema_extractor.py
│       ├── type_mapping.py
│       ├── ddl_generator.py
│       ├── data_transfer.py
│       ├── validation.py
│       ├── notifications.py
│       └── utils.py
└── .env
```

### 2. Configure Environment Variables

Create a `.env` file in your DAGs directory (or set as Airflow Variables):

```bash
# Copy the template
cp .env.corporate .env

# Edit with your database details
nano .env
```

**Required Variables:**

```bash
# SQL Server Source (Kerberos Auth)
MSSQL_HOST=sqlserver.corp.com
MSSQL_PORT=1433
MSSQL_DATABASE=ProductionDB
MSSQL_USERNAME=                    # Empty for Kerberos
MSSQL_PASSWORD=                    # Empty for Kerberos

# PostgreSQL Target
POSTGRES_HOST=postgres.corp.com
POSTGRES_PORT=5432
POSTGRES_DATABASE=warehouse
POSTGRES_USERNAME=etl_user
POSTGRES_PASSWORD=your_password

# Performance Tuning
MAX_PARTITIONS=8                   # Parallel partitions for large tables
```

### 3. Set Up Airflow Connections (Alternative to .env)

If your environment uses Airflow Connections instead of .env files:

**SQL Server Connection (with Kerberos):**
```bash
airflow connections add mssql_source \
  --conn-type mssql \
  --conn-host sqlserver.corp.com \
  --conn-port 1433 \
  --conn-schema ProductionDB \
  --conn-extra '{"driver": "ODBC Driver 18 for SQL Server", "TrustServerCertificate": "yes", "Trusted_Connection": "yes"}'
```

**PostgreSQL Connection:**
```bash
airflow connections add postgres_target \
  --conn-type postgres \
  --conn-host postgres.corp.com \
  --conn-port 5432 \
  --conn-schema warehouse \
  --conn-login etl_user \
  --conn-password your_password
```

### 4. Verify DAGs Load Successfully

```bash
# List DAGs
airflow dags list | grep mssql

# Parse DAGs for syntax errors
airflow dags list-import-errors
```

You should see:
- `mssql_to_postgres_migration`
- `validate_migration_env`

### 5. Test Connection

Run the validation DAG first to verify connectivity:

```bash
airflow dags trigger validate_migration_env \
  --conf '{"source_schema": "dbo", "target_schema": "public"}'
```

Monitor the logs to ensure:
- ✓ SQL Server connection succeeds (using Kerberos)
- ✓ PostgreSQL connection succeeds

### 6. Run Your First Migration

Trigger the migration DAG:

```bash
airflow dags trigger mssql_to_postgres_migration \
  --conf '{
    "source_schema": "dbo",
    "target_schema": "public",
    "chunk_size": 100000,
    "exclude_tables": ["sysdiagrams"]
  }'
```

## Configuration Options

### DAG Parameters

The migration DAG accepts these parameters (via UI or `--conf`):

| Parameter | Default | Description |
|-----------|---------|-------------|
| `source_conn_id` | `mssql_source` | Airflow connection ID for SQL Server |
| `target_conn_id` | `postgres_target` | Airflow connection ID for PostgreSQL |
| `source_schema` | `dbo` | Source schema in SQL Server |
| `target_schema` | `public` | Target schema in PostgreSQL |
| `chunk_size` | `100000` | Rows per batch (100-500,000) |
| `exclude_tables` | `[]` | Table patterns to skip (supports wildcards) |
| `create_foreign_keys` | `true` | Create foreign key constraints after transfer |
| `use_unlogged_tables` | `true` | Use UNLOGGED tables during load (faster) |

### Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `MSSQL_HOST` | Yes | SQL Server hostname |
| `MSSQL_PORT` | No | SQL Server port (default: 1433) |
| `MSSQL_DATABASE` | Yes | Source database name |
| `MSSQL_USERNAME` | No | Leave empty for Kerberos |
| `MSSQL_PASSWORD` | No | Leave empty for Kerberos |
| `POSTGRES_HOST` | Yes | PostgreSQL hostname |
| `POSTGRES_PORT` | No | PostgreSQL port (default: 5432) |
| `POSTGRES_DATABASE` | Yes | Target database name |
| `POSTGRES_USERNAME` | Yes | PostgreSQL username |
| `POSTGRES_PASSWORD` | Yes | PostgreSQL password |
| `MAX_PARTITIONS` | No | Parallel partitions (default: 8) |

## Kerberos Authentication

### How It Works

The DAGs automatically use Kerberos/Windows Authentication when:
1. `MSSQL_USERNAME` and `MSSQL_PASSWORD` are **empty or not set**
2. Your Airflow environment has Kerberos configured
3. The Airflow worker has a valid Kerberos ticket

The connection string will include `Trusted_Connection=yes`, which tells ODBC to use Windows Authentication.

### Verify Kerberos Ticket

On the Airflow worker, check for a valid ticket:

```bash
klist
```

Expected output:
```
Ticket cache: FILE:/tmp/krb5cc_1000
Default principal: svc_airflow@CORP.COM

Valid starting     Expires            Service principal
12/15/25 10:00:00  12/15/25 20:00:00  krbtgt/CORP.COM@CORP.COM
```

### Service Account Setup

Best practice is to use a dedicated service account:

1. Create AD service account: `svc_airflow@CORP.COM`
2. Grant SQL Server read permissions
3. Configure keytab for automated renewal (ask your AD admin)
4. Set Airflow to run as this service account

## File Structure

### Required Files

**DAGs:**
- `dags/mssql_to_postgres_migration.py` - Main migration orchestration
- `dags/validate_migration_env.py` - Validation helper DAG

**Include Modules:**
- `include/mssql_pg_migration/__init__.py` - Package initialization
- `include/mssql_pg_migration/schema_extractor.py` - SQL Server schema discovery
- `include/mssql_pg_migration/type_mapping.py` - Data type conversions
- `include/mssql_pg_migration/ddl_generator.py` - PostgreSQL DDL generation
- `include/mssql_pg_migration/data_transfer.py` - Streaming data transfer
- `include/mssql_pg_migration/validation.py` - Row count validation
- `include/mssql_pg_migration/notifications.py` - Optional notifications
- `include/mssql_pg_migration/utils.py` - Helper utilities

**Configuration:**
- `.env` - Environment variables (or use Airflow Variables)

### Optional Files (Not Needed for Deployment)

These are only needed for local Docker development:
- ❌ `Dockerfile`
- ❌ `docker-compose.yml`
- ❌ `airflow_settings.yaml`
- ❌ `requirements.txt` (packages should already be in your Airflow environment)

## Performance Tuning

### Adjust Parallelism

For your Airflow worker capacity:

```bash
# Conservative (4-8 workers)
MAX_PARTITIONS=4

# Standard (8-16 workers)
MAX_PARTITIONS=8

# High capacity (16+ workers)
MAX_PARTITIONS=12
```

### Chunk Size Tuning

Adjust based on network speed and row width:

```python
# Fast network, narrow rows
"chunk_size": 200000

# Standard
"chunk_size": 100000

# Slow network, wide rows
"chunk_size": 50000
```

### Large Table Partitioning

Tables >5M rows are automatically partitioned by primary key range into `MAX_PARTITIONS` parallel tasks. This is handled automatically by the DAG.

## Notifications (Optional)

### Enable Slack Notifications

1. Create a Slack webhook: https://api.slack.com/messaging/webhooks
2. Set environment variables:

```bash
NOTIFICATION_ENABLED=true
NOTIFICATION_CHANNELS=slack
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/YOUR/WEBHOOK/URL
```

### Enable Email Notifications

```bash
NOTIFICATION_ENABLED=true
NOTIFICATION_CHANNELS=email
SMTP_HOST=smtp.corp.com
SMTP_PORT=587
SMTP_USER=airflow@corp.com
SMTP_PASSWORD=your_password
NOTIFICATION_EMAIL_FROM=airflow@corp.com
NOTIFICATION_EMAIL_TO=data-team@corp.com
```

## Troubleshooting

### Connection Errors

**"Login failed for user"**
- Verify Kerberos ticket is valid (`klist`)
- Check SQL Server allows Windows Authentication
- Ensure service account has permissions

**"Can't open lib 'ODBC Driver 18 for SQL Server'"**
- ODBC driver not installed on Airflow worker
- Ask your Airflow admin to install: `ACCEPT_EULA=Y apt-get install -y msodbcsql18`

**"SSL Security error"**
- Add `TrustServerCertificate=yes` to connection extras
- Or configure proper SSL certificates

### Performance Issues

**Migration is slow**
- Increase `MAX_PARTITIONS` (more parallel tasks)
- Increase `chunk_size` (larger batches)
- Check network latency between Airflow and databases
- Enable `use_unlogged_tables=true` for faster loads

**Out of Memory**
- Decrease `MAX_PARTITIONS` (fewer parallel tasks)
- Decrease `chunk_size` (smaller batches)

### DAG Import Errors

**"No module named 'mssql_pg_migration'"**
- Verify `include/` directory is in the same parent as `dags/`
- Check `PYTHONPATH` includes the DAGs directory

**"No module named 'pyodbc'"**
- pyodbc not installed in Airflow environment
- Install: `pip install pyodbc>=5.0.0`

## Security Best Practices

1. **Use Kerberos** - Avoid storing SQL passwords in environment variables
2. **Restrict Service Account** - Grant only necessary SQL Server read permissions
3. **Encrypt .env File** - Use Airflow's Secrets Backend instead of plaintext .env
4. **Audit Logging** - Enable Airflow task logging for compliance
5. **Network Security** - Use private networks or VPN for database connections

## Example: Full Migration Workflow

```bash
# 1. Trigger migration for specific schema
airflow dags trigger mssql_to_postgres_migration \
  --conf '{
    "source_schema": "sales",
    "target_schema": "sales",
    "chunk_size": 100000,
    "exclude_tables": ["temp_*", "staging_*"]
  }'

# 2. Monitor progress
airflow dags list-runs -d mssql_to_postgres_migration --state running

# 3. Check logs
airflow tasks logs mssql_to_postgres_migration <run_id> transfer_table_data 0

# 4. Validate after completion
airflow dags trigger validate_migration_env \
  --conf '{"source_schema": "sales", "target_schema": "sales"}'
```

## Support

For issues or questions:
- Check the [main README.md](README.md) for architecture details
- Review [KERBEROS_SETUP.md](KERBEROS_SETUP.md) for Kerberos configuration
- Check Airflow logs for detailed error messages
- Contact your Airflow administrator for infrastructure issues

## Migration Checklist

- [ ] Copy DAGs and include files to Airflow directory
- [ ] Configure `.env` or Airflow Connections
- [ ] Verify Kerberos ticket is valid
- [ ] Test with validation DAG
- [ ] Run migration on small test table
- [ ] Validate row counts match
- [ ] Run full migration on production schema
- [ ] Enable notifications (optional)
- [ ] Schedule regular validation runs
