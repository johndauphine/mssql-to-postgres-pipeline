# Corporate Deployment - Quick Start

For existing Airflow environments with Kerberos already configured.

## 3-Step Deployment

### Step 1: Copy Files
```bash
# Copy to your Airflow DAGs directory
cp -r dags/ /path/to/airflow/dags/
cp -r include/ /path/to/airflow/
```

### Step 2: Configure Database Connections
```bash
# Create .env file
cp .env.corporate .env

# Edit with your database details
nano .env
```

**Required settings:**
```bash
MSSQL_HOST=your-sqlserver.corp.com
MSSQL_DATABASE=YourSourceDB
MSSQL_USERNAME=              # Empty for Kerberos
MSSQL_PASSWORD=              # Empty for Kerberos

POSTGRES_HOST=your-postgres.corp.com
POSTGRES_DATABASE=YourTargetDB
POSTGRES_USERNAME=etl_user
POSTGRES_PASSWORD=your_password
```

### Step 3: Run Migration
```bash
# Test validation first
airflow dags trigger validate_migration_env

# Run migration
airflow dags trigger mssql_to_postgres_migration \
  --conf '{"source_schema": "dbo", "target_schema": "public"}'
```

## What Gets Deployed

### Required Files Only
```
your-airflow-dags/
├── dags/
│   ├── mssql_to_postgres_migration.py    ← Main migration DAG
│   └── validate_migration_env.py          ← Validation DAG
├── include/
│   └── mssql_pg_migration/                ← Python modules
│       └── *.py
└── .env                                    ← Your configuration
```

### NOT Needed (Docker-only)
- ❌ Dockerfile
- ❌ docker-compose.yml
- ❌ airflow_settings.yaml
- ❌ requirements.txt (packages already in your Airflow)

## Prerequisites

Your Airflow environment needs:
- ✅ Microsoft ODBC Driver 18 for SQL Server
- ✅ Kerberos configured
- ✅ `pyodbc>=5.0.0` installed
- ✅ `psycopg2-binary>=2.9.9` installed

## Full Documentation

- **Complete Guide**: [CORPORATE_DEPLOYMENT.md](CORPORATE_DEPLOYMENT.md)
- **Deployment Checklist**: [DEPLOYMENT_CHECKLIST.md](DEPLOYMENT_CHECKLIST.md)
- **Architecture Details**: [README.md](README.md)
