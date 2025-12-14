# Kerberos Setup Guide for ODBC with Windows Authentication

This guide walks you through setting up the pipeline to use Microsoft ODBC Driver with Kerberos/Windows Authentication for your corporate SQL Server environment.

## Prerequisites

- Active Directory domain with Kerberos enabled
- Service account (e.g., `svc_airflow`) with read access to SQL Server
- Domain controller hostnames and addresses
- SQL Server hostname in your corporate environment

## Step 1: Configure Kerberos

### 1.1 Copy and customize krb5.conf

```bash
cd config
cp krb5.conf.template krb5.conf
```

### 1.2 Edit krb5.conf with your domain settings

Replace these values:
- `CORP.LOCAL` → Your AD domain in UPPERCASE (e.g., `CONTOSO.COM`)
- `corp.local` → Your AD domain in lowercase (e.g., `contoso.com`)
- `dc01.corp.local` → Your primary domain controller
- `dc02.corp.local` → Your secondary domain controller (or remove if only one)

Example for CONTOSO.COM domain:
```ini
[libdefaults]
    default_realm = CONTOSO.COM

[realms]
    CONTOSO.COM = {
        kdc = dc1.contoso.com
        kdc = dc2.contoso.com
        admin_server = dc1.contoso.com
    }

[domain_realm]
    .contoso.com = CONTOSO.COM
    contoso.com = CONTOSO.COM
```

## Step 2: Configure Connection String

### 2.1 Edit .env file

```bash
cp .env.example .env
nano .env
```

### 2.2 Set your connection strings

**For Windows Authentication (Kerberos):**
```bash
# SQL Server with Windows Auth
AIRFLOW_CONN_MSSQL_SOURCE='mssql+pyodbc://CORP\svc_airflow@sqlprod01.corp.local:1433/ProductionDB?driver=ODBC+Driver+18+for+SQL+Server&Trusted_Connection=yes&TrustServerCertificate=yes'

# PostgreSQL target
AIRFLOW_CONN_POSTGRES_TARGET='postgresql://postgres:YourPassword@postgres-host.corp.local:5432/warehouse'
```

**Replace:**
- `CORP\svc_airflow` → Your domain\username
- `sqlprod01.corp.local` → Your SQL Server hostname
- `ProductionDB` → Your source database name
- `postgres-host.corp.local` → Your PostgreSQL server
- `warehouse` → Your target database name

### 2.3 Connection string variations

**Named Instance:**
```bash
AIRFLOW_CONN_MSSQL_SOURCE='mssql+pyodbc://CORP\svc_airflow@sqlserver\INST01/DB?driver=ODBC+Driver+18+for+SQL+Server&Trusted_Connection=yes&TrustServerCertificate=yes'
```

**With Encryption:**
```bash
AIRFLOW_CONN_MSSQL_SOURCE='mssql+pyodbc://CORP\svc_airflow@sqlserver:1433/DB?driver=ODBC+Driver+18+for+SQL+Server&Trusted_Connection=yes&Encrypt=yes'
```

**Non-standard Port:**
```bash
AIRFLOW_CONN_MSSQL_SOURCE='mssql+pyodbc://CORP\svc_airflow@sqlserver:1435/DB?driver=ODBC+Driver+18+for+SQL+Server&Trusted_Connection=yes&TrustServerCertificate=yes'
```

## Step 3: Update data_transfer.py

Run the update script to convert from pymssql to pyodbc:

```bash
# Copy script to mounted volume (will be accessible in container)
# Already in repo root

# After containers are built and started:
docker exec -it airflow-scheduler python3 /opt/airflow/update_data_transfer.py
```

Or manually apply changes from `ODBC_MIGRATION_CHANGES.md`.

## Step 4: Build and Start

```bash
# Build new image with ODBC driver
docker-compose build

# Start services
docker-compose up -d

# Wait for services to be healthy
docker-compose ps
```

## Step 5: Obtain Kerberos Ticket

### Option A: Interactive (Manual)

```bash
# Get ticket for service account
docker exec -it airflow-scheduler kinit svc_airflow@CORP.LOCAL

# Enter password when prompted

# Verify ticket
docker exec airflow-scheduler klist
```

### Option B: Automated with Keytab (Recommended for Production)

#### 5.1 Create keytab file

On a Windows machine with AD access:
```powershell
ktpass -princ svc_airflow@CORP.LOCAL -mapuser CORP\svc_airflow `
  -crypto AES256-SHA1 -ptype KRB5_NT_PRINCIPAL `
  -out svc_airflow.keytab -pass ServiceAccountPassword
```

#### 5.2 Copy keytab to config directory

```bash
# Copy the keytab file
cp /path/to/svc_airflow.keytab config/

# Set restrictive permissions
chmod 600 config/svc_airflow.keytab
```

#### 5.3 Update docker-compose.yml for auto-init

Add to scheduler service:
```yaml
airflow-scheduler:
  <<: *airflow-common
  container_name: airflow-scheduler
  command: >
    bash -c "
      kinit -kt /opt/airflow/config/svc_airflow.keytab svc_airflow@CORP.LOCAL &&
      airflow scheduler
    "
  volumes:
    - ./config/svc_airflow.keytab:/opt/airflow/config/svc_airflow.keytab:ro
```

#### 5.4 Restart scheduler

```bash
docker-compose restart airflow-scheduler
```

## Step 6: Test Connection

### 6.1 Test Kerberos ticket

```bash
# Check ticket is valid
docker exec airflow-scheduler klist

# Should show:
# Ticket cache: FILE:/tmp/krb5cc_airflow
# Default principal: svc_airflow@CORP.LOCAL
#
# Valid starting     Expires            Service principal
# 12/14/25 09:00:00  12/14/25 19:00:00  krbtgt/CORP.LOCAL@CORP.LOCAL
```

### 6.2 Test ODBC connection

```bash
docker exec airflow-scheduler python3 << 'EOF'
import pyodbc

conn_str = (
    'DRIVER={ODBC Driver 18 for SQL Server};'
    'SERVER=sqlprod01.corp.local;'
    'DATABASE=ProductionDB;'
    'Trusted_Connection=yes;'
    'TrustServerCertificate=yes;'
)

try:
    print("Connecting to SQL Server...")
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    print("✓ Connection successful!")

    cursor.execute('SELECT @@VERSION')
    version = cursor.fetchone()[0]
    print(f"SQL Server Version: {version[:80]}...")

    cursor.execute('SELECT DB_NAME()')
    db_name = cursor.fetchone()[0]
    print(f"Connected to database: {db_name}")

    conn.close()
    print("\n✅ ODBC connection test passed!")

except Exception as e:
    print(f"\n❌ Connection failed: {e}")
EOF
```

### 6.3 Test Airflow connections

```bash
# Test SQL Server connection
docker exec airflow-scheduler airflow connections test mssql_source

# Test PostgreSQL connection
docker exec airflow-scheduler airflow connections test postgres_target
```

## Step 7: Run Migration

```bash
# Trigger the migration DAG
docker exec airflow-scheduler airflow dags trigger mssql_to_postgres_migration

# Monitor progress
docker-compose logs airflow-scheduler -f
```

## Troubleshooting

### Issue: "Cannot find KDC for realm"

**Solution:** Check DNS and krb5.conf settings
```bash
# Test domain controller connectivity
docker exec airflow-scheduler nslookup dc01.corp.local

# Verify krb5.conf syntax
docker exec airflow-scheduler cat /opt/airflow/config/krb5.conf
```

### Issue: "Clock skew too great"

**Solution:** Sync time between container and AD
```bash
# Add to docker-compose.yml environment:
TZ: America/Chicago  # Match your AD timezone
```

### Issue: "Encryption type not supported"

**Solution:** Update permitted_enctypes in krb5.conf
```ini
[libdefaults]
    permitted_enctypes = aes256-cts-hmac-sha1-96 aes128-cts-hmac-sha1-96 rc4-hmac
```

### Issue: "Login failed for user"

**Possible causes:**
1. Service account doesn't have permissions
2. Kerberos ticket expired (run `kinit` again)
3. SPNs not registered correctly

**Check:**
```bash
# Verify ticket is still valid
docker exec airflow-scheduler klist

# Re-authenticate if expired
docker exec -it airflow-scheduler kinit svc_airflow@CORP.LOCAL
```

### Issue: "Cannot open server requested by the login"

**Solution:** Verify SQL Server hostname and network connectivity
```bash
# Test network connectivity
docker exec airflow-scheduler ping sqlprod01.corp.local

# Test port connectivity
docker exec airflow-scheduler nc -zv sqlprod01.corp.local 1433
```

## Ticket Renewal

Kerberos tickets expire (default: 24 hours). Options:

### Option 1: Keytab (Recommended)
- Automatically renews tickets
- No manual intervention needed
- See Step 5, Option B above

### Option 2: Cron job
Add to scheduler container:
```bash
# Renew ticket every 12 hours
0 */12 * * * kinit -R || kinit -kt /opt/airflow/config/svc_airflow.keytab svc_airflow@CORP.LOCAL
```

### Option 3: Manual renewal
```bash
# Renew existing ticket
docker exec airflow-scheduler kinit -R

# Or get new ticket
docker exec -it airflow-scheduler kinit svc_airflow@CORP.LOCAL
```

## Security Best Practices

1. **Never commit .env or keytab files to git** (already in .gitignore)
2. **Use restrictive permissions on keytab**: `chmod 600 config/*.keytab`
3. **Use read-only service account** for SQL Server
4. **Enable encryption** if your SQL Server requires it (`Encrypt=yes`)
5. **Rotate service account passwords** regularly
6. **Recreate keytab files** after password changes

## Next Steps

Once Kerberos is working:
1. Review and customize DAG parameters in Airflow UI
2. Test with a small table first
3. Monitor migration performance
4. Set up validation checks
5. Document your specific environment settings for your team
