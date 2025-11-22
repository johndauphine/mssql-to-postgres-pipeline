#!/bin/bash
set -e

echo "=== StackOverflow2010 Database Setup ==="
echo "Download and setup for SQL Server to PostgreSQL migration testing"
echo ""

# Configuration
MSSQL_SA_PASSWORD="YourStrong@Passw0rd"
POSTGRES_PASSWORD="PostgresPassword123"
DB_DOWNLOAD_URL="https://downloads.brentozar.com/StackOverflow2010.7z"
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

cd "$SCRIPT_DIR"

# Create directories
echo "[1/8] Creating directories..."
mkdir -p ./stackoverflow_db

# Check if database files already exist
if [ -f "./stackoverflow_db/StackOverflow2010.mdf" ]; then
    echo "Database files already exist. Skipping download."
else
    # Download StackOverflow2010
    echo "[2/8] Downloading StackOverflow2010 database (~1GB)..."
    echo "From: $DB_DOWNLOAD_URL"

    if command -v wget &> /dev/null; then
        wget --progress=bar:force:noscroll -O ./stackoverflow_db/StackOverflow2010.7z "$DB_DOWNLOAD_URL"
    elif command -v curl &> /dev/null; then
        curl -L --progress-bar -o ./stackoverflow_db/StackOverflow2010.7z "$DB_DOWNLOAD_URL"
    else
        echo "ERROR: Neither wget nor curl is available. Please install one of them."
        exit 1
    fi

    # Extract database files
    echo "[3/8] Extracting database files (this may take a few minutes)..."
    if command -v 7z &> /dev/null; then
        cd ./stackoverflow_db
        7z x -y StackOverflow2010.7z
        cd "$SCRIPT_DIR"
    elif command -v 7za &> /dev/null; then
        cd ./stackoverflow_db
        7za x -y StackOverflow2010.7z
        cd "$SCRIPT_DIR"
    else
        echo "ERROR: 7z is not available. Please install p7zip-full."
        echo "On Ubuntu/Debian: sudo apt-get install p7zip-full"
        exit 1
    fi

    # Clean up archive to save space
    echo "Cleaning up archive..."
    rm -f ./stackoverflow_db/StackOverflow2010.7z
fi

echo "Database files ready:"
ls -lh ./stackoverflow_db/*.mdf ./stackoverflow_db/*.ldf 2>/dev/null || ls -lh ./stackoverflow_db/

# Check if astro_default network exists
echo "[4/8] Checking Docker network..."
if ! docker network ls | grep -q astro_default; then
    echo "Creating astro_default network..."
    docker network create astro_default
else
    echo "astro_default network already exists"
fi

# Start containers
echo "[5/8] Starting database containers..."
docker-compose up -d

# Wait for SQL Server to be ready
echo "[6/8] Waiting for SQL Server to start (this may take 1-2 minutes)..."
sleep 30

MAX_RETRIES=30
RETRY_COUNT=0
while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
    if docker exec mssql-server /opt/mssql-tools18/bin/sqlcmd \
        -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -C \
        -Q "SELECT 1" -b &>/dev/null; then
        echo "SQL Server is ready!"
        break
    fi
    RETRY_COUNT=$((RETRY_COUNT + 1))
    echo "Waiting for SQL Server... ($RETRY_COUNT/$MAX_RETRIES)"
    sleep 10
done

if [ $RETRY_COUNT -eq $MAX_RETRIES ]; then
    echo "ERROR: SQL Server failed to start in time"
    docker logs mssql-server | tail -50
    exit 1
fi

# Attach StackOverflow2010 database
echo "[7/8] Attaching StackOverflow2010 database..."
docker exec mssql-server /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -C \
  -Q "
    IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'StackOverflow2010')
    BEGIN
        CREATE DATABASE StackOverflow2010 ON
        (FILENAME = '/tmp/stackoverflow_db/StackOverflow2010.mdf'),
        (FILENAME = '/tmp/stackoverflow_db/StackOverflow2010_log.ldf')
        FOR ATTACH;
        PRINT 'Database attached successfully';
    END
    ELSE
    BEGIN
        PRINT 'Database already exists';
    END
  "

# Verify database and get table statistics
echo "[8/8] Verifying database..."
echo ""
echo "=== StackOverflow2010 Table Statistics ==="
docker exec mssql-server /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -C \
  -d StackOverflow2010 \
  -Q "
    SELECT
        t.name AS [Table],
        FORMAT(SUM(p.rows), 'N0') AS [Rows]
    FROM sys.tables t
    JOIN sys.partitions p ON t.object_id = p.object_id
    WHERE p.index_id IN (0, 1)
    GROUP BY t.name
    ORDER BY SUM(p.rows) DESC
  " -W

# Check PostgreSQL
echo ""
echo "=== PostgreSQL Status ==="
docker exec postgres-target psql -U postgres -c "SELECT version();" 2>/dev/null | head -3
docker exec postgres-target psql -U postgres -c "\l" 2>/dev/null | grep stackoverflow || echo "Database 'stackoverflow' exists"

echo ""
echo "=========================================="
echo "           SETUP COMPLETE!"
echo "=========================================="
echo ""
echo "SQL Server:"
echo "  Container: mssql-server"
echo "  Host (from Airflow): mssql-server"
echo "  Host (from local): localhost"
echo "  Port: 1433"
echo "  Database: StackOverflow2010"
echo "  Username: sa"
echo "  Password: $MSSQL_SA_PASSWORD"
echo ""
echo "PostgreSQL:"
echo "  Container: postgres-target"
echo "  Host (from Airflow): postgres-target"
echo "  Host (from local): localhost"
echo "  Port: 5433 (external), 5432 (internal)"
echo "  Database: stackoverflow"
echo "  Username: postgres"
echo "  Password: $POSTGRES_PASSWORD"
echo ""
echo "Next Steps:"
echo "1. Run 'astro dev stop && astro dev start' to reload Airflow with new connections"
echo "2. Access Airflow UI at http://localhost:8080"
echo "3. Trigger the 'mssql_to_postgres_migration' DAG"
echo ""