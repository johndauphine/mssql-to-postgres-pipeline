#!/bin/bash
# =============================================================================
# Setup Airflow Connections from Environment Variables
# =============================================================================
# This script creates Airflow connections using values from .env file.
# Run this after starting Airflow to configure database connections.
#
# Usage:
#   ./scripts/setup_connections.sh
#
# Or via docker:
#   docker exec <scheduler-container> /usr/local/airflow/scripts/setup_connections.sh
# =============================================================================

set -e

echo "Setting up Airflow connections from environment variables..."

# Source connection (MSSQL)
echo "Creating mssql_source connection..."
airflow connections delete mssql_source 2>/dev/null || true
airflow connections add mssql_source \
  --conn-type mssql \
  --conn-host "${MSSQL_HOST:-mssql-server}" \
  --conn-port "${MSSQL_PORT:-1433}" \
  --conn-login "${MSSQL_USER:-sa}" \
  --conn-password "${MSSQL_PASSWORD:-YourStrong@Passw0rd}" \
  --conn-schema "${MSSQL_DATABASE:-StackOverflow2013}"

echo "  Host: ${MSSQL_HOST:-mssql-server}"
echo "  Database: ${MSSQL_DATABASE:-StackOverflow2013}"

# Target connection (PostgreSQL)
echo "Creating postgres_target connection..."
airflow connections delete postgres_target 2>/dev/null || true
airflow connections add postgres_target \
  --conn-type postgres \
  --conn-host "${PG_HOST:-postgres-target}" \
  --conn-port "${PG_PORT:-5432}" \
  --conn-login "${PG_USER:-postgres}" \
  --conn-password "${PG_PASSWORD:-PostgresPassword123}" \
  --conn-schema "${PG_DATABASE:-stackoverflow}"

echo "  Host: ${PG_HOST:-postgres-target}"
echo "  Database: ${PG_DATABASE:-stackoverflow}"

echo ""
echo "Connections configured successfully!"
echo ""
echo "To verify, run: airflow connections list"
