# AI Handoff Document - Astronomer to Vanilla Airflow Migration

## Project Context

**Repository**: mssql-to-postgres-pipeline
**Current Branch**: `feature/migrate-to-vanilla-airflow`
**Objective**: Migrate from Astronomer Runtime to vanilla Apache Airflow 3.0 using Docker Compose with LocalExecutor
**Date**: 2025-12-14

## What Has Been Completed

### Phase 1: Astronomer Testing (✓ COMPLETE)
- Successfully tested StackOverflow 2010 migration with Astronomer Runtime
- Downloaded SO2010 dataset (1.1GB, 19.3M rows across 9 tables)
- Migration completed in 18 minutes with 100% validation success
- Baseline performance established

### Phase 2: Vanilla Airflow Migration (IN PROGRESS)

#### Completed Changes:

1. **DAG Code Updates** (✓ COMPLETE)
   - File: `dags/mssql_to_postgres_migration.py`
   - Changed imports from `airflow.sdk` to `airflow.decorators`
   - Changed `from airflow.sdk import Asset` to `from airflow.datasets import Dataset as Asset`
   - **TEMP FIX**: Commented out failure callbacks (Airflow 3.0 limitation - see GitHub issue #44354)

2. **Validation DAG Updates** (✓ COMPLETE)
   - File: `dags/validate_migration_env.py`
   - Updated imports from Astronomer SDK to vanilla decorators

3. **Dockerfile Complete Rewrite** (✓ COMPLETE)
   - Changed base image from Astronomer Runtime to `apache/airflow:3.0.0-python3.11`
   - Added FreeTDS system packages for SQL Server connectivity
   - Added performance optimizations matching Astronomer settings
   - **CRITICAL FIX**: Added `ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow"` to resolve `include` module imports

4. **requirements.txt Updates** (✓ COMPLETE)
   - Added explicit `apache-airflow==3.0.0` dependency
   - Kept all database providers and drivers

5. **docker-compose.yml** (✓ COMPLETE)
   - Added 7 services: postgres-metadata, webserver, scheduler, dag-processor, triggerer, init, + existing databases
   - Changed from CeleryExecutor to LocalExecutor
   - **CRITICAL FIX**: Added `AIRFLOW__CORE__INTERNAL_API_URL: http://airflow-webserver:8080` for task execution
   - Replaced airflow_settings.yaml with environment variable connections:
     - `AIRFLOW_CONN_MSSQL_SOURCE`
     - `AIRFLOW_CONN_POSTGRES_TARGET`
   - Added include volume mount
   - Updated webserver command from `webserver` to `api-server` (Airflow 3.0 change)

6. **Feature Branch** (✓ COMPLETE)
   - Branch created: `feature/migrate-to-vanilla-airflow`
   - Pushed to remote

7. **Astronomer Artifacts Archived**
   - `.astro/` → `.astro.backup/`
   - `airflow_settings.yaml` → `airflow_settings.yaml.astronomer`

## Current Status

### What's Working:
- ✓ All 7 Docker containers healthy (except healthcheck warnings - see below)
- ✓ DAG processor successfully parsing DAGs after callback fix
- ✓ Main migration DAG loading: `mssql_to_postgres_migration`
- ✓ Validation DAG loading: `validate_migration_env`
- ✓ Database connections configured via environment variables
- ✓ Internal API communication configured

### Known Issues & Workarounds:

1. **Healthcheck Failures** (NON-BLOCKING)
   - Webserver and scheduler show as "unhealthy" in docker-compose ps
   - Root cause: `/health` endpoint returns 404
   - Impact: Visual only - services function correctly
   - Action: Can be ignored or healthchecks can be adjusted later

2. **Airflow 3.0 Callback Limitation** (FIXED with workaround)
   - Error: `NotImplementedError: Haven't coded Task callback yet`
   - GitHub Issue: https://github.com/apache/airflow/issues/44354
   - Fix Applied: Commented out `on_failure_callback` on line 92 and 94 of `mssql_to_postgres_migration.py`
   - Impact: Notification system temporarily disabled
   - Note: Callbacks edited inside container - changes need to be propagated to git

3. **File Permissions**
   - DAG files owned by UID 50000 (airflow container user)
   - Edits require `docker exec` commands to modify files
   - Example: `docker exec airflow-scheduler vi /opt/airflow/dags/file.py`

## Next Steps (IN ORDER)

### Immediate: Test Migration on Vanilla Airflow

1. **Trigger New Migration Run**:
   ```bash
   docker exec airflow-scheduler airflow dags trigger mssql_to_postgres_migration
   ```

2. **Monitor Progress**:
   ```bash
   docker-compose logs airflow-scheduler -f
   ```

3. **Check Task Execution**:
   - Watch for "extract_source_schema" and "create_target_schema" tasks to start
   - Monitor for connection errors (should be resolved with internal API fix)
   - Migration should complete in ~18 minutes matching Astronomer baseline

4. **Validate Results**:
   ```bash
   docker exec airflow-scheduler airflow dags trigger validate_migration_env
   ```
   - Expect 100% validation match (9 tables, 19.3M rows)

### After Successful Migration:

1. **Copy DAG Changes to Host**:
   ```bash
   docker cp airflow-scheduler:/opt/airflow/dags/mssql_to_postgres_migration.py \
     /home/johnd/repos/mssql-to-postgres-pipeline/dags/
   ```

2. **Update Documentation** (README.md):
   - Replace all `astro dev` commands with `docker-compose` equivalents:
     - `astro dev start` → `docker-compose up -d`
     - `astro dev stop` → `docker-compose down`
     - `astro dev logs -s -f` → `docker-compose logs airflow-scheduler -f`
   - Update connection configuration section (remove airflow_settings.yaml references)
   - Add note about Airflow 3.0 callback limitation
   - Update login credentials (user: airflow, pass: airflow)
   - Update container count: 5 → 7

3. **Update Documentation** (CLAUDE.md):
   - Update "Common Development Commands" section
   - Update DAG import examples
   - Add Airflow 3.0 limitations section
   - Update local development environment details

4. **Commit All Changes**:
   ```bash
   git add -A
   git commit -m "Complete Astronomer to vanilla Airflow 3.0 migration

   - Migrated from Astronomer Runtime to apache/airflow:3.0.0
   - Updated DAG imports from airflow.sdk to airflow.decorators
   - Replaced airflow_settings.yaml with environment variables
   - Added internal API URL configuration for task execution
   - Added PYTHONPATH for include module resolution
   - Temporarily disabled failure callbacks (Airflow 3.0 limitation)
   - Updated docker-compose with 7-service architecture

   Tested with StackOverflow2010:
   - 9 tables, 19.3M rows
   - 100% validation match
   - Performance maintained vs Astronomer baseline

   Known limitations:
   - Failure notifications disabled pending Airflow issue #44354
   - Healthcheck endpoints return 404 (non-blocking)

   🤖 Generated with Claude Code"
   ```

5. **Create Pull Request**:
   ```bash
   git push origin feature/migrate-to-vanilla-airflow
   gh pr create --title "Migrate from Astronomer to vanilla Airflow 3.0" --body "$(cat <<'EOF'
   ## Summary
   Migrates project from Astronomer Runtime to vanilla Apache Airflow 3.0 using Docker Compose with LocalExecutor for improved deployment flexibility.

   ## Changes
   - Updated base Docker image and all dependencies
   - Migrated DAG imports to vanilla Airflow decorators API
   - Replaced proprietary airflow_settings.yaml with standard env vars
   - Added comprehensive docker-compose stack (7 services)
   - Configured internal API for Airflow 3.0 task execution

   ## Testing
   - ✅ SO2010 migration: 9 tables, 19.3M rows, 18 min
   - ✅ 100% validation match
   - ✅ Performance matches Astronomer baseline

   ## Known Limitations
   - Failure callbacks temporarily disabled (Airflow 3.0 issue #44354)
   - Healthcheck endpoints need adjustment (non-blocking)

   🤖 Generated with Claude Code
   EOF
   )"
   ```

## Important File Locations

### Modified Files:
- `/home/johnd/repos/mssql-to-postgres-pipeline/dags/mssql_to_postgres_migration.py` (callbacks commented)
- `/home/johnd/repos/mssql-to-postgres-pipeline/dags/validate_migration_env.py` (imports updated)
- `/home/johnd/repos/mssql-to-postgres-pipeline/Dockerfile` (complete rewrite)
- `/home/johnd/repos/mssql-to-postgres-pipeline/docker-compose.yml` (7 services added)
- `/home/johnd/repos/mssql-to-postgres-pipeline/requirements.txt` (airflow added)

### Archived Files:
- `.astro.backup/` (original .astro directory)
- `airflow_settings.yaml.astronomer` (original connection config)

### Log Files:
- Background monitoring: `/tmp/claude/tasks/b835681.output` (may no longer be active)

## Commands Reference

### Start/Stop Services:
```bash
docker-compose up -d              # Start all services
docker-compose down               # Stop all services
docker-compose ps                 # Check service status
docker-compose logs <service> -f  # Follow logs
```

### DAG Management:
```bash
docker exec airflow-scheduler airflow dags list
docker exec airflow-scheduler airflow dags list-runs mssql_to_postgres_migration
docker exec airflow-scheduler airflow dags trigger <dag_id>
```

### Debugging:
```bash
docker exec -it airflow-scheduler bash
docker exec airflow-scheduler airflow connections list
docker exec airflow-scheduler airflow connections test mssql_source
```

### Database Access:
```bash
# SQL Server
docker exec -it mssql-server /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P 'YourStrong@Passw0rd' -C

# PostgreSQL Target
docker exec -it postgres-target psql -U postgres -d stackoverflow

# Airflow Metadata
docker exec -it airflow-postgres psql -U airflow -d airflow
```

## Critical Environment Variables

In `docker-compose.yml` under `x-airflow-common.environment`:

```yaml
# REQUIRED for task execution in Airflow 3.0
AIRFLOW__CORE__INTERNAL_API_URL: http://airflow-webserver:8080

# Database connections (instead of airflow_settings.yaml)
AIRFLOW_CONN_MSSQL_SOURCE: 'mssql://sa:YourStrong@Passw0rd@mssql-server:1433/StackOverflow2010'
AIRFLOW_CONN_POSTGRES_TARGET: 'postgresql://postgres:PostgresPassword123@postgres-target:5432/stackoverflow'

# Performance tuning
AIRFLOW__CORE__PARALLELISM: 128
AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG: 64
MAX_PARTITIONS: 8
```

## Troubleshooting Tips

### If DAGs don't load:
1. Check dag-processor logs: `docker-compose logs airflow-dag-processor`
2. Verify PYTHONPATH includes /opt/airflow
3. Check for import errors in DAG files

### If tasks fail with "Connection refused":
1. Verify AIRFLOW__CORE__INTERNAL_API_URL is set
2. Check webserver is running: `docker-compose ps airflow-webserver`
3. Verify network connectivity: `docker exec airflow-scheduler curl http://airflow-webserver:8080`

### If migration is slow:
1. Check MAX_PARTITIONS setting (should be 8 for 32GB RAM)
2. Monitor PostgreSQL resources: `docker stats postgres-target`
3. Check connection pool settings in docker-compose.yml

## Success Criteria

The migration is complete when:
- ✅ DAG triggers without errors
- ✅ All 9 tables migrate successfully
- ✅ Validation shows 100% row count match
- ✅ Migration completes in ~18 minutes (± 20%)
- ✅ No connection errors in logs
- ✅ Documentation updated with new commands
- ✅ Pull request created and ready for review

## Context from Previous Session

This work is a continuation from a previous session. The user's goal is to remove dependency on Astronomer's proprietary tooling to gain deployment flexibility. The approach was:

1. **Phase 1**: Validate the migration works with current Astronomer setup (COMPLETED - 18 min, 100% success)
2. **Phase 2**: Migrate infrastructure to vanilla Airflow while preserving functionality (IN PROGRESS)

The user explicitly chose:
- Docker Compose over Kubernetes
- LocalExecutor over Celery
- Test-first approach (validate before migrating)

## Notes for Next AI

- The user prefers concise, action-oriented communication
- Focus on completing the migration test first before moving to documentation
- The business logic in `include/mssql_pg_migration/` directory is 100% compatible - no changes needed there
- The notification system will need to be re-enabled once Airflow 3.0 implements callbacks
- File permissions require using `docker exec` for edits - remember to copy changes back to host
- The healthcheck warnings can be ignored for now - they're cosmetic

## Last Known State

**Time**: 2025-12-14 02:15 UTC
**Services**: All 7 containers running (2 showing unhealthy but functional)
**DAG Status**: Successfully loading after callback fix
**Next Action**: Trigger migration run and monitor for success
**Blocking Issues**: None - ready to test
