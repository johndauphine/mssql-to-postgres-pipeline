# Low Memory Configuration Guide

This document describes optimized settings for running the MSSQL to PostgreSQL migration pipeline on systems with limited RAM (16 GB or less).

## Configuration Changes

### Docker Container Memory (docker-compose.yml)

| Container | Original | Low Memory | Notes |
|-----------|----------|------------|-------|
| SQL Server mem_limit | 4 GB | 2 GB | Minimum for SQL Server |
| SQL Server mem_reservation | 2 GB | 1 GB | |
| SQL Server CPUs | 8 | 4 | |
| PostgreSQL mem_limit | 4 GB | 1.5 GB | |
| PostgreSQL mem_reservation | 2 GB | 768 MB | |
| PostgreSQL CPUs | 6 | 3 | |

### PostgreSQL Tuning Parameters

| Parameter | Original | Low Memory | Purpose |
|-----------|----------|------------|---------|
| max_connections | 200 | 100 | Reduced connection overhead |
| shared_buffers | 1 GB | 256 MB | Main memory cache |
| effective_cache_size | 2 GB | 512 MB | Query planner hint |
| maintenance_work_mem | 256 MB | 64 MB | Index creation memory |
| wal_buffers | 64 MB | 16 MB | WAL write buffer |
| max_wal_size | 4 GB | 1 GB | WAL retention |

### DAG Parallelism (mssql_to_postgres_migration.py)

| Setting | Original | Low Memory | Impact |
|---------|----------|------------|--------|
| max_active_tasks | 64 | 16 | Fewer concurrent transfers |
| Max partitions per table | 8 | 4 | Reduced parallel partitions |

## Memory Budget (16 GB System)

| Component | Allocation |
|-----------|------------|
| SQL Server container | 2 GB |
| PostgreSQL container | 1.5 GB |
| Airflow containers (~5) | ~2 GB |
| **Total Docker/WSL** | **~5.5 GB** |
| Windows OS + apps | ~10 GB |

## Performance Results

Tested against StackOverflow2010 dataset (19.3M rows, 9 tables):

| Metric | High Memory | Low Memory | Difference |
|--------|-------------|------------|------------|
| Migration Duration | ~2.5 min | ~2.2 min | Similar |
| Peak SQL Server Memory | 4 GB | 1.47 GB | -63% |
| Peak PostgreSQL Memory | 4 GB | 1.05 GB | -74% |
| Validation | 9/9 tables | 9/9 tables | No change |

### Observed Peak Memory Usage

| Container | Limit | Peak | Headroom |
|-----------|-------|------|----------|
| SQL Server | 2 GB | 1.47 GB | 27% free |
| PostgreSQL | 1.5 GB | 1.05 GB | 30% free |

## When to Use Low Memory Settings

Use these settings when:
- Running on a system with 16 GB RAM or less
- Running Docker Desktop on Windows with WSL2
- Needing to leave resources for other applications
- Experiencing out-of-memory errors or system slowdowns

## When to Use High Memory Settings

Revert to original settings when:
- Running on a system with 32+ GB RAM
- Running on dedicated Linux servers
- Migrating very large datasets (100M+ rows)
- Maximum throughput is required

## Applying Low Memory Configuration

The configuration changes are already applied in:
- `docker-compose.yml` - Container resource limits and PostgreSQL tuning
- `dags/mssql_to_postgres_migration.py` - DAG parallelism settings

To revert to high-memory settings, increase the values back to their original levels as documented above.

## Troubleshooting

### Migration Still Slow

If migration is slower than expected:
1. Check `docker stats` for memory pressure
2. Increase container memory limits if headroom exists
3. Verify WSL2 memory limit in `%UserProfile%\.wslconfig`

### Out of Memory Errors

If you see OOM errors:
1. Reduce `max_active_tasks` further (try 8)
2. Reduce partition count (try 2)
3. Increase WSL2 swap in `.wslconfig`

### WSL2 Configuration

Create/edit `%UserProfile%\.wslconfig`:

```ini
[wsl2]
memory=8GB
swap=4GB
```

Then restart WSL: `wsl --shutdown`

## Test Results

**Test Date:** 2025-12-04
**Environment:** WSL2 on Windows with 16 GB RAM
**Dataset:** StackOverflow2010 (19.3M rows)

| Run | Duration | Status |
|-----|----------|--------|
| Run 1 | 2 min 18 sec | Success |
| Run 2 | 2 min 13 sec | Success |

All 9 tables migrated and validated successfully with 100% row count match.
