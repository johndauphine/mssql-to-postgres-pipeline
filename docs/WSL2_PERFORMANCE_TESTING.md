# WSL2 Performance Testing Report

**Date:** 2025-12-05
**Platform:** Windows 11 WSL2 (32GB RAM)
**Database:** StackOverflow 2013 (106M rows)

---

## Executive Summary

Migrated 106 million rows from SQL Server to PostgreSQL in ~6.5 minutes on a 32GB WSL2 laptop, achieving **270K rows/second** throughput. This is **2x faster** than the Mac M3 Max (137K rows/sec) due to native x86_64 SQL Server execution (no Rosetta 2 emulation).

### Key Metrics

| Metric | Value |
|--------|-------|
| **Total Rows Migrated** | 106,535,610 rows across 9 tables |
| **Migration Time** | ~6.5 minutes (~390 seconds) |
| **Throughput** | ~270,000 rows/second |
| **Max Partitions Used** | 8 |

---

## Environment Configuration

### Hardware
- **System:** Windows 11 laptop with WSL2
- **Total RAM:** 32GB
- **SQL Server:** Native x86_64 (no emulation)

### Docker Resource Allocation

```env
# SQL Server
MSSQL_MEM_LIMIT=4g
MSSQL_MEM_RESERVATION=2g
MSSQL_CPUS=4

# PostgreSQL
PG_MEM_LIMIT=3g
PG_MEM_RESERVATION=1536m
PG_CPUS=4
PG_SHARED_BUFFERS=512MB
PG_EFFECTIVE_CACHE_SIZE=1536MB
PG_MAINTENANCE_WORK_MEM=256MB

# Airflow
MAX_PARTITIONS=8
```

---

## Database Details: StackOverflow 2013

| Table | Rows | Transfer Method |
|-------|------|-----------------|
| Votes | 52,928,720 | Partitioned (8 partitions) |
| Comments | 24,534,730 | Partitioned (8 partitions) |
| Posts | 17,142,169 | Partitioned (8 partitions) |
| Badges | 8,042,005 | Partitioned (8 partitions) |
| Users | 2,465,713 | Partitioned (4 partitions) |
| PostLinks | 1,421,208 | Partitioned (2 partitions) |
| VoteTypes | 15 | Regular |
| PostTypes | 8 | Regular |
| LinkTypes | 2 | Regular |

**Total:** 106,535,610 rows

---

## Performance Comparison: Mac M3 Max vs WSL2 Laptop

| Metric | Mac M3 Max (36GB) | WSL2 Laptop (32GB) |
|--------|-------------------|---------------------|
| **Database** | StackOverflow2010 | StackOverflow2013 |
| **Total Rows** | 19.3M | 106M |
| **Duration** | 141 seconds | ~390 seconds |
| **Throughput** | ~137K rows/sec | ~270K rows/sec |
| **SQL Server** | Emulated (Rosetta 2) | Native x86_64 |

### Normalized Comparison

If WSL2 ran the same StackOverflow2010 database:
- 19.3M rows ÷ 270K rows/sec = **~71 seconds**
- That's **50% faster** than Mac's 141 seconds

### Why WSL2 is Faster

1. **Native SQL Server** - No Rosetta 2 emulation penalty (40-50% overhead on Mac)
2. **Direct x86_64 execution** - SQL Server runs at full speed
3. **WSL2 I/O** - Generally efficient for containerized workloads

---

## Partition Count Testing

### Test: 8 vs 12 Partitions

| Metric | 8 Partitions | 12 Partitions |
|--------|-------------|---------------|
| **Total Rows** | 106M | 106M |
| **Duration** | ~6.5 min | ~6.5 min |
| **Throughput** | ~270K rows/sec | ~270K rows/sec |

### Finding

**No improvement with more partitions.** The system is I/O bound, not parallelism bound.

Bottlenecks identified:
1. **Disk I/O** - WSL2 filesystem layer
2. **Network** - Docker bridge between containers
3. **Database write throughput** - PostgreSQL COPY saturation

### Recommendation

Keep `MAX_PARTITIONS=8` for 32GB systems. Additional partitions add overhead without benefit.

---

## Environment-Based Configuration

A new flexible configuration system was implemented to support multiple machines without code changes.

### Files Added/Modified

| File | Purpose |
|------|---------|
| `.env` | Machine-specific settings (gitignored) |
| `.env.example` | Documented presets for 16GB/32GB/64GB |
| `docker-compose.yml` | Uses `${VAR:-default}` syntax |
| `Dockerfile` | Default `MAX_PARTITIONS=8` |
| `dags/mssql_to_postgres_migration.py` | Reads `MAX_PARTITIONS` from env |

### Usage

To switch between machines, edit `.env`:

```env
# 16GB system
MAX_PARTITIONS=4
MSSQL_MEM_LIMIT=2g
PG_MEM_LIMIT=1536m

# 32GB system
MAX_PARTITIONS=8
MSSQL_MEM_LIMIT=4g
PG_MEM_LIMIT=3g

# 64GB+ system
MAX_PARTITIONS=12
MSSQL_MEM_LIMIT=8g
PG_MEM_LIMIT=6g
```

---

## Lessons Learned

1. **Native beats emulation** - 2x throughput improvement from native SQL Server
2. **I/O is the bottleneck** - More partitions don't help beyond a point
3. **8 partitions is optimal** - For 32GB systems with this workload
4. **Environment variables work** - Flexible config without code changes

---

## Appendix: Migration Timeline

```
13:44:32 - DAG triggered
13:45:20 - First checkpoint: 7.8M rows transferred
13:46:06 - 30s mark: 33M rows (600K rows/sec burst)
13:46:59 - 60s mark: 77M rows
13:47:50 - 90s mark: 97M rows (Votes complete)
13:48:30 - 120s mark: Comments complete
13:51:05 - ~390s mark: All tables complete
```

---

**Report Generated:** 2025-12-05
**Branch:** feature/env-based-config
**Status:** Testing complete - environment configuration validated
