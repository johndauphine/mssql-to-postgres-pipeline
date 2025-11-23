# Migration Performance Report

## Run: 2025-11-23T17:18:15Z

**Run ID:** `manual__2025-11-23T17:18:15.048851+00:00`
**Final Status:** Success (validation DAG triggered and passed)
**Total Duration:** 00:02:33.58 (153.58s)

### Migration Timeline (UTC)

| Phase | Start | End | Duration |
|-------|-------|-----|----------|
| DAG Start | 17:18:15.233 | 17:20:48.816 | 00:02:33.58 |
| extract_source_schema | 17:18:15.290 | 17:18:17.522 | 00:00:02.23 |
| create_target_schema | 17:18:15.290 | 17:18:16.159 | 00:00:00.87 |
| create_target_tables | 17:18:18.375 | 17:18:18.624 | 00:00:00.25 |
| transfer_table_data (parallel) | 17:18:19.156 | 17:20:14.652 | 00:01:55.50 |
| create_foreign_keys | 17:20:15.658 | 17:20:15.812 | 00:00:00.15 |
| trigger_validation_dag | 17:20:16.741 | 17:20:46.847 | 00:00:30.11 |
| generate_migration_summary | 17:20:47.756 | 17:20:47.863 | 00:00:00.11 |
| validate_migration_env DAG | 17:20:17.786 | 17:20:19.389 | 00:00:01.60 |

### Table Transfer Performance (Run 2025-11-23)

| Table (map idx) | Start | End | Duration | Rows | Rows/sec |
|-----------------|-------|-----|----------|------|----------|
| Users (0) | 17:18:19.156 | 17:18:27.195 | 00:00:08.04 | 299,398 | 37,242 |
| Posts (1) | 17:18:19.156 | 17:19:07.117 | 00:00:47.96 | 3,729,195 | 77,755 |
| LinkTypes (2) | 17:18:19.189 | 17:18:19.626 | 00:00:00.44 | 2 | 5 |
| PostLinks (3) | 17:18:19.189 | 17:18:20.790 | 00:00:01.60 | 161,519 | 100,873 |
| Votes (4) | 17:18:19.221 | 17:20:14.652 | 00:01:55.43 | 10,143,364 | 87,874 |
| PostTypes (5) | 17:18:19.637 | 17:18:19.789 | 00:00:00.15 | 8 | 53 |
| Badges (6) | 17:18:19.799 | 17:18:29.995 | 00:00:10.20 | 1,102,019 | 108,086 |
| Comments (7) | 17:18:20.310 | 17:19:19.985 | 00:00:59.68 | 3,875,183 | 64,938 |
| VoteTypes (8) | 17:18:20.805 | 17:18:21.661 | 00:00:00.86 | 15 | 18 |

**Total Rows Migrated:** ~19.3M (≈125,700 rows/sec overall)

Key observations:
- Streaming COPY kept memory flat while allowing adaptive chunks (no retries observed).
- Map idx 4 (Votes) remains the dominant critical path at ~115s; all other tables completed in <60s.
- Validation DAG (`validate_migration_env`) succeeded in 1.6s immediately after the transfer phase, confirming row-count parity.

---

**Run Date:** 2025-11-22
**Run ID:** `manual__2025-11-22T22:33:42.629105+00:00`
**Final Status:** Failed (validation task - data transfer successful)

---

## Execution Environment

### Host System
| Property | Value |
|----------|-------|
| OS | Darwin 25.1.0 (macOS) |
| Architecture | arm64 |
| CPU | Apple M3 Max |
| Host Memory | 36 GB |

### Docker Resources
| Property | Value |
|----------|-------|
| Docker CPUs | 14 |
| Docker Memory | 7.7 GB |

### Database Versions
| Database | Version |
|----------|---------|
| SQL Server | Microsoft SQL Server 2022 (RTM-CU21) - 16.0.4215.2 (X64) |
| PostgreSQL | PostgreSQL 16.11 on aarch64-unknown-linux-musl |

---

## Source Database (StackOverflow2010)

| Table | Rows |
|-------|------|
| Votes | 10,143,364 |
| Comments | 3,875,183 |
| Posts | 3,729,195 |
| Badges | 1,102,019 |
| Users | 299,398 |
| PostLinks | 161,519 |
| VoteTypes | 15 |
| PostTypes | 8 |
| LinkTypes | 2 |
| **Total** | **19,310,703** |

---

## Migration Timeline

| Phase | Start Time (UTC) | End Time (UTC) | Duration |
|-------|------------------|----------------|----------|
| DAG Start | 22:33:43 | - | - |
| extract_source_schema | 22:33:43 | 22:33:45 | 2.2s |
| create_target_schema | 22:33:43 | 22:33:44 | 0.98s |
| create_target_tables | 22:33:46 | 22:33:46 | 0.27s |
| transfer_table_data (parallel) | 22:33:47 | 22:49:48 | ~16 min |
| create_foreign_keys | 22:49:48 | 22:49:49 | 0.30s |
| validate_migration | 22:49:48 | 22:51:22 | Failed (XCom error) |
| **Total DAG Duration** | 22:33:43 | 22:51:24 | **17 min 41 sec** |

---

## Table Transfer Performance

| Table | Start (UTC) | End (UTC) | Duration | Rows | Rows/sec |
|-------|-------------|-----------|----------|------|----------|
| LinkTypes (idx 2) | 22:33:47 | 22:33:48 | 0.6s | 2 | 3 |
| PostTypes (idx 5) | 22:33:48 | 22:33:49 | 1.0s | 8 | 8 |
| VoteTypes (idx 8) | 22:33:49 | 22:33:50 | 0.9s | 15 | 17 |
| PostLinks (idx 3) | 22:33:47 | 22:33:53 | 5.3s | 161,519 | 30,475 |
| Users (idx 0) | 22:33:47 | 22:34:16 | 28.6s | 299,398 | 10,468 |
| Badges (idx 6) | 22:33:48 | 22:34:08 | 19.3s | 1,102,019 | 57,099 |
| Posts (idx 1) | 22:33:47 | 22:39:40 | 5 min 53s | 3,729,195* | 10,566 |
| Comments (idx 7) | 22:33:49 | 22:44:44 | 10 min 55s | 3,875,183 | 5,917 |
| Votes (idx 4) | 22:33:47 | 22:49:48 | 16 min 1s | 10,143,364 | 10,556 |

*Note: Posts table transfer may be incomplete (2,910,000 rows in target vs 3,729,195 source)

---

## Data Verification

| Table | Source Rows | Target Rows | Match |
|-------|-------------|-------------|-------|
| votes | 10,143,364 | 10,143,364 | Yes |
| comments | 3,875,183 | 3,875,183 | Yes |
| posts | 3,729,195 | 2,910,000 | **No** |
| badges | 1,102,019 | 1,102,019 | Yes |
| users | 299,398 | 299,398 | Yes |
| postlinks | 161,519 | 161,519 | Yes |
| votetypes | 15 | 15 | Yes |
| posttypes | 8 | 8 | Yes |
| linktypes | 2 | 2 | Yes |

---

## Performance Metrics

| Metric | Value |
|--------|-------|
| Total Rows Migrated | ~18,491,508 |
| Overall Throughput | ~17,450 rows/sec |
| Parallelism | 9 concurrent table transfers |
| Chunk Size | 10,000 rows (default) |
| Largest Table Duration | 16 min 1 sec (Votes - 10.1M rows) |

---

## Issues Encountered

1. **Validation Task Failure**: XCom not found error for offset=9 in `transfer_table_data`. The validation task retried 3 times and failed, causing `generate_final_report` to be skipped.

2. **Posts Table Incomplete**: Target has 2,910,000 rows vs 3,729,195 source rows (78% complete). This may be a chunking issue where the last chunk didn't complete successfully despite task success status.

---

## Recommendations

1. Investigate the Posts table transfer issue - may need to add explicit row count verification before marking task as success
2. Fix XCom handling in validation task to gracefully handle missing return values
3. Consider increasing chunk_size for large tables to reduce overhead
4. Add retry logic at the chunk level for more resilient transfers
