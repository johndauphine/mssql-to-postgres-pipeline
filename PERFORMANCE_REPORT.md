# Migration Performance Report

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
