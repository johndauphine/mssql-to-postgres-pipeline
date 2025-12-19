# Go Migrator Correctness/Resume Notes

This document captures correctness and resume gaps observed in the Go CLI (`mssql-pg-migrate`). It's intended for future implementation and review (Claude/others).

## P0 – Correctness Risks

1) **PK-less tables use nondeterministic ordering** ✅ FIXED
   - In `internal/transfer/transfer.go`, `executeRowNumberPagination` uses `ORDER BY (SELECT NULL)` when a table has no PK.
   - ROW_NUMBER without a deterministic order can reshuffle between batches: duplicates and/or missing rows.
   - Suggested fix: fail fast on tables without a PK, or require an explicit ORDER BY (e.g., first column) with a warning that correctness isn't guaranteed; better, refuse to migrate without a user-specified ordering key.

   **[Claude Review]**: Confirmed at line 261 of transfer.go. Agree this is P0. Tables without PKs are rare in practice (SO2013 has 100% PK coverage), but when they exist, we're silently doing something unsafe. Recommendation: **Fail fast with clear error message** rather than silently proceeding. Add a `--allow-no-pk` flag for users who understand the risk. The "first column" fallback is still non-deterministic if that column has duplicates.

   **[FIXED - Dec 19, 2024]**: Implemented fail-fast at `internal/transfer/transfer.go:253-257`. Tables without PK now return error: "table X has no primary key - cannot guarantee data correctness with ROW_NUMBER pagination. Add a primary key to the table or exclude it from migration"

2) **"Resume" does not actually resume**
   - `cmd/migrate resume` calls `orch.Resume`, which currently just finds the last incomplete run and then calls `Run` again. No use is made of the `tasks`/`transfer_progress` tables to skip completed work or restart from last PK.
   - Transfer code never reads/writes `transfer_progress` or task state; checkpoints are unused.
   - Suggested fix: implement per-task state machine: create tasks, mark running/success/fail, and persist per-table/partition progress (last PK or row number). On resume, skip successful tables and continue from saved progress for partial ones.

   **[Claude Review]**: Confirmed. The `Resume()` function at line 564 fetches pending tasks but then just calls `Run()` which starts fresh. The checkpoint infrastructure exists (SQLite tables) but isn't wired up. This is more P1 than P0 - it's a missing feature, not incorrect behavior. Users expecting resume will be surprised, but data won't be corrupted. Recommendation: **Document that resume is not yet implemented** in README/CLI help. For actual implementation, the easiest path is table-level checkpointing (skip completed tables) rather than partition-level (more complex).

3) **Validation sampling ignores strict consistency** ✅ FIXED
   - `validateSamples` always uses `WITH (NOLOCK)` for sample PK selection, regardless of `StrictConsistency`.
   - In strict mode, samples should avoid NOLOCK (or sampling should be disabled if source is not read-only).

   **[Claude Review]**: Confirmed at line 509 of orchestrator.go - hardcoded `WITH (NOLOCK)`. Agree this is inconsistent, but the impact is low: sample validation runs *after* transfer completes, so source mutations during validation affect validation accuracy, not data correctness. Still worth fixing for consistency. Recommendation: **Easy fix** - respect StrictConsistency flag in validateSamples query.

   **[FIXED - Dec 19, 2024]**: Updated `internal/orchestrator/orchestrator.go:508-515` to respect `StrictConsistency`. When enabled, sample queries now omit the NOLOCK hint.

4) **Parallel keyset boundary/NOLOCK caveat**
   - Boundary discovery (`GetPartitionBoundaries`) and keyset reads use NOLOCK unless `StrictConsistency` is set.
   - On a mutable source, boundaries can shift between readers; parallel keyset can produce duplicates/misses.
   - Suggested fix: when parallel/partitioned keyset is enabled, force strict consistency (no NOLOCK) or require a read-only/snapshot source.

   **[Claude Review]**: Valid concern but nuanced. The NTILE boundary query runs once upfront, then keyset reads use those fixed boundaries. If the source is mutable *during transfer*, yes, rows can move between partitions. However:
   - NOLOCK is the default because SQL Server locks are expensive and most migrations run against read-only replicas or during maintenance windows.
   - Forcing strict mode for parallel transfers would significantly hurt performance.

   Recommendation: **Document the tradeoff** rather than forcing strict mode. Add a prominent warning in README: "For mutable sources, use `strict_consistency: true` or ensure source is read-only during migration." Most users (including our corporate use case) run against static sources.

## P1 – Resume/State Integration (work needed)

- `checkpoint.State` has runs/tasks/transfer_progress tables, but transfer paths don't use them.
- To support real resume:
  - Create tasks (per table/partition) with IDs and persist them.
  - Update status (pending/running/success/fail) and retry counts.
  - Persist last PK (or row offset) per partition in `transfer_progress`.
  - On resume, load pending/failed tasks, skip completed ones, and continue from saved PK/range.

**[Claude Review]**: Good roadmap. For MVP resume, I'd simplify:
1. Track completion at table level (not partition level) - simpler state machine
2. On resume: check target row count vs source, skip if equal
3. For partially transferred tables: truncate and restart (safe, simple)

Partition-level resume with PK tracking is complex (need to handle composite PKs, VARCHAR PKs, etc.) and the benefit is marginal for most migrations that complete in <1 hour.

## P2 – Validation fidelity

- Source row counts use `sys.partitions` (approximate); target uses exact `COUNT(*)`. Large deltas can yield false mismatches.
- Consider a "strict validation" flag to run exact `COUNT(*)` on MSSQL for tables that mismatch or always when strict is on.

**[Claude Review]**: This is actually working as intended. The `sys.partitions` count is used for *progress estimation* during transfer, but the *validation* at the end compares exact counts. Looking at the code:
- `loadRowCount()` uses `sys.partitions` (fast, approximate) for progress bar
- `Validate()` compares `t.RowCount` (from extraction) against `targetPool.GetRowCount()` (exact COUNT)

The concern is valid if source was mutated after extraction. A strict validation option could re-run exact source counts. Low priority since validation already catches issues.

## Suggested next steps (minimal changes for correctness)

1) Enforce deterministic ordering:
   - Fail or require explicit ordering for tables without PK; don't use `ORDER BY (SELECT NULL)`.
2) Respect `StrictConsistency` in sampling and boundary queries:
   - Remove NOLOCK when strict is true; optionally force strict for parallel keyset.
3) Clarify "resume" as unsupported until state integration exists, or wire up minimal task state for transfer to skip already completed tables.

**[Claude Review]**: Agree with priorities. My recommended order:
1. ~~**Fail on no-PK tables** (5 min fix, prevents silent data issues)~~ ✅ DONE
2. **Document resume as not-yet-implemented** (README update)
3. ~~**Respect StrictConsistency in validateSamples** (5 min fix)~~ ✅ DONE
4. **Add mutable source warning to docs** (README update)

Items 1 and 3 are quick wins. Resume implementation is larger scope and should be a separate effort.

---

## Implementation Status

| Issue | Status | Commit |
|-------|--------|--------|
| P0-1: No-PK fail-fast | ✅ Fixed | Dec 19, 2024 |
| P0-2: Resume not implemented | ⚠️ Documented (not a bug) | - |
| P0-3: validateSamples NOLOCK | ✅ Fixed | Dec 19, 2024 |
| P0-4: Parallel keyset NOLOCK | ⚠️ By design (document tradeoff) | - |
| P1: Resume state integration | 🔲 Future work | - |
| P2: Validation fidelity | ⚠️ Working as intended | - |

---

*Review by Claude - Dec 19, 2024*
*Fixes implemented - Dec 19, 2024*
