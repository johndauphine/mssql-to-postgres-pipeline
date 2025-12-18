# Correctness Priorities (MSSQL → Postgres Pipeline)

This document ranks the highest-impact correctness risks discovered during a code review, and proposes remediation approaches.

Scope:
- Primary migration DAG: `dags/mssql_to_postgres_migration.py`
- Transfer engine: `include/mssql_pg_migration/data_transfer.py`
- Type mapping / DDL: `include/mssql_pg_migration/type_mapping.py`, `include/mssql_pg_migration/ddl_generator.py`
- Validation DAG: `dags/validate_migration_env.py`

Audience:
- Intended for future AI/code assistants (e.g., Claude) and maintainers who will implement fixes.
- Prioritizes correctness over speed.

## P0 — Must Fix Before Trusting Results

### P0.1 NULL vs empty-string corruption in COPY

**What happens**
- `_normalize_value()` converts `None` → empty string (`''`) (`include/mssql_pg_migration/data_transfer.py`).
- COPY uses `NULL ''` (empty string treated as NULL) (`include/mssql_pg_migration/data_transfer.py`).
- Result: a real empty string in SQL Server becomes NULL in Postgres; NULL and empty string are conflated.

**Why it matters**
- Silent data corruption for any text columns with legitimate empty strings.

**Suggested remediation**
- Use a dedicated NULL marker for COPY (common: `\\N`) and emit it only for `None`.
- Ensure the CSV writer never emits that marker for non-null values.
- Update COPY options accordingly (e.g., `NULL '\\N'`) and ensure strings containing `\\N` are handled safely.

**Notes**
- This is one of the easiest fixes with the highest correctness payoff.

### P0.2 Binary data corruption (`varbinary`, `image`, etc.)

**What happens**
- Bytes are decoded as UTF-8 with `ignore` (`include/mssql_pg_migration/data_transfer.py`), which drops/changes data.

**Why it matters**
- Silent corruption for all binary payloads (attachments, hashes, compressed blobs, etc.).

**Suggested remediation**
- Emit Postgres `bytea` in a safe textual representation (recommended: hex with `\\x...` prefix).
- Ideally normalize based on target column type (BYTEA vs text), not only Python value type.
  - Option A: query Postgres target column types once per table and pass that metadata into `_normalize_value()`.
  - Option B: carry mapped types from schema extraction/DDL generation into `table_info` and use that to normalize.

### P0.3 “Large table” can be silently skipped from migration

**What happens**
- Large tables (`row_count >= LARGE_TABLE_THRESHOLD`) are excluded from `prepare_regular_tables(...)` and are expected to be handled by partition tasks.
- Partition planning can `continue` (skip) if identifier validation fails (`validate_sql_identifier`) or boundary queries fail.
- Net effect: a large table may end up with **no regular transfer and no partitions**, and thus never be migrated.

**Where**
- Table categorization: `dags/mssql_to_postgres_migration.py` (regular vs partitioned)
- Partition planning: `dags/mssql_to_postgres_migration.py`

**Suggested remediation**
- Never drop tables on partition planning failure:
  - Fallback to regular transfer for that table (even if large).
  - Or mark it as failed explicitly and stop the DAG (if correctness-first strict mode).
- Add explicit logging and a final “expected vs planned vs executed” table list check.

### P0.4 `NOLOCK` breaks correctness when source is changing

**What happens**
- MSSQL reads use `WITH (NOLOCK)` in both keyset and row-number pagination.

**Why it matters**
- Under concurrent writes/updates, NOLOCK can produce missing rows, duplicates, and inconsistent reads.

**Suggested remediation**
- For correctness-first runs:
  - Remove NOLOCK, or
  - Use snapshot isolation / consistent snapshot strategy, or
  - Require the source be quiesced/read-only during migration.
- At minimum: add a “STRICT_CONSISTENCY=true” mode that disables NOLOCK and documents its impact.

## P1 — High Priority (Fix Soon; May Cause Edge-Case Data Issues)

### P1.1 Keyset pagination can skip rows if ordering is not unique

**What happens**
- Keyset uses `WHERE pk > last_key` with `ORDER BY pk`.
- If `pk` (or fallback ordering column) is not unique, any rows equal to `last_key` after a chunk boundary can be skipped.

**Where**
- `include/mssql_pg_migration/data_transfer.py` (`_read_chunk_keyset`)

**Suggested remediation**
- Only use keyset when ordering column(s) guarantee a strict total order (actual PK/unique index).
- Otherwise force `ROW_NUMBER()` pagination with a deterministic `ORDER BY` that is unique:
  - composite ordering (pk cols), or
  - stable tie-breaker (e.g., include additional columns) if there is no PK (still risky).

### P1.2 NTILE partitions use inclusive bounds and can overlap in edge cases

**What happens**
- Partition WHERE clauses are built as inclusive ranges (`>= min AND <= max`) between partitions.
- If partition key values repeat at boundaries, rows can fall into multiple partitions.

**Where**
- `dags/mssql_to_postgres_migration.py` (NTILE boundary generation and WHERE clause construction)

**Suggested remediation**
- Prefer disjoint partitioning strategies:
  - Filter by `partition_id` directly (derive partition id per row and filter on it).
  - Or compute half-open ranges using the next partition’s minimum (`>= min AND < next_min`) and treat the last partition as `>= last_min`.

### P1.3 Partition task success criteria treats “0 rows” as failure

**What happens**
- `transfer_partition` sets success to “no errors AND rows_transferred > 0”.

**Why it matters**
- Legitimately empty partitions (skewed boundaries, filters, sparse ranges) get flagged as failures.

**Suggested remediation**
- Define success as “no errors” and log warnings for 0-row partitions.
- Let validation decide table-level correctness.

## P2 — Medium Priority (Correctness/Operability Improvements)

### P2.1 Validation DAG uses approximate MSSQL counts (`sys.partitions`)

**What happens**
- `dags/validate_migration_env.py` derives MSSQL “row_count” from `sys.partitions` joined via `INFORMATION_SCHEMA`.
- Postgres side uses exact `COUNT(*)`.

**Risk**
- False mismatches if `sys.partitions` is stale/inaccurate for a given table/storage pattern.

**Suggested remediation**
- Add a strict mode that uses exact `COUNT(*)` on MSSQL (possibly only for tables that mismatch).
- Keep the fast path for most tables if needed.

### P2.2 Identity/sequence alignment after loading into SERIAL/BIGSERIAL

**What happens**
- Identity columns map to `SERIAL/BIGSERIAL` (`include/mssql_pg_migration/type_mapping.py`), but COPY loads explicit values.
- Postgres sequences may not be advanced to `MAX(id)`.

**Risk**
- Future inserts can fail with duplicate key or produce incorrect IDs.

**Suggested remediation**
- After load, set sequences to `MAX(id)` for identity/serial columns (only where applicable).

## Implementation Strategy (Recommended Order)

1. Fix COPY NULL marker + normalization (P0.1).
2. Fix BYTEA handling (P0.2).
3. Add “never silently skip large tables” guarantees (P0.3).
4. Add strict consistency option for NOLOCK (P0.4).
5. Make pagination/partitioning robust against non-unique orderings (P1.1, P1.2).
6. Clean up partition success semantics (P1.3).
7. Improve validation and post-load sequence alignment (P2.1, P2.2).

## Acceptance Criteria (Correctness-First)

- For every migrated table:
  - Row counts match under strict counting, or mismatches are explicitly explained and approved.
  - No NULL/empty-string conflation in text columns.
  - Bytea values round-trip correctly (byte-for-byte) for representative samples.
  - No table is “missing” due to planning/partitioning fallthrough.
- Validation should have a mode that is slow but authoritative (exact counts, no approximations).

## Appendix: Implementation Notes (Code Pointers)

This section is intentionally concrete (file + function level) so an assistant can implement changes quickly and safely.

### A. Fix NULL vs empty-string corruption (P0.1)

**Where**
- COPY settings: `include/mssql_pg_migration/data_transfer.py` in `DataTransfer._write_chunk(...)`
- Normalization: `include/mssql_pg_migration/data_transfer.py` in `DataTransfer._normalize_value(...)`

**Suggested approach**
- Choose a NULL sentinel that is unlikely to appear in real data (common: `\\N`).
- Change COPY option from `NULL ''` to `NULL '\\N'`.
- Change normalization:
  - `None` → `\\N` (string literal), not `''`.
  - Keep `''` as `''` for genuine empty strings.

**Gotchas**
- Ensure the CSV writer does not escape the sentinel into something else.
- Verify that Postgres COPY treats the sentinel as NULL under the chosen format.

### B. Correct BYTEA loading (P0.2)

**Where**
- Normalization currently decodes bytes: `include/mssql_pg_migration/data_transfer.py` in `DataTransfer._normalize_value(...)`

**Suggested approach**
- Emit bytea in hex form: `\\x` + `value.hex()`.
- Apply this only for target columns that are `BYTEA`, not for all “bytes-like” values.

**How to know target column types**
- Minimal-change option: query Postgres once per table before COPY:
  - `SELECT column_name, data_type FROM information_schema.columns WHERE table_schema=%s AND table_name=%s`
  - Cache as a dict `{column_name: data_type}` for the table transfer.
- Then in `_normalize_value`, pass `column_name` (or precomputed column-type list aligned to `columns`) and branch:
  - if pg_type == `bytea`: emit hex.
  - else: emit a safe textual representation (or error if unexpected).

**Gotchas**
- “varbinary” mapped to BYTEA is common; `timestamp/rowversion` is also mapped to BYTEA in `include/mssql_pg_migration/type_mapping.py`.
- Avoid UTF-8 decode with `ignore`; it silently corrupts.

### C. Prevent “large table skipped” fallthrough (P0.3)

**Where**
- Regular/partition split: `dags/mssql_to_postgres_migration.py` in `prepare_regular_tables(...)` and `prepare_large_table_partitions(...)`

**Suggested approach**
- Ensure every table ends up in exactly one of:
  - regular transfer list, or
  - partition list (>=1 partition produced), or
  - explicit failure list (and optionally fail-fast if correctness-first).

**Implementation options**
- Conservative: if a table is “large” but partition planning fails, append it to the regular transfer list.
- Strict: if a “large” table cannot be partitioned and correctness mode requires partitioning, raise to fail the DAG early.

**Concrete check**
- After partition planning, compute:
  - `expected = set(created_tables)`
  - `planned = set(regular_tables) ∪ set(partitioned_tables)`
  - If `expected != planned`, log and either fail or fall back.

### D. Remove `NOLOCK` under strict correctness mode (P0.4)

**Where**
- MSSQL reads: `include/mssql_pg_migration/data_transfer.py` in `_read_chunk_keyset(...)` and `_read_chunk_row_number(...)` (both use `WITH (NOLOCK)`).

**Suggested approach**
- Add a configuration flag (env var or DAG param), e.g. `STRICT_CONSISTENCY` / `use_nolock`.
- When strict mode is enabled:
  - omit `WITH (NOLOCK)`.
  - (Optional) request/assume snapshot isolation at the connection/session level if available in your environment.

**Gotchas**
- Removing NOLOCK can reduce throughput and increase blocking; that is acceptable when correctness is the goal.

### E. Keyset pagination safety (P1.1)

**Where**
- `include/mssql_pg_migration/data_transfer.py` in `transfer_table(...)` and `_read_chunk_keyset(...)`

**Suggested approach**
- Only use keyset when you can prove uniqueness and determinism of the ordering:
  - a primary key (current behavior) is generally OK.
  - any fallback “first column” is not safe.
- If no PK exists:
  - use ROW_NUMBER pagination (full table or partitions), or
  - force a strict mode failure and require the user to supply an ordering key.

### F. Make NTILE partitions disjoint (P1.2)

**Where**
- `dags/mssql_to_postgres_migration.py` NTILE boundary query and WHERE clause construction.

**Suggested approach**
- Best correctness: don’t translate NTILE into min/max ranges; instead compute `partition_id` per row and filter directly:
  - `WITH numbered AS (...) SELECT ... WHERE partition_id = ?`
  - This avoids boundary overlap completely, at the cost of repeating the window function per partition query.
- If range-based must remain:
  - use half-open intervals with `next_min_pk`:
    - partition i: `pk >= min_i AND pk < min_{i+1}`
    - last partition: `pk >= min_last`

### G. Partition success semantics (P1.3)

**Where**
- `dags/mssql_to_postgres_migration.py` in `transfer_partition(...)` success assignment.

**Suggested approach**
- Define partition success as: no errors.
- Log (warning) when `rows_transferred == 0` and include partition bounds in logs.

### H. Make validation authoritative (P2.1)

**Where**
- `dags/validate_migration_env.py` discovery query uses `sys.partitions` row counts.

**Suggested approach**
- Add a strict option:
  - For any table where counts mismatch (or for all tables in strict mode), run `SELECT COUNT(*) FROM [schema].[table]` on MSSQL.
- Keep the existing fast path for initial discovery or when strict mode is off.

### I. Align sequences after COPY (P2.2)

**Where**
- Identity mapping: `include/mssql_pg_migration/type_mapping.py` maps identity to `SERIAL/BIGSERIAL`.

**Suggested approach**
- After load, for each serial/identity column:
  - `SELECT setval(pg_get_serial_sequence('schema.table','col'), (SELECT MAX(col) FROM schema.table), true);`
- This should run after data load and before handing the DB off to consumers.
