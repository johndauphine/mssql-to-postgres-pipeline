# Plan: Add Instance Name to Target Schema Name

**Status: IMPLEMENTED** (2026-01-08)

## Current Behavior

The `derive_target_schema()` function in `plugins/mssql_pg_migration/table_config.py` creates PostgreSQL schema names in the format:

```
{sourcedb}__{sourceschema}
```

Example: `StackOverflow2010` database with `dbo` schema becomes `stackoverflow2010__dbo`

## Proposed Change

Add SQL Server instance name as a prefix to the target schema name:

```
{instancename}__{sourcedb}__{sourceschema}
```

Example: Instance `SQLPROD01`, database `StackOverflow2010`, schema `dbo` becomes `sqlprod01__stackoverflow2010__dbo`

## Implementation Approach

### Option A: Extract from Connection Host

SQL Server connection hosts can include instance names:
- Named instance: `server\instancename` or `server\instancename,port`
- Default instance: `server` or `server,port`

Modify `derive_target_schema()` to:
1. Accept an optional `instance_name` parameter
2. Parse instance from connection host if not provided
3. Prepend sanitized instance name to schema

### Option B: New DAG Parameter

Add `instance_name` as an explicit DAG parameter that users must provide.

### Option C: Connection Extra Field

Store instance name in the Airflow connection's `extra` JSON field.

## Files to Modify

1. **`plugins/mssql_pg_migration/table_config.py`**
   - Update `derive_target_schema()` signature to accept instance name
   - Add `get_instance_name()` helper function
   - Update `build_table_info_list()` and `get_unique_target_schemas()`

2. **`plugins/mssql_pg_migration/odbc_helper.py`**
   - Add method to extract instance name from connection

3. **DAG files** (migration, schema, incremental, validation)
   - Pass instance name through the pipeline

4. **Documentation**
   - Update README.md identifier naming section
   - Update CLAUDE.md if needed

## Edge Cases

1. **Default instance**: Use hostname or a placeholder like `default`
2. **Truncation**: PostgreSQL has 63-char limit; `instance__db__schema` may exceed
3. **Collision**: Different instances with same db/schema would now be distinct (desired)

## Questions for Review

1. Should default instances use hostname, `default`, or be omitted?
2. How to handle very long combined names (>63 chars)?
3. Should this be opt-in (new parameter) or always-on?

---

## AI Recommendations

### Gemini CLI Feedback

**1. Default Instance Naming: Use Hostname**
- "Default" is too generic and risks collision if migrating multiple default instances from different servers
- Using hostname (e.g., `server01`) ensures uniqueness
- If host is `server01\instanceA`, sanitize to `server01_instancea`

**2. Handling 63-Char Limit: Deterministic Prefix Hashing**
- Simply truncating might cut off the schema part, which is critical for uniqueness
- Strategy:
  1. Construct full string: `raw_name = f"{instance}__{db}__{schema}".lower()`
  2. If `len(raw_name) <= 63`, use it
  3. If `len(raw_name) > 63`: Calculate short hash (MD5 first 8 chars) of `{instance}__{db}` prefix, construct `{hash}__{schema}`
  4. Alternative: Truncate instance and db to fixed lengths but **always preserve full schema name**

**3. Opt-in Recommended**
- Changing target schema names is a **breaking change** for existing migrations
- Will duplicate data and break downstream views/queries
- Mechanism: Add DAG parameter (`use_instance_in_schema: bool`) or env var (`INCLUDE_INSTANCE_IN_SCHEMA`) defaulting to `False`

### Codex CLI Feedback

**1. Default Instance: Use Hostname**
- Avoid literal `default` unless also including host (ambiguous)
- Allow override via config/env if hostname is not stable

**2. Identifier Length: Truncation + Hash Scheme**
- Use `prefix = first N chars` + `__` + `hash8`
- Keep hash derived from full `{instance}__{db}__{schema}` so it's reversible by lookup

**3. Opt-in Initially**
- Plan a migration path (dual-write or alias mapping) to avoid breaking downstream
- Consider future always-on default once adopted

---

## Consensus Recommendations

| Question | Recommendation |
|----------|----------------|
| Default instance | Use **hostname** (sanitized), not "default" |
| Length handling | **Truncate + hash** preserving schema name; hash from full string |
| Opt-in vs always-on | **Opt-in** with parameter/env var, default `False` |

## Revised Implementation Plan

1. Add `get_instance_name(mssql_conn_id)` to extract/sanitize instance from connection host
2. Add `INCLUDE_INSTANCE_IN_SCHEMA` env var (default: `False`)
3. Add `use_instance_in_schema` DAG parameter (default: `False`)
4. Update `derive_target_schema()` to accept optional instance name
5. Implement truncation + hash for names > 63 chars
6. Update all DAGs to pass instance when opt-in enabled
7. Document in README.md
