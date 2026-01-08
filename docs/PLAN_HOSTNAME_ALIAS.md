# Plan: Hostname to Alias Mapping for Target Schema Names

## Overview

Map SQL Server hostnames to user-defined aliases for use in PostgreSQL target schema names. This allows meaningful, consistent naming across environments (e.g., `prod_sales` instead of `sqlprod01_domain_com`).

## Requirements

1. Create a `hostname_alias` configuration file
2. Look up hostname in file to get alias
3. Use alias instead of hostname in schema name
4. **Fail the DAG** if hostname not found (no fallback)
5. Log clear error message to Airflow logs

## File Format

**File**: `config/hostname_alias.txt`

**Format**: Simple `hostname = alias` format (one per line)

```
# Hostname to Alias Mapping
# Format: hostname = alias
# Lines starting with # are comments
# Hostname matching is case-insensitive

mssql-server = dev
sqlprod01 = prod_sales
sqlprod01.company.com = prod_sales
sqlprod02\INSTANCE1 = prod_reporting
sqldev.internal.net = dev_test
```

**Why this format**:
- Easy to read and edit (no JSON/YAML syntax to remember)
- Consistent with existing `*_include_tables.txt` config files
- Supports comments for documentation
- One mapping per line for easy diff/merge

## Schema Name Examples

| Hostname | Alias | Database | Schema | Target Schema |
|----------|-------|----------|--------|---------------|
| `mssql-server` | `dev` | `StackOverflow2010` | `dbo` | `dev__stackoverflow2010__dbo` |
| `sqlprod01.company.com` | `prod_sales` | `SalesDB` | `orders` | `prod_sales__salesdb__orders` |

## Implementation

### 1. Add `load_hostname_aliases()` function

Location: `plugins/mssql_pg_migration/table_config.py`

```python
def load_hostname_aliases() -> Dict[str, str]:
    """
    Load hostname to alias mappings from config file.

    File: config/hostname_alias.txt
    Format: hostname = alias (one per line, # for comments)

    Returns:
        Dict mapping lowercase hostnames to aliases

    Raises:
        FileNotFoundError: If config file doesn't exist
    """
```

### 2. Add `get_alias_for_hostname()` function

```python
def get_alias_for_hostname(hostname: str) -> str:
    """
    Look up alias for hostname from config file.

    Args:
        hostname: SQL Server hostname (with or without instance/port)

    Returns:
        Alias for the hostname

    Raises:
        ValueError: If hostname not found in mapping file
    """
```

### 3. Modify `get_instance_name()` function

Current behavior:
- Extract hostname/instance from connection
- Sanitize and return

New behavior:
- Extract hostname from connection
- Look up alias in `hostname_alias.txt`
- **Fail with clear error if not found**
- Return sanitized alias

### 4. Update validation DAG

The `validate_migration_env.py` uses `parse_instance_from_host()` directly. Update to use the same alias lookup.

## Files to Modify

| File | Changes |
|------|---------|
| `plugins/mssql_pg_migration/table_config.py` | Add `load_hostname_aliases()`, `get_alias_for_hostname()`, modify `get_instance_name()` |
| `dags/validate_migration_env.py` | Update `parse_instance_from_host()` to use alias lookup |
| `config/hostname_alias.txt` | New file with mappings |
| `README.md` | Document hostname alias configuration |

## Error Handling

When hostname not found:

```
ERROR - Hostname 'sqlserver01.company.com' not found in config/hostname_alias.txt
ERROR - Available mappings:
ERROR -   mssql-server = dev
ERROR -   sqlprod01 = prod_sales
ERROR - Add a mapping for this hostname and retry.
```

DAG will fail with `ValueError` and the error will be visible in:
- Airflow task logs
- DAG run failure status

## Edge Cases

1. **File not found**: Fail with clear error pointing to missing file
2. **Empty file**: Fail with error "No hostname mappings defined"
3. **Duplicate hostnames**: Last mapping wins (with warning log)
4. **Whitespace**: Strip whitespace from hostname and alias
5. **Case sensitivity**: Hostname lookup is case-insensitive
6. **Instance in hostname**: Match full string including `\instance` if present

## Matching Strategy

The lookup will try to match in order:
1. Exact match (case-insensitive): `sqlprod01\INST1` → `sqlprod01\inst1`
2. Hostname without port: `sqlprod01,1433` → `sqlprod01`
3. Hostname without instance: `sqlprod01\INST1` → `sqlprod01`

If none match, fail with error listing available mappings.
