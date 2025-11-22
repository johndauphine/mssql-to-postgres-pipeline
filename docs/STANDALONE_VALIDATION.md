# Standalone Validation DAG

## Purpose

This document describes the standalone validation DAG created to address XCom resolution issues in Airflow 3 when validating SQL Server to PostgreSQL migrations.

## Problem

The original `mssql_to_postgres_migration` DAG's validation task fails with an XCom error when trying to collect results from dynamically mapped tasks. Specifically:

```
XCom not found ... offset=9 not found for task 'transfer_table_data'
```

This appears to be a bug in Airflow 3's handling of XCom collection for mapped tasks, where it requests an invalid offset (9) when only indices 0-8 exist for 9 tables.

## Solution

Create a standalone validation DAG (`validate_migration_standalone.py`) that:

1. **Operates independently** - No dependency on other DAGs or XCom data
2. **Uses single task architecture** - All validation logic in one task to avoid inter-task data passing
3. **Dynamically discovers tables** - Queries INFORMATION_SCHEMA from source database
4. **Directly queries both databases** - Connects to both SQL Server and PostgreSQL for real-time counts
5. **Generates inline reports** - Logs validation results without using XCom for large data

## Implementation Details

### Key Features

- **Dynamic Table Discovery**: Uses SQL Server's INFORMATION_SCHEMA.TABLES to find all tables in source schema
- **Efficient Counting**: Retrieves row counts using system tables when possible for performance
- **Flexible Configuration**: Supports different schemas and connection IDs via DAG params
- **Comprehensive Reporting**: Generates detailed validation report with pass/fail status per table
- **Error Handling**: Gracefully handles missing tables in target database

### DAG Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `source_conn_id` | `mssql_source` | Airflow connection ID for SQL Server |
| `target_conn_id` | `postgres_target` | Airflow connection ID for PostgreSQL |
| `source_schema` | `dbo` | Source schema in SQL Server |
| `target_schema` | `public` | Target schema in PostgreSQL |
| `exclude_tables` | `["sysdiagrams"]` | Tables to exclude from validation |

### Usage

1. Run the main migration DAG first:
   ```bash
   airflow dags trigger mssql_to_postgres_migration
   ```

2. After migration completes, run the standalone validation:
   ```bash
   airflow dags trigger validate_migration_standalone
   ```

3. Check the task logs for the detailed validation report

### Report Format

The validation generates a report showing:
- Table name
- Source row count
- Target row count
- Difference
- Pass/Fail status

Example output:
```
[✓] users                | Source:      299,398 | Target:      299,398 | Diff:          0
[✗] posts                | Source:    3,729,195 | Target:    2,910,000 | Diff:   -819,195
[?] missing_table        | Source:        1,000 | Target:      MISSING | Diff:        N/A
```

## Benefits

1. **Reliability**: Avoids Airflow 3 XCom bugs by not using mapped tasks
2. **Independence**: Can run anytime to validate migration state
3. **Transparency**: Clear reporting of validation results
4. **Flexibility**: Works with any SQL Server to PostgreSQL migration
5. **Performance**: Single task execution is faster than multiple task orchestration

## Testing

### Local Testing
```bash
# Validate DAG syntax
astro dev parse

# Start Airflow
astro dev start

# Trigger validation
airflow dags trigger validate_migration_standalone
```

### Verification Steps

1. Check DAG appears in Airflow UI
2. Verify task completes successfully
3. Review task logs for validation report
4. Confirm row counts match expected values

## Troubleshooting

### Common Issues

1. **Connection errors**: Verify connection IDs exist in Airflow
2. **Schema not found**: Check source_schema and target_schema parameters
3. **Table name case**: PostgreSQL typically uses lowercase; SQL Server may use mixed case
4. **Missing tables**: Check exclude_tables parameter if system tables appear

## Future Enhancements

Potential improvements for this validation approach:

1. **Data sampling validation**: Compare sample rows, not just counts
2. **Column-level validation**: Verify data types and column properties
3. **Performance metrics**: Track validation execution time
4. **Historical tracking**: Store validation results in a database table
5. **Alerting**: Send notifications on validation failures