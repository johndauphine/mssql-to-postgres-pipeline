# Pipeline Enhancements from postgres-to-postgres-pipeline

This document describes the enhancements incorporated from the postgres-to-postgres-pipeline repository into the mssql-to-postgres-pipeline project.

## Overview

These enhancements improve the robustness, observability, and security of the migration pipeline while maintaining backward compatibility with existing functionality.

---

## 1. Notification System

### Features

Complete notification infrastructure for monitoring DAG execution status via email and Slack.

**Module**: `include/mssql_pg_migration/notifications.py`

### Capabilities

- **Multi-Channel Support**:
  - Email notifications via SMTP (supports TLS, SSL, and standard connections)
  - Slack notifications via incoming webhooks
  - Configurable channels (can enable/disable individually)

- **Notification Types**:
  - DAG success notifications with migration statistics
  - DAG failure notifications with error details
  - Task failure notifications with retry information
  - Custom notifications for arbitrary events

- **Migration Statistics**:
  - Tables migrated count
  - Total rows transferred
  - Throughput calculation (rows/second)
  - Duration tracking
  - Table list with details

- **Airflow 3.0 Compatibility**:
  - Exception capture decorator (`@capture_exceptions`)
  - Multiple exception retrieval methods
  - DagRun database fetching for complete context
  - Workarounds for callback limitations

### Configuration

Add to `.env`:

```env
# Enable/disable notifications
NOTIFICATION_ENABLED=true

# Channels: email, slack (comma-separated)
NOTIFICATION_CHANNELS=email,slack

# Email settings
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=your-email@gmail.com
SMTP_PASSWORD=your-app-password
NOTIFICATION_EMAIL_FROM=your-email@gmail.com
NOTIFICATION_EMAIL_TO=team@example.com,manager@example.com

# Slack settings
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/YOUR/WEBHOOK/URL
```

### Usage in DAGs

#### Option 1: DAG-level callbacks (legacy)

```python
from include.mssql_pg_migration.notifications import on_dag_success, on_dag_failure, on_task_failure

@dag(
    on_success_callback=on_dag_success,
    on_failure_callback=on_dag_failure,
    default_args={'on_failure_callback': on_task_failure},
    ...
)
def my_migration_dag():
    # DAG tasks
    pass
```

**Note**: DAG-level callbacks may not execute reliably in Airflow 3.0. Use task-based notifications instead.

#### Option 2: Task-based notifications (recommended)

```python
from include.mssql_pg_migration.notifications import send_success_notification, send_failure_notification
from datetime import datetime, timezone

@task
def notify_success(**context):
    """Send success notification with migration stats."""
    dag_run = context['dag_run']
    start_date = dag_run.start_date
    duration = (datetime.now(timezone.utc) - start_date).total_seconds()

    stats = {
        'tables_migrated': 9,
        'total_rows': 50_000_000,
        'rows_per_second': 15_000,
        'tables_list': ['users', 'posts', 'comments'],
    }

    send_success_notification(
        dag_id=dag_run.dag_id,
        run_id=dag_run.run_id,
        start_date=start_date,
        duration_seconds=duration,
        stats=stats
    )
```

#### Option 3: Exception capture decorator

```python
from include.mssql_pg_migration.notifications import capture_exceptions

@task
@capture_exceptions  # Must be after @task
def risky_operation(**context):
    # This exception will be captured for failure notifications
    raise ValueError("Something went wrong!")
```

### Email Templates

**Success Email** includes:
- ✅ Green header with checkmark
- Run details table (DAG ID, Run ID, timestamps, duration)
- Migration statistics table (tables, rows, throughput)
- Bulleted list of migrated tables
- HTML formatting with styled tables

**Failure Email** includes:
- ❌ Red header with X mark
- Run details table
- Error message in styled pre-formatted block
- Link to Airflow UI for details

### Slack Messages

Uses Slack attachments with:
- Color-coded sidebar (green for success, red for failure, amber for warnings)
- Structured fields for key information
- Concise message summaries
- Timestamp metadata

---

## 2. Composite Primary Key Support

### Features

Enhanced primary key detection that supports composite (multi-column) primary keys for more robust keyset pagination.

**Module**: `include/mssql_pg_migration/data_transfer.py`

### Changes

#### New Method: `_get_primary_key_columns()`

```python
def _get_primary_key_columns(
    self,
    schema_name: str,
    table_name: str,
    columns: List[str]
) -> List[str]:
    """
    Get all primary key columns for keyset pagination (supports composite PKs).

    Returns:
        List of PK column names in ordinal order
    """
```

**Behavior**:
1. Queries SQL Server system tables for PRIMARY KEY index
2. Returns ALL columns in the PK in ordinal order
3. Falls back to 'Id' column if no PK found
4. Final fallback to first column

#### Updated Method: `_get_primary_key_column()`

Now delegates to `_get_primary_key_columns()` and returns the first column for backward compatibility.

### Benefits

- **Accurate PK Detection**: Uses database metadata instead of heuristics
- **Composite PK Support**: Foundation for tuple-based keyset pagination
- **Backward Compatible**: Existing code continues to work
- **Fallback Strategy**: Graceful degradation when PK metadata unavailable

### Future Work

The keyset pagination logic (`_read_chunk_keyset()`) can be updated to use tuple comparison for composite PKs:

```sql
-- Single PK (current):
WHERE "id" > %s

-- Composite PK (future):
WHERE ("user_id", "post_id") > (%s, %s)
```

This provides more efficient pagination for tables with composite primary keys.

---

## 3. Input Validation and Utilities

### Features

Comprehensive validation and utility functions for security and data handling.

**Module**: `include/mssql_pg_migration/utils.py`

### Functions

#### 1. `validate_sql_identifier()`

Validates SQL identifiers (table names, schema names, column names) to prevent SQL injection.

```python
from include.mssql_pg_migration.utils import validate_sql_identifier

# In DAG params:
source_schema = validate_sql_identifier(params['source_schema'], 'schema name')
target_table = validate_sql_identifier(params['target_table'], 'table name')
```

**Rules**:
- Non-empty
- Max 128 characters
- Must start with letter or underscore
- Only alphanumeric characters and underscores
- Pattern: `[a-zA-Z_][a-zA-Z0-9_]*`

**Example**:
```python
>>> validate_sql_identifier("users_2023")
'users_2023'

>>> validate_sql_identifier("table'; DROP--")
ValueError: Invalid identifier 'table'; DROP--': must contain only alphanumeric characters
```

#### 2. `validate_where_clause_template()`

Validates WHERE clause templates used in data transfer to prevent SQL injection.

```python
from include.mssql_pg_migration.utils import validate_where_clause_template

# Before using where_clause in transfer:
where_sql, where_params = partition_info['where_clause']
validate_where_clause_template(where_sql)
```

**Allowed Patterns**:
- `"id" > %s`
- `"id" > %s AND "id" <= %s`
- `("user_id", "post_id") > (%s, %s)`

**Rejected Patterns**:
- Unquoted identifiers
- SQL keywords (DROP, DELETE, etc.)
- Multiple statements
- Comment markers (--,  /*)

#### 3. `quote_sql_literal()`

Safely quotes VALUES for use in WHERE clauses (not identifiers).

```python
from include.mssql_pg_migration.utils import quote_sql_literal

# For dynamic WHERE values:
user_id = quote_sql_literal(user_input)  # Safe quoting with escaped quotes
```

**Behavior**:
- Integers: returned as-is (no quotes)
- Strings: single-quoted with SQL standard quote escaping (`'` → `''`)

**Example**:
```python
>>> quote_sql_literal(123)
'123'

>>> quote_sql_literal("O'Brien")
"'O''Brien'"

>>> quote_sql_literal("admin'; DROP TABLE--")
"'admin''; DROP TABLE--'"
```

#### 4. Helper Utilities

**`sanitize_table_name()`**: Safe logging of table names

```python
>>> sanitize_table_name("table'; DROP--")
'table_DROP'
```

**`format_bytes()`**: Human-readable byte sizes

```python
>>> format_bytes(1073741824)
'1.0 GB'
```

**`truncate_string()`**: String truncation with ellipsis

```python
>>> truncate_string("a" * 150, max_length=20)
'aaaaaaaaaaaaaaaaa...'
```

### Security Benefits

- **Defense in Depth**: Validation at DAG parameter entry point
- **Complements psycopg2.sql**: Works alongside query-time protection
- **Clear Error Messages**: Immediate feedback on invalid input
- **Prevents Accidents**: Catches typos and mistakes before execution

---

## 4. Configuration Enhancements

### Notification Settings

Added to `.env` for easy configuration without code changes:

```env
# Notifications
NOTIFICATION_ENABLED=false
NOTIFICATION_CHANNELS=email,slack
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=your-email@gmail.com
SMTP_PASSWORD=your-app-password
NOTIFICATION_EMAIL_FROM=your-email@gmail.com
NOTIFICATION_EMAIL_TO=recipient@example.com
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/YOUR/WEBHOOK/URL
```

### Future Configuration Options

Additional settings from postgres-to-postgres-pipeline that can be added:

```env
# DAG Behavior
SOURCE_SCHEMA=dbo
TARGET_SCHEMA=public
CHUNK_SIZE=100000
PARTITION_THRESHOLD=1000000
MAX_PARTITIONS=8

# Performance Tuning
PG_POOL_MINCONN=2
PG_POOL_MAXCONN=8
```

---

## Implementation Status

| Feature | Status | Notes |
|---------|--------|-------|
| Notification System | ✅ Complete | Fully functional |
| Composite PK Detection | ✅ Complete | Ready for use |
| Composite PK Pagination | ⚠️ Partial | Detection ready, pagination update pending |
| Input Validation | ✅ Complete | Available via utils module |
| WHERE Clause Validation | ✅ Complete | Available via utils module |
| Configuration | ✅ Complete | Notifications configured |

---

## Usage Examples

### Example 1: DAG with Notifications

```python
from airflow.sdk import dag, task
from pendulum import datetime
from include.mssql_pg_migration.notifications import (
    send_success_notification,
    on_task_failure,
    capture_exceptions
)

@dag(
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    default_args={'on_failure_callback': on_task_failure, 'retries': 3},
    tags=["migration"],
)
def migration_with_notifications():

    @task
    @capture_exceptions
    def transfer_data(**context):
        # Migration logic
        return {'rows': 1000000}

    @task
    def notify(**context):
        dag_run = context['dag_run']
        stats = {
            'tables_migrated': 5,
            'total_rows': 10_000_000,
            'rows_per_second': 50_000,
        }
        send_success_notification(
            dag_id=dag_run.dag_id,
            run_id=dag_run.run_id,
            start_date=dag_run.start_date,
            duration_seconds=200,
            stats=stats
        )

    result = transfer_data()
    notify() >> result

migration_with_notifications()
```

### Example 2: Validated DAG Parameters

```python
from airflow.sdk import dag, task, Param
from include.mssql_pg_migration.utils import validate_sql_identifier

@dag(
    start_date=datetime(2025, 1, 1),
    params={
        "source_schema": Param(default="dbo", type="string"),
        "target_schema": Param(default="public", type="string"),
        "table_name": Param(type="string"),
    },
)
def validated_migration(**context):
    params = context['params']

    # Validate all identifiers at entry point
    source_schema = validate_sql_identifier(params['source_schema'], 'source schema')
    target_schema = validate_sql_identifier(params['target_schema'], 'target schema')
    table_name = validate_sql_identifier(params['table_name'], 'table name')

    # Use validated identifiers safely
    @task
    def migrate():
        # migration logic with validated params
        pass

    migrate()

validated_migration()
```

---

## Testing

### Notification Testing

1. **Enable notifications** in `.env`:
   ```env
   NOTIFICATION_ENABLED=true
   NOTIFICATION_CHANNELS=email
   ```

2. **Configure email** settings with valid SMTP credentials

3. **Trigger a DAG** and check email inbox for notifications

4. **Test failure** by causing a task to fail (all retries exhausted)

### Validation Testing

```python
from include.mssql_pg_migration.utils import validate_sql_identifier, validate_where_clause_template

# Test valid identifiers
assert validate_sql_identifier("users") == "users"
assert validate_sql_identifier("_temp_table") == "_temp_table"

# Test invalid identifiers
try:
    validate_sql_identifier("table'; DROP--")
    assert False, "Should have raised ValueError"
except ValueError as e:
    assert "must contain only alphanumeric" in str(e)

# Test WHERE clause validation
validate_where_clause_template('"id" > %s')  # Valid
validate_where_clause_template('"id" > %s AND "id" <= %s')  # Valid

try:
    validate_where_clause_template('DROP TABLE users')
    assert False, "Should have raised ValueError"
except ValueError:
    pass  # Expected
```

---

## Migration from postgres-to-postgres-pipeline

These enhancements were adapted from the postgres-to-postgres-pipeline repository with the following changes:

1. **Module naming**: `pg_migration` → `mssql_pg_migration`
2. **Notification messages**: Updated to reference "MSSQL to PostgreSQL Migration"
3. **Source database**: Adapted for SQL Server instead of PostgreSQL source
4. **Metadata queries**: Uses SQL Server system tables instead of PostgreSQL catalogs

The core logic and patterns remain identical, ensuring consistency across both pipeline implementations.

---

## Future Enhancements

Additional features from postgres-to-postgres-pipeline that can be added:

1. **Automatic Table Partitioning**:
   - NTILE-based balanced partitioning
   - Dynamic partition count determination
   - Parallel partition processing

2. **Enhanced Configuration**:
   - Drop vs Truncate strategy
   - Table exclusion patterns
   - Chunk size optimization parameters

4. **Session Optimization**:
   - PostgreSQL bulk load settings
   - Maintenance work memory tuning
   - Synchronous commit configuration

---

## References

- Source repository: `/Users/john/repos/postgres-to-postgres-pipeline`
- Security fixes: `SECURITY_FIXES_SUMMARY.md`
- PostgreSQL identifier quoting: https://www.postgresql.org/docs/current/sql-syntax-lexical.html
- psycopg2.sql module: https://www.psycopg.org/docs/sql.html
- Slack incoming webhooks: https://api.slack.com/messaging/webhooks
