# Test Suite for MSSQL to PostgreSQL Migration Pipeline

This directory contains comprehensive tests for the migration pipeline components.

## Test Structure

```
tests/
├── dags/                          # DAG-level tests
│   ├── test_dag_example.py        # DAG structure validation
│   ├── test_mssql_pg_migration.py # Migration DAG integration tests
│   └── test_restartability.py     # Restart/recovery tests
│
└── plugins/                       # Unit tests for migration modules
    ├── test_type_mapping.py       # Type mapping and sanitization tests
    ├── test_validation.py         # Validation module tests
    └── test_odbc_helper.py        # ODBC connection helper tests
```

## Running Tests

### Run All Tests
```bash
# Inside Docker container
docker exec airflow-scheduler pytest /opt/airflow/tests/

# With verbose output
docker exec airflow-scheduler pytest /opt/airflow/tests/ -v

# With coverage
docker exec airflow-scheduler pytest /opt/airflow/tests/ --cov=mssql_pg_migration --cov-report=html
```

### Run Specific Test Files
```bash
# Test type mapping only
docker exec airflow-scheduler pytest /opt/airflow/tests/plugins/test_type_mapping.py -v

# Test validation only
docker exec airflow-scheduler pytest /opt/airflow/tests/plugins/test_validation.py -v

# Test ODBC helper only
docker exec airflow-scheduler pytest /opt/airflow/tests/plugins/test_odbc_helper.py -v
```

### Run Specific Test Classes or Methods
```bash
# Run specific test class
docker exec airflow-scheduler pytest /opt/airflow/tests/plugins/test_type_mapping.py::TestSanitizeIdentifier -v

# Run specific test method
docker exec airflow-scheduler pytest /opt/airflow/tests/plugins/test_type_mapping.py::TestSanitizeIdentifier::test_sql_injection_in_identifier -v
```

## Test Coverage

### Type Mapping Tests (`test_type_mapping.py`)

**Coverage:** 100+ test cases covering:
- SQL Server to PostgreSQL type mappings
- Identifier sanitization and SQL injection prevention
- Edge cases (NULL values, MAX types, Unicode)
- Security (injection attempts, special characters)

**Key Test Classes:**
- `TestSanitizeIdentifier`: Identifier sanitization logic
- `TestMapType`: Data type mapping correctness
- `TestMapColumn`: Column definition mapping
- `TestMapDefaultValue`: Default value expression conversion
- `TestMapTableSchema`: Complete table schema mapping
- `TestSecurityConcerns`: SQL injection prevention

**Critical Tests:**
- `test_sql_injection_in_identifier`: Verifies SQL injection attempts are neutralized
- `test_unicode_injection`: Tests Unicode-based injection attempts
- `test_very_long_column_name`: Boundary condition testing

### Validation Tests (`test_validation.py`)

**Coverage:** 50+ test cases covering:
- Row count validation accuracy
- Sample data comparison
- Batch validation workflows
- Security (parameterization, identifier quoting)

**Key Test Classes:**
- `TestMigrationValidator`: Core validation logic
- `TestGenerateMigrationReport`: Report generation
- `TestValidateMigration`: End-to-end validation
- `TestSecurity`: SQL injection prevention

**Critical Tests:**
- `test_sql_injection_prevention_in_schema_name`: Validates parameterization
- `test_identifier_quoting_in_postgres_queries`: Verifies psycopg2.sql usage
- `test_validate_row_count_null_result`: Edge case handling

### ODBC Helper Tests (`test_odbc_helper.py`)

**Coverage:** 40+ test cases covering:
- Connection string generation
- SQL Server authentication (SQL Auth, Windows Auth)
- Query execution and parameterization
- Connection pooling
- Resource cleanup and error handling

**Key Test Classes:**
- `TestOdbcConnectionHelper`: Core ODBC operations
- `TestConnectionPool`: Connection pool integration
- `TestErrorHandling`: Error scenarios
- `TestSecurity`: SQL injection prevention

**Critical Tests:**
- `test_parameterization_prevents_sql_injection`: Validates parameter binding
- `test_connection_cleanup_on_error`: Resource leak prevention
- `test_rollback_on_error`: Transaction integrity

## Test Categories

### Security Tests
Focus on preventing SQL injection and protecting credentials:
- SQL injection attempts in identifiers
- Parameter sanitization
- Credential masking in logs
- Unicode-based injection attacks

**Files:** All test files include security test classes

### Correctness Tests
Validate data integrity and edge cases:
- NULL value handling
- Empty result sets
- Type mapping accuracy
- Boundary conditions (max lengths, zero precision)

**Files:**
- `test_type_mapping.py::TestEdgeCases`
- `test_validation.py::TestEdgeCases`
- `test_odbc_helper.py::TestEdgeCases`

### Performance Tests
*(To be added)*
- Large table handling
- Connection pool efficiency
- Memory usage profiling

## Mock Strategy

Tests use `unittest.mock` to avoid requiring live database connections:

```python
@patch('mssql_pg_migration.odbc_helper.pyodbc.connect')
def test_get_records_executes_query(self, mock_connect, helper):
    # Setup mocks
    mock_cursor = Mock()
    mock_cursor.fetchall.return_value = [(1, 'Alice')]
    mock_connection = Mock()
    mock_connection.cursor.return_value = mock_cursor
    mock_connect.return_value = mock_connection

    # Execute test
    results = helper.get_records('SELECT * FROM Users')

    # Assertions
    assert len(results) == 1
    mock_cursor.execute.assert_called_once()
```

**Benefits:**
- Fast test execution (no database I/O)
- Deterministic results
- Easy to simulate error conditions
- No external dependencies

## Security Test Examples

### SQL Injection Prevention
```python
def test_sql_injection_in_identifier(self):
    """SQL injection attempts in identifiers should be sanitized."""
    malicious = "user'; DROP TABLE users; --"
    result = sanitize_identifier(malicious)

    # Should remove all SQL-special characters
    assert "'" not in result
    assert ";" not in result
    assert "-" not in result
    assert result == "user_drop_table_users"
```

### Parameterization Verification
```python
def test_parameterization_prevents_sql_injection(self, mock_connect, helper):
    """SECURITY: Test that parameterized queries prevent SQL injection."""
    malicious_input = "1; DROP TABLE Users; --"

    # Execute with proper parameterization
    helper.get_records(
        'SELECT * FROM Users WHERE UserID = ?',
        parameters=[malicious_input]
    )

    # Verify parameters passed separately, not interpolated
    call_args = mock_cursor.execute.call_args
    assert call_args[0][0] == 'SELECT * FROM Users WHERE UserID = ?'
    assert call_args[0][1] == [malicious_input]
```

## Known Issues / TODO

### Tests to Add
1. **Integration Tests** with real databases (Docker test containers)
2. **Performance Tests** for large data volumes
3. **Schema Extractor Tests** (pending implementation)
4. **Data Transfer Tests** (pending implementation)
5. **DDL Generator Tests** (pending implementation)

### Coverage Gaps
- Binary COPY functionality
- Parallel reader threads
- Connection pool edge cases under high concurrency
- Network failure scenarios

## Contributing

When adding new tests:

1. **Follow naming convention:** `test_<module_name>.py`
2. **Use descriptive test names:** `test_<what>_<scenario>`
3. **Include docstrings:** Explain what the test validates
4. **Add to appropriate class:** Group related tests
5. **Mock external dependencies:** Avoid live database calls
6. **Test security:** Always include SQL injection tests for new query logic

### Example Test Structure
```python
class TestNewFeature:
    """Test description."""

    @pytest.fixture
    def setup(self):
        """Create test fixtures."""
        pass

    def test_normal_case(self, setup):
        """Test normal operation."""
        pass

    def test_edge_case(self, setup):
        """Test edge case."""
        pass

    def test_security_concern(self, setup):
        """SECURITY: Test security aspect."""
        pass
```

## CI/CD Integration

These tests should be run:
- **Pre-commit:** On developer machines before commit
- **Pull Request:** Automatically on PR creation
- **Pre-deployment:** Before any production deployment

### Example GitHub Actions Workflow
```yaml
name: Test Migration Pipeline

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Start Airflow
        run: docker-compose up -d
      - name: Run Tests
        run: docker exec airflow-scheduler pytest /opt/airflow/tests/ -v
      - name: Upload Coverage
        # Pin to specific commit SHA for security (see: https://docs.github.com/en/actions/security-guides/security-hardening-for-github-actions)
        uses: codecov/codecov-action@b9fd7d16f6d7d1b5d2bec1a2887e65ceed900238 # v4.6.0
```

## References

- [pytest Documentation](https://docs.pytest.org/)
- [unittest.mock Guide](https://docs.python.org/3/library/unittest.mock.html)
- [Security Review](../SECURITY_REVIEW.md)
- [Gemini Review Summary](../GEMINI_REVIEW_SUMMARY.md)
