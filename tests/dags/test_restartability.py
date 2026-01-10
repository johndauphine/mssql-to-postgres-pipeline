"""
Tests for Migration Restartability Feature

This module contains comprehensive tests for the restartability functionality
including state management, resume logic, and partition tracking.
"""

import os
import sys
import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
import json

# Add parent directories to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from airflow.models import DagBag


class TestDAGRestartabilityParameters:
    """Test DAG parameters and automatic restartability configuration.

    Note: As of PR #32, explicit resume parameters (resume_mode, reset_state,
    retry_failed_only, force_refresh_tables) were removed. The DAG now handles
    restartability automatically by detecting failures and zombie states.
    """

    @pytest.fixture
    def dag_bag(self):
        """Create a DagBag for testing."""
        return DagBag(dag_folder="dags", include_examples=False)

    def test_dag_has_expected_params(self, dag_bag):
        """Test that the DAG has the current expected parameters."""
        dag = dag_bag.get_dag("mssql_to_postgres_migration")
        assert dag is not None

        params = dag.params
        expected_params = [
            "source_conn_id",
            "target_conn_id",
            "chunk_size",
            "include_tables",
            "skip_schema_dag",
        ]

        for param in expected_params:
            assert param in params, f"Missing expected parameter: {param}"

    def test_dag_has_initialize_migration_state_task(self, dag_bag):
        """Test that the DAG has the initialize_migration_state task."""
        dag = dag_bag.get_dag("mssql_to_postgres_migration")
        task_ids = [task.task_id for task in dag.tasks]
        assert "initialize_migration_state" in task_ids


class TestIncrementalStateManagerSchema:
    """Test IncrementalStateManager schema creation and migration."""

    @pytest.fixture
    def mock_pg_hook(self):
        """Create a mocked PostgresHook."""
        with patch('mssql_pg_migration.incremental_state.PostgresHook') as mock:
            mock_instance = MagicMock()
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
            mock_instance.get_conn.return_value = mock_conn
            mock.return_value = mock_instance
            yield mock_instance, mock_cursor

    def test_state_table_ddl_has_new_columns(self):
        """Test that STATE_TABLE_DDL includes new columns."""
        from mssql_pg_migration.incremental_state import STATE_TABLE_DDL

        assert "migration_type" in STATE_TABLE_DDL
        assert "dag_run_id" in STATE_TABLE_DDL
        assert "partition_info" in STATE_TABLE_DDL

    def test_schema_migration_ddl_exists(self):
        """Test that SCHEMA_MIGRATION_DDL is defined."""
        from mssql_pg_migration.incremental_state import SCHEMA_MIGRATION_DDL

        assert "migration_type" in SCHEMA_MIGRATION_DDL
        assert "dag_run_id" in SCHEMA_MIGRATION_DDL
        assert "partition_info" in SCHEMA_MIGRATION_DDL
        assert "IF NOT EXISTS" in SCHEMA_MIGRATION_DDL

    def test_ensure_state_table_exists_runs_migration(self, mock_pg_hook):
        """Test that ensure_state_table_exists runs schema migration."""
        from mssql_pg_migration.incremental_state import IncrementalStateManager

        mock_instance, mock_cursor = mock_pg_hook
        mgr = IncrementalStateManager("test_conn")

        mgr.ensure_state_table_exists()

        # Should have executed multiple SQL statements
        assert mock_cursor.execute.call_count >= 3


class TestIncrementalStateManagerMethods:
    """Test IncrementalStateManager restartability methods."""

    @pytest.fixture
    def mock_pg_hook(self):
        """Create a mocked PostgresHook."""
        with patch('mssql_pg_migration.incremental_state.PostgresHook') as mock:
            mock_instance = MagicMock()
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
            mock_instance.get_conn.return_value = mock_conn
            mock.return_value = mock_instance
            yield mock_instance, mock_cursor

    def test_start_sync_accepts_new_params(self, mock_pg_hook):
        """Test that start_sync accepts migration_type, dag_run_id, partition_info."""
        from mssql_pg_migration.incremental_state import IncrementalStateManager

        mock_instance, mock_cursor = mock_pg_hook
        mock_cursor.fetchone.return_value = (1,)  # Return sync_id

        mgr = IncrementalStateManager("test_conn")

        # Should not raise an error
        sync_id = mgr.start_sync(
            table_name="users",
            source_schema="dbo",
            target_schema="public",
            source_row_count=1000,
            migration_type='full',
            dag_run_id='manual__2026-01-01',
            partition_info={'total_partitions': 2, 'partitions': []},
        )

        assert sync_id == 1

    def test_get_tables_by_status_method_exists(self, mock_pg_hook):
        """Test that get_tables_by_status method exists and is callable."""
        from mssql_pg_migration.incremental_state import IncrementalStateManager

        mock_instance, mock_cursor = mock_pg_hook
        mock_cursor.fetchall.return_value = []

        mgr = IncrementalStateManager("test_conn")
        result = mgr.get_tables_by_status(['completed', 'failed'])

        assert isinstance(result, list)

    def test_reset_table_state_method_exists(self, mock_pg_hook):
        """Test that reset_table_state method exists and is callable."""
        from mssql_pg_migration.incremental_state import IncrementalStateManager

        mock_instance, mock_cursor = mock_pg_hook
        mock_cursor.rowcount = 1

        mgr = IncrementalStateManager("test_conn")
        result = mgr.reset_table_state("users", "dbo", "public")

        assert result is True

    def test_reset_all_state_method_exists(self, mock_pg_hook):
        """Test that reset_all_state method exists and is callable."""
        from mssql_pg_migration.incremental_state import IncrementalStateManager

        mock_instance, mock_cursor = mock_pg_hook
        mock_cursor.rowcount = 5

        mgr = IncrementalStateManager("test_conn")
        result = mgr.reset_all_state(migration_type='full')

        assert result == 5

    def test_reset_stale_running_states_method_exists(self, mock_pg_hook):
        """Test that reset_stale_running_states method exists and is callable."""
        from mssql_pg_migration.incremental_state import IncrementalStateManager

        mock_instance, mock_cursor = mock_pg_hook
        mock_cursor.rowcount = 2

        mgr = IncrementalStateManager("test_conn")
        result = mgr.reset_stale_running_states(
            current_dag_run_id='manual__2026-01-01',
            migration_type='full'
        )

        assert result == 2

    def test_update_partition_status_method_exists(self, mock_pg_hook):
        """Test that update_partition_status method exists and is callable."""
        from mssql_pg_migration.incremental_state import IncrementalStateManager

        mock_instance, mock_cursor = mock_pg_hook
        mock_cursor.fetchone.return_value = (
            {'total_partitions': 4, 'partitions': [{'id': 0, 'status': 'pending'}]},
        )

        mgr = IncrementalStateManager("test_conn")
        # Should not raise
        mgr.update_partition_status(
            sync_id=1,
            partition_id=0,
            status='completed',
            rows_transferred=10000
        )

    def test_get_incomplete_partitions_method_exists(self, mock_pg_hook):
        """Test that get_incomplete_partitions method exists and is callable."""
        from mssql_pg_migration.incremental_state import IncrementalStateManager

        mock_instance, mock_cursor = mock_pg_hook
        mock_cursor.fetchone.return_value = (
            {
                'total_partitions': 4,
                'partitions': [
                    {'id': 0, 'status': 'completed'},
                    {'id': 1, 'status': 'failed'},
                    {'id': 2, 'status': 'pending'},
                ]
            },
        )

        mgr = IncrementalStateManager("test_conn")
        result = mgr.get_incomplete_partitions(sync_id=1)

        # Should return failed and pending partitions
        assert len(result) == 2

    def test_get_migration_summary_method_exists(self, mock_pg_hook):
        """Test that get_migration_summary method exists and is callable."""
        from mssql_pg_migration.incremental_state import IncrementalStateManager

        mock_instance, mock_cursor = mock_pg_hook
        mock_cursor.fetchall.return_value = [
            ('completed', 5, ['Table1', 'Table2', 'Table3', 'Table4', 'Table5']),
            ('failed', 2, ['TableX', 'TableY']),
        ]

        mgr = IncrementalStateManager("test_conn")
        result = mgr.get_migration_summary(migration_type='full')

        assert 'completed' in result
        assert 'failed' in result
        assert result['completed']['count'] == 5
        assert result['failed']['count'] == 2


class TestPartitionStateTracking:
    """Test partition state tracking logic."""

    def test_partition_info_structure(self):
        """Test the expected partition_info JSONB structure."""
        partition_info = {
            "total_partitions": 4,
            "partitions": [
                {"id": 0, "status": "completed", "rows": 500000, "min_pk": 1, "max_pk": 500000},
                {"id": 1, "status": "completed", "rows": 500000, "min_pk": 500001, "max_pk": 1000000},
                {"id": 2, "status": "failed", "rows": 0, "min_pk": 1000001, "max_pk": 1500000, "error": "timeout"},
                {"id": 3, "status": "pending", "min_pk": 1500001, "max_pk": 2000000},
            ]
        }

        # Validate structure
        assert partition_info["total_partitions"] == 4
        assert len(partition_info["partitions"]) == 4

        # Validate completed partitions
        completed = [p for p in partition_info["partitions"] if p["status"] == "completed"]
        assert len(completed) == 2

        # Validate failed partitions
        failed = [p for p in partition_info["partitions"] if p["status"] == "failed"]
        assert len(failed) == 1
        assert "error" in failed[0]

        # Validate pending partitions
        pending = [p for p in partition_info["partitions"] if p["status"] == "pending"]
        assert len(pending) == 1

    def test_partition_pk_boundaries(self):
        """Test that partition boundaries don't overlap."""
        partition_info = {
            "total_partitions": 3,
            "partitions": [
                {"id": 0, "status": "completed", "min_pk": 1, "max_pk": 100},
                {"id": 1, "status": "completed", "min_pk": 101, "max_pk": 200},
                {"id": 2, "status": "completed", "min_pk": 201, "max_pk": 300},
            ]
        }

        partitions = partition_info["partitions"]
        for i in range(len(partitions) - 1):
            current_max = partitions[i]["max_pk"]
            next_min = partitions[i + 1]["min_pk"]
            assert next_min > current_max, f"Partition {i} and {i+1} overlap"


class TestResumeModeLogic:
    """Test resume mode filtering logic."""

    def test_skip_completed_in_resume_mode(self):
        """Test that completed tables are skipped in resume mode."""
        # Simulate migration_state summary
        summary = {
            'completed': {'count': 3, 'tables': ['Users', 'Posts', 'Comments']},
            'failed': {'count': 1, 'tables': ['Votes']},
            'running': {'count': 0, 'tables': []},
            'pending': {'count': 1, 'tables': ['Badges']},
        }

        resume_mode = True
        retry_failed_only = False
        force_refresh_tables = []

        completed_tables = set(summary['completed']['tables'])
        failed_tables = set(summary['failed']['tables'])
        force_refresh_set = set(force_refresh_tables)

        # Simulate filtering logic
        tables_to_process = ['Users', 'Posts', 'Comments', 'Votes', 'Badges']
        processed = []
        skipped = []

        for table in tables_to_process:
            if resume_mode:
                if table in completed_tables and table not in force_refresh_set:
                    skipped.append(table)
                    continue
            processed.append(table)

        assert len(skipped) == 3
        assert 'Users' in skipped
        assert len(processed) == 2
        assert 'Votes' in processed
        assert 'Badges' in processed

    def test_retry_failed_only_mode(self):
        """Test that only failed tables are processed in retry_failed_only mode."""
        summary = {
            'completed': {'count': 3, 'tables': ['Users', 'Posts', 'Comments']},
            'failed': {'count': 1, 'tables': ['Votes']},
            'running': {'count': 0, 'tables': []},
            'pending': {'count': 1, 'tables': ['Badges']},
        }

        resume_mode = True
        retry_failed_only = True
        force_refresh_tables = []

        completed_tables = set(summary['completed']['tables'])
        failed_tables = set(summary['failed']['tables'])
        running_tables = set(summary['running']['tables'])
        force_refresh_set = set(force_refresh_tables)

        tables_to_process = ['Users', 'Posts', 'Comments', 'Votes', 'Badges']
        processed = []
        skipped = []

        for table in tables_to_process:
            if resume_mode:
                if table in completed_tables and table not in force_refresh_set:
                    skipped.append(table)
                    continue
                if retry_failed_only:
                    if table not in failed_tables and table not in running_tables:
                        if table not in force_refresh_set:
                            skipped.append(table)
                            continue
            processed.append(table)

        assert len(processed) == 1
        assert 'Votes' in processed
        assert 'Badges' in skipped  # Pending table skipped in retry_failed_only

    def test_force_refresh_overrides_completed(self):
        """Test that force_refresh_tables processes completed tables."""
        summary = {
            'completed': {'count': 3, 'tables': ['Users', 'Posts', 'Comments']},
            'failed': {'count': 0, 'tables': []},
            'running': {'count': 0, 'tables': []},
            'pending': {'count': 0, 'tables': []},
        }

        resume_mode = True
        force_refresh_tables = ['Users', 'Posts']

        completed_tables = set(summary['completed']['tables'])
        force_refresh_set = set(force_refresh_tables)

        tables_to_process = ['Users', 'Posts', 'Comments']
        processed = []
        skipped = []

        for table in tables_to_process:
            if resume_mode:
                if table in completed_tables and table not in force_refresh_set:
                    skipped.append(table)
                    continue
            processed.append(table)

        assert len(processed) == 2
        assert 'Users' in processed
        assert 'Posts' in processed
        assert 'Comments' in skipped


class TestZombieStateHandling:
    """Test handling of zombie 'running' states from crashed DAGs."""

    @pytest.fixture
    def mock_pg_hook(self):
        """Create a mocked PostgresHook."""
        with patch('mssql_pg_migration.incremental_state.PostgresHook') as mock:
            mock_instance = MagicMock()
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
            mock_instance.get_conn.return_value = mock_conn
            mock.return_value = mock_instance
            yield mock_instance, mock_cursor

    def test_reset_stale_running_states_excludes_current_run(self, mock_pg_hook):
        """Test that current DAG run's running states are not reset."""
        from mssql_pg_migration.incremental_state import IncrementalStateManager

        mock_instance, mock_cursor = mock_pg_hook
        mock_cursor.rowcount = 1

        mgr = IncrementalStateManager("test_conn")
        result = mgr.reset_stale_running_states(
            current_dag_run_id='manual__2026-01-06',
            migration_type='full'
        )

        # Check that the query excludes current run
        call_args = mock_cursor.execute.call_args
        query = call_args[0][0]
        params = call_args[0][1]

        assert "dag_run_id != %s" in query or "dag_run_id IS NULL OR dag_run_id != %s" in query
        assert 'manual__2026-01-06' in params


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
