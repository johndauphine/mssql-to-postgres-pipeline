"""
Tests for MSSQL to PostgreSQL Migration Plugin Modules

This package contains comprehensive tests for the migration pipeline core modules.
"""

import os
import sys

# Add plugins directory to Python path (Airflow does this automatically at runtime)
plugins_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'plugins'))
if plugins_dir not in sys.path:
    sys.path.insert(0, plugins_dir)
