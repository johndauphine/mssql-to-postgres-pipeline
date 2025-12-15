FROM apache/airflow:3.0.0-python3.11

# Install Microsoft ODBC Driver 18 for SQL Server and Kerberos support
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         curl \
         gnupg2 \
         unixodbc-dev \
         krb5-user \
         libkrb5-dev \
  && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
  && curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list \
  && apt-get update \
  && ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow

# Install Python dependencies
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Optimize Airflow for high parallelism (40+ partition tasks for large datasets)
ENV AIRFLOW__CORE__PARALLELISM=128
ENV AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG=64

# Scheduler optimizations for faster task pickup
ENV AIRFLOW__SCHEDULER__MIN_FILE_PROCESS_INTERVAL=10
ENV AIRFLOW__SCHEDULER__SCHEDULER_HEARTBEAT_SEC=5

# Database connection pool for parallel tasks
ENV AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_SIZE=10
ENV AIRFLOW__DATABASE__SQL_ALCHEMY_MAX_OVERFLOW=20

# Migration parallelization (override via .env: MAX_PARTITIONS=4 for 16GB, 8 for 32GB)
ENV MAX_PARTITIONS=8

# Add /opt/airflow to PYTHONPATH for include module
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow"
