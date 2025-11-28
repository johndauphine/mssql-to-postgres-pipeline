FROM astrocrpublic.azurecr.io/runtime:3.1-5

# Optimize Airflow for high parallelism (22+ partition tasks)
ENV AIRFLOW__CORE__PARALLELISM=64
ENV AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG=32
ENV AIRFLOW__CELERY__WORKER_CONCURRENCY=32

# No additional packages needed - pymssql uses FreeTDS which is included in the base image
