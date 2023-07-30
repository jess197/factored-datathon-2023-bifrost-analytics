FROM quay.io/astronomer/astro-runtime:8.8.0

# install dbt into a virtual environment
RUN python3 -m venv dbt_venv && \
    source dbt_venv/bin/activate && \
    pip3 install --no-cache-dir dbt.snowflake && \
    deactivate
