from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
import os 
from datetime import datetime 

profile_config = ProfileConfig(
    profile_name="bifrost-analytics",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_conn",
        profile_args={"schema": "amz_data"},
    ),
)

dag_dbt_update = DbtDag(
    project_config=ProjectConfig(
        "/usr/local/airflow/dags/dbt/bifrost_analytics",
    ),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",
    ),
    # normal dag parameters
    schedule_interval="0 19-23,0-2 * * *",
    start_date=datetime(2023, 7 , 31),
    catchup=False,
    dag_id="dag_dbt_update",
)



