from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from cosmos import DbtTaskGroup, ExecutionConfig, ExecutionMode, ProfileConfig, ProjectConfig

DBT_PROJECT_DIR = "/opt/airflow/dbt_project"

DUCKDB_POOL = "duckdb_writer"

profile_config = ProfileConfig(
    profile_name="fuzzy_factory",
    target_name="dev",
    profiles_yml_filepath=f"{DBT_PROJECT_DIR}/profiles.yml",
)

execution_config = ExecutionConfig(execution_mode=ExecutionMode.LOCAL)

with DAG(
    dag_id="fuzzy_factory_elt",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["portfolio", "elt", "dbt", "cosmos"],
) as dag:

    extract_load = BashOperator(
        task_id="extract_load_postgres_to_duckdb",
        bash_command="python /opt/airflow/scripts/extract_load.py",
        pool=DUCKDB_POOL,
    )

    dbt_models = DbtTaskGroup(
        group_id="dbt_models",
        project_config=ProjectConfig(DBT_PROJECT_DIR),
        profile_config=profile_config,
        execution_config=execution_config,
        operator_args={"install_deps": False, "pool": DUCKDB_POOL},
    )

    dbt_docs = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt docs generate",
        pool=DUCKDB_POOL,
    )

    extract_load >> dbt_models >> dbt_docs
