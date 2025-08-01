from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="banking_data_quality_dag",
    schedule="0 0 * * *",  # Chạy vào lúc 00:00 mỗi ngày
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    tags=["banking", "data_quality"],
) as dag:
    
    generate_data_task = BashOperator(
        task_id="run_data_generation",
        bash_command="python -u /opt/airflow/src/generate_data.py",
    )

    quality_audit_task = BashOperator(
        task_id="run_data_quality_checks",
        bash_command="python -u /opt/airflow/src/monitoring_audit.py",
    )

    generate_data_task >> quality_audit_task