from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

with DAG(
    dag_id="test_dag",
    start_date=days_ago(1),
    schedule="@once",
    catchup=False,
) as dag:
    hello = BashOperator(
        task_id="hello",
        bash_command="echo Airflow is reading DAGs correctly",
    )
