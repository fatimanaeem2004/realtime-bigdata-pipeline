from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {"owner": "airflow"}

with DAG(
    dag_id="bda_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="*/1 * * * *",
    catchup=False,
) as dag:

    ensure_ingestion = BashOperator(
        task_id="ensure_ingestion",
        bash_command=r"""
            {% raw %}
            docker ps --format '{{.Names}}' | grep -qx kafka-producer || docker start kafka-producer
            docker ps --format '{{.Names}}' | grep -qx kafka-consumer || docker start kafka-consumer
            {% endraw %}
            """,
    )

    check_ingestion = BashOperator(
        task_id="check_ingestion",
        bash_command="python /opt/airflow/scripts/ingestion_health.py",
    )

    seed_dims = BashOperator(
        task_id="seed_dims",
        bash_command="python /opt/airflow/scripts/seed_dims.py",
    )

    run_spark_kpi_job = BashOperator(
        task_id="run_spark_kpi_job",
        bash_command=(
            "docker exec spark-master bash -lc "
            "\"/opt/spark/bin/spark-submit "
            "--packages org.mongodb.spark:mongo-spark-connector_2.12:10.2.0,org.postgresql:postgresql:42.7.3 "
            "/opt/spark/jobs/kpi_job.py\""
        ),
    )

    archive_policy = BashOperator(
        task_id="archive_policy",
        bash_command="python /opt/airflow/scripts/archive_policy.py"
    )

    ensure_ingestion >> check_ingestion >> seed_dims >> run_spark_kpi_job >> archive_policy
