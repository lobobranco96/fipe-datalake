from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
from airflow.operators.python import PythonOperator
import airflow

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

spark_master = "spark://spark:7077"

with DAG(
    'spark_submit_example',
    default_args=default_args,
    description='Submit a job to Spark cluster',
    schedule_interval=None,
    start_date=datetime(2023, 12, 1),
    catchup=False,
) as dag:
    
    start = PythonOperator(
        task_id="start",
        python_callable = lambda: print("Jobs started"),
        dag=dag
    )
    
    spark_submit_task = SparkSubmitOperator(
        task_id='spark_submit_task',
        application='/spark_job/printspark.py',  # Caminho dentro do contêiner do Spark
        conn_id='spark_default',
        conf={
            "spark.executor.memory": "2g",
            "spark.executor.cores": "2",
        },
        verbose=True,
    )

    end = PythonOperator(
        task_id="end",
        python_callable = lambda: print("Jobs completed successfully"),
        dag=dag
    )

    start >> spark_submit_task >> end