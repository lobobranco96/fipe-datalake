#from airflow.providers.google.cloud.operators.gcs import GoogleCloudStorageUploadFileOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from python.data_ingestion import upload_files_to_gcs



default_args = {
    'owner': 'lobobranco',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

spark_master = "spark://spark:7077"

with DAG(
    'etl_datapiline',
    default_args=default_args,
    description='extração, transformação e load gcp',
    schedule_interval=None,
   start_date=datetime(2023, 12, 1),
    catchup=False,
) as dag:

    start = PythonOperator(
        task_id="start",
        python_callable = lambda: print("Jobs started"),
        dag=dag
    )

    bucket_name = 'lobobranco-datalake'  # nome do bucket
    source_folder = '/opt/airflow/dags/python/data/terceira_parte'  # Caminho para o diretório local
    destination_folder = 'raw'  # camada raw dentro do bucket lobobranco-datalake no google storage


    data_ingestion = PythonOperator(
        task_id="data_ingestion_gcs",
        python_callable=upload_files_to_gcs,
        op_args=[bucket_name, source_folder, destination_folder],
        dag=dag
    )

    spark_submit_task = SparkSubmitOperator(
        task_id='spark_submit_task',
        application='/spark_job/data_transformation.py',  # Caminho dentro do contêiner do Spark
        conn_id='spark_default',
        conf={
            "spark.executor.memory": "2g",
            "spark.executor.cores": "2",
            #"spark.hadoop.google.cloud.auth.service.account.json.keyfile": "/opt/airflow/dags/credential/google_credential.json",
            "spark.jars": "/opt/airflow/dags/credential/gcs-connector-hadoop3-2.2.6-shaded.jar",
            "spark.hadoop.fs.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
            "spark.hadoop.google.cloud.auth.service.account.enable": "true"
           # "spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/opt/airflow/credential/google_credential.json"
        },
        verbose=True,
    )

    end = PythonOperator(
       task_id="end",
       python_callable = lambda: print("Jobs completed successfully"),
       dag=dag
    )
    start >> data_ingestion >> spark_submit_task >> end