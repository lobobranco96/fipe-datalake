import airflow
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
#from airflow.providers.google.cloud.operators.gcs import GoogleCloudStorageUploadFileOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from python.teste import extractt_app

BUCKET_NAME = "meu-bucket"
LOCAL_FILE_PATH = "/caminho/para/o/arquivo.txt"
DESTINATION_BLOB_NAME = "destino/arquivo.txt"

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
    
#No Airflow Web UI, vá até Admin > Connections.
#Adicione uma nova conexão:
#Conn Id: google_cloud_default (ou outro nome de sua escolha).
#Conn Type: Google Cloud.
#Configure sua autenticação (chave JSON ou configurações de aplicação).

    start = PythonOperator(
        task_id="start",
        python_callable = lambda: print("Jobs started"),
        dag=dag
    )

    extract_info = PythonOperator(
        task_id="extractt_info",
        python_callable = extractt_app,
        dag=dag
    )

    #upload_file = GoogleCloudStorageUploadFileOperator(
     #   task_id="upload_file_to_gcs",
      #  bucket_name=BUCKET_NAME,
       # src=LOCAL_FILE_PATH,
        #dst=DESTINATION_BLOB_NAME,
        #gzip=False,  # Coloque como True se desejar compactar o arquivo antes de enviar
        #google_cloud_storage_conn_id="google_cloud_default",
    #)
    
   # spark_submit_task = SparkSubmitOperator(
    #    task_id='spark_submit_task',
     #   application='/spark_job/printspark.py',  # Caminho dentro do contêiner do Spark
      #  conn_id='spark_default',
       # conf={
        #    "spark.executor.memory": "2g",
        #    "spark.executor.cores": "2",
        #},
        #verbose=True,
    #)

    end = PythonOperator(
        task_id="end",
        python_callable = lambda: print("Jobs completed successfully"),
        dag=dag
    )

    start >> extract_info >> end#>> upload_file >> spark_submit_task >> end