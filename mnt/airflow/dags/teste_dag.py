import airflow
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
#from airflow.providers.google.cloud.operators.gcs import GoogleCloudStorageUploadFileOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
#from python.teste import extractt_app
from python.extract_one import extract_one
from python.extract_two import extract_two
from python.extract_three import last_extract


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
    'extract_dag',
    default_args=default_args,
    description='extraindo dados de veiculos com suas tabelas fipe',
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

    first_extract = PythonOperator(
        task_id="primeira_parte",
        python_callable = extract_one,
        dag=dag
    )

    second_extract = PythonOperator(
        task_id="segunda_parte",
        python_callable = extract_two,
        dag=dag
    )
    preparing_fipe_data = PythonOperator(
        task_id="preparing_fipe_data",
        python_callable = last_extract,
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

    start >> first_extract >> second_extract >> preparing_fipe_data >> end #>> upload_file >> spark_submit_task >> end