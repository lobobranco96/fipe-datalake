from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from python.extract_one import extract_one
from python.extract_two import extract_two
from python.extract_three import last_extract


default_args = {
    'owner': 'lobobranco',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}


with DAG(
    'data_source_dag',
    default_args=default_args,
    description='extraindo dados de veiculos com suas tabelas fipe',
    schedule_interval=None,
    start_date=datetime(2023, 12, 1),
    catchup=False,
) as dag:
    
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


    end = PythonOperator(
        task_id="end",
        python_callable = lambda: print("Jobs completed successfully"),
        dag=dag
    )

    start >> first_extract >> second_extract >> preparing_fipe_data >> end