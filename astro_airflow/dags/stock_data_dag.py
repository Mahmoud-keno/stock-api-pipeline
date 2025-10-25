from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from include.Scripts.main_script import main 
default_args = {
    'start_date': datetime(2025,10,24),
    'catchup': False,
}
dag = DAG(
    'stock_data_dag',
    default_args=default_args,
    schedule='*/5 * * * *',
    description='DAG to fetch and process stock data every 5 minutes',

)    

with dag:
    main_script_for_ETL = PythonOperator(
        task_id='main_script_for_ETL',
        python_callable=main

    )



main_script_for_ETL