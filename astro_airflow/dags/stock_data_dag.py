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
    main_script_for_ETL_and_loaded_into_snowflake = PythonOperator(
        task_id='main_script_for_ETL',
        python_callable=main

    )
    transform_data_using_dbt = BashOperator(
        task_id='transform_data_using_dbt',
        bash_command='cd /usr/local/airflow/stock_dbt &&  dbt run --profiles-dir /usr/local/airflow/stock_dbt/.dbt'
    )


main_script_for_ETL_and_loaded_into_snowflake >> transform_data_using_dbt