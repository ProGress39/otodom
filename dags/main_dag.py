from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

import sys
sys.path.append('../otodom')
from otodom import run_otodom_scrapper
from olx import run_olx_scrapper
from grouped_tables import run_summary_spark


dag_args = {
    'owner':'airflow',
    'depends_on_past':False,
    'start_date' : datetime.now(),
    'email':['sylwestronowak@gmail.com'],
    'email_on_failure':True,
    'email_on_retry':False,
    'retries':1,
    'retry_delay':timedelta(minutes=1) 
}

dag1 = DAG(
    dag_id = 'apartments_web_scrapper',
    default_args = dag_args,
    description = 'Download posts from otodom & olx, insert into mysql db and make transformations',
    schedule_interval=timedelta(days=1)
)

run_otodom_webscrapper = PythonOperator(
    task_id='otodom_scrapper',
    python_callable=run_otodom_scrapper,
    dag=dag1
)

run_olx_webscrapper = PythonOperator(
    task_id='olx_scrapper',
    python_callable=run_olx_scrapper,
    dag=dag1
)

run_summary = PythonOperator(
    task_id='run_summary_spark',
    python_callable=run_summary_spark,
    dag=dag1
)

run_otodom_webscrapper >> run_olx_webscrapper >> run_summary

