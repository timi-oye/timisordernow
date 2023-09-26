
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

from helper import create_bucket
from extraction import extract_customers_to_lake, extract_merchants_to_lake, extract_orders_to_lake
from transformation import (clean_customers_data, clean_orders_data, clean_merchants_data
                            , agg_customer_order_rejection, agg_customers_per_state, agg_total_orders_per_month, agg_orders_list)


with DAG(
    "ordernow",
    default_args={
        "depends_on_past": False,
        "email": ["dataengineering@10alytics.org"],
        "email_on_failure": True,
        "email_on_retry": True,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        # 'end_date': datetime(2016, 1, 1),
    },
    description="A simple tutorial DAG",
    schedule='@once',
    start_date=datetime(2023, 9, 24),
) as dag:
    
    begin_execution = DummyOperator(task_id='begin_execution')

    # ========== Extraction
    create__bucket = PythonOperator(task_id='create__bucket', python_callable=create_bucket)
    
    extract__customers = PythonOperator(task_id='extract__customers', python_callable=extract_customers_to_lake)
    extract__orders = PythonOperator(task_id='extract__orders', python_callable=extract_orders_to_lake)
    extract__merchants = PythonOperator(task_id='extract__merchants', python_callable=extract_merchants_to_lake)

    # ========== Cleaning

    clean__customers_data = PythonOperator(task_id='clean__customers_data', python_callable=clean_customers_data)
    clean__orders_data = PythonOperator(task_id='clean__orders_data', python_callable=clean_orders_data)
    clean__merchants_data = PythonOperator(task_id='clean__merchants_data', python_callable=clean_merchants_data)

    agg__execution = DummyOperator(task_id = 'agg__execution')

    # ========== Aggregation

    agg__customer_order_rejection = PythonOperator(task_id='agg__customer_order_rejection', python_callable=agg_customer_order_rejection)
    agg__customers_per_state = PythonOperator(task_id='agg__customers_per_state', python_callable=agg_customers_per_state)
    agg__orders_list = PythonOperator(task_id='agg__orders_list', python_callable=agg_orders_list)
    agg__total_orders_per_month = PythonOperator(task_id='agg__total_orders_per_month', python_callable=agg_total_orders_per_month)


    end_execution = DummyOperator(task_id='end_execution')

    begin_execution >> create__bucket

    create__bucket >> extract__customers
    create__bucket >> extract__orders
    create__bucket >> extract__merchants

    extract__customers >> clean__customers_data
    extract__orders >> clean__orders_data
    extract__merchants >> clean__merchants_data
    
    clean__customers_data >> agg__execution
    clean__orders_data >> agg__execution
    clean__merchants_data >> agg__execution

    agg__execution >> agg__customer_order_rejection
    agg__execution >> agg__customers_per_state
    agg__execution >> agg__orders_list
    agg__execution >> agg__total_orders_per_month

    agg__customer_order_rejection >> end_execution
    agg__customers_per_state >> end_execution
    agg__orders_list >> end_execution
    agg__total_orders_per_month >> end_execution







