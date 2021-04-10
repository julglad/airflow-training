from datetime import timedelta, datetime
from random import randint

from airflow import DAG
from airflow.contrib.operators.dataproc_operator import DataProcHiveOperator

USERNAME = 'ygladkikh'

default_args = {
    'owner': USERNAME,
    'start_date': datetime(2013, 1, 1, 0, 0, 0)
}

dag = DAG(
    USERNAME + '_data_lake_etl',
    default_args=default_args,
    description='Data Lake ETL tasks',
    schedule_interval="0 0 1 1 *",
)

ods_billing = DataProcHiveOperator(
    task_id='ods_billing',
    dag=dag,
    query="""
        insert overwrite table ygladkikh.ods_billing partition (year='{{ execution_date.year }}') 
        select * from ygladkikh.stg_billing where year(created_at) = {{ execution_date.year }};
    """,
    cluster_name='cluster-dataproc',
    job_name=USERNAME + '_ods_billing_{{ execution_date.year }}_{{ params.job_suffix }}',
    params={"job_suffix": randint(0, 100000)},
    region='europe-west3',
)
ods_issue = DataProcHiveOperator(
    task_id='ods_issue',
    dag=dag,
    query="""
        insert overwrite table ygladkikh.ods_issue partition (year='{{ execution_date.year }}') 
        select * from ygladkikh.stg_issue where year(created_at) = {{ execution_date.year }};
    """,
    cluster_name='cluster-dataproc',
    job_name=USERNAME + '_ods_issue_{{ execution_date.year }}_{{ params.job_suffix }}',
    params={"job_suffix": randint(0, 100000)},
    region='europe-west3',
)
ods_payment = DataProcHiveOperator(
    task_id='ods_payment',
    dag=dag,
    query="""
        insert overwrite table ygladkikh.ods_payment partition (year='{{ execution_date.year }}') 
        select * from ygladkikh.stg_payment where year(created_at) = {{ execution_date.year }};
    """,
    cluster_name='cluster-dataproc',
    job_name=USERNAME + '_ods_payment_{{ execution_date.year }}_{{ params.job_suffix }}',
    params={"job_suffix": randint(0, 100000)},
    region='europe-west3',
)
ods_traffic = DataProcHiveOperator(
    task_id='ods_traffic',
    dag=dag,
    query="""
        insert overwrite table ygladkikh.ods_traffic partition (year='{{ execution_date.year }}') 
        select * from ygladkikh.stg_traffic where year(from_unixtime(`timestamp` DIV 1000)) = {{ execution_date.year }};
    """,
    cluster_name='cluster-dataproc',
    job_name=USERNAME + '_ods_traffic_{{ execution_date.year }}_{{ params.job_suffix }}',
    params={"job_suffix": randint(0, 100000)},
    region='europe-west3',
)
dm_bytes_received= DataProcHiveOperator(
    task_id='dm_bytes_received',
    dag=dag,
    query="""
        insert overwrite table ygladkikh.dm_bytes_received partition (year='{{ execution_date.year }}') 
        select user_id, max(bytes_received), min(bytes_received), ceil(avg(bytes_received)) 
        from ygladkikh.ods_traffic 
        group by user_id
        where year(created_at) = {{ execution_date.year }};
    """,
    cluster_name='cluster-dataproc',
    job_name=USERNAME + '_dm_bytes_received_{{ execution_date.year }}_{{ params.job_suffix }}',
    params={"job_suffix": randint(0, 100000)},
    region='europe-west3',
)