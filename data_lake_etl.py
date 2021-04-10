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

tasks  = {'billing': 'created_at',
          'issue': 'start_time',
          'payment': 'pay_date',
          'traffic': 'from_unixtime(`timestamp` DIV 1000)'}

dm_traffic= DataProcHiveOperator(
    task_id='dm_traffic',
    dag=dag,
    query="""
        insert overwrite table ygladkikh.dm_traffic partition (year='{{ execution_date.year }}') 
        select user_id, max(bytes_received), min(bytes_received), ceil(avg(bytes_received)) 
        from ygladkikh.ods_traffic         
        where year(created_at) = {{ execution_date.year }}
        group by user_id;
    """,
    cluster_name='cluster-dataproc',
    job_name=USERNAME + '_dm_traffic_{{ execution_date.year }}_{{ params.job_suffix }}',
    params={"job_suffix": randint(0, 100000)},
    region='europe-west3',
)

for task, partcolumn in tasks.items():
    query = 'insert overwrite table ygladkikh.ods_' + task + " partition (year='{{ execution_date.year }}') " \
            'select * from ygladkikh.stg_'+ task + ' where year(' + partcolumn + ') = {{ execution_date.year }};'
    ods = DataProcHiveOperator(
        task_id='ods_'+task,
        dag=dag,
        query=query,
        cluster_name='cluster-dataproc',
        job_name=USERNAME + '_ods_'+task+'_{{ execution_date.year }}_{{ params.job_suffix }}',
        params={"job_suffix": randint(0, 100000)},
        region='europe-west3',
    )
    if task == 'traffic':
        ods >> dm_traffic
