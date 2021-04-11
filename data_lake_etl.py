from datetime import timedelta, datetime
from random import randint

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.dataproc_operator import DataProcHiveOperator

USERNAME = 'ygladkikh'

default_args = {
    'owner': USERNAME,
    'start_date': datetime(2012, 1, 1, 0, 0, 0)
}

dag = DAG(
    '%s_data_lake_etl' % USERNAME,
    default_args=default_args,
    description='Data Lake ETL tasks',
    schedule_interval="0 0 1 1 *",
    concurrency=1,
    max_active_runs=1,
)
tables = ['traffic', 'billing', 'issue', 'payment']

load_op = PythonOperator(
    task_id='load_components',
    python_callable=lambda: 1,
    dag=dag
)

query = ''
ods_queries = []
for table in tables:
    if table == 'traffic':
        query = '''
            insert overwrite table ygladkikh.ods_traffic partition (year={{ execution_date.year }})
            select
              `user_id`
              , `timestamp`
              , `device_id`
              , cast(regexp_extract(`device_ip_addr`,"(\\d+)\\.(\\d+)\\.(\\d+)\\.(\\d+)",1) as bigint) * 16777216 + cast(regexp_extract(`device_ip_addr`,"(\\d+)\\.(\\d+)\\.(\\d+)\\.(\\d+)",2) as bigint) * 65536 + cast(regexp_extract(`device_ip_addr`,"(\\d+)\\.(\\d+)\\.(\\d+)\\.(\\d+)",3) as bigint) * 256 + cast(regexp_extract(`device_ip_addr`,"(\\d+)\\.(\\d+)\\.(\\d+)\\.(\\d+)",4) as bigint) as device_ip_addr
              , `bytes_sent`
              , `bytes_received`
            from
              ygladkikh.stg_traffic
            where
              year(from_unixtime(cast(`timestamp`/1000 as bigint))) = {{ execution_date.year }}
        '''
    elif table == 'billing':
        query = '''
            insert overwrite table ygladkikh.ods_billing partition (year={{ execution_date.year }})
            select
                `user_id`
              , cast(concat(`billing_period`,'-01') as DATE)
              , `service`
              , `tariff`
              , cast(`sum` as        DECIMAL(9,2))
              , cast(`created_at` as TIMESTAMP)
            from
                ygladkikh.stg_billing
            where
                year(cast(`created_at` as TIMESTAMP)) = {{ execution_date.year }}
        '''
    elif table == 'issue':
        query = '''
            insert overwrite table ygladkikh.ods_issue partition (year={{ execution_date.year }})
            select
                cast(`user_id` AS    BIGINT)
              , cast(`start_time` AS TIMESTAMP)
              , cast(`end_time` AS   TIMESTAMP)
              , `title`
              , `description`
              , `service`
            from
                ygladkikh.stg_issue
            where
                year(cast(`start_time` AS TIMESTAMP)) = {{ execution_date.year }}
        '''
    elif table == 'payment':
        query = '''
            insert overwrite table ygladkikh.ods_payment partition (year={{ execution_date.year }})
            select
                `user_id`
              , `pay_doc_type`
              , `pay_doc_num`
              , `account`
              , `phone`
              , cast(concat(`billing_period`,'-01') as DATE)
              , cast(`pay_date` as                     DATE)
              , `sum`
            from
                ygladkikh.stg_payment
            where
                year(cast(`pay_date` as DATE)) = {{ execution_date.year }}
        '''

    ods = DataProcHiveOperator(
        task_id='ods_%s' % table,
        dag=dag,
        query=query,
        job_name='%s_{{ execution_date.year }}_ods_%s_{{ params.job_suffix }}' % (USERNAME, table),
        params={"job_suffix": randint(0, 100000)},
        cluster_name='cluster-dataproc',
        region='europe-west3',
    )
    load_op >> ods

    if table == 'traffic':
        dm = DataProcHiveOperator(
            task_id='dm_%s' % table,
            dag=dag,
            query='''
                insert overwrite table ygladkikh.dm_traffic partition (year={{ execution_date.year }})
                select
                    `user_id`
                    , max(`bytes_received`)
                    , min(`bytes_received`)
                    , avg(`bytes_received`)
                from
                    ygladkikh.ods_traffic
                where
                    year = {{ execution_date.year }}
                group by `user_id`
            ''',
            job_name='%s_{{ execution_date.year }}_ods_%s_{{ params.job_suffix }}' % (USERNAME, table),
            params={"job_suffix": randint(0, 100000)},
            cluster_name='cluster-dataproc',
            region='europe-west3',
        )
        ods >> dm
