from datetime import timedelta, datetime
from random import randint

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator

SQL_CONTEXT = {
    'LOAD_PAYMENT_REPORT_TMP': """
           create table ygladkikh.payment_report_tmp_{{ execution_date.year }} as
              with raw_data as (
                select 
                      legal_type,
                      district,
                      extract(year from registered_at) as registration_year,
                      is_vip,
                      extract(year from billing_period_key::date) as billing_year,
                      sum as billing_sum
                from ygladkikh.dds_link_payment l 
                join ygladkikh.dds_hub_billing_period hbp on l.BILLING_PERIOD_PK = hbp.BILLING_PERIOD_PK
                join ygladkikh.dds_sat_payment_details spd  on l.PAY_PK = spd.PAY_PK
                left join ygladkikh.dds_sat_user_mdm_details sumd on l.USER_PK = sumd.USER_PK
                where extract(year from billing_period_key::date) = {{ execution_date.year }}	
              )		
              select billing_year, legal_type, district, registration_year, is_vip, sum(billing_sum)
              from raw_data
              group by billing_year, legal_type, district, registration_year, is_vip
              order by billing_year, legal_type, district, registration_year, is_vip;
    """,
    'DIMENSIONS': {
            'DIM_BILLING_YEAR':  """      
                    insert into ygladkikh.payment_report_dim_billing_year(billing_year_key)
                    select distinct billing_year as billing_year_key 
                    from ygladkikh.payment_report_tmp_{{ execution_date.year }} tmp
                    left join ygladkikh.payment_report_dim_billing_year by on by.billing_year_key = tmp.billing_year
                    where by.billing_year_key is null;
            """,
            'DIM_LEGAL_TYPE':  """
                    insert into ygladkikh.payment_report_dim_legal_type(legal_type_key)
                    select distinct legal_type as legal_type_key 
                    from ygladkikh.payment_report_tmp_{{ execution_date.year }} tmp
                    left join ygladkikh.payment_report_dim_legal_type lt on lt.legal_type_key = tmp.legal_type
                    where lt.legal_type_key is null;
            """,
            'DIM_DISTRICT':  """
                    insert into ygladkikh.payment_report_dim_district(district_key)
                    select distinct district as district_key 
                    from ygladkikh.payment_report_tmp_{{ execution_date.year }} tmp
                    left join ygladkikh.payment_report_dim_district d on d.district_key = tmp.district
                    where d.district_key is null;
            """,
            'DIM_REGISTRATION_YEAR':  """
                    insert into ygladkikh.payment_report_dim_registration_year(registration_year_key)
                    select distinct registration_year as registration_year_key 
                    from ygladkikh.payment_report_tmp_{{ execution_date.year }} tmp
                    left join ygladkikh.payment_report_dim_registration_year ry on ry.registration_year_key = tmp.registration_year
                    where ry.registration_year_key is null;
            """},
    'FACTS': {
            'REPORT_FACT':  """
                    insert into ygladkikh.payment_report_fct(
                                billing_year_id,
                                legal_type_id,
                                district_id,
                                registration_year_id,
                                is_vip,
                                sum 
                            )
                    select biy.id, lt.id, d.id, ry.id, is_vip, raw.sum
                    from ygladkikh.payment_report_tmp_{{ execution_date.year }} raw
                    join ygladkikh.payment_report_dim_billing_year biy on raw.billing_year = biy.billing_year_key
                    join ygladkikh.payment_report_dim_legal_type lt on raw.legal_type = lt.legal_type_key
                    join ygladkikh.payment_report_dim_district d on raw.district = d.district_key
                    join ygladkikh.payment_report_dim_registration_year ry on raw.registration_year = ry.registration_year_key; 
            """},
    'DROP_PAYMENT_REPORT_TMP': """
          drop table if exists ygladkikh.payment_report_tmp_{{ execution_date.year }};
     """
}

def get_phase_context(task_phase):
    tasks = []
    for task in SQL_CONTEXT[task_phase]:
        query = SQL_CONTEXT[task_phase][task]
        tasks.append(PostgresOperator(
            task_id='dm_{}_{}'.format(task_phase, task),
            dag=dag,
            sql=query
        ))
    return tasks

username = 'ygladkikh'
default_args = {
    'owner': username,
    'depends_on_past': False,
    'start_date': datetime(2013, 1, 1, 0, 0, 0),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=120),
}

dag = DAG(
    username + '.dwh_payment_report_dm',
    default_args=default_args,
    description='DWH Payment Report DM',
    schedule_interval="0 0 1 1 *",
    concurrency=1,
    max_active_runs=1,
)

load_payment_report_tmp = PostgresOperator(
    task_id='LOAD_PAYMENT_REPORT_TMP',
    dag=dag,
    sql=SQL_CONTEXT['LOAD_PAYMENT_REPORT_TMP']
)

all_dims_loaded = DummyOperator(task_id="all_dims_loaded", dag=dag)
all_facts_loaded = DummyOperator(task_id="all_facts_loaded", dag=dag)

drop_payment_report_tmp = PostgresOperator(
    task_id='DROP_PAYMENT_REPORT_TMP',
    dag=dag,
    sql=SQL_CONTEXT['DROP_PAYMENT_REPORT_TMP']
)

load_payment_report_tmp >> get_phase_context('DIMENSIONS') >> all_dims_loaded >> get_phase_context('FACTS') >> all_facts_loaded >>  drop_payment_report_tmp
