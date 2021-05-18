from datetime import timedelta, datetime
from random import randint

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator


USERNAME = 'ygladkikh'

default_args = {
    'owner': USERNAME,
    'start_date': datetime(2013, 1, 1, 0, 0, 0),
    'depends_on_past': False
}

dag = DAG(
    USERNAME  + '.final_project_report_dm.py',
    default_args=default_args,
    description='DWH Report DM',
    schedule_interval="0 0 1 1 *",
    concurrency=1,
    max_active_runs=1,
)

fill_report_tmp = PostgresOperator(
    task_id='fill_report_tmp',
    dag=dag,
    sql="""
           create table ygladkikh.project_report_tmp_{{ execution_date.year }} as
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
                left join ygladkikh.dds_sat_mdm_details sumd on l.USER_PK = sumd.USER_PK
                where extract(year from billing_period_key::date) = {{ execution_date.year }}	
              )		
              select billing_year, legal_type, district, registration_year, is_vip, sum(billing_sum)
              from raw_data
              group by billing_year, legal_type, district, registration_year, is_vip
              order by billing_year, legal_type, district, registration_year, is_vip;
    """
)

dm_dim_billing_year = PostgresOperator(
    task_id="dm_dim_billing_year",
    dag=dag,
    sql="""      
            insert into ygladkikh.project_report_dim_billing_year(billing_year_key)
            select distinct billing_year as billing_year_key 
            from ygladkikh.project_report_tmp_{{ execution_date.year }} tmp
            left join ygladkikh.project_report_dim_billing_year by on by.billing_year_key = tmp.billing_year
            where by.billing_year_key is null;
            """
)
dm_dim_legal_type  = PostgresOperator(
    task_id="dm_dim_legal_type",
    dag=dag,
    sql="""
            insert into ygladkikh.project_report_dim_legal_type(legal_type_key)
            select distinct legal_type as legal_type_key 
            from ygladkikh.project_report_tmp_{{ execution_date.year }} tmp
            left join ygladkikh.project_report_dim_legal_type lt on lt.legal_type_key = tmp.legal_type
            where lt.legal_type_key is null;
            """
)
dm_dim_district  = PostgresOperator(
    task_id="dm_dim_district",
    dag=dag,
    sql="""
            insert into ygladkikh.project_report_dim_district(district_key)
            select distinct district as district_key 
            from ygladkikh.project_report_tmp_{{ execution_date.year }} tmp
            left join ygladkikh.project_report_dim_district d on d.district_key = tmp.district
            where d.district_key is null;
            """
)

dm_dim_registration_year  = PostgresOperator(
    task_id="dm_dim_registration_year",
    dag=dag,
    sql="""
            insert into ygladkikh.project_report_dim_registration_year(registration_year_key)
            select distinct registration_year as registration_year_key 
            from ygladkikh.project_report_tmp_{{ execution_date.year }} tmp
            left join ygladkikh.project_report_dim_registration_year ry on ry.registration_year_key = tmp.registration_year
            where ry.registration_year_key is null;
            """
)

all_dims_loaded = DummyOperator(task_id="all_dims_loaded", dag=dag)


fill_report_tmp >> dm_dim_billing_year >> all_dims_loaded
fill_report_tmp >> dm_dim_legal_type >> all_dims_loaded
fill_report_tmp >> dm_dim_district >> all_dims_loaded
fill_report_tmp >> dm_dim_registration_year >> all_dims_loaded

dm_fct  = PostgresOperator(
    task_id="dm_fct",
    dag=dag,
    sql="""
            insert into ygladkikh.project_report_fct(
                        billing_year_id,
                        legal_type_id,
                        district_id,
                        registration_year_id,
                        is_vip,
                        sum 
                    )
            select biy.id, lt.id, d.id, ry.id, is_vip, raw.sum
            from ygladkikh.project_report_tmp_{{ execution_date.year }} raw
            join ygladkikh.project_report_dim_billing_year biy on raw.billing_year = biy.billing_year_key
            join ygladkikh.project_report_dim_legal_type lt on raw.legal_type = lt.legal_type_key
            join ygladkikh.project_report_dim_district d on raw.district = d.district_key
            join ygladkikh.project_report_dim_registration_year ry on raw.registration_year = ry.registration_year_key; 
            """
)

drop_payment_report_tmp = PostgresOperator(
    task_id='drop_project_report_tmp',
    dag=dag,
    sql="""
          drop table if exists ygladkikh.project_report_tmp_{{ execution_date.year }};
     """
)
all_dims_loaded >> dm_fct >>  drop_payment_report_tmp
