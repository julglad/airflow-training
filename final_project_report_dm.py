from datetime import timedelta, datetime
from random import randint

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator


USERNAME = 'ygladkikh'

default_args = {
    'owner': USERNAME,
    'start_date': datetime(2012, 1, 1, 0, 0, 0),
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
with payment as
(
with raw_data as (
		select
			  user_pk::text as user_pk,
			  extract(year from to_date(billing_period_key, 'YYYY-MM')) as billing_year,
			  sum as payment_sum
		from ygladkikh.project_dds_link_payment lp
		join ygladkikh.project_dds_sat_payment_details sp on lp.pay_pk = sp.pay_pk
		join ygladkikh.project_dds_hub_billing_period hbp  on lp.billing_period_pk = hbp.billing_period_pk
		)
select billing_year, user_pk, sum(payment_sum) as payment_sum
from raw_data
group by billing_year, user_pk
order by billing_year, user_pk
)
,
billing as (
with raw_data as (
		select
			  user_pk::text as user_pk,
			  extract(year from to_date(billing_period_key, 'YYYY-MM')) as billing_year,
			  sb.sum as billing_sum,
			  row_number() OVER (PARTITION BY user_pk, lb.billing_pk ORDER by created_at desc) AS rn
		from ygladkikh.project_dds_link_billing lb
		join ygladkikh.project_dds_sat_billing_details sb on lb.billing_pk = sb.billing_pk
		join ygladkikh.project_dds_hub_billing_period hbp  on lb.billing_period_pk = hbp.billing_period_pk
		)
select billing_year, user_pk, sum(billing_sum) as billing_sum
from raw_data
where rn = 1
group by billing_year, user_pk
order by billing_year, user_pk

),
traffic as (
with raw_data as (
		select
			  user_pk::text as user_pk,
			  extract(year from time_stamp::date) billing_year,
			  bytes_sent + bytes_received as traffic_amount
		from ygladkikh.project_dds_link_traffic lt
		join ygladkikh.project_dds_sat_traffic_details s on lt.traffic_pk = s.traffic_pk
		)
select billing_year, user_pk, sum(traffic_amount) as traffic_amount
from raw_data
group by billing_year, user_pk
order by billing_year, user_pk
),
issue as (
with raw_data as (
		select
			  user_pk::text as user_pk,
			  extract(year from start_time::date) as billing_year
		from ygladkikh.project_dds_link_issue li
		join ygladkikh.project_dds_sat_issue_details s on li.issue_pk = s.issue_pk
		)
select billing_year, user_pk, count(*) as issue_cnt
from raw_data
group by billing_year, user_pk
order by billing_year, user_pk
),
raw_data as(
		select
			  billing.billing_year,
			  legal_type_key legal_type,
			  district_key district,
			  billing_mode_key billing_mode,
			  extract(year from sm.registered_at) as registration_year,
			  sm.is_vip,
			  coalesce(payment.payment_sum,0) payment_sum,
			  billing.billing_sum,
			  coalesce(issue.issue_cnt,0) issue_cnt,
			  coalesce(traffic.traffic_amount,0) traffic_amount
		from  billing
		left join payment on billing.user_pk = payment.user_pk and billing.billing_year= payment.billing_year
		left join traffic on traffic.user_pk = billing.user_pk and traffic.billing_year= billing.billing_year
		left join issue on issue.user_pk = billing.user_pk and issue.billing_year= billing.billing_year
		left join ygladkikh.project_dds_link_mdm lm on lm.user_pk = billing.user_pk
		join ygladkikh.project_dds_sat_mdm_details sm on sm.mdm_pk = lm.mdm_pk
		join ygladkikh.project_dds_hub_legal_type hlt on hlt.legal_type_pk = lm.legal_type_pk
		join ygladkikh.project_dds_hub_billing_mode hbm on hbm.billing_mode_pk = lm.billing_mode_pk
		join ygladkikh.project_dds_hub_district hd on hd.district_pk = lm.district_pk
        where billing.billing_year = {{ execution_date.year }}
		)
select billing_year, legal_type, district, billing_mode,registration_year, is_vip, sum(payment_sum) payment_sum, sum(billing_sum) billing_sum, sum(issue_cnt) issue_cnt, sum(traffic_amount) traffic_amount
from raw_data
group by billing_year, legal_type, district, billing_mode,registration_year, is_vip
order by billing_year, legal_type, district, billing_mode,registration_year, is_vip;
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

dm_dim_billing_mode  = PostgresOperator(
    task_id="dm_dim_billing_mode",
    dag=dag,
    sql="""
            insert into ygladkikh.project_report_dim_billing_mode(billing_mode_key)
            select distinct billing_mode::int as billing_mode_key 
            from ygladkikh.project_report_tmp_{{ execution_date.year }} tmp
            left join ygladkikh.project_report_dim_billing_mode d on d.billing_mode_key = tmp.billing_mode::int
            where d.billing_mode_key is null;
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
fill_report_tmp >> dm_dim_billing_mode >> all_dims_loaded
fill_report_tmp >> dm_dim_registration_year >> all_dims_loaded

dm_fct  = PostgresOperator(
    task_id="dm_fct",
    dag=dag,
    sql="""
            insert into ygladkikh.project_report_fct(
                        billing_year_id,
                        legal_type_id,
                        district_id,
                        billing_mode_id,                        
                        registration_year_id,
                        is_vip,
                        payment_sum, 
                        billing_sum, 
                        issue_cnt, 
                        traffic_amount 
                    )
            select biy.id, lt.id, d.id, ry.id, is_vip, payment_sum, billing_sum, issue_cnt, traffic_amount
            from ygladkikh.project_report_tmp_{{ execution_date.year }} raw
            join ygladkikh.project_report_dim_billing_year biy on raw.billing_year = biy.billing_year_key
            join ygladkikh.project_report_dim_legal_type lt on raw.legal_type = lt.legal_type_key
            join ygladkikh.project_report_dim_district d on raw.district = d.district_key
            join ygladkikh.project_report_dim_billing_mode bim on raw.billing_mode = bim.billing_mode_key            
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
