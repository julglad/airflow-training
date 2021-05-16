from datetime import timedelta, datetime
from random import randint

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator

USERNAME = 'ygladkikh'

default_args = {
    'owner': USERNAME,
    'start_date': datetime(2013, 1, 1, 0, 0, 0),
    'depends_on_past': True
}

dag = DAG(
    USERNAME + '_final_project_dwh_etl_issue',
    default_args=default_args,
    description='Final project DWH ETL ISSUE',
    schedule_interval="0 0 1 1 *",
    max_active_runs = 1,
)

clear_ods = PostgresOperator(
    task_id="clear_ods",
    dag=dag,
    sql="""
        DELETE FROM ygladkikh.project_ods_issue WHERE EXTRACT(YEAR FROM start_time) = {{ execution_date.year }}
    """
)

fill_ods = PostgresOperator(
    task_id="fill_ods",
    dag=dag,
    sql="""
        INSERT INTO ygladkikh.project_ods_issue
        SELECT 	* 
        FROM ygladkikh.project_stg_issue 
        WHERE EXTRACT(YEAR FROM start_time::timestamp) = {{ execution_date.year }}
    """
)

clear_ods_hashed = PostgresOperator(
    task_id="clear_ods_hashed",
    dag=dag,
    sql="""
        DELETE FROM ygladkikh.project_ods_issue_hashed WHERE EXTRACT(YEAR FROM start_time) = {{ execution_date.year }}
    """
)

fill_ods_hashed = PostgresOperator(
    task_id="fill_ods_hashed",
    dag=dag,
    sql="""
        INSERT INTO ygladkikh.project_ods_issue_hashed
        SELECT *, '{{ execution_date }}'::TIMESTAMP AS LOAD_DT FROM ygladkikh.project_ods_v_issue 
        WHERE EXTRACT(YEAR FROM start_time) = {{ execution_date.year }}
    """
)

ods_loaded = DummyOperator(task_id="ods_loaded", dag=dag)

clear_ods >> fill_ods >> clear_ods_hashed >> fill_ods_hashed >> ods_loaded

dds_hub_user = PostgresOperator(
    task_id="dds_hub_user",
    dag=dag,
    sql="""
        INSERT INTO "rtk_de"."ygladkikh"."project_dds_hub_user" 
        SELECT *
        FROM "rtk_de"."ygladkikh"."project_view_hub_user_issue_etl"
    """
)

dds_hub_service = PostgresOperator(
    task_id="dds_hub_service",
    dag=dag,
    sql="""
        INSERT INTO "rtk_de"."ygladkikh"."project_dds_hub_service" 
        SELECT * FROM "rtk_de"."ygladkikh"."project_view_hub_service_issue_etl"
    """
)

all_hubs_loaded = DummyOperator(task_id="all_hubs_loaded", dag=dag)
ods_loaded >> dds_hub_user >> all_hubs_loaded
ods_loaded >> dds_hub_service >> all_hubs_loaded

dds_link_issue = PostgresOperator(
    task_id="dds_link_issue",
    dag=dag,
    sql="""
        INSERT INTO "rtk_de"."ygladkikh"."project_dds_link_issue" 
        SELECT * FROM "rtk_de"."ygladkikh"."project_view_link_issue_etl"
    """
)

all_hubs_loaded >> dds_link_issue

all_links_loaded = DummyOperator(task_id="all_links_loaded", dag=dag)

dds_link_issue >> all_links_loaded

dds_sat_issue_details = PostgresOperator(
    task_id="dds_sat_issue_details",
    dag=dag,
    sql="""
        INSERT INTO rtk_de.ygladkikh.project_dds_sat_issue_details(issue_pk, issue_hashdiff, start_time, end_time, title, description, effective_from, load_date, record_source)
        WITH source_data AS (
            SELECT a.issue_pk, a.issue_hashdiff, a.start_time, a.end_time, a.title, a.description,a.effective_from, a.load_date, a.record_source
            FROM rtk_de.ygladkikh.project_ods_issue_hashed AS a
            WHERE a.load_date = '{{ execution_date }}'::TIMESTAMP
        ),
        update_records AS (
            SELECT a.issue_pk, a.issue_hashdiff, a.start_time, a.end_time,  a.title, a.description, a.effective_from, a.load_date, a.record_source
            FROM rtk_de.ygladkikh.project_dds_sat_issue_details as a
            JOIN source_data as b
            ON a.issue_pk = b.issue_pk AND a.load_date <= (SELECT max(load_date) from source_data)
        ),
        latest_records AS (
            SELECT * FROM (
                SELECT c.issue_pk, c.issue_hashdiff, c.load_date,
                    CASE WHEN RANK() OVER (PARTITION BY c.issue_pk ORDER BY c.load_date DESC) = 1
                    THEN 'Y' ELSE 'N' END AS latest
                FROM update_records as c
            ) as s
            WHERE latest = 'Y'
        ),
        records_to_insert AS (
            SELECT DISTINCT e.issue_pk, e.issue_hashdiff, e.start_time, e.end_time, e.title, e.description,e.effective_from, e.load_date, e.record_source
            FROM source_data AS e
            LEFT JOIN latest_records
            ON latest_records.issue_hashdiff = e.issue_hashdiff AND latest_records.issue_pk = e.issue_pk
            WHERE latest_records.issue_hashdiff IS NULL
        )
        SELECT * FROM records_to_insert
    """
)


all_links_loaded >> dds_sat_issue_details
