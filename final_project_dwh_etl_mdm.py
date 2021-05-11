from datetime import timedelta, datetime
from random import randint

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator

USERNAME = 'ygladkikh'

default_args = {
    'owner': USERNAME,
    'start_date': datetime(2010, 1, 1, 0, 0, 0),
    'depends_on_past': True
}

dag = DAG(
    USERNAME + '_final_project_dwh_etl_mdm',
    default_args=default_args,
    description='Final project DWH ETL MDM',
    schedule_interval="0 0 1 1 *",
    max_active_runs = 1,
)

clear_ods = PostgresOperator(
    task_id="clear_ods",
    dag=dag,
    sql="""
        DELETE FROM ygladkikh.project_ods_mdm WHERE EXTRACT(YEAR FROM registered_at) = {{ execution_date.year }}
    """
)

fill_ods = PostgresOperator(
    task_id="fill_ods",
    dag=dag,
    sql="""
        INSERT INTO ygladkikh.project_ods_mdm
        SELECT *  
        FROM mdm.user 
        WHERE EXTRACT(YEAR FROM registered_at) = {{ execution_date.year }}
    """
)

clear_ods_hashed = PostgresOperator(
    task_id="clear_ods_hashed",
    dag=dag,
    sql="""
        DELETE FROM ygladkikh.project_ods_mdm_hashed WHERE EXTRACT(YEAR FROM registered_at) = {{ execution_date.year }}
    """
)

fill_ods_hashed = PostgresOperator(
    task_id="fill_ods_hashed",
    dag=dag,
    sql="""
        INSERT INTO ygladkikh.project_ods_mdm_hashed
        SELECT *, '{{ execution_date }}'::TIMESTAMP AS LOAD_DT FROM ygladkikh.project_ods_v_mdm 
        WHERE EXTRACT(YEAR FROM registered_at) = {{ execution_date.year }}
    """
)

ods_loaded = DummyOperator(task_id="ods_loaded", dag=dag)

clear_ods >> fill_ods >> clear_ods_hashed >> fill_ods_hashed >> ods_loaded

dds_hub_user = PostgresOperator(
    task_id="dds_hub_user",
    dag=dag,
    sql="""
        INSERT INTO "rtk_de"."ygladkikh"."project_dds_hub_user" 
        SELECT * FROM "rtk_de"."ygladkikh"."project_view_hub_user_mdm_etl"
    """
)

dds_hub_pay_doc_type = PostgresOperator(
    task_id="dds_hub_pay_doc_type",
    dag=dag,
    sql="""
        INSERT INTO "rtk_de"."ygladkikh"."project_dds_hub_legal_type" 
        SELECT * FROM "rtk_de"."ygladkikh"."project_view_hub_legal_type_mdm_etl"
    """
)

dds_hub_billing_period = PostgresOperator(
    task_id="dds_hub_billing_period",
    dag=dag,
    sql="""
        INSERT INTO "rtk_de"."ygladkikh"."project_dds_hub_district_period" 
        SELECT * FROM "rtk_de"."ygladkikh"."project_view_hub_district_mdm_etl"
    """
)
dds_hub_account = PostgresOperator(
    task_id="dds_hub_account",
    dag=dag,
    sql="""
        INSERT INTO "rtk_de"."ygladkikh"."project_dds_hub_billing_mode"   
        SELECT * FROM "rtk_de"."ygladkikh"."project_view_hub_billing_mode_mdm_etl"
    """
)

all_hubs_loaded = DummyOperator(task_id="all_hubs_loaded", dag=dag)
ods_loaded >> dds_hub_user >> all_hubs_loaded
ods_loaded >> dds_hub_pay_doc_type >> all_hubs_loaded
ods_loaded >> dds_hub_billing_period >> all_hubs_loaded
ods_loaded >> dds_hub_account >> all_hubs_loaded

dds_link_payment = PostgresOperator(
    task_id="dds_link_payment",
    dag=dag,
    sql="""
        INSERT INTO "rtk_de"."ygladkikh"."project_dds_link_mdm" 
        SELECT * FROM "rtk_de"."ygladkikh"."project_view_link_mdm_etl"
    """
)

all_hubs_loaded >> dds_link_payment

all_links_loaded = DummyOperator(task_id="all_links_loaded", dag=dag)

dds_link_payment >> all_links_loaded

dds_sat_payment_details = PostgresOperator(
    task_id="dds_sat_mdm_details",
    dag=dag,
    sql="""
        INSERT INTO "rtk_de"."ygladkikh"."project_dds_sat_mdm_details" 
        WITH source_data AS (
            SELECT a.mdm_pk, a.mdm_hashdiff, a.is_vip, a.registered_at, a.effective_from, a.load_date, a.record_source
            FROM "rtk_de"."ygladkikh"."project_ods_mdm_hashed" AS a
            WHERE a.load_date = '{{ execution_date }}'::TIMESTAMP
        ),
        update_records AS (
            SELECT a.mdm_pk, a.mdm_hashdiff, a.is_vip, a.registered_at, a.effective_from, a.load_date, a.record_source
            FROM "rtk_de"."ygladkikh"."project_dds_sat_mdm_details" as a
            JOIN source_data as b
            ON a.mdm_pk = b.mdm_pk AND a.load_date <= (SELECT max(load_date) from source_data)
        ),
        latest_records AS (
            SELECT * FROM (
                SELECT c.mdm_pk, c.mdm_hashdiff, c.load_date,
                    CASE WHEN RANK() OVER (PARTITION BY c.mdm_pk ORDER BY c.load_date DESC) = 1
                    THEN 'Y' ELSE 'N' END AS latest
                FROM update_records as c
            ) as s
            WHERE latest = 'Y'
        ),
        records_to_insert AS (
            SELECT DISTINCT e.mdm_pk, e.mdm_hashdiff, e.is_vip, e.registered_at, e.effective_from, e.load_date, e.record_source
            FROM source_data AS e
            LEFT JOIN latest_records
            ON latest_records.mdm_hashdiff = e.mdm_hashdiff AND latest_records.mdm_pk = e.mdm_pk
            WHERE latest_records.mdm_hashdiff IS NULL
        )
        SELECT * FROM records_to_insert
    """
)

all_links_loaded >> dds_sat_payment_details
