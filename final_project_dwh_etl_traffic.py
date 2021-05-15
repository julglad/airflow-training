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
    USERNAME + '_final_project_dwh_etl_traffic',
    default_args=default_args,
    description='Final project DWH ETL traffic',
    schedule_interval="0 0 1 1 *",
    max_active_runs = 1,
)

clear_ods = PostgresOperator(
    task_id="clear_ods",
    dag=dag,
    sql="""
        DELETE FROM ygladkikh.project_ods_traffic WHERE EXTRACT(YEAR FROM time_stamp) = {{ execution_date.year }}
    """
)

fill_ods = PostgresOperator(
    task_id="fill_ods",
    dag=dag,
    sql="""
        INSERT INTO ygladkikh.project_ods_traffic
        SELECT 	user_id,to_timestamp("timestamp"/1000), device_id,	device_ip_addr,	bytes_sent,	bytes_received 
        FROM ygladkikh.project_stg_traffic 
        WHERE EXTRACT(YEAR FROM to_timestamp("timestamp"/1000)) = {{ execution_date.year }}
    """
)

clear_ods_hashed = PostgresOperator(
    task_id="clear_ods_hashed",
    dag=dag,
    sql="""
        DELETE FROM ygladkikh.project_ods_traffic_hashed WHERE EXTRACT(YEAR FROM time_stamp) = {{ execution_date.year }}
    """
)

fill_ods_hashed = PostgresOperator(
    task_id="fill_ods_hashed",
    dag=dag,
    sql="""
        INSERT INTO ygladkikh.project_ods_traffic_hashed
        SELECT *, '{{ execution_date }}'::TIMESTAMP AS LOAD_DT FROM ygladkikh.project_ods_v_traffic 
        WHERE EXTRACT(YEAR FROM time_stamp) = {{ execution_date.year }}
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
        FROM "rtk_de"."ygladkikh"."project_view_hub_user_traffic_etl"
    """
)

dds_hub_device = PostgresOperator(
    task_id="dds_hub_device",
    dag=dag,
    sql="""
        INSERT INTO "rtk_de"."ygladkikh"."project_dds_hub_device" 
        SELECT * FROM "rtk_de"."ygladkikh"."project_view_hub_device_traffic_etl"
    """
)

dds_hub_ip_addr = PostgresOperator(
    task_id="dds_hub_ip_addr",
    dag=dag,
    sql="""
        INSERT INTO "rtk_de"."ygladkikh"."project_dds_hub_ip_addr" 
        SELECT * FROM "rtk_de"."ygladkikh"."project_view_hub_ip_addr_traffic_etl"
    """
)

all_hubs_loaded = DummyOperator(task_id="all_hubs_loaded", dag=dag)
ods_loaded >> dds_hub_user >> all_hubs_loaded
ods_loaded >> dds_hub_device >> all_hubs_loaded
ods_loaded >> dds_hub_ip_addr >> all_hubs_loaded


dds_link_payment = PostgresOperator(
    task_id="dds_link_traffic",
    dag=dag,
    sql="""
        INSERT INTO "rtk_de"."ygladkikh"."project_dds_link_traffic" 
        SELECT * FROM "rtk_de"."ygladkikh"."project_view_link_traffic_etl"
    """
)

all_hubs_loaded >> dds_link_traffic

all_links_loaded = DummyOperator(task_id="all_links_loaded", dag=dag)

dds_link_traffic >> all_links_loaded


dds_sat_traffic_details = PostgresOperator(
    task_id="dds_sat_traffic_details",
    dag=dag,
    sql="""
        INSERT INTO rtk_de.ygladkikh.project_dds_sat_traffic_details(traffic_pk, traffic_hashdiff, bytes_sent, bytes_received,effective_from, load_date, record_source)
        WITH source_data AS (
            SELECT a.traffic_pk, a.traffic_hashdiff, a.bytes_sent, a.bytes_received, a.effective_from, a.load_date, a.record_source
            FROM rtk_de.ygladkikh.project_ods_traffic_hashed AS a
            WHERE a.load_date = '{{ execution_date }}'::TIMESTAMP
        ),
        update_records AS (
            SELECT a.traffic_pk, a.traffic_hashdiff, a.bytes_sent, a.bytes_received, a.effective_from, a.load_date, a.record_source
            FROM rtk_de.ygladkikh.project_dds_sat_traffic_details as a
            JOIN source_data as b
            ON a.traffic_pk = b.traffic_pk AND a.load_date <= (SELECT max(load_date) from source_data)
        ),
        latest_records AS (
            SELECT * FROM (
                SELECT c.traffic_pk, c.traffic_hashdiff, c.load_date,
                    CASE WHEN RANK() OVER (PARTITION BY c.traffic_pk ORDER BY c.load_date DESC) = 1
                    THEN 'Y' ELSE 'N' END AS latest
                FROM update_records as c
            ) as s
            WHERE latest = 'Y'
        ),
        records_to_insert AS (
            SELECT DISTINCT e.traffic_pk, e.traffic_hashdiff, e.bytes_sent, e.bytes_received, e.effective_from, e.load_date, e.record_source
            FROM source_data AS e
            LEFT JOIN latest_records
            ON latest_records.traffic_hashdiff = e.traffic_hashdiff AND latest_records.traffic_pk = e.traffic_pk
            WHERE latest_records.traffic_hashdiff IS NULL
        )
        SELECT * FROM records_to_insert
    """
)

all_links_loaded >> dds_sat_traffic_details
