from datetime import timedelta, datetime
from random import randint

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator

USERNAME = 'ygladkikh'

default_args = {
    'owner': USERNAME,
    'start_date': datetime(2012, 1, 1, 0, 0, 0),
    'depends_on_past': True
}

dag = DAG(
    USERNAME + '_dwh_etl',
    default_args=default_args,
    description='DWH ETL tasks',
    schedule_interval="0 0 1 1 *",
)

clear_ods = PostgresOperator(
    task_id="clear_ods",
    dag=dag,
    sql="""
        DELETE FROM ygladkikh.ods_payment WHERE EXTRACT(YEAR FROM pay_date::DATE) = {{ execution_date.year }}
    """
)

fill_ods = PostgresOperator(
    task_id="fill_ods",
    dag=dag,
    sql="""
        INSERT INTO ygladkikh.ods_payment(user_id,pay_doc_type text,pay_doc_num,account,phone,billing_period,pay_date,sum)
        SELECT user_id,pay_doc_type text,pay_doc_num,account,phone,billing_period::DATE,pay_date::DATE,sum FROM ygladkikh.stg_payment 
        WHERE EXTRACT(YEAR FROM pay_date::DATE) = {{ execution_date.year }}
    """
)

clear_ods_hashed = PostgresOperator(
    task_id="clear_ods_hashed",
    dag=dag,
    sql="""
        DELETE FROM ygladkikh.ods_payment_hashed WHERE EXTRACT(YEAR FROM pay_date::DATE) = {{ execution_date.year }}
    """
)

fill_ods_hashed = PostgresOperator(
    task_id="fill_ods_hashed",
    dag=dag,
    sql="""
        INSERT INTO ygladkikh.ods_payment_hashed
        SELECT *, '{{ execution_date }}'::TIMESTAMP AS LOAD_DT FROM ygladkikh.ods_v_payment 
        WHERE EXTRACT(YEAR FROM pay_date::DATE) = {{ execution_date.year }}
    """
)

ods_loaded = DummyOperator(task_id="ods_loaded", dag=dag)

clear_ods >> fill_ods >> clear_ods_hashed >> fill_ods_hashed >> ods_loaded

dds_hub_user = PostgresOperator(
    task_id="dds_hub_user",
    dag=dag,
    sql="""
        INSERT INTO "rtk_de"."ygladkikh"."dds_hub_user" ("user_pk", "user_key", "load_date", "record_source")
        SELECT "user_pk", "user_key", "load_date", "record_source"
        FROM "rtk_de"."ygladkikh"."view_hub_user_etl"
    """
)

dds_hub_pay_doc_type = PostgresOperator(
    task_id="dds_hub_pay_doc_type",
    dag=dag,
    sql="""
        INSERT INTO "rtk_de"."ygladkikh"."dds_hub_pay_doc_type" ("pay_doc_type_pk", "pay_doc_type_key", "load_date", "record_source")
        SELECT "pay_doc_type_pk", "pay_doc_type_key", "load_date", "record_source"
        FROM "rtk_de"."ygladkikh"."view_hub_pay_doc_type_etl"
    """
)

dds_hub_billing_period = PostgresOperator(
    task_id="dds_hub_billing_period",
    dag=dag,
    sql="""
        INSERT INTO "rtk_de"."ygladkikh"."dds_hub_billing_period" ("billing_period_pk", "billing_period_key", "load_date", "record_source")
        SELECT "billing_period_pk", "billing_period_key", "load_date", "record_source"
        FROM "rtk_de"."ygladkikh"."view_hub_billing_period_etl"
    """
)
dds_hub_account = PostgresOperator(
    task_id="dds_hub_account",
    dag=dag,
    sql="""
        INSERT INTO "rtk_de"."ygladkikh"."dds_hub_account" ("account_pk", "account_key", "load_date", "record_source")
        SELECT "account_pk", "account_key", "load_date", "record_source"
        FROM "rtk_de"."ygladkikh"."view_hub_account_etl"
    """
)

ods_loaded >> dds_hub_user
ods_loaded >> dds_hub_pay_doc_type
ods_loaded >> dds_hub_billing_period
ods_loaded >> dds_hub_account

all_hubs_loaded = DummyOperator(task_id="all_hubs_loaded", dag=dag)

dds_hub_user           >> all_hubs_loaded
dds_hub_pay_doc_type   >> all_hubs_loaded
dds_hub_billing_period >> all_hubs_loaded
dds_hub_account        >> all_hubs_loaded

dds_link_payment = PostgresOperator(
    task_id="dds_link_payment",
    dag=dag,
    sql="""
        INSERT INTO "rtk_de"."ygladkikh"."dds_link_payment" ("pay_pk", "user_pk", "account_pk", "pay_doc_type_pk", "billing_period_pk", "effective_from", "load_date", "record_source")
        SELECT "pay_pk", "user_pk", "account_pk", "pay_doc_type_pk", "billing_period_pk", "effective_from", "load_date", "record_source"
        FROM "rtk_de"."ygladkikh"."view_link_payment_etl"
    """
)

all_hubs_loaded >> dds_link_payment

all_links_loaded = DummyOperator(task_id="all_links_loaded", dag=dag)

dds_link_payment >> all_links_loaded

dds_sat_user_details = PostgresOperator(
    task_id="dds_sat_user_details",
    dag=dag,
    sql="""
        INSERT INTO "rtk_de"."ygladkikh"."dds_sat_user_details" ("user_pk", "user_hashdiff", "phone", "effective_from", "load_date", "record_source")
        WITH source_data AS (
            SELECT a.user_pk, a.user_hashdiff, a.phone, a.effective_from, a.load_date, a.record_source
            FROM "rtk_de"."ygladkikh"."ods_payment_hashed" AS a
            WHERE a.load_date <= '{{ execution_date }}'::TIMESTAMP
        ),
        update_records AS (
            SELECT a.user_pk, a.user_hashdiff, a.phone, a.effective_from, a.load_date, a.record_source
            FROM "rtk_de"."ygladkikh"."dds_sat_user_details" as a
            JOIN source_data as b
            ON a.user_pk = b.user_pk AND a.load_date <= (SELECT max(load_date) from source_data)
        ),
        latest_records AS (
            SELECT * FROM (
                SELECT c.user_pk, c.user_hashdiff, c.load_date,
                    CASE WHEN RANK() OVER (PARTITION BY c.user_pk ORDER BY c.load_date DESC) = 1
                    THEN 'Y' ELSE 'N' END AS latest
                FROM update_records as c
            ) as s
            WHERE latest = 'Y'
        ),
        records_to_insert AS (
            SELECT DISTINCT e.user_pk, e.user_hashdiff, e.phone, e.effective_from, e.load_date, e.record_source
            FROM source_data AS e
            LEFT JOIN latest_records
            ON latest_records.user_hashdiff = e.user_hashdiff AND latest_records.user_pk = e.user_pk
            WHERE latest_records.user_hashdiff IS NULL
        )
        SELECT * FROM records_to_insert
    """
)
dds_sat_payment_details = PostgresOperator(
    task_id="dds_sat_payment_details",
    dag=dag,
    sql="""
        INSERT INTO "rtk_de"."ygladkikh"."dds_sat_payment_details" ("pay_pk", "payment_hashdiff", "pay_doc_num", "sum","effective_from", "load_date", "record_source")
        WITH source_data AS (
            SELECT a.pay_pk, a.payment_hashdiff, a.pay_doc_num, a.sum, a.effective_from, a.load_date, a.record_source
            FROM "rtk_de"."ygladkikh"."ods_payment_hashed" AS a
            WHERE a.load_date <= '{{ execution_date }}'::TIMESTAMP
        ),
        update_records AS (
            SELECT a.pay_pk, a.payment_hashdiff, a.pay_doc_num, a.sum, a.effective_from, a.load_date, a.record_source
            FROM "rtk_de"."ygladkikh"."dds_sat_payment_details" as a
            JOIN source_data as b
            ON a.pay_pk = b.pay_pk AND a.load_date <= (SELECT max(load_date) from source_data)
        ),
        latest_records AS (
            SELECT * FROM (
                SELECT c.pay_pk, c.payment_hashdiff, c.load_date,
                    CASE WHEN RANK() OVER (PARTITION BY c.pay_pk ORDER BY c.load_date DESC) = 1
                    THEN 'Y' ELSE 'N' END AS latest
                FROM update_records as c
            ) as s
            WHERE latest = 'Y'
        ),
        records_to_insert AS (
            SELECT DISTINCT e.pay_pk, e.payment_hashdiff, e.pay_doc_num, e.sum, e.effective_from, e.load_date, e.record_source
            FROM source_data AS e
            LEFT JOIN latest_records
            ON latest_records.payment_hashdiff = e.payment_hashdiff AND latest_records.pay_pk = e.pay_pk
            WHERE latest_records.payment_hashdiff IS NULL
        )
        SELECT * FROM records_to_insert
    """
)

all_links_loaded >> dds_sat_user_details
all_links_loaded >> dds_sat_payment_details
