from datetime import datetime, timedelta
from random import randint

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator

USERNAME = 'ygladkikh'

default_args = {
    "owner": USERNAME,
    "start_date": datetime(2012, 1, 1, 0, 0, 0)
}

dag = DAG(
    USERNAME + '_dwh_etl',
    default_args=default_args,
    description='RTDE. DV 2.0',
    schedule_interval="0 0 1 1 *"
)

# Dummy operators
ods_start = DummyOperator(task_id="ods_start", dag=dag)
ods_finished = DummyOperator(task_id="ods_finished", dag=dag)

all_hubs_loaded = DummyOperator(task_id="all_hubs_loaded", dag=dag)
all_link_loaded = DummyOperator(task_id="all_link_loaded", dag=dag)
all_sat_loaded = DummyOperator(task_id="all_sat_loaded", dag=dag)

dds_start = DummyOperator(task_id="dds_start", dag=dag)
dds_finished = DummyOperator(task_id="dds_finished", dag=dag)

# -----ODS. LOAD DATA

ods_truncate_partition = PostgresOperator(
    task_id='ods_truncate_partition',
    dag=dag,
    sql="""
    ALTER TABLE ygladkikh.ods_payment TRUNCATE PARTITION "{{ execution_date.year }}";
    """
)

ods_load_from_stg = PostgresOperator(
    task_id='ods_load_from_stg',
    dag=dag,
    sql="""
    insert into ygladkikh.ods_payment (
               user_id,
               pay_doc_type,
               pay_doc_num,
               account,
               phone,
               billing_period,
               pay_date,
               sum) (
    select
               user_id,
               pay_doc_type,
               pay_doc_num,
               account,
               phone,
               billing_period,
               pay_date,
               sum
    from ygladkikh.stg_payment
    where extract('year' from pay_date) = '{{ execution_date.year }}'
);
    """
)

# -----DDS. HUBS

dds_user_hub = PostgresOperator(
    task_id='dds_user_hub',
    dag=dag,
    sql="""
insert into ygladkikh.dds_hub_user(
                                   user_pk,
                                   user_key,
                                   --
                                   load_date,
                                   record_source)(
    select user_pk,
           user_key,
           --
           load_date,
           record_source
    from ygladkikh.dds_hub_user_view_etl
);  
    """
)

dds_pay_doc_type_hub = PostgresOperator(
    task_id='dds_pay_doc_type_hub',
    dag=dag,
    sql="""
insert into ygladkikh.dds_hub_pay_doc_type (pay_doc_type_pk,
                                            pay_doc_type_key,
                                            --
                                            load_date,
                                            record_source)(
    select pay_doc_type_pk,
           pay_doc_type_key,
           --
           load_date,
           record_source
    from ygladkikh.dds_hub_pay_doc_type_view_etl
);    
    """
)

dds_account_hub = PostgresOperator(
    task_id='dds_account_hub',
    dag=dag,
    sql="""
insert into ygladkikh.dds_hub_account (account_pk,
                                       account_key,
                                       --
                                       load_date,
                                       record_source)(
    select account_pk,
           account_key,
           --
           load_date,
           record_source
    from ygladkikh.dds_hub_account_view_etl
);

    """
)

dds_billing_period_hub = PostgresOperator(
    task_id='dds_billing_period_hub',
    dag=dag,
    sql="""
insert into ygladkikh.dds_billing_period (billing_period_pk,
                                          billing_period_key,
                                          --
                                          load_date,
                                          record_source)(
    select billing_period_pk,
           billing_period_key,
           --
           load_date,
           record_source
    from ygladkikh.dds_billing_period_view_etl
);
    """
)

# -----DDS. LINKS

dds_payment_link_t = PostgresOperator(
    task_id='dds_payment_link_t',
    dag=dag,
    sql="""
insert into ygladkikh.dds_link_t_payment (pay_pk,
                                          --
                                          user_pk,
                                          account_pk,
                                          billing_period_pk,
                                          --
                                          load_date,
                                          record_source)
    (
        select pay_pk,
               --
               user_pk,
               account_pk,
               billing_period_pk,
               --
               load_date,
               record_source
        from ygladkikh.dds_link_t_payment_view_etl
);
    """
)

# -----DDS. SATS

dds_user_sat = PostgresOperator(
    task_id='dds_user_sat',
    dag=dag,
    sql="""
insert into ygladkikh.dds_sat_user (user_pk,
                                    --
                                    user_hashdiff,
                                    --
                                    phone,
                                    effective_from,
                                    --
                                    load_date,
                                    record_source)(
    select user_pk,
           --
           user_hashdiff,
           --
           phone,
           effective_from,
           --
           load_date,
           record_source
    from ygladkikh.dds_sat_user_view_etl
);
    """
)

dds_payment_sat = PostgresOperator(
    task_id='dds_payment_sat',
    dag=dag,
    sql="""
insert into ygladkikh.dds_sat_payment (pay_pk,
                                       --
                                       pay_doc_num,
                                       pay_date,
                                       sum,
                                       --
                                       payment_hashdiff,
                                       effective_from,
                                       --
                                       load_date,
                                       record_source)(
    select pay_pk,
           --
           pay_doc_num,
           pay_date,
           sum,
           --
           payment_hashdiff,
           effective_from,
           --
           load_date,
           record_source
    from ygladkikh.dds_sat_payment_view_etl
);
    """
)

# truncate ods_payment year partition
ods_start >> ods_truncate_partition

# load year partition from stg
ods_truncate_partition >> ods_load_from_stg
ods_load_from_stg >> ods_finished

# load all hubs
ods_finished >> dds_start
dds_start >> dds_user_hub >> all_hubs_loaded
dds_start >> dds_pay_doc_type_hub >> all_hubs_loaded
dds_start >> dds_account_hub >> all_hubs_loaded
dds_start >> dds_billing_period_hub >> all_hubs_loaded

# load all links
all_hubs_loaded >> dds_payment_link_t >> all_link_loaded

# load all sats
all_link_loaded >> dds_user_sat >> all_sat_loaded
all_link_loaded >> dds_payment_sat >> all_sat_loaded

# finish all
all_sat_loaded >> dds_finished
