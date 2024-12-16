import os
import pandas as pd
import yaml
import pytz
from datetime import datetime
from airflow.decorators import dag, task
from airflow.utils.email import send_email
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

with open("dags/resources/dynamic-dag/list_tables_australian.yaml") as f:
    list_tables = yaml.safe_load(f)

@dag(
    dag_id            = "final_project_etl",
    schedule_interval = "0 7 * * *",
    start_date        = datetime(2024, 12, 14, tzinfo=pytz.timezone("Asia/Jakarta")),
    catchup           = True,
    tags              = ["exercise"],
)

def final_project_etl():
    schema = "final_etl_airflow"

    create_schema = SQLExecuteQueryOperator(
        task_id = "create_schema",
        conn_id = "redshift_dibimbing",
        sql     = f"CREATE SCHEMA IF NOT EXISTS {schema}",
    )

    trigger_dag_datamart = TriggerDagRunOperator(
        task_id             = "trigger_dag_datamart",
        trigger_dag_id      = "final_project_datamart",
        wait_for_completion = True,
        poke_interval       = 5,
    )


    for table in list_tables:
        staging_folder = f"data/{schema}/{table}"

        @task(task_id="extract_table")
        def extract(table, staging_folder):
            postgres_hook = PostgresHook("postgres_dibimbing").get_sqlalchemy_engine()

            with postgres_hook.connect() as conn:
                df = pd.read_sql(f"SELECT * FROM {table}", conn)

            os.makedirs(staging_folder, exist_ok=True)
            df.to_parquet(f"{staging_folder}/extracted.parquet", index=False)

        @task(task_id="transform_table")
        def transform(staging_folder, table):
            df = pd.read_parquet(f"{staging_folder}/extracted.parquet")
            
            # Custom transform based on table name
            if table == "australian_accident":
                df = df.drop(columns=['National Road Type','National Remoteness Areas','SA4 Name 2016','National LGA Name 2017','Heavy Rigid Truck Involvement'])
                df.columns = df.columns.str.replace(' ', '_')
                df.drop_duplicates()
                df.dropna()
                df['Speed_Limit'] = df['Speed_Limit'].replace(r'\D', '', regex=True)
                df['Speed_Limit'] = pd.to_numeric(df['Speed_Limit'], errors='coerce').fillna(0).astype(int)
            elif table == "australian_property":
                df = df.drop(columns=['building_size','land_size','preferred_size','open_date','latitude','longitude', 'breadcrumb', 'category_name', 'location_type','location_name', 'address_1'])
                df['price'] = df['price'].replace(r'\D', '', regex=True)
                df['price'] = pd.to_numeric(df['price'], errors='coerce').fillna(0).astype(int)
                df['listing_agency'] = df.listing_agency.apply(lambda x: x.lower() if pd.notnull(x) else x)
                df['RunDate'] = pd.to_datetime(df['RunDate'])
            elif table == "australian_population":
                df = df.iloc[:-6]
                df = df.replace('-', 0)
                for col in df.columns:
                    if '(no.)' in col:  
                        df[col] = df[col].replace(',', '', regex=True)  
                        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)  
                    elif '(%)' or '(persons/km2)' or '(years)' or '(rate)' in col:
                        df[col] = df[col].replace(',', '', regex=True)  
                        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0).astype(float) 
                    elif 'Code' or 'Label' in col:
                        df[col] = df[col].astype(str)

            df['extracted_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            df.to_parquet(f"{staging_folder}/transformed.parquet", index=False)

        @task(task_id="load_table")
        def load(table, schema, staging_folder):
            redshift_hook = RedshiftSQLHook("redshift_dibimbing").get_sqlalchemy_engine()

            df = pd.read_parquet(f"{staging_folder}/transformed.parquet")
            with redshift_hook.connect() as conn:
                df.to_sql(
                    name      = table,
                    con       = conn,
                    index     = False,
                    schema    = schema,
                    if_exists = "replace",
                    chunksize = 100,
                    method    = "multi",
                )


        create_schema >> extract.override(task_id=f"extract_{table}")(table, staging_folder)  \
              >> transform.override(task_id=f"transform_{table}")(staging_folder, table) \
              >> load.override(task_id=f"load_{table}")(table, schema, staging_folder) \
              >> trigger_dag_datamart


    
final_project_etl()