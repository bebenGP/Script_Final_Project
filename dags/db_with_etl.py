import os
import pandas as pd
from airflow.decorators import dag, task
from airflow.utils.email import send_email
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

@dag()
def db_with_etl():
   table          = "district"
   schema         = "etl_airflow"
   staging_folder = f"data/{schema}/{table}"

   create_schema = SQLExecuteQueryOperator(
       task_id = "create_schema",
       conn_id = "redshift_dibimbing",
       sql     = f"CREATE SCHEMA IF NOT EXISTS {schema}",
   )

   @task(task_id=f"extract_table_{table}")
   def extract(table, staging_folder):
       postgres_hook = PostgresHook("postgres_dibimbing").get_sqlalchemy_engine()

       with postgres_hook.connect() as conn:
           df = pd.read_sql(f"SELECT * FROM {table}", conn)

       os.makedirs(staging_folder, exist_ok=True)
       df.to_parquet(f"{staging_folder}/extracted.parquet", index=False)

   @task(task_id=f"transform_table_{table}")
   def transform(staging_folder):
       df = pd.read_parquet(f"{staging_folder}/extracted.parquet")
       df = df.drop(columns=["geometry"])
       df.to_parquet(f"{staging_folder}/transformed.parquet", index=False)



   @task(task_id=f"load_table_{table}")
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

   @task(task_id=f"send_email_table_{table}")
   def send_email_table(table, schema):
       redshift_hook = RedshiftSQLHook("redshift_dibimbing").get_sqlalchemy_engine()

       with redshift_hook.connect() as conn:
           df = pd.read_sql(f"SELECT COUNT(*) FROM {schema}.{table}", conn)

       send_email(
           to           = ["your-email@gmail.com"],
           subject      = f"Dibimbing | Table {schema}.{table} Analysis",
           html_content = f"<b>Total Rows</b>: <i>{df.iloc[0, 0]} rows</i><br>"
       )


   create_schema >> extract(table, staging_folder) >> transform(staging_folder) >> load(table, schema, staging_folder) >> send_email_table(table, schema)

db_with_etl()



