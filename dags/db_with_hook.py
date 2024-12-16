from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator

@dag()
def db_with_hook():
   start_task = EmptyOperator(task_id="start_task")
   end_task   = EmptyOperator(task_id="end_task")

   @task
   def query_postgres():
       import pandas as pd
       from airflow.providers.postgres.hooks.postgres import PostgresHook

       postgres_hook = PostgresHook("postgres_dibimbing").get_sqlalchemy_engine()

       with postgres_hook.connect() as conn:
           df = pd.read_sql(
               sql = "SELECT * FROM district",
               con = conn,
           )

       print(df)


   @task
   def query_redshift():
       import pandas as pd
       from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook

       redshift_hook = RedshiftSQLHook("redshift_dibimbing").get_sqlalchemy_engine()

       with redshift_hook.connect() as conn:
           df = pd.read_sql(
               sql = "SELECT * FROM bronze.district",
               con = conn,
           )

       print(df)

   start_task >> [query_postgres(), query_redshift()] >> end_task

db_with_hook()



