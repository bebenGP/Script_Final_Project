from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

@dag()
def db_with_operator_direct():
   start_task = EmptyOperator(task_id="start_task")
   end_task   = EmptyOperator(task_id="end_task")

   query_postgres = SQLExecuteQueryOperator(
       task_id = "query_postgres",
       conn_id = "postgres_dibimbing",
       sql     = "SELECT * FROM district"
   )

   query_redshift = SQLExecuteQueryOperator(
       task_id = "query_redshift",
       conn_id = "redshift_dibimbing",
       sql     = "SELECT * FROM bronze.district"
   )

   start_task >> [query_postgres, query_redshift] >> end_task

db_with_operator_direct()


