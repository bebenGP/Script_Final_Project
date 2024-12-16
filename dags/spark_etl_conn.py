# filename: spark_etl_conn.py

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

@dag()
def spark_etl_conn():
   start_task = EmptyOperator(task_id="start_task")
   end_task   = EmptyOperator(task_id="end_task")

   etl = SparkSubmitOperator(
       task_id     = "etl",
       conn_id     = "spark_dibimbing",
       application = "dags/resources/scripts/spark/etl_conn.py",
       verbose     = True,
       packages    = ",".join([
           "org.postgresql:postgresql:42.5.0",
           "com.amazon.redshift:redshift-jdbc42:2.1.0.9",
       ]),
       application_args = [
           "--table=district",
           "--psql_host={{ conn.postgres_dibimbing.host }}",           # psql_host
           "--psql_port={{ conn.postgres_dibimbing.port }}",           # psql_port
           "--psql_login={{ conn.postgres_dibimbing.login }}",         # psql_user
           "--psql_password={{ conn.postgres_dibimbing.password }}",   # psql_pass
           "--psql_schema={{ conn.postgres_dibimbing.schema }}",       # psql_db
           "--rs_host={{ conn.redshift_dibimbing.host }}",             # rs_host
           "--rs_port={{ conn.redshift_dibimbing.port }}",             # rs_port
           "--rs_login={{ conn.redshift_dibimbing.login }}",           # rs_user
           "--rs_password={{ conn.redshift_dibimbing.password }}",     # rs_pass
           "--rs_schema={{ conn.redshift_dibimbing.schema }}",         # rs_db
       ],
   )

   start_task >> etl >> end_task

spark_etl_conn()




