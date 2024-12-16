# filename: spark_etl.py

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

@dag()
def spark_etl():
   start_task = EmptyOperator(task_id="start_task")
   end_task   = EmptyOperator(task_id="end_task")

   etl = SparkSubmitOperator(
       task_id     = "etl",
       conn_id     = "spark_dibimbing",
       application = "dags/resources/scripts/spark/etl.py",
       verbose     = True,
       packages    = ",".join([
           "org.postgresql:postgresql:42.5.0",
           "com.amazon.redshift:redshift-jdbc42:2.1.0.9",
       ]),
       application_args = [
           "--table=district",
       ],
   )

   start_task >> etl >> end_task

spark_etl()




