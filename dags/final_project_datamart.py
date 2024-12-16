import io
import boto3
import pandas as pd
import sqlalchemy as sa
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import pytz

# Koneksi Redshift
creds_rs = {
   "hostname": "beben-redshift.533267303425.ap-southeast-3.redshift-serverless.amazonaws.com",
   "port": 5439,
   "username": "admin",
   "password": "Bonaparte15",
   "dbname": "dev",
}

# Membuat koneksi Redshift
engine_rs = sa.create_engine(
    f"redshift://{creds_rs['username']}:{creds_rs['password']}@{creds_rs['hostname']}:{creds_rs['port']}/{creds_rs['dbname']}"
)

# Membuat koneksi S3
s3_client = boto3.client(
    's3',
    aws_access_key_id='AKIAXYKJVJQARLE2XKX6',
    aws_secret_access_key='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',
    region_name='ap-southeast-3'
)

# Fungsi untuk eksekusi query ke Redshift
def execute_rs(query):
    with engine_rs.begin() as conn:
        conn.execute(sa.text(query))

# Fungsi untuk menyimpan hasil query ke Redshift
def load_to_redshift(df, table, schema):
    with engine_rs.begin() as conn:
        df.to_sql(
            name=table,
            con=conn,
            schema=schema,
            index=False,
            if_exists="replace",
            chunksize=100,
            method="multi"
        )

# DAG Airflow
@dag(
    dag_id="final_project_datamart",
    schedule_interval=None,
    start_date=datetime(2024, 12, 14, tzinfo=pytz.timezone("Asia/Jakarta")),
    catchup=False,
    tags=["datamart", "final_project"]
)
def final_project_datamart():
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    @task
    def query_and_load():
        # Query SQL untuk hasil datamart
        query = """
        WITH ses_summary AS (
            SELECT 
                kecamatan,
                SUM(total_ses_a) AS total_ses_a
            FROM 
                final_etl_airflow.demografi
            GROUP BY 
                kecamatan
        ),
        poi_summary AS (
            SELECT 
                upper(district) AS kecamatan,
                COUNT(poi_name) AS total_poi
            FROM 
                final_etl_airflow.data_services
            GROUP BY 
                district
        )
        SELECT 
            COALESCE(ses.kecamatan, poi.kecamatan) AS kecamatan,
            COALESCE(ses.total_ses_a, 0) AS total_ses_a,
            COALESCE(poi.total_poi, 0) AS total_poi
        FROM 
            ses_summary AS ses
        FULL OUTER JOIN 
            poi_summary AS poi
        ON 
            ses.kecamatan = poi.kecamatan
        ORDER BY 
            kecamatan;
        """
        # Eksekusi query dan simpan hasil dalam DataFrame
        df_result = pd.read_sql(query, engine_rs)

        # Load hasil ke Redshift (schema: gold, table: final_project_result)
        load_to_redshift(df_result, table="final_project_result", schema="gold")
        print("Data berhasil dimuat ke Redshift pada schema gold")

    start >> query_and_load() >> end

final_project_datamart()
