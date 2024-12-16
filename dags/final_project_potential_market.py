import os
import pandas as pd
import yaml
import pytz
from datetime import datetime
from airflow.decorators import dag, task
from airflow.utils.email import send_email
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook  # Import S3Hook untuk koneksi ke S3
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook

from io import StringIO

with open("dags/resources/dynamic-dag/list_table_final_project.yaml") as f:
    list_tables = yaml.safe_load(f)

@dag(
    dag_id            = "final_project_potential_market",
    schedule_interval = "0 7 * * *",
    start_date        = datetime(2024, 12, 14, tzinfo=pytz.timezone("Asia/Jakarta")),
    catchup           = True,
    tags              = ["exercise"],
)
def final_project_potential_market():
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
            # Koneksi ke PostgreSQL untuk ekstraksi data
            postgres_hook = PostgresHook("postgres_dibimbing").get_sqlalchemy_engine()

            # Query untuk setiap tabel berdasarkan nama
            if table == "favorite_place":
                query = """
                WITH clean_fp AS (
                    SELECT * 
                    FROM favorite_place
                    WHERE "total popular time" >= 1000
                ),
                filter_searchstring AS (
                    SELECT * 
                    FROM clean_fp 
                    WHERE "searchString" = 'restoran Surabaya' OR "searchString" = 'perbelanjaan Surabaya'
                )
                SELECT * FROM filter_searchstring;
                """
            elif table == "demografi":
                query = """
                SELECT 
                    lls2020_prov AS "Provinsi",
                    lls2020_kabkot AS "Kab/Kota",
                    lls2020_kec AS "Kecamatan",
                    lls2020_desa AS "Desa/Kelurahan",
                    first_d19_jumlah_pen AS "Jumlah_Penduduk",
                    first_d19_u15 AS "Jumlah_Usia_15-19_tahun",
                    first_d19_u20 AS "Jumlah_Usia_20-24_tahun",
                    first_d19_u25 AS "Jumlah_Usia_25-29_tahun",
                    first_d19_u30 AS "Jumlah_Usia_30-34_tahun",
                    first_d19_u35 AS "Jumlah_Usia_35-39_tahun",
                    first_d19_u40 AS "Jumlah_Usia_40-44_tahun",
                    first_d19_pelajar_ma AS "Jumlah_Pelajar&Mahasiswa",
                    first_d19_aparatur_p AS "Jumlah_Pekerja_ASN",
                    first_d19_tenaga_pen AS "Jumlah_Pekerja_Pengajar",
                    first_d19_wiraswasta AS "Jumlah_Pekerja_Wiraswasta",
                    first_d19_tenaga_kes AS "Jumlah_Pekerja_Nakes",
                    first_d19_pensiunan AS "Jumlah_Pensiunan",
                    first_d19_lainnya AS "Jumlah_Pekerjaan_Lainnya",
                    first_lls2021_avg_income AS "Pendapatan_Rata_Rata",
                    first_lls2021_avg_expend AS "Pengeluaran_Rata_Rata",
                    first_sen19_pop_a AS "Jumlah_SES_A",
                    first_sen19_pop_a1 AS "Jumlah_SES_A1",
                    first_sen19_pop_a2 AS "Jumlah_SES_A2",
                    first_sen19_pop_b AS "Jumlah_SES_B",
                    "WKT" 
                FROM public.demografi
                ORDER BY first_sen19_pop_a DESC;
                """
            elif table == "data_services":
                query = """
                WITH clean_ds AS (
                    SELECT * 
                    FROM public.data_services
                    WHERE source IS NOT NULL
                ),
                filter_tags AS (
                    SELECT * 
                    FROM clean_ds
                    WHERE tags LIKE '%%Retail%%'
                )
                SELECT * 
                FROM filter_tags;
                """
            else:
                query = f"SELECT * FROM {table}"

            # Eksekusi query dan ambil hasilnya dalam DataFrame
            with postgres_hook.connect() as conn:
                df = pd.read_sql(query, conn)

            # Menyimpan data CSV dalam buffer
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)

            # Inisialisasi koneksi S3 dari Airflow
            s3_hook = S3Hook(aws_conn_id="aws_default")  # Gunakan koneksi AWS default atau yang telah Anda buat

            # Nama file dan lokasi S3
            file_name = f"{table}_extracted.csv"
            bucket_name = 'beben'

            # Unggah file CSV ke S3
            s3_hook.load_string(
                string_data=csv_buffer.getvalue(),
                key=f"final_project/{file_name}",
                bucket_name=bucket_name,
                replace=True  # Jika ingin menggantikan file yang sudah ada
            )

            # Menyimpan file ke S3 selesai
            print(f"File CSV untuk {table} berhasil diunggah ke S3://{bucket_name}/final_project/{file_name}")


        @task(task_id="transform_table")
        def transform(staging_folder, table):
            # Inisialisasi S3Hook
            s3_hook = S3Hook(aws_conn_id="aws_default")
            bucket_name = 'beben'

            # Ambil file CSV dari S3
            s3_key = f"final_project/{table}_extracted.csv"
            file_obj = s3_hook.get_key(s3_key, bucket_name)
            df = pd.read_csv(file_obj.get()["Body"])

            try:
                # Transformasi untuk tabel 'favorite_place'
                if table == "favorite_place":
                    df_transformed = df.groupby("categories/0")["total popular time"].sum().reset_index()
                    df_transformed = df_transformed.sort_values(by="total popular time", ascending=False)

                # Transformasi untuk tabel 'demografi'
                elif table == "demografi":
                    df_transformed = df.groupby(
                        ["Provinsi", "Kab/Kota", "Kecamatan"]
                    )["Jumlah_SES_A"].sum().reset_index()
                    df_transformed.rename(columns={"Jumlah_SES_A": "Total_SES_A"}, inplace=True)
                    df_transformed = df_transformed.sort_values(by="Total_SES_A", ascending=False)

                # Transformasi untuk tabel 'data_services'
                elif table == "data_services":
                    df_transformed = df[df["address"].notnull()]

                else:
                    raise ValueError(f"Transformasi untuk tabel {table} belum diimplementasikan")

                # Menyimpan hasil transformasi ke S3
                transformed_key = f"final_project/{table}_transformed.csv"
                buffer = StringIO()
                df_transformed.to_csv(buffer, index=False)
                s3_hook.load_string(
                    string_data=buffer.getvalue(),
                    key=transformed_key,
                    bucket_name=bucket_name,
                    replace=True
                )

                print(f"Data berhasil ditransformasi dan disimpan di S3://{bucket_name}/{transformed_key}")
            except Exception as e:
                print(f"Error during transformation for table {table}: {e}")
                raise


        @task(task_id="load_table")
        def load(table, schema, staging_folder):
            # Koneksi ke Redshift
            redshift_hook = RedshiftSQLHook("redshift_dibimbing").get_sqlalchemy_engine()

            # Mengambil data hasil transformasi dari S3 (format CSV)
            s3_hook = S3Hook(aws_conn_id="aws_default")
            bucket_name = 'beben'
            s3_key = f"final_project/{table}_transformed.csv"  # Sesuaikan dengan path file yang disimpan di S3

            # Mengambil file CSV dari S3
            file_obj = s3_hook.get_key(s3_key, bucket_name)
            df = pd.read_csv(file_obj.get()["Body"])

            # Memuat data ke Redshift
            with redshift_hook.connect() as conn:
                df.to_sql(
                    name      = table,
                    con       = conn,
                    index     = False,
                    schema    = schema,
                    if_exists = "replace",  # Pilihan ini bisa disesuaikan (misalnya, append atau replace)
                    chunksize = 100,  # Membatasi jumlah baris per insert untuk menghindari masalah performa
                    method    = "multi",  # Menggunakan metode multi untuk optimisasi batch insert
                )

            print(f"Data untuk {table} berhasil dimuat ke Redshift schema {schema}.")


        create_schema >> extract.override(task_id=f"extract_{table}")(table, staging_folder)  \
              >> transform.override(task_id=f"transform_{table}")(staging_folder, table) \
              >> load.override(task_id=f"load_{table}")(table, schema, staging_folder) \
              >> trigger_dag_datamart


    
final_project_potential_market()