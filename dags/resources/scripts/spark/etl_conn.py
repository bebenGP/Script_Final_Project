# filename: resources/scripts/spark/etl_conn.py

import argparse
from pyspark.sql import SparkSession, functions as f

parser = argparse.ArgumentParser()
parser.add_argument("--table")
parser.add_argument("--psql_host")
parser.add_argument("--psql_port")
parser.add_argument("--psql_login")
parser.add_argument("--psql_password")
parser.add_argument("--psql_schema")
parser.add_argument("--rs_host")
parser.add_argument("--rs_port")
parser.add_argument("--rs_login")
parser.add_argument("--rs_password")
parser.add_argument("--rs_schema")
args = parser.parse_args()

spark = SparkSession.builder \
                   .master("spark://spark:7077") \
                   .appName("etl") \
                   .getOrCreate()

creds_psql = {
   "hostname": args.psql_host,
   "port"    : args.psql_port,
   "username": args.psql_login,
   "password": args.psql_password,
   "dbname"  : args.psql_schema,
}

creds_rs = {
   "hostname": args.rs_host,
   "port"    : args.rs_port,
   "username": args.rs_login,
   "password": args.rs_password,
   "dbname"  : args.rs_schema,
}


df = spark.read \
          .format("jdbc") \
          .option("driver","org.postgresql.Driver") \
          .option("url", f"jdbc:postgresql://{creds_psql['hostname']}:{creds_psql['port']}/{creds_psql['dbname']}") \
          .option("user", creds_psql["username"]) \
          .option("password", creds_psql["password"]) \
          .option("dbtable", args.table) \
          .load()

df = df.withColumn("extracted_at", f.now())
df = df.drop("geometry")

df.printSchema()

df.repartition(4).write \
   .format("jdbc") \
   .option("driver", "com.amazon.redshift.jdbc42.Driver") \
   .option("url", f"jdbc:redshift://{creds_rs['hostname']}:{creds_rs['port']}/{creds_rs['dbname']}") \
   .option("dbtable", f"bronze.{args.table}") \
   .option("user", creds_rs["username"]) \
   .option("password", creds_rs["password"]) \
   .option("batchsize", 25) \
   .mode("overwrite") \
   .save()

spark.stop()


