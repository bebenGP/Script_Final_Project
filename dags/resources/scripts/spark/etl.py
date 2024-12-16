# filename: resources/scripts/spark/etl.py

import argparse
from pyspark.sql import SparkSession, functions as f

parser = argparse.ArgumentParser()
parser.add_argument("--table")
args = parser.parse_args()

spark = SparkSession.builder \
                   .master("spark://spark:7077") \
                   .appName("etl") \
                   .getOrCreate()

creds_psql = {
   "hostname": "bebengp.cfc428gck5mp.ap-southeast-3.rds.amazonaws.com",
   "port"    : 5432,
   "username": "postgres",
   "password": "Bonaparte15",
   "dbname"  : "postgres",
}

creds_rs = {
   "hostname": "beben-redshift.533267303425.ap-southeast-3.redshift-serverless.amazonaws.com",
   "port"    : 5439,
   "username": "admin",
   "password": "Bonaparte15",
   "dbname"  : "dev",
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



