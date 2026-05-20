"""
Spark Job: Bronze -> Silver
Reads Parquet from bronze bucket, aggregates by customer, writes Iceberg silver table.
"""

import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

parser = argparse.ArgumentParser()
parser.add_argument("--date", required=True)
args = parser.parse_args()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_USER = os.getenv("MINIO_ROOT_USER", "minio_admin")
MINIO_PASS = os.getenv("MINIO_ROOT_PASSWORD", "minio_secure_pass_2024")

spark = (
    SparkSession.builder
    .appName(f"silver-transform-{args.date}")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lakehouse.type", "hadoop")
    .config("spark.sql.catalog.lakehouse.warehouse", "s3a://gold/warehouse")
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_USER)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASS)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

year, month, day = args.date[:4], args.date[5:7], args.date[8:]
bronze_path = f"s3a://bronze/sales/year={year}/month={month}/day={day}/orders.parquet"

df_bronze = spark.read.parquet(bronze_path)

df_silver = (
    df_bronze
    .withColumn("order_date", F.to_date(F.col("partition_date")))
    .groupBy("customer_id", "order_date")
    .agg(
        F.count("id").alias("order_count"),
        F.sum("total_price").alias("total_revenue"),
        F.avg("total_price").alias("avg_order_value"),
        F.sum("quantity").alias("total_items"),
    )
    .withColumn("processed_at", F.current_timestamp())
)

spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")
spark.sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.silver.customer_daily_orders (
        customer_id STRING,
        order_date DATE,
        order_count BIGINT,
        total_revenue DOUBLE,
        avg_order_value DOUBLE,
        total_items BIGINT,
        processed_at TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (order_date)
""")

df_silver.writeTo("lakehouse.silver.customer_daily_orders").overwritePartitions()

print(f"Silver: wrote {df_silver.count()} rows for {args.date}")
spark.stop()
