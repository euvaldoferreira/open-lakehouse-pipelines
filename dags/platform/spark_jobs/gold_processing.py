"""
Spark Job: Bronze -> Gold Iceberg
Reads bronze data, computes product-level revenue metrics, writes to gold Iceberg table.
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
    .appName(f"gold-processing-{args.date}")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lakehouse.type", "hadoop")
    .config("spark.sql.catalog.lakehouse.warehouse", "s3a://gold/warehouse")
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_USER)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASS)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

year, month, day = args.date[:4], args.date[5:7], args.date[8:]
bronze_path = f"s3a://bronze/sales/year={year}/month={month}/day={day}/orders.parquet"

df = spark.read.parquet(bronze_path)

df_gold = (
    df
    .withColumn("order_date", F.to_date(F.col("partition_date")))
    .groupBy("product_id", "order_date")
    .agg(
        F.count("id").alias("total_orders"),
        F.sum("quantity").alias("units_sold"),
        F.sum("total_price").alias("gross_revenue"),
        F.avg("unit_price").alias("avg_unit_price"),
        F.countDistinct("customer_id").alias("unique_customers"),
    )
    .withColumn("revenue_per_unit", F.round(F.col("gross_revenue") / F.col("units_sold"), 4))
    .withColumn("processed_at", F.current_timestamp())
)

spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")
spark.sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.gold.product_daily_metrics (
        product_id STRING,
        order_date DATE,
        total_orders BIGINT,
        units_sold BIGINT,
        gross_revenue DOUBLE,
        avg_unit_price DOUBLE,
        unique_customers BIGINT,
        revenue_per_unit DOUBLE,
        processed_at TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (order_date)
    TBLPROPERTIES (
        'write.format.default' = 'parquet',
        'write.parquet.compression-codec' = 'snappy'
    )
""")

df_gold.writeTo("lakehouse.gold.product_daily_metrics").overwritePartitions()

count = df_gold.count()
print(f"Gold: wrote {count} product-day records for {args.date}")

spark.sql(f"""
    SELECT product_id, gross_revenue, units_sold
    FROM lakehouse.gold.product_daily_metrics
    WHERE order_date = '{args.date}'
    ORDER BY gross_revenue DESC
    LIMIT 5
""").show()

spark.stop()
