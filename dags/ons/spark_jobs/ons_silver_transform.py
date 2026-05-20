"""
Spark Job: ONS Bronze Parquet → Silver Iceberg

Reads the annual bronze Parquet for balanco_energia_subsistema_ho,
deduplicates on (id_subsistema, din_instante), and overwrites the year
partition in lakehouse.silver.ons_balanco_energia.

Silver table schema:
  id_subsistema       STRING
  nom_subsistema      STRING
  din_instante        TIMESTAMP
  val_carga           DOUBLE
  val_gerhidraulica   DOUBLE
  val_gertermica      DOUBLE
  val_gereolica       DOUBLE
  val_gersolar        DOUBLE
  val_intercambio     DOUBLE
  data_referencia     DATE      (partition)
  ingested_at         TIMESTAMP
  processed_at        TIMESTAMP
"""

import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

parser = argparse.ArgumentParser()
parser.add_argument("--year", required=True, type=int, help="Target year YYYY")
args = parser.parse_args()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
DATASET = "balanco_energia_subsistema_ho"

spark = (
    SparkSession.builder
    .appName(f"ons-silver-balanco-energia-{args.year}")
    .getOrCreate()
)

bronze_path = f"s3a://bronze/ons/{DATASET}/year={args.year}/data.parquet"
df_bronze = spark.read.parquet(bronze_path)

df_silver = (
    df_bronze
    .withColumn("din_instante", F.col("din_instante").cast("timestamp"))
    .withColumn("val_carga", F.col("val_carga").cast("double"))
    .withColumn("val_gerhidraulica", F.col("val_gerhidraulica").cast("double"))
    .withColumn("val_gertermica", F.col("val_gertermica").cast("double"))
    .withColumn("val_gereolica", F.col("val_gereolica").cast("double"))
    .withColumn("val_gersolar", F.col("val_gersolar").cast("double"))
    .withColumn("val_intercambio", F.col("val_intercambio").cast("double"))
    .withColumn("data_referencia", F.to_date(F.col("din_instante")))
    .withColumn("processed_at", F.current_timestamp())
    .select(
        "id_subsistema",
        "nom_subsistema",
        "din_instante",
        "val_carga",
        "val_gerhidraulica",
        "val_gertermica",
        "val_gereolica",
        "val_gersolar",
        "val_intercambio",
        "data_referencia",
        "ingested_at",
        "processed_at",
    )
    .dropDuplicates(["id_subsistema", "din_instante"])
    .filter(F.col("id_subsistema").isNotNull() & F.col("din_instante").isNotNull())
)

spark.sql("CREATE NAMESPACE IF NOT EXISTS silver.ons")
spark.sql("""
    CREATE TABLE IF NOT EXISTS silver.ons_balanco_energia (
        id_subsistema     STRING    NOT NULL,
        nom_subsistema    STRING,
        din_instante      TIMESTAMP NOT NULL,
        val_carga         DOUBLE,
        val_gerhidraulica DOUBLE,
        val_gertermica    DOUBLE,
        val_gereolica     DOUBLE,
        val_gersolar      DOUBLE,
        val_intercambio   DOUBLE,
        data_referencia   DATE,
        ingested_at       TIMESTAMP,
        processed_at      TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (years(data_referencia))
    TBLPROPERTIES (
        'write.format.default'            = 'parquet',
        'write.parquet.compression-codec' = 'snappy',
        'write.metadata.delete-after-commit.enabled' = 'true',
        'write.metadata.previous-versions-max'       = '10'
    )
""")

df_silver.writeTo("silver.ons_balanco_energia").overwritePartitions()

row_count = df_silver.count()
print(f"Silver: wrote {row_count:,} rows for year {args.year}")

spark.sql(f"""
    SELECT id_subsistema, COUNT(*) AS registros,
           ROUND(AVG(val_carga), 1) AS media_carga_mw,
           ROUND(AVG(val_gereolica), 1) AS media_eolica_mw,
           ROUND(AVG(val_gersolar), 1) AS media_solar_mw
    FROM silver.ons_balanco_energia
    WHERE year(data_referencia) = {args.year}
    GROUP BY id_subsistema
    ORDER BY id_subsistema
""").show(truncate=False)

spark.stop()
