"""
Spark Job: ONS Bronze Parquet → Silver Iceberg (dados_hidrologicos_ho)

Reads the monthly bronze Parquet for dados_hidrologicos_ho,
deduplicates on (id_reservatorio, din_instante), and overwrites the month
partition in lakehouse.silver.ons_hidrologicos.

Silver table schema:
  id_subsistema                STRING
  nom_subsistema               STRING
  tip_reservatorio             STRING
  nom_bacia                    STRING
  id_reservatorio              STRING    NOT NULL
  nom_reservatorio             STRING
  cod_usina                    DOUBLE
  din_instante                 TIMESTAMP NOT NULL
  val_nivelmontante            DOUBLE    (m)
  val_niveljusante             DOUBLE    (m)
  val_volumeutil               DOUBLE    (%)
  val_vazaoafluente            DOUBLE    (m³/s)
  val_vazaodefluente           DOUBLE    (m³/s)
  val_vazaoturbinada           DOUBLE    (m³/s)
  val_vazaovertida             DOUBLE    (m³/s)
  val_vazaooutrasestruturas    DOUBLE    (m³/s)
  val_vazaotransferida         DOUBLE    (m³/s)
  val_vazaovertidanaoturbinavel DOUBLE   (m³/s)
  data_referencia              DATE      (partition)
  ingested_at                  TIMESTAMP
  processed_at                 TIMESTAMP
"""

import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

parser = argparse.ArgumentParser()
parser.add_argument("--yearmonth", required=True, help="Target year-month YYYY-MM")
args = parser.parse_args()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
DATASET = "dados_hidrologicos_ho"

spark = (
    SparkSession.builder
    .appName(f"ons-hidrologicos-silver-{args.yearmonth}")
    .getOrCreate()
)

bronze_path = f"s3a://bronze/ons/{DATASET}/yearmonth={args.yearmonth}/data.parquet"
df_bronze = spark.read.parquet(bronze_path)

numeric_cols = [
    "val_nivelmontante", "val_niveljusante", "val_volumeutil",
    "val_vazaoafluente", "val_vazaodefluente", "val_vazaoturbinada",
    "val_vazaovertida", "val_vazaooutrasestruturas",
    "val_vazaotransferida", "val_vazaovertidanaoturbinavel",
    "cod_usina",
]

df_silver = df_bronze.withColumn("din_instante", F.col("din_instante").cast("timestamp"))

for col in numeric_cols:
    if col in df_bronze.columns:
        df_silver = df_silver.withColumn(col, F.col(col).cast("double"))

df_silver = (
    df_silver
    .withColumn("data_referencia", F.to_date(F.col("din_instante")))
    .withColumn("processed_at", F.current_timestamp())
    .select(
        "id_subsistema",
        "nom_subsistema",
        "tip_reservatorio",
        "nom_bacia",
        "id_reservatorio",
        "nom_reservatorio",
        "cod_usina",
        "din_instante",
        "val_nivelmontante",
        "val_niveljusante",
        "val_volumeutil",
        "val_vazaoafluente",
        "val_vazaodefluente",
        "val_vazaoturbinada",
        "val_vazaovertida",
        "val_vazaooutrasestruturas",
        "val_vazaotransferida",
        "val_vazaovertidanaoturbinavel",
        "data_referencia",
        "ingested_at",
        "processed_at",
    )
    .dropDuplicates(["id_reservatorio", "din_instante"])
    .filter(F.col("id_reservatorio").isNotNull() & F.col("din_instante").isNotNull())
)

spark.sql("CREATE NAMESPACE IF NOT EXISTS silver.ons")
spark.sql("""
    CREATE TABLE IF NOT EXISTS silver.ons_hidrologicos (
        id_subsistema                 STRING,
        nom_subsistema                STRING,
        tip_reservatorio              STRING,
        nom_bacia                     STRING,
        id_reservatorio               STRING    NOT NULL,
        nom_reservatorio              STRING,
        cod_usina                     DOUBLE,
        din_instante                  TIMESTAMP NOT NULL,
        val_nivelmontante             DOUBLE,
        val_niveljusante              DOUBLE,
        val_volumeutil                DOUBLE,
        val_vazaoafluente             DOUBLE,
        val_vazaodefluente            DOUBLE,
        val_vazaoturbinada            DOUBLE,
        val_vazaovertida              DOUBLE,
        val_vazaooutrasestruturas     DOUBLE,
        val_vazaotransferida          DOUBLE,
        val_vazaovertidanaoturbinavel DOUBLE,
        data_referencia               DATE,
        ingested_at                   TIMESTAMP,
        processed_at                  TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (months(data_referencia))
    TBLPROPERTIES (
        'write.format.default'            = 'parquet',
        'write.parquet.compression-codec' = 'snappy',
        'write.metadata.delete-after-commit.enabled' = 'true',
        'write.metadata.previous-versions-max'       = '10'
    )
""")

df_silver.writeTo("silver.ons_hidrologicos").overwritePartitions()

row_count = df_silver.count()
print(f"Silver: wrote {row_count:,} rows for yearmonth {args.yearmonth}")

spark.sql(f"""
    SELECT id_reservatorio, nom_reservatorio, nom_subsistema,
           COUNT(*) AS registros,
           ROUND(AVG(val_volumeutil), 1) AS volume_medio_pct,
           ROUND(AVG(val_vazaoafluente), 1) AS vazao_afluente_media_m3s,
           ROUND(AVG(val_vazaoturbinada), 1) AS vazao_turbinada_media_m3s
    FROM silver.ons_hidrologicos
    WHERE month(data_referencia) = {int(args.yearmonth.split('-')[1])}
      AND year(data_referencia)  = {int(args.yearmonth.split('-')[0])}
    GROUP BY id_reservatorio, nom_reservatorio, nom_subsistema
    ORDER BY nom_subsistema, nom_reservatorio
""").show(50, truncate=False)

spark.stop()
