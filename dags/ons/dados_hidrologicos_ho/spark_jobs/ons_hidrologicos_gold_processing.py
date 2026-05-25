"""
Spark Job: ONS Silver Iceberg → Gold Iceberg (dados_hidrologicos_ho)

Reads lakehouse.silver.ons_hidrologicos for a given month and computes
daily hydraulic metrics per reservoir, writing to gold.ons_hidrologicos_diario.

Gold metrics (per reservoir per day):
  vazaoafluente_media_m3s      — avg affluent flow (m³/s)
  vazaodefluente_media_m3s     — avg defluent/total outflow (m³/s)
  vazaoturbinada_media_m3s     — avg turbined flow (m³/s)
  vazaovertida_media_m3s       — avg spilled flow (m³/s)
  volumeutil_min_pct           — min useful volume (%)
  volumeutil_max_pct           — max useful volume (%)
  volumeutil_medio_pct         — avg useful volume (%)
  nivelmontante_min_m          — min upstream water level (m)
  nivelmontante_max_m          — max upstream water level (m)
  registros                    — hourly records available
  pct_completude               — % of expected 24 records present
"""

import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

parser = argparse.ArgumentParser()
parser.add_argument("--yearmonth", required=True, help="Target year-month YYYY-MM")
args = parser.parse_args()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
EXPECTED_HOURS_PER_DAY = 24

year, month = args.yearmonth.split("-")

spark = (
    SparkSession.builder
    .appName(f"ons-hidrologicos-gold-diario-{args.yearmonth}")
    .getOrCreate()
)

df_silver = spark.sql(f"""
    SELECT id_reservatorio, nom_reservatorio, id_subsistema, nom_subsistema,
           nom_bacia, tip_reservatorio, din_instante, data_referencia,
           val_vazaoafluente, val_vazaodefluente, val_vazaoturbinada,
           val_vazaovertida, val_volumeutil, val_nivelmontante
    FROM silver.ons_hidrologicos
    WHERE year(data_referencia)  = {year}
      AND month(data_referencia) = {month}
""")

df_gold = (
    df_silver
    .groupBy(
        "data_referencia", "id_reservatorio", "nom_reservatorio",
        "id_subsistema", "nom_subsistema", "nom_bacia", "tip_reservatorio",
    )
    .agg(
        F.round(F.avg("val_vazaoafluente"), 1).alias("vazaoafluente_media_m3s"),
        F.round(F.avg("val_vazaodefluente"), 1).alias("vazaodefluente_media_m3s"),
        F.round(F.avg("val_vazaoturbinada"), 1).alias("vazaoturbinada_media_m3s"),
        F.round(F.avg("val_vazaovertida"), 1).alias("vazaovertida_media_m3s"),
        F.round(F.min("val_volumeutil"), 2).alias("volumeutil_min_pct"),
        F.round(F.max("val_volumeutil"), 2).alias("volumeutil_max_pct"),
        F.round(F.avg("val_volumeutil"), 2).alias("volumeutil_medio_pct"),
        F.round(F.min("val_nivelmontante"), 2).alias("nivelmontante_min_m"),
        F.round(F.max("val_nivelmontante"), 2).alias("nivelmontante_max_m"),
        F.count("din_instante").alias("registros"),
    )
    .withColumn(
        "pct_completude",
        F.round(F.col("registros") / F.lit(EXPECTED_HOURS_PER_DAY) * 100, 1),
    )
    .withColumn("processed_at", F.current_timestamp())
)

spark.sql("CREATE NAMESPACE IF NOT EXISTS gold.ons")
spark.sql("""
    CREATE TABLE IF NOT EXISTS gold.ons_hidrologicos_diario (
        data_referencia          DATE      NOT NULL,
        id_reservatorio          STRING    NOT NULL,
        nom_reservatorio         STRING,
        id_subsistema            STRING,
        nom_subsistema           STRING,
        nom_bacia                STRING,
        tip_reservatorio         STRING,
        vazaoafluente_media_m3s  DOUBLE,
        vazaodefluente_media_m3s DOUBLE,
        vazaoturbinada_media_m3s DOUBLE,
        vazaovertida_media_m3s   DOUBLE,
        volumeutil_min_pct       DOUBLE,
        volumeutil_max_pct       DOUBLE,
        volumeutil_medio_pct     DOUBLE,
        nivelmontante_min_m      DOUBLE,
        nivelmontante_max_m      DOUBLE,
        registros                BIGINT,
        pct_completude           DOUBLE,
        processed_at             TIMESTAMP
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

df_gold.writeTo("gold.ons_hidrologicos_diario").overwritePartitions()

row_count = df_gold.count()
print(f"Gold: wrote {row_count:,} reservoir-day records for yearmonth {args.yearmonth}")

spark.sql(f"""
    SELECT id_reservatorio, nom_reservatorio, nom_subsistema,
           vazaoafluente_media_m3s, vazaoturbinada_media_m3s,
           volumeutil_min_pct, volumeutil_max_pct, pct_completude
    FROM gold.ons_hidrologicos_diario
    WHERE year(data_referencia)  = {year}
      AND month(data_referencia) = {month}
      AND data_referencia = (
          SELECT MAX(data_referencia)
          FROM gold.ons_hidrologicos_diario
          WHERE year(data_referencia) = {year} AND month(data_referencia) = {month}
      )
    ORDER BY nom_subsistema, nom_reservatorio
""").show(50, truncate=False)

spark.stop()
