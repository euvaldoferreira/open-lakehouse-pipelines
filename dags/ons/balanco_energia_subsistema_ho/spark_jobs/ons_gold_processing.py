"""
Spark Job: ONS Silver Iceberg → Gold Iceberg

Reads lakehouse.silver.ons_balanco_energia for a given year and computes
daily business metrics per subsystem, writing to lakehouse.gold.ons_balanco_diario.

Gold metrics (per subsystem per day):
  carga_max_mw, carga_min_mw, carga_media_mw  — load statistics
  carga_total_mwh                              — daily energy (MWmed × 1 h per record)
  gerhidraulica_media_mw                       — avg hydraulic generation
  gertermica_media_mw                          — avg thermal generation
  gereolica_media_mw                           — avg wind generation
  gersolar_media_mw                            — avg solar generation
  intercambio_medio_mw                         — avg interchange
  registros                                    — hourly records available
  pct_completude                               — % of expected 24 records present
"""

import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

parser = argparse.ArgumentParser()
parser.add_argument("--year", required=True, type=int, help="Target year YYYY")
args = parser.parse_args()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
EXPECTED_HOURS_PER_DAY = 24

spark = (
    SparkSession.builder
    .appName(f"ons-gold-balanco-diario-{args.year}")
    .getOrCreate()
)

df_silver = spark.sql(f"""
    SELECT id_subsistema, nom_subsistema, din_instante, data_referencia,
           val_carga, val_gerhidraulica, val_gertermica,
           val_gereolica, val_gersolar, val_intercambio
    FROM silver.ons_balanco_energia
    WHERE year(data_referencia) = {args.year}
""")

df_gold = (
    df_silver
    .groupBy("data_referencia", "id_subsistema", "nom_subsistema")
    .agg(
        F.round(F.max("val_carga"), 1).alias("carga_max_mw"),
        F.round(F.min("val_carga"), 1).alias("carga_min_mw"),
        F.round(F.avg("val_carga"), 1).alias("carga_media_mw"),
        # Each reading covers 1 hour → energy = MWmed × 1 h = MWh
        F.round(F.sum("val_carga"), 1).alias("carga_total_mwh"),
        F.round(F.avg("val_gerhidraulica"), 1).alias("gerhidraulica_media_mw"),
        F.round(F.avg("val_gertermica"), 1).alias("gertermica_media_mw"),
        F.round(F.avg("val_gereolica"), 1).alias("gereolica_media_mw"),
        F.round(F.avg("val_gersolar"), 1).alias("gersolar_media_mw"),
        F.round(F.avg("val_intercambio"), 1).alias("intercambio_medio_mw"),
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
    CREATE TABLE IF NOT EXISTS gold.ons_balanco_diario (
        data_referencia        DATE      NOT NULL,
        id_subsistema          STRING    NOT NULL,
        nom_subsistema         STRING,
        carga_max_mw           DOUBLE,
        carga_min_mw           DOUBLE,
        carga_media_mw         DOUBLE,
        carga_total_mwh        DOUBLE,
        gerhidraulica_media_mw DOUBLE,
        gertermica_media_mw    DOUBLE,
        gereolica_media_mw     DOUBLE,
        gersolar_media_mw      DOUBLE,
        intercambio_medio_mw   DOUBLE,
        registros              BIGINT,
        pct_completude         DOUBLE,
        processed_at           TIMESTAMP
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

df_gold.writeTo("gold.ons_balanco_diario").overwritePartitions()

row_count = df_gold.count()
print(f"Gold: wrote {row_count:,} subsystem-day records for year {args.year}")

spark.sql(f"""
    SELECT id_subsistema, nom_subsistema,
           carga_max_mw, carga_min_mw, carga_media_mw, carga_total_mwh,
           gereolica_media_mw, gersolar_media_mw, pct_completude
    FROM gold.ons_balanco_diario
    WHERE year(data_referencia) = {args.year}
      AND data_referencia = (SELECT MAX(data_referencia)
                             FROM gold.ons_balanco_diario
                             WHERE year(data_referencia) = {args.year})
    ORDER BY id_subsistema
""").show(truncate=False)

spark.stop()
