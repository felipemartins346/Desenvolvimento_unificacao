# Databricks notebook source
# MAGIC %pip install openpyxl
# MAGIC

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

# Bloco 1: Leitura da aba PO

import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

EXCEL_PATH = "/Volumes/dev_desenvolvimento/dev_volume/arquivos_financeiros/excell/Dados Financeiros.xlsx"
SHEET_NAME = "PO"

print(f"📄 Arquivo Excel: {EXCEL_PATH}")
print(f"📑 Aba alvo: {SHEET_NAME}")

# Lê a aba PO como está
df_po_pd = pd.read_excel(EXCEL_PATH, sheet_name=SHEET_NAME)

# Corrigir: converter tudo para string para evitar erros de Arrow
df_po_pd = df_po_pd.astype(str)

print("✅ PO lida via pandas:")
print(df_po_pd.shape)
display(df_po_pd.head(5))

# Converte para Spark DataFrame
df_po_spark_raw = spark.createDataFrame(df_po_pd)

print("✅ DataFrame Spark (raw) criado:")
df_po_spark_raw.printSchema()
df_po_spark_raw.show(5, truncate=False)


# COMMAND ----------

# Bloco 2: Normalização dos nomes das colunas da PO

rename_map_po = {
    "PO":                 "po",
    "RC":                 "rc",
    "FOLHA DE SERVIÇO":   "folha_servico",
    "VALOR PO":           "valor_po"
}

df_po_renamed = df_po_spark_raw
for old_name, new_name in rename_map_po.items():
    df_po_renamed = df_po_renamed.withColumnRenamed(old_name, new_name)

print("✅ Colunas renomeadas (PO):")
print(df_po_renamed.columns)
df_po_renamed.printSchema()
df_po_renamed.show(5, truncate=False)


# COMMAND ----------

# Bloco 3: Cast de todas as colunas para string (PO)

from pyspark.sql import functions as F

df_po_all_string = df_po_renamed.select(
    [F.col(c).cast("string").alias(c) for c in df_po_renamed.columns]
)

print("✅ Todas as colunas convertidas para string (PO):")
df_po_all_string.printSchema()
df_po_all_string.show(5, truncate=False)


# COMMAND ----------

# Bloco 4: Escrita da PO em Parquet

BASE_PARQUET_DIR = "/Volumes/dev_desenvolvimento/dev_volume/arquivos_financeiros/parquet"
TARGET_PATH_PO = f"{BASE_PARQUET_DIR}/PO"

print(f"💾 Gravando PO em Parquet em: {TARGET_PATH_PO}")

(
    df_po_all_string
        .write
        .mode("overwrite")
        .parquet(TARGET_PATH_PO)
)

print("✅ Parquet da PO gravado com sucesso.")

