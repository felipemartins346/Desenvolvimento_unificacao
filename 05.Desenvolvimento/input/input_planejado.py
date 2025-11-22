# Databricks notebook source
# MAGIC %pip install openpyxl
# MAGIC

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

# Bloco 1: Leitura da aba fPLanejado (com coerção segura para string)

import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

EXCEL_PATH = "/Volumes/dev_desenvolvimento/dev_volume/arquivos_financeiros/excell/Dados Financeiros.xlsx"
SHEET_NAME = "fPLanejado"

print(f"📄 Arquivo Excel: {EXCEL_PATH}")
print(f"📑 Aba alvo: {SHEET_NAME}")

# Lê a aba como está
df_fplan_pd = pd.read_excel(EXCEL_PATH, sheet_name=SHEET_NAME)

# *** Correção crítica ***
# Converte todas as colunas para string ANTES de passar para Spark
df_fplan_pd = df_fplan_pd.astype(str)

print("✅ fPLanejado lida via pandas (coerção -> string):")
print(df_fplan_pd.shape)
display(df_fplan_pd.head(5))

# Agora converte para Spark DataFrame sem risco de Arrow
df_fplan_spark_raw = spark.createDataFrame(df_fplan_pd)

print("✅ DataFrame Spark (raw) criado com sucesso:")
df_fplan_spark_raw.printSchema()
df_fplan_spark_raw.show(5, truncate=False)


# COMMAND ----------

# Bloco 2: Normalização dos nomes das colunas de fPLanejado

from pyspark.sql import DataFrame

rename_map_fplan = {
    "ID Fornecedor":                 "id_fornecedor",
    "FORNECEDOR":                    "fornecedor",
    "ID Item Planejado":             "id_item_planejado",
    "Item Planejado":                "item_planejado",
    "ID_Orcamento /Ordem (índice)":  "id_orcamento",
    "Data_Competência":              "data_competencia",
    "Data_faturamento":              "data_faturamento",
    "Preço":                         "preco",
    "Observação":                    "observacao",
    "É DESPESA ANTECIPADA":          "e_despesa_antecipada",
    "RATEIO":                        "rateio",
    "PROPOSTA":                      "proposta",
    "PRODUTO":                       "produto",
    "PO":                            "po"
}

df_fplan_renamed = df_fplan_spark_raw
for old_name, new_name in rename_map_fplan.items():
    df_fplan_renamed = df_fplan_renamed.withColumnRenamed(old_name, new_name)

print("✅ Colunas renomeadas:")
print(df_fplan_renamed.columns)
df_fplan_renamed.printSchema()
df_fplan_renamed.show(5, truncate=False)


# COMMAND ----------

# Bloco 3: Cast de todas as colunas para string

from pyspark.sql import functions as F

df_fplan_all_string = df_fplan_renamed.select(
    [F.col(c).cast("string").alias(c) for c in df_fplan_renamed.columns]
)

print("✅ Todas as colunas convertidas para string:")
df_fplan_all_string.printSchema()
df_fplan_all_string.show(5, truncate=False)


# COMMAND ----------

# Bloco 4: Escrita do fPLanejado em Parquet

BASE_PARQUET_DIR = "/Volumes/dev_desenvolvimento/dev_volume/arquivos_financeiros/parquet"
TARGET_PATH_FPLAN = f"{BASE_PARQUET_DIR}/fPLanejado"

print(f"💾 Gravando fPLanejado em Parquet em: {TARGET_PATH_FPLAN}")

(
    df_fplan_all_string
        .write
        .mode("overwrite")
        .parquet(TARGET_PATH_FPLAN)
)

print("✅ Parquet de fPLanejado gravado com sucesso.")

