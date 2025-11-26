# Databricks notebook source
# MAGIC %pip install openpyxl
# MAGIC

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

# Bloco 1: Leitura da aba fRealizado

import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

EXCEL_PATH = "/Volumes/dev_desenvolvimento/dev_volume/arquivos_financeiros/excell/Dados Financeiros.xlsx"
SHEET_NAME = "fRealizado"

print(f"📄 Arquivo Excel: {EXCEL_PATH}")
print(f"📑 Aba alvo: {SHEET_NAME}")

# Lê a aba como está
df_freal_pd = pd.read_excel(EXCEL_PATH, sheet_name=SHEET_NAME)

# Para evitar problemas de conversão (Arrow), já convertemos tudo para string
df_freal_pd = df_freal_pd.astype(str)

print("✅ fRealizado lida via pandas (coerção -> string):")
print(df_freal_pd.shape)
display(df_freal_pd.head(5))

# Converte para Spark DataFrame
df_freal_spark_raw = spark.createDataFrame(df_freal_pd)

print("✅ DataFrame Spark (raw) criado com sucesso:")
df_freal_spark_raw.printSchema()
df_freal_spark_raw.show(5, truncate=False)


# COMMAND ----------

# Bloco 2: Normalização dos nomes das colunas de fRealizado

from pyspark.sql import DataFrame

rename_map_freal = {
    "ID_Conta":                      "id_conta",
    "ID_Contrato":                   "id_contrato",
    "ID_Item_Contrato":              "id_item_contrato",
    "Objeto do contrato":            "objeto_contrato",
    "ID Item Planejado":             "id_item_planejado",
    "ID_Orcamento /Ordem (índice)":  "id_orcamento",
    "FORNECEDOR":                    "fornecedor",
    "ID REF DO FORNECEDOR":          "id_ref_fornecedor",
    "NF":                            "nf",
    "TASK RC":                       "task_rc",
    "RC":                            "rc",
    "PO":                            "po",
    "FOLHA DE SERVIÇO":              "folha_servico",
    "PROPOSTA":                      "proposta",
    "PRODUTO":                       "produto",
    "É DESPESA ANTECIPADA":          "e_despesa_antecipada",
    "RATEIO":                        "rateio",
    "Data de Competência":           "data_competencia",
    "Valor pago":                    "valor_pago",
    "Data de Pagamento Real":        "data_pagamento_real",
    "Observação":                    "observacao"
}

df_freal_renamed = df_freal_spark_raw
for old_name, new_name in rename_map_freal.items():
    df_freal_renamed = df_freal_renamed.withColumnRenamed(old_name, new_name)

print("✅ Colunas renomeadas (fRealizado):")
print(df_freal_renamed.columns)
df_freal_renamed.printSchema()
df_freal_renamed.show(5, truncate=False)


# COMMAND ----------

# Bloco 3: Cast de todas as colunas para string (fRealizado)

from pyspark.sql import functions as F

df_freal_all_string = df_freal_renamed.select(
    [F.col(c).cast("string").alias(c) for c in df_freal_renamed.columns]
)

print("✅ Todas as colunas convertidas para string (fRealizado):")
df_freal_all_string.printSchema()
df_freal_all_string.show(5, truncate=False)


# COMMAND ----------

# Bloco 4: Escrita do fRealizado em Parquet

BASE_PARQUET_DIR = "/Volumes/dev_desenvolvimento/dev_volume/arquivos_financeiros/parquet"
TARGET_PATH_FREAL = f"{BASE_PARQUET_DIR}/fRealizado"

print(f"💾 Gravando fRealizado em Parquet em: {TARGET_PATH_FREAL}")

(
    df_freal_all_string
        .write
        .mode("overwrite")
        .parquet(TARGET_PATH_FREAL)
)

print("✅ Parquet de fRealizado gravado com sucesso.")

