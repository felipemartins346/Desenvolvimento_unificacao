# Databricks notebook source
# MAGIC %pip install openpyxl
# MAGIC

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

# Bloco 1: Leitura da aba Fornecedores

import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

EXCEL_PATH = "/Volumes/dev_desenvolvimento/dev_volume/arquivos_financeiros/excell/Dados Financeiros.xlsx"
SHEET_NAME = "Fornecedores"  # conforme lista de abas

print(f"📄 Arquivo Excel: {EXCEL_PATH}")
print(f"📑 Aba alvo: {SHEET_NAME}")

# Leitura via pandas, tudo como string
df_forn_pd = pd.read_excel(
    EXCEL_PATH,
    sheet_name=SHEET_NAME,
    dtype=str
)

print("📊 Shape da Fornecedores:", df_forn_pd.shape)
display(df_forn_pd.head(5))

if df_forn_pd.empty:
    raise Exception("❌ A aba Fornecedores está vazia. Verifique o arquivo Excel.")


# COMMAND ----------

# Bloco 2: Normalização dos nomes das colunas de Fornecedores

rename_map_forn = {
    "ID Fornecedor":   "id_fornecedor",
    "FORNECEDOR":      "fornecedor",
    "CNPJ_Fornecedor": "cnpj_fornecedor",
    "Endereço":        "endereco",
    "Cidade":          "cidade",
    "Estado":          "estado"
}

df_forn_renamed_pd = df_forn_pd.rename(columns=rename_map_forn)

print("📑 Colunas após rename (Fornecedores):")
print(list(df_forn_renamed_pd.columns))
display(df_forn_renamed_pd.head(5))


# COMMAND ----------

# Bloco 3: Conversão para Spark e cast para string

from pyspark.sql import functions as F

# garante tudo como string antes de ir pro Spark
df_forn_renamed_pd = df_forn_renamed_pd.astype(str)

df_forn_spark = spark.createDataFrame(df_forn_renamed_pd)

df_forn_all_string = df_forn_spark.select(
    [F.col(c).cast("string").alias(c) for c in df_forn_spark.columns]
)

print("📘 Schema final de Fornecedores (tudo string):")
df_forn_all_string.printSchema()
df_forn_all_string.show(5, truncate=False)


# COMMAND ----------

# Bloco 4: Escrita em Parquet - Fornecedores

BASE_PARQUET_DIR = "/Volumes/dev_desenvolvimento/dev_volume/arquivos_financeiros/parquet"
TARGET_PATH = f"{BASE_PARQUET_DIR}/Fornecedores"

print(f"💾 Gravando Fornecedores em: {TARGET_PATH}")

(
    df_forn_all_string
        .write
        .mode("overwrite")
        .parquet(TARGET_PATH)
)

print("✅ Parquet de Fornecedores gravado com sucesso.")

