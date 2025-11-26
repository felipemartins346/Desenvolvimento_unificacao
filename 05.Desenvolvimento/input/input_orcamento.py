# Databricks notebook source
# MAGIC %pip install openpyxl
# MAGIC

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

# Bloco 1: Leitura da aba dOrcamento

import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

EXCEL_PATH = "/Volumes/dev_desenvolvimento/dev_volume/arquivos_financeiros/excell/Dados Financeiros.xlsx"
SHEET_NAME = "dOrcamento"

print(f"📄 Arquivo Excel: {EXCEL_PATH}")
print(f"📑 Aba alvo: {SHEET_NAME}")

# Lê somente a aba dOrcamento como está
df_orc_pd = pd.read_excel(
    EXCEL_PATH,
    sheet_name=SHEET_NAME
)

print("✅ dOrcamento lida via pandas:")
print(df_orc_pd.shape)
display(df_orc_pd.head(5))

# Converte para Spark DataFrame mantendo schema original
df_orc_spark_raw = spark.createDataFrame(df_orc_pd)

print("✅ DataFrame Spark (raw) criado com schema original:")
df_orc_spark_raw.printSchema()
df_orc_spark_raw.show(5, truncate=False)


# COMMAND ----------

# Bloco 2: Normalização dos nomes das colunas

from pyspark.sql import DataFrame

# Mapeamento explícito: coluna original -> coluna normalizada
rename_map = {
    "ID_Orcamento /Ordem (índice)": "id_orcamento",
    "Data (use sempre dia 1)":      "data_inicio",
    "Data fim":                     "data_fim",
    "Data_faturamento":             "data_faturamento",
    "Serviço":                      "servico",
    "Descrição":                    "descricao",
    "Responsável":                  "responsavel",
    "Solicitado por":               "solicitado_por",
    "Valor_Orcado":                 "valor_orcado",
    "ID_Conta":                     "id_conta",
    "Status":                       "status",
    "Obs":                          "obs"
}

# Aplica os renames no DataFrame Spark
df_orc_renamed = df_orc_spark_raw
for old_name, new_name in rename_map.items():
    df_orc_renamed = df_orc_renamed.withColumnRenamed(old_name, new_name)

print("✅ Colunas renomeadas:")
print(df_orc_renamed.columns)
df_orc_renamed.printSchema()
df_orc_renamed.show(5, truncate=False)


# COMMAND ----------

# Bloco 3: Cast de todas as colunas para string

from pyspark.sql import functions as F

df_orc_all_string = df_orc_renamed.select(
    [F.col(c).cast("string").alias(c) for c in df_orc_renamed.columns]
)

print("✅ Todas as colunas convertidas para string:")
df_orc_all_string.printSchema()
df_orc_all_string.show(5, truncate=False)


# COMMAND ----------

# Bloco 4: Escrita em Parquet no Volume

BASE_PARQUET_DIR = "/Volumes/dev_desenvolvimento/dev_volume/arquivos_financeiros/parquet"
TARGET_PATH = f"{BASE_PARQUET_DIR}/dOrcamento"

print(f"💾 Gravando dOrcamento em Parquet em: {TARGET_PATH}")

(
    df_orc_all_string
        .write
        .mode("overwrite")   # sobrescreve a partição da dOrcamento se já existir
        .parquet(TARGET_PATH)
)

print("✅ Parquet da dOrcamento gravado com sucesso.")

