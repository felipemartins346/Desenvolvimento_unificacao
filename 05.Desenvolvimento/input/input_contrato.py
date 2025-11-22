# Databricks notebook source
# MAGIC %pip install openpyxl
# MAGIC

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

# Bloco 1: Leitura da aba dContratos

import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

EXCEL_PATH = "/Volumes/dev_desenvolvimento/dev_volume/arquivos_financeiros/excell/Dados Financeiros.xlsx"
SHEET_NAME = "dContratos "   # <-- ATENÇÃO: existe espaço no final da aba dentro do Excel

print(f"📄 Arquivo Excel: {EXCEL_PATH}")
print(f"📑 Aba alvo: {SHEET_NAME!r}")  # usa !r para mostrar o espaço

df_ct_pd = pd.read_excel(
    EXCEL_PATH,
    sheet_name=SHEET_NAME,
    dtype=str
)

print("📊 Shape da dContratos:", df_ct_pd.shape)
display(df_ct_pd.head(5))

if df_ct_pd.empty:
    raise Exception("❌ A aba dContratos está vazia. Verifique o arquivo Excel.")


# COMMAND ----------

# Bloco 2: Normalização dos nomes das colunas

rename_map_ct = {
    "ID_contrato_elaw": "id_contrato_elaw",
    "fornecedor": "fornecedor",
    "CNPJ_Fornecedor": "cnpj_fornecedor",
    "Contratante": "contratante",
    "CNPJ_Contratante": "cnpj_contratante",
    "PROPOSTA": "proposta",
    "Data inicio_contrato": "data_inicio_contrato",
    "Data fim_contrato": "data_fim_contrato",
    "Indice_Reajuste": "indice_reajuste",
    "ID_Item_Contrato": "id_item_contrato",
    "Item do contrato": "item_contrato",
    "PRODUTO": "produto",
    "ID Item Planejado": "id_item_planejado",
    "Data inicio_item": "data_inicio_item",
    "Data fim_item": "data_fim_item",
    "Tempo_item_contrato": "tempo_item_contrato",
    "Qtd_item": "qtd_item",
    "unid_medida": "unidade_medida",
    "Preco_unit": "preco_unit",
    "Valor_total_sem_imposto": "valor_total_sem_imposto",
    "Valor_perc_cobrado_fornec": "valor_perc_cobrado_fornec",
    "Valor_base_calculo_imposto": "valor_base_calculo_imposto",
    "Perc_tributos": "perc_tributos",
    "Valor_tributos": "valor_tributos",
    "Valor_total_bruto_item": "valor_total_bruto_item",
    "tipo de cobrança": "tipo_cobranca",
    "Valor consumido": "valor_consumido",
    "ID_Orcamento /Ordem (índice)": "id_orcamento",
    "Observação": "observacao"
}

df_ct_renamed = df_ct_pd.rename(columns=rename_map_ct)

print("📑 Colunas após rename:")
print(list(df_ct_renamed.columns))
display(df_ct_renamed.head(5))


# COMMAND ----------

# Bloco 3: Conversão para Spark + cast para string

df_ct_spark = spark.createDataFrame(df_ct_renamed.astype(str))

from pyspark.sql import functions as F

df_ct_all_string = df_ct_spark.select(
    [F.col(c).cast("string").alias(c) for c in df_ct_spark.columns]
)

print("📘 Schema final (tudo string):")
df_ct_all_string.printSchema()
df_ct_all_string.show(5, truncate=False)


# COMMAND ----------

# Bloco 4: Escrita em Parquet

BASE_PARQUET_DIR = "/Volumes/dev_desenvolvimento/dev_volume/arquivos_financeiros/parquet"
TARGET_PATH = f"{BASE_PARQUET_DIR}/dContratos"

print(f"💾 Gravando dContratos em: {TARGET_PATH}")

(
    df_ct_all_string
        .write
        .mode("overwrite")
        .parquet(TARGET_PATH)
)

print("✅ Parquet da dContratos gravado com sucesso.")

