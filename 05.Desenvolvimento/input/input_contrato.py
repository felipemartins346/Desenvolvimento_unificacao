# Databricks notebook source
# MAGIC %pip install openpyxl
# MAGIC

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

# Bloco 2: Normalização dos nomes das colunas de dContratos

rename_map_dcont = {
    "Unnamed: 0":  "col_00",
    "Unnamed: 1":  "col_01",
    "Unnamed: 2":  "col_02",
    "Unnamed: 3":  "col_03",
    "Unnamed: 4":  "col_04",
    "Unnamed: 5":  "col_05",
    "Unnamed: 6":  "col_06",
    "Unnamed: 7":  "col_07",
    "Unnamed: 8":  "col_08",
    "Unnamed: 9":  "col_09",
    "Unnamed: 10": "col_10",
    "Unnamed: 11": "col_11",

    "relacionado com a tabela fPlanejado": "relacionado_fplanejado",

    "Unnamed: 13": "col_13",
    "Unnamed: 14": "col_14",
    "Unnamed: 15": "col_15",
    "Unnamed: 16": "col_16",
    "Unnamed: 17": "col_17",
    "Unnamed: 18": "col_18",
    "Unnamed: 19": "col_19",

    "Percentual cobrado para pagto de imposto pelo fornecedor": "perc_imposto_fornecedor",
    "Fórmula = Valor_total_sem_imposto / Valor_perc_cobrado_fornec": "formula_base_calculo",

    "Unnamed: 22": "col_22",

    "Valor_cobrado_fornecedor*Perc_tributos": "valor_cobrado_fornecedor_perc_tributos",

    "Unnamed: 24": "col_24",
    "Unnamed: 25": "col_25",
    "Unnamed: 26": "col_26",
    "Unnamed: 27": "col_27",
    "Unnamed: 28": "col_28"
}

df_dcont_renamed = df_dcont_spark_raw
for old_name, new_name in rename_map_dcont.items():
    if old_name in df_dcont_renamed.columns:
        df_dcont_renamed = df_dcont_renamed.withColumnRenamed(old_name, new_name)

print("✅ Colunas renomeadas (dContratos):")
print(df_dcont_renamed.columns)
df_dcont_renamed.printSchema()
df_dcont_renamed.show(5, truncate=False)


# COMMAND ----------

# Bloco 3: Cast de todas as colunas para string (dContratos)

from pyspark.sql import functions as F

df_dcont_all_string = df_dcont_renamed.select(
    [F.col(c).cast("string").alias(c) for c in df_dcont_renamed.columns]
)

print("✅ Todas as colunas convertidas para string (dContratos):")
df_dcont_all_string.printSchema()
df_dcont_all_string.show(5, truncate=False)


# COMMAND ----------

# Bloco 4: Escrita da dContratos em Parquet

BASE_PARQUET_DIR = "/Volumes/dev_desenvolvimento/dev_volume/arquivos_financeiros/parquet"
TARGET_PATH_DCONT = f"{BASE_PARQUET_DIR}/dContratos"

print(f"💾 Gravando dContratos em Parquet em: {TARGET_PATH_DCONT}")

(
    df_dcont_all_string
        .write
        .mode("overwrite")
        .parquet(TARGET_PATH_DCONT)
)

print("✅ Parquet da dContratos gravado com sucesso.")


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

