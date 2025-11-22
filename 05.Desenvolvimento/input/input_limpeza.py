# Databricks notebook source
# ============================================================
# NOTEBOOK: Limpeza das pastas e Parquets do Volume Financeiro
# ============================================================

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

BASE_DIR = "/Volumes/dev_desenvolvimento/dev_volume/arquivos_financeiros/parquet"

print("📂 Diretório alvo:", BASE_DIR)
print("🔍 Listando conteúdos atuais...\n")

# Lista conteúdo atual
try:
    itens = dbutils.fs.ls(BASE_DIR)
    for i in itens:
        print(f"- {i.path} ({'DIR' if i.isDir() else 'FILE'})")
except Exception as e:
    print("❌ Erro ao acessar diretório:", e)


# COMMAND ----------

# ============================================================
# BLOCO 2: Identificar pastas que serão removidas
# ============================================================

print("\n📌 Selecionando apenas diretórios para limpeza...\n")

pastas_para_apagar = []

for item in dbutils.fs.ls(BASE_DIR):
    if item.isDir():
        pastas_para_apagar.append(item.path)

if not pastas_para_apagar:
    print("⚠️ Nenhuma pasta encontrada para apagar.")
else:
    print("🗂️ Pastas identificadas para remoção:")
    for p in pastas_para_apagar:
        print(" -", p)


# COMMAND ----------

# ============================================================
# BLOCO 3: Remover pastas e seus Parquets
# ============================================================

print("\n🧹 Iniciando limpeza...\n")

for pasta in pastas_para_apagar:
    try:
        dbutils.fs.rm(pasta, recurse=True)
        print(f"✅ Removido: {pasta}")
    except Exception as e:
        print(f"❌ Erro removendo {pasta}: {e}")


# COMMAND ----------

# ============================================================
# BLOCO 4: Validar que tudo foi realmente limpo
# ============================================================

print("\n🔍 Validando limpeza...\n")

_remaining = dbutils.fs.ls(BASE_DIR)

if len(_remaining) == 0:
    print("🎉 Tudo limpo! Diretório está vazio.")
else:
    print("⚠️ Ainda existem itens no diretório:")
    for i in _remaining:
        print(" -", i.path)

