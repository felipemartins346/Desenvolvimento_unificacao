import os
import pandas as pd
from sqlalchemy import create_engine, text

# 1) CONFIGURAÇÃO
SERVER   = r"localhost\MSSQLSERVER"
DATABASE = "master"
DRIVER   = "ODBC Driver 17 for SQL Server"
CONN_STR = (
    f"mssql+pyodbc://@{SERVER}/{DATABASE}"
    f"?driver={DRIVER.replace(' ', '+')}"
    "&trusted_connection=yes"
)

# 2) CAMINHO DO ARQUIVO EXCEL E TABELA DESTINO
# Atualize aqui com o nome real do seu arquivo
EXCEL_PATH = os.path.join("01.init", "cadastros.xlsx")
SHEET_NAME = "Sheet1"
TABLE_NAME = "dbo.SuaTabelaUnificada"

def main():
    # 3) Verifica existência do Excel
    if not os.path.exists(EXCEL_PATH):
        raise FileNotFoundError(f"Arquivo Excel não encontrado em: {EXCEL_PATH}")

    # 4) Lê o Excel
    df = pd.read_excel(EXCEL_PATH, sheet_name=SHEET_NAME)

    # 5) Conexão e carga
    engine = create_engine(CONN_STR, fast_executemany=True)
    with engine.begin() as conn:
        conn.execute(text(f"IF OBJECT_ID('{TABLE_NAME}', 'U') IS NOT NULL DROP TABLE {TABLE_NAME}"))
        df.to_sql(
            name=TABLE_NAME.split(".")[-1],
            schema=TABLE_NAME.split(".")[0],
            con=conn,
            index=False,
            if_exists="replace"
        )
        print(f"[OK] Tabela {TABLE_NAME} criada com {len(df)} registros.")

if __name__ == "__main__":
    main()
