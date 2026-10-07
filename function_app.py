host  = sv-univille-ca.database.windows.net
database = db-univille


import logging
import azure.functions as func
import os

#importar a biblioteca de banco de dados
import pyodbc

app = func.FunctionApp()

@app.timer_trigger(schedule="0 * * * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False) 
def extract_chamado(myTimer: func.TimerRequest) -> None:

    #capturar variaveis de ambiente
    host_sql = os.getenv("HOST")
    database_sql = os.getenv("DATABASE")
    user_sql = os.getenv("USER")
    pass_sql = os.getenv("PASSWORD")

    #como montar uma string de conexao com PYODBC  AZURE DATABASE SQL
    conn_str_source = (
            "DRIVER={ODBC Driver 18 for SQL Server};"
            f"SERVER={host_sql};"
            f"DATABASE={database_sql};"
            f"UID={user_sql};"
            f"PWD={pass_sql};"
            "Encrypt=yes;"
            "TrustServerCertificate=no;"
            "Connection Timeout=30;"
        )

    # Abrir a conexão 
    # fazer uma select 
    # imprimir usando logging


