import csv
import io
import logging
import os
from datetime import datetime, timezone

import pyodbc
import azure.functions as func
from azure.storage.filedatalake import DataLakeServiceClient

app = func.FunctionApp()

# Horários (UTC) escalonados para respeitar as integridades (pai antes do filho):
#   03:00  fila, categoria, cliente_organizacao, sla
#   03:10  analista, solicitante
#   03:20  chamado
#   03:30  chamado_sla, chamado_status_historico, csat_avaliacao
# 03:00 UTC = 00:00 em Brasília

@app.timer_trigger(schedule="0 0 3 * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False)
def extract_fila(myTimer: func.TimerRequest) -> None:
    logging.info('tabela fila')


    # Banco de origem
    sql_server = os.getenv("HOST")
    sql_database = os.getenv("DATABASE")
    sql_user = os.getenv("USER")
    sql_pass = os.getenv("PASSWORD")

    # Banco de destino
    dest_server = os.getenv("DEST_HOST")
    dest_database = os.getenv("DEST_DATABASE")
    dest_user = os.getenv("DEST_USER")
    dest_pass = os.getenv("DEST_PASSWORD")


    # Strings de conexão
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={sql_server};"
        f"DATABASE={sql_database};"
        f"UID={sql_user};"
        f"PWD={sql_pass};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    dest_conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={dest_server};"
        f"DATABASE={dest_database};"
        f"UID={dest_user};"
        f"PWD={dest_pass};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    try:
    
        # Leitura da tabela no banco de origem
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()

            query = "select * from itsm.fila"
            cursor.execute(query)

            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]

            logging.info(f'{len(rows)} linhas lidas de itsm.fila')

    
        # Gravação do CSV no contêiner bronze do Data Lake
    
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(columns)
        writer.writerows(rows)

        # Caminho particionado por data
        now = datetime.now(timezone.utc)
        file_path = f"fila/{now:%Y/%m/%d}/fila_{now:%Y%m%d_%H%M%S}.csv"

        datalake = DataLakeServiceClient.from_connection_string(os.getenv("DATALAKE_CONNECTION_STRING"))
        file_client = datalake.get_file_system_client("bronze").get_file_client(file_path)
        file_client.upload_data(buffer.getvalue().encode("utf-8"), overwrite=True)

        logging.info(f'Arquivo gravado em bronze/{file_path}')

    
        # Gravação no banco de dados de destino (dbo) - full refresh
    
        # DELETE + INSERT na mesma transação: não duplica dados a cada execução
        # e, se der erro, o rollback mantém os dados anteriores.
        col_list = ", ".join(f"[{c}]" for c in columns)
        placeholders = ", ".join("?" for _ in columns)
        insert_sql = f"INSERT INTO dbo.fila ({col_list}) VALUES ({placeholders})"

        dest_conn = pyodbc.connect(dest_conn_str, autocommit=False)
        try:
            dest_cursor = dest_conn.cursor()

            dest_cursor.execute("DELETE FROM dbo.fila")

            if rows:
                dest_cursor.fast_executemany = True
                dest_cursor.executemany(insert_sql, [tuple(r) for r in rows])

            dest_conn.commit()
            logging.info(f'{len(rows)} linhas gravadas em dbo.fila no banco de destino')
        except Exception:
            dest_conn.rollback()
            raise
        finally:
            dest_conn.close()

    except Exception as e:
        logging.error(f"Erro ao processar itsm.fila: {str(e)}")
        raise



# itsm.categoria  ->  dbo.categoria

@app.timer_trigger(schedule="0 0 3 * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False)
def extract_categoria(myTimer: func.TimerRequest) -> None:
    logging.info('tabela categoria')


    # Variáveis de ambiente (configuradas no Function App)

    # Banco de origem
    sql_server = os.getenv("HOST")
    sql_database = os.getenv("DATABASE")
    sql_user = os.getenv("USER")
    sql_pass = os.getenv("PASSWORD")

    # Banco de destino
    dest_server = os.getenv("DEST_HOST")
    dest_database = os.getenv("DEST_DATABASE")
    dest_user = os.getenv("DEST_USER")
    dest_pass = os.getenv("DEST_PASSWORD")


    # Strings de conexão

    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={sql_server};"
        f"DATABASE={sql_database};"
        f"UID={sql_user};"
        f"PWD={sql_pass};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    dest_conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={dest_server};"
        f"DATABASE={dest_database};"
        f"UID={dest_user};"
        f"PWD={dest_pass};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    try:
    
        # Leitura da tabela no banco de origem
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()

            query = "select * from itsm.categoria"
            cursor.execute(query)

            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]

            logging.info(f'{len(rows)} linhas lidas de itsm.categoria')

    
        # Gravação do CSV no contêiner bronze do Data Lake
    
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(columns)
        writer.writerows(rows)

        # Caminho particionado por data
        now = datetime.now(timezone.utc)
        file_path = f"categoria/{now:%Y/%m/%d}/categoria_{now:%Y%m%d_%H%M%S}.csv"

        datalake = DataLakeServiceClient.from_connection_string(os.getenv("DATALAKE_CONNECTION_STRING"))
        file_client = datalake.get_file_system_client("bronze").get_file_client(file_path)
        file_client.upload_data(buffer.getvalue().encode("utf-8"), overwrite=True)

        logging.info(f'Arquivo gravado em bronze/{file_path}')

    
        # Gravação no banco de dados de destino (dbo) - full refresh
    
        # DELETE + INSERT na mesma transação: não duplica dados a cada execução
        # e, se der erro, o rollback mantém os dados anteriores.
        col_list = ", ".join(f"[{c}]" for c in columns)
        placeholders = ", ".join("?" for _ in columns)
        insert_sql = f"INSERT INTO dbo.categoria ({col_list}) VALUES ({placeholders})"

        dest_conn = pyodbc.connect(dest_conn_str, autocommit=False)
        try:
            dest_cursor = dest_conn.cursor()

            dest_cursor.execute("DELETE FROM dbo.categoria")

            if rows:
                dest_cursor.fast_executemany = True
                dest_cursor.executemany(insert_sql, [tuple(r) for r in rows])

            dest_conn.commit()
            logging.info(f'{len(rows)} linhas gravadas em dbo.categoria no banco de destino')
        except Exception:
            dest_conn.rollback()
            raise
        finally:
            dest_conn.close()

    except Exception as e:
        logging.error(f"Erro ao processar itsm.categoria: {str(e)}")
        raise



# itsm.cliente_organizacao  ->  dbo.cliente_organizacao

@app.timer_trigger(schedule="0 0 3 * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False)
def extract_cliente_organizacao(myTimer: func.TimerRequest) -> None:
    logging.info('tabela cliente_organizacao')


    # Variáveis de ambiente (configuradas no Function App)

    # Banco de origem
    sql_server = os.getenv("HOST")
    sql_database = os.getenv("DATABASE")
    sql_user = os.getenv("USER")
    sql_pass = os.getenv("PASSWORD")

    # Banco de destino
    dest_server = os.getenv("DEST_HOST")
    dest_database = os.getenv("DEST_DATABASE")
    dest_user = os.getenv("DEST_USER")
    dest_pass = os.getenv("DEST_PASSWORD")


    # Strings de conexão

    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={sql_server};"
        f"DATABASE={sql_database};"
        f"UID={sql_user};"
        f"PWD={sql_pass};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    dest_conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={dest_server};"
        f"DATABASE={dest_database};"
        f"UID={dest_user};"
        f"PWD={dest_pass};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    try:
    
        # Leitura da tabela no banco de origem
    
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()


            query = "select * from itsm.cliente_organizacao"
            cursor.execute(query)

            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]

            logging.info(f'{len(rows)} linhas lidas de itsm.cliente_organizacao')

    
        # Gravação do CSV no contêiner bronze do Data Lake
    
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(columns)
        writer.writerows(rows)

        # Caminho particionado por data
        now = datetime.now(timezone.utc)
        file_path = f"cliente_organizacao/{now:%Y/%m/%d}/cliente_organizacao_{now:%Y%m%d_%H%M%S}.csv"

        datalake = DataLakeServiceClient.from_connection_string(os.getenv("DATALAKE_CONNECTION_STRING"))
        file_client = datalake.get_file_system_client("bronze").get_file_client(file_path)
        file_client.upload_data(buffer.getvalue().encode("utf-8"), overwrite=True)

        logging.info(f'Arquivo gravado em bronze/{file_path}')

    
        # Gravação no banco de dados de destino (dbo) - full refresh
    
        # DELETE + INSERT na mesma transação: não duplica dados a cada execução
        # e, se der erro, o rollback mantém os dados anteriores.
        col_list = ", ".join(f"[{c}]" for c in columns)
        placeholders = ", ".join("?" for _ in columns)
        insert_sql = f"INSERT INTO dbo.cliente_organizacao ({col_list}) VALUES ({placeholders})"

        dest_conn = pyodbc.connect(dest_conn_str, autocommit=False)
        try:
            dest_cursor = dest_conn.cursor()

            dest_cursor.execute("DELETE FROM dbo.cliente_organizacao")

            if rows:
                dest_cursor.fast_executemany = True
                dest_cursor.executemany(insert_sql, [tuple(r) for r in rows])

            dest_conn.commit()
            logging.info(f'{len(rows)} linhas gravadas em dbo.cliente_organizacao no banco de destino')
        except Exception:
            dest_conn.rollback()
            raise
        finally:
            dest_conn.close()

    except Exception as e:
        logging.error(f"Erro ao processar itsm.cliente_organizacao: {str(e)}")
        raise



# itsm.sla  ->  dbo.sla

@app.timer_trigger(schedule="0 0 3 * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False)
def extract_sla(myTimer: func.TimerRequest) -> None:
    logging.info('tabela sla')


    # Variáveis de ambiente (configuradas no Function App)

    # Banco de origem
    sql_server = os.getenv("HOST")
    sql_database = os.getenv("DATABASE")
    sql_user = os.getenv("USER")
    sql_pass = os.getenv("PASSWORD")

    # Banco de destino
    dest_server = os.getenv("DEST_HOST")
    dest_database = os.getenv("DEST_DATABASE")
    dest_user = os.getenv("DEST_USER")
    dest_pass = os.getenv("DEST_PASSWORD")


    # Strings de conexão

    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={sql_server};"
        f"DATABASE={sql_database};"
        f"UID={sql_user};"
        f"PWD={sql_pass};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    dest_conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={dest_server};"
        f"DATABASE={dest_database};"
        f"UID={dest_user};"
        f"PWD={dest_pass};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    try:
    
        # Leitura da tabela no banco de origem
    
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()

            query = "select * from itsm.sla"
            cursor.execute(query)

            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]

            logging.info(f'{len(rows)} linhas lidas de itsm.sla')

    
        # Gravação do CSV no contêiner bronze do Data Lake
    
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(columns)
        writer.writerows(rows)

        # Caminho particionado por data
        now = datetime.now(timezone.utc)
        file_path = f"sla/{now:%Y/%m/%d}/sla_{now:%Y%m%d_%H%M%S}.csv"

        datalake = DataLakeServiceClient.from_connection_string(os.getenv("DATALAKE_CONNECTION_STRING"))
        file_client = datalake.get_file_system_client("bronze").get_file_client(file_path)
        file_client.upload_data(buffer.getvalue().encode("utf-8"), overwrite=True)

        logging.info(f'Arquivo gravado em bronze/{file_path}')

    
        # Gravação no banco de dados de destino (dbo) - full refresh
    
        # DELETE + INSERT na mesma transação: não duplica dados a cada execução
        # e, se der erro, o rollback mantém os dados anteriores.
        col_list = ", ".join(f"[{c}]" for c in columns)
        placeholders = ", ".join("?" for _ in columns)
        insert_sql = f"INSERT INTO dbo.sla ({col_list}) VALUES ({placeholders})"

        dest_conn = pyodbc.connect(dest_conn_str, autocommit=False)
        try:
            dest_cursor = dest_conn.cursor()

            dest_cursor.execute("DELETE FROM dbo.sla")

            if rows:
                dest_cursor.fast_executemany = True
                dest_cursor.executemany(insert_sql, [tuple(r) for r in rows])

            dest_conn.commit()
            logging.info(f'{len(rows)} linhas gravadas em dbo.sla no banco de destino')
        except Exception:
            dest_conn.rollback()
            raise
        finally:
            dest_conn.close()

    except Exception as e:
        logging.error(f"Erro ao processar itsm.sla: {str(e)}")
        raise



# itsm.analista  ->  dbo.analista

@app.timer_trigger(schedule="0 10 3 * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False)
def extract_analista(myTimer: func.TimerRequest) -> None:
    logging.info('tabela analista')


    # Variáveis de ambiente (configuradas no Function App)

    # Banco de origem
    sql_server = os.getenv("HOST")
    sql_database = os.getenv("DATABASE")
    sql_user = os.getenv("USER")
    sql_pass = os.getenv("PASSWORD")

    # Banco de destino
    dest_server = os.getenv("DEST_HOST")
    dest_database = os.getenv("DEST_DATABASE")
    dest_user = os.getenv("DEST_USER")
    dest_pass = os.getenv("DEST_PASSWORD")


    # Strings de conexão

    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={sql_server};"
        f"DATABASE={sql_database};"
        f"UID={sql_user};"
        f"PWD={sql_pass};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    dest_conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={dest_server};"
        f"DATABASE={dest_database};"
        f"UID={dest_user};"
        f"PWD={dest_pass};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    try:
    
        # Leitura da tabela no banco de origem
    
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()

            # As colunas devem ter os mesmos nomes de dbo.analista. Se forem diferentes,
            # troque o * por: coluna_origem AS coluna_destino, ...
            # (dt_inclusao é NOT NULL no destino: se a origem não tiver, use
            #  sysutcdatetime() AS dt_inclusao)
            query = "select * from itsm.analista"
            cursor.execute(query)

            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]

            logging.info(f'{len(rows)} linhas lidas de itsm.analista')

    
        # Gravação do CSV no contêiner bronze do Data Lake
    
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(columns)
        writer.writerows(rows)

        # Caminho particionado por data
        now = datetime.now(timezone.utc)
        file_path = f"analista/{now:%Y/%m/%d}/analista_{now:%Y%m%d_%H%M%S}.csv"

        datalake = DataLakeServiceClient.from_connection_string(os.getenv("DATALAKE_CONNECTION_STRING"))
        file_client = datalake.get_file_system_client("bronze").get_file_client(file_path)
        file_client.upload_data(buffer.getvalue().encode("utf-8"), overwrite=True)

        logging.info(f'Arquivo gravado em bronze/{file_path}')

    
        # Gravação no banco de dados de destino (dbo) - full refresh
    
        # DELETE + INSERT na mesma transação: não duplica dados a cada execução
        # e, se der erro, o rollback mantém os dados anteriores.
        col_list = ", ".join(f"[{c}]" for c in columns)
        placeholders = ", ".join("?" for _ in columns)
        insert_sql = f"INSERT INTO dbo.analista ({col_list}) VALUES ({placeholders})"

        dest_conn = pyodbc.connect(dest_conn_str, autocommit=False)
        try:
            dest_cursor = dest_conn.cursor()

            dest_cursor.execute("DELETE FROM dbo.analista")

            if rows:
                dest_cursor.fast_executemany = True
                dest_cursor.executemany(insert_sql, [tuple(r) for r in rows])

            dest_conn.commit()
            logging.info(f'{len(rows)} linhas gravadas em dbo.analista no banco de destino')
        except Exception:
            dest_conn.rollback()
            raise
        finally:
            dest_conn.close()

    except Exception as e:
        logging.error(f"Erro ao processar itsm.analista: {str(e)}")
        raise



# itsm.solicitante  ->  dbo.solicitante

@app.timer_trigger(schedule="0 10 3 * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False)
def extract_solicitante(myTimer: func.TimerRequest) -> None:
    logging.info('tabela solicitante')


    # Variáveis de ambiente (configuradas no Function App)

    # Banco de origem
    sql_server = os.getenv("HOST")
    sql_database = os.getenv("DATABASE")
    sql_user = os.getenv("USER")
    sql_pass = os.getenv("PASSWORD")

    # Banco de destino
    dest_server = os.getenv("DEST_HOST")
    dest_database = os.getenv("DEST_DATABASE")
    dest_user = os.getenv("DEST_USER")
    dest_pass = os.getenv("DEST_PASSWORD")


    # Strings de conexão

    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={sql_server};"
        f"DATABASE={sql_database};"
        f"UID={sql_user};"
        f"PWD={sql_pass};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    dest_conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={dest_server};"
        f"DATABASE={dest_database};"
        f"UID={dest_user};"
        f"PWD={dest_pass};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    try:
    
        # Leitura da tabela no banco de origem
    
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()

            # As colunas devem ter os mesmos nomes de dbo.solicitante. Se forem diferentes,
            # troque o * por: coluna_origem AS coluna_destino, ...
            # (dt_inclusao é NOT NULL no destino: se a origem não tiver, use
            #  sysutcdatetime() AS dt_inclusao)
            query = "select * from itsm.solicitante"
            cursor.execute(query)

            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]

            logging.info(f'{len(rows)} linhas lidas de itsm.solicitante')

    
        # Gravação do CSV no contêiner bronze do Data Lake
    
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(columns)
        writer.writerows(rows)

        # Caminho particionado por data
        now = datetime.now(timezone.utc)
        file_path = f"solicitante/{now:%Y/%m/%d}/solicitante_{now:%Y%m%d_%H%M%S}.csv"

        datalake = DataLakeServiceClient.from_connection_string(os.getenv("DATALAKE_CONNECTION_STRING"))
        file_client = datalake.get_file_system_client("bronze").get_file_client(file_path)
        file_client.upload_data(buffer.getvalue().encode("utf-8"), overwrite=True)

        logging.info(f'Arquivo gravado em bronze/{file_path}')

    
        # Gravação no banco de dados de destino (dbo) - full refresh
    
        # DELETE + INSERT na mesma transação: não duplica dados a cada execução
        # e, se der erro, o rollback mantém os dados anteriores.
        col_list = ", ".join(f"[{c}]" for c in columns)
        placeholders = ", ".join("?" for _ in columns)
        insert_sql = f"INSERT INTO dbo.solicitante ({col_list}) VALUES ({placeholders})"

        dest_conn = pyodbc.connect(dest_conn_str, autocommit=False)
        try:
            dest_cursor = dest_conn.cursor()

            dest_cursor.execute("DELETE FROM dbo.solicitante")

            if rows:
                dest_cursor.fast_executemany = True
                dest_cursor.executemany(insert_sql, [tuple(r) for r in rows])

            dest_conn.commit()
            logging.info(f'{len(rows)} linhas gravadas em dbo.solicitante no banco de destino')
        except Exception:
            dest_conn.rollback()
            raise
        finally:
            dest_conn.close()

    except Exception as e:
        logging.error(f"Erro ao processar itsm.solicitante: {str(e)}")
        raise



# itsm.chamado  ->  dbo.chamado

@app.timer_trigger(schedule="0 20 3 * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False)
def extract_chamado(myTimer: func.TimerRequest) -> None:
    logging.info('tabela chamado')


    # Variáveis de ambiente (configuradas no Function App)

    # Banco de origem
    sql_server = os.getenv("HOST")
    sql_database = os.getenv("DATABASE")
    sql_user = os.getenv("USER")
    sql_pass = os.getenv("PASSWORD")

    # Banco de destino
    dest_server = os.getenv("DEST_HOST")
    dest_database = os.getenv("DEST_DATABASE")
    dest_user = os.getenv("DEST_USER")
    dest_pass = os.getenv("DEST_PASSWORD")


    # Strings de conexão

    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={sql_server};"
        f"DATABASE={sql_database};"
        f"UID={sql_user};"
        f"PWD={sql_pass};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    dest_conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={dest_server};"
        f"DATABASE={dest_database};"
        f"UID={dest_user};"
        f"PWD={dest_pass};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    try:
    
        # Leitura da tabela no banco de origem
    
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()

            # As colunas devem ter os mesmos nomes de dbo.chamado. Se forem diferentes,
            # troque o * por: coluna_origem AS coluna_destino, ...
            # (dt_inclusao é NOT NULL no destino: se a origem não tiver, use
            #  sysutcdatetime() AS dt_inclusao)
            query = "select * from itsm.chamado"
            cursor.execute(query)

            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]

            logging.info(f'{len(rows)} linhas lidas de itsm.chamado')

    
        # Gravação do CSV no contêiner bronze do Data Lake
    
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(columns)
        writer.writerows(rows)

        # Caminho particionado por data
        now = datetime.now(timezone.utc)
        file_path = f"chamado/{now:%Y/%m/%d}/chamado_{now:%Y%m%d_%H%M%S}.csv"

        datalake = DataLakeServiceClient.from_connection_string(os.getenv("DATALAKE_CONNECTION_STRING"))
        file_client = datalake.get_file_system_client("bronze").get_file_client(file_path)
        file_client.upload_data(buffer.getvalue().encode("utf-8"), overwrite=True)

        logging.info(f'Arquivo gravado em bronze/{file_path}')

    
        # Gravação no banco de dados de destino (dbo) - full refresh
    
        # DELETE + INSERT na mesma transação: não duplica dados a cada execução
        # e, se der erro, o rollback mantém os dados anteriores.
        col_list = ", ".join(f"[{c}]" for c in columns)
        placeholders = ", ".join("?" for _ in columns)
        insert_sql = f"INSERT INTO dbo.chamado ({col_list}) VALUES ({placeholders})"

        dest_conn = pyodbc.connect(dest_conn_str, autocommit=False)
        try:
            dest_cursor = dest_conn.cursor()

            dest_cursor.execute("DELETE FROM dbo.chamado")

            if rows:
                dest_cursor.fast_executemany = False  # chamado tem NVARCHAR(MAX) (ds_descricao)
                dest_cursor.executemany(insert_sql, [tuple(r) for r in rows])

            dest_conn.commit()
            logging.info(f'{len(rows)} linhas gravadas em dbo.chamado no banco de destino')
        except Exception:
            dest_conn.rollback()
            raise
        finally:
            dest_conn.close()

    except Exception as e:
        logging.error(f"Erro ao processar itsm.chamado: {str(e)}")
        raise



# itsm.chamado_sla  ->  dbo.chamado_sla

@app.timer_trigger(schedule="0 30 3 * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False)
def extract_chamado_sla(myTimer: func.TimerRequest) -> None:
    logging.info('tabela chamado_sla')


    # Variáveis de ambiente (configuradas no Function App)

    # Banco de origem
    sql_server = os.getenv("HOST")
    sql_database = os.getenv("DATABASE")
    sql_user = os.getenv("USER")
    sql_pass = os.getenv("PASSWORD")

    # Banco de destino
    dest_server = os.getenv("DEST_HOST")
    dest_database = os.getenv("DEST_DATABASE")
    dest_user = os.getenv("DEST_USER")
    dest_pass = os.getenv("DEST_PASSWORD")


    # Strings de conexão

    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={sql_server};"
        f"DATABASE={sql_database};"
        f"UID={sql_user};"
        f"PWD={sql_pass};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    dest_conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={dest_server};"
        f"DATABASE={dest_database};"
        f"UID={dest_user};"
        f"PWD={dest_pass};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    try:
    
        # Leitura da tabela no banco de origem
    
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()

            # As colunas devem ter os mesmos nomes de dbo.chamado_sla. Se forem diferentes,
            # troque o * por: coluna_origem AS coluna_destino, ...
            # (dt_inclusao é NOT NULL no destino: se a origem não tiver, use
            #  sysutcdatetime() AS dt_inclusao)
            query = "select * from itsm.chamado_sla"
            cursor.execute(query)

            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]

            logging.info(f'{len(rows)} linhas lidas de itsm.chamado_sla')

    
        # Gravação do CSV no contêiner bronze do Data Lake
    
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(columns)
        writer.writerows(rows)

        # Caminho particionado por data
        now = datetime.now(timezone.utc)
        file_path = f"chamado_sla/{now:%Y/%m/%d}/chamado_sla_{now:%Y%m%d_%H%M%S}.csv"

        datalake = DataLakeServiceClient.from_connection_string(os.getenv("DATALAKE_CONNECTION_STRING"))
        file_client = datalake.get_file_system_client("bronze").get_file_client(file_path)
        file_client.upload_data(buffer.getvalue().encode("utf-8"), overwrite=True)

        logging.info(f'Arquivo gravado em bronze/{file_path}')

    
        # Gravação no banco de dados de destino (dbo) - full refresh
    
        # DELETE + INSERT na mesma transação: não duplica dados a cada execução
        # e, se der erro, o rollback mantém os dados anteriores.
        col_list = ", ".join(f"[{c}]" for c in columns)
        placeholders = ", ".join("?" for _ in columns)
        insert_sql = f"INSERT INTO dbo.chamado_sla ({col_list}) VALUES ({placeholders})"

        dest_conn = pyodbc.connect(dest_conn_str, autocommit=False)
        try:
            dest_cursor = dest_conn.cursor()

            dest_cursor.execute("DELETE FROM dbo.chamado_sla")

            if rows:
                dest_cursor.fast_executemany = True
                dest_cursor.executemany(insert_sql, [tuple(r) for r in rows])

            dest_conn.commit()
            logging.info(f'{len(rows)} linhas gravadas em dbo.chamado_sla no banco de destino')
        except Exception:
            dest_conn.rollback()
            raise
        finally:
            dest_conn.close()

    except Exception as e:
        logging.error(f"Erro ao processar itsm.chamado_sla: {str(e)}")
        raise



# itsm.chamado_status_historico  ->  dbo.chamado_status_historico

@app.timer_trigger(schedule="0 30 3 * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False)
def extract_chamado_status_historico(myTimer: func.TimerRequest) -> None:
    logging.info('tabela chamado_status_historico')


    # Variáveis de ambiente (configuradas no Function App)

    # Banco de origem
    sql_server = os.getenv("HOST")
    sql_database = os.getenv("DATABASE")
    sql_user = os.getenv("USER")
    sql_pass = os.getenv("PASSWORD")

    # Banco de destino
    dest_server = os.getenv("DEST_HOST")
    dest_database = os.getenv("DEST_DATABASE")
    dest_user = os.getenv("DEST_USER")
    dest_pass = os.getenv("DEST_PASSWORD")


    # Strings de conexão

    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={sql_server};"
        f"DATABASE={sql_database};"
        f"UID={sql_user};"
        f"PWD={sql_pass};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    dest_conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={dest_server};"
        f"DATABASE={dest_database};"
        f"UID={dest_user};"
        f"PWD={dest_pass};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    try:
    
        # Leitura da tabela no banco de origem
    
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()

            # As colunas devem ter os mesmos nomes de dbo.chamado_status_historico. Se forem diferentes,
            # troque o * por: coluna_origem AS coluna_destino, ...
            # (dt_inclusao é NOT NULL no destino: se a origem não tiver, use
            #  sysutcdatetime() AS dt_inclusao)
            query = "select * from itsm.chamado_status_historico"
            cursor.execute(query)

            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]

            logging.info(f'{len(rows)} linhas lidas de itsm.chamado_status_historico')

    
        # Gravação do CSV no contêiner bronze do Data Lake
    
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(columns)
        writer.writerows(rows)

        # Caminho particionado por data
        now = datetime.now(timezone.utc)
        file_path = f"chamado_status_historico/{now:%Y/%m/%d}/chamado_status_historico_{now:%Y%m%d_%H%M%S}.csv"

        datalake = DataLakeServiceClient.from_connection_string(os.getenv("DATALAKE_CONNECTION_STRING"))
        file_client = datalake.get_file_system_client("bronze").get_file_client(file_path)
        file_client.upload_data(buffer.getvalue().encode("utf-8"), overwrite=True)

        logging.info(f'Arquivo gravado em bronze/{file_path}')

    
        # Gravação no banco de dados de destino (dbo) - full refresh
    
        # DELETE + INSERT na mesma transação: não duplica dados a cada execução
        # e, se der erro, o rollback mantém os dados anteriores.
        col_list = ", ".join(f"[{c}]" for c in columns)
        placeholders = ", ".join("?" for _ in columns)
        insert_sql = f"INSERT INTO dbo.chamado_status_historico ({col_list}) VALUES ({placeholders})"

        dest_conn = pyodbc.connect(dest_conn_str, autocommit=False)
        try:
            dest_cursor = dest_conn.cursor()

            dest_cursor.execute("DELETE FROM dbo.chamado_status_historico")

            if rows:
                dest_cursor.fast_executemany = True
                dest_cursor.executemany(insert_sql, [tuple(r) for r in rows])

            dest_conn.commit()
            logging.info(f'{len(rows)} linhas gravadas em dbo.chamado_status_historico no banco de destino')
        except Exception:
            dest_conn.rollback()
            raise
        finally:
            dest_conn.close()

    except Exception as e:
        logging.error(f"Erro ao processar itsm.chamado_status_historico: {str(e)}")
        raise



# itsm.csat_avaliacao  ->  dbo.csat_avaliacao

@app.timer_trigger(schedule="0 30 3 * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False)
def extract_csat_avaliacao(myTimer: func.TimerRequest) -> None:
    logging.info('tabela csat_avaliacao')


    # Variáveis de ambiente (configuradas no Function App)

    # Banco de origem
    sql_server = os.getenv("HOST")
    sql_database = os.getenv("DATABASE")
    sql_user = os.getenv("USER")
    sql_pass = os.getenv("PASSWORD")

    # Banco de destino
    dest_server = os.getenv("DEST_HOST")
    dest_database = os.getenv("DEST_DATABASE")
    dest_user = os.getenv("DEST_USER")
    dest_pass = os.getenv("DEST_PASSWORD")


    # Strings de conexão

    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={sql_server};"
        f"DATABASE={sql_database};"
        f"UID={sql_user};"
        f"PWD={sql_pass};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    dest_conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={dest_server};"
        f"DATABASE={dest_database};"
        f"UID={dest_user};"
        f"PWD={dest_pass};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    try:
    
        # Leitura da tabela no banco de origem
    
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()

            query = "select * from itsm.csat_avaliacao"
            cursor.execute(query)

            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]

            logging.info(f'{len(rows)} linhas lidas de itsm.csat_avaliacao')

    
        # Gravação do CSV no contêiner bronze do Data Lake
    
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(columns)
        writer.writerows(rows)

        # Caminho particionado por data
        now = datetime.now(timezone.utc)
        file_path = f"csat_avaliacao/{now:%Y/%m/%d}/csat_avaliacao_{now:%Y%m%d_%H%M%S}.csv"

        datalake = DataLakeServiceClient.from_connection_string(os.getenv("DATALAKE_CONNECTION_STRING"))
        file_client = datalake.get_file_system_client("bronze").get_file_client(file_path)
        file_client.upload_data(buffer.getvalue().encode("utf-8"), overwrite=True)

        logging.info(f'Arquivo gravado em bronze/{file_path}')

    
        # Gravação no banco de dados de destino (dbo) - full refresh
    
        # DELETE + INSERT na mesma transação: não duplica dados a cada execução
        # e, se der erro, o rollback mantém os dados anteriores.
        col_list = ", ".join(f"[{c}]" for c in columns)
        placeholders = ", ".join("?" for _ in columns)
        insert_sql = f"INSERT INTO dbo.csat_avaliacao ({col_list}) VALUES ({placeholders})"

        dest_conn = pyodbc.connect(dest_conn_str, autocommit=False)
        try:
            dest_cursor = dest_conn.cursor()

            dest_cursor.execute("DELETE FROM dbo.csat_avaliacao")

            if rows:
                dest_cursor.fast_executemany = True
                dest_cursor.executemany(insert_sql, [tuple(r) for r in rows])

            dest_conn.commit()
            logging.info(f'{len(rows)} linhas gravadas em dbo.csat_avaliacao no banco de destino')
        except Exception:
            dest_conn.rollback()
            raise
        finally:
            dest_conn.close()

    except Exception as e:
        logging.error(f"Erro ao processar itsm.csat_avaliacao: {str(e)}")
        raise