# Imports
import pandas as pd
from sqlalchemy import create_engine, inspect
from datetime import datetime as dt
import os
from pathlib import Path

# Variáveis
data_hoje = dt.today().strftime(format="%Y-%m-%d")
diretorio_postgres = Path.cwd() / 'local' / 'postgres'
diretorio_csv = Path.cwd() / 'local' / 'csv'
# Variáveis de conexão com o banco de dados
usuario='outputdb_user'
senha='thewindisblowing'
host='127.0.0.1'
port=5433
database='outputdb'
connection_str = f"postgresql://{usuario}:{senha}@{host}:{port}/{database}"


def exec_pipe2(dia):

    print(f"\n\nConectando ao banco de dados {database} ...\n")

    try:
        # Criando conexão com o banco de dados
        engine = create_engine(connection_str)

        # Criando o objeto inspector
        inspector = inspect(engine)
        print("Conectado com sucesso!!!\n\n")

        # Loop dentro pasta postgres. Captura as tabelas CSV e salva no banco.
        print(f"------- Carregando tabelas para o banco {database} -----------")
        for filename in os.listdir(diretorio_postgres/dia):
            try:
                print(f"Carregando tabela {filename}.")
                data = pd.read_csv(f'{diretorio_postgres}/{dia}/{filename}')
                data.to_sql(filename[0:-4], engine)
            except:
                print(f"ERROR! A tabela {filename} já existe.")

    # Loop dentro pasta csv. Captura as tabelas CSV e salva no banco.
        for filename in os.listdir(diretorio_csv/dia):
            try:
                print(f"Carregando tabela {filename}.")
                data = pd.read_csv(f'{diretorio_csv}/{dia}/{filename}')
                data.to_sql(filename[0:-4], engine)
            except:
                print(f"ERROR! A tabela {filename} já existe.")
                
        
        print(f"------- Criando resultado da query -----------\n")

        #Criando consulta no banco de dados.
        df = pd.read_sql_query("SELECT * FROM orders o LEFT JOIN order_details od ON O.order_id = OD.order_id ", engine)

        #Criando pasta com a data de hoje e salvando o resultado da query.
        path = Path.cwd()/'local'/'resultados'/dia
        path.mkdir(parents=True, exist_ok=True)
        df.to_csv(path/"resultado_query.csv", index=False, encoding='latin-1')

        print(f"------------ Etapa 2 Finalizada ---------------\n")

    except:
        print(f"Algo de errado na conexão com o banco de dados {database}!")
    finally:
        if engine:
            del engine
            print("     ------- Conexão Encerrada -------   ")
