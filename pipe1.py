# Imports
import pandas as pd
from sqlalchemy import create_engine, inspect
from datetime import datetime as dt
from pathlib import Path
import os

# Variáveis
data_hoje = dt.today().strftime(format="%Y-%m-%d")
diretorio_local = Path.cwd()/'local'
diretorio_data = Path.cwd()/'data'

# Variáveis de conexão com o banco de dados
usuario='northwind_user'
senha='thewindisblowing'
host='127.0.0.1'
port=5432
database='northwind'
connection_str = f"postgresql://{usuario}:{senha}@{host}:{port}/{database}"

def exec_pipe1():
    print(f"\n\nConectando ao banco de dados {database} ...\n")

    try:
        #Criando conexão com o bando de dados
        engine = create_engine(connection_str)

        # Criando o objeto inspector
        inspector = inspect(engine)

        print("Conectado com sucesso!!!\n\n")
        print(f"-----------   Convertendo tabelas do banco {database}  -----------\n")

        # Coletando nome das tabelas
        tabelas = inspector.get_table_names()

        for tabela in tabelas:
            print(f'Convertendo a tabela {tabela} para {tabela}.csv')
            
            # Lendo a tabela no banco e salvando em arquivo.csv em repositório local
            df = pd.read_sql(f"select * from {tabela}", engine)
            path = diretorio_local / 'postgres' / data_hoje
            path.mkdir(parents=True, exist_ok=True)
            df.to_csv(path / f"{tabela}.csv")

        print("Carregando o arquivo order_details.csv para ambiente local...")

        # Lendo o arquivos.csv e salvando em repositório local
        order_details_df = pd.read_csv(f'{diretorio_data}/order_details.csv')
        print("Salvando os arquivos no ambiente local... ")
        path = diretorio_local / 'csv' / data_hoje
        path.mkdir(parents=True, exist_ok=True)
        order_details_df.to_csv(path / f"order_details.csv")

        print("\n---------   Conversão Finalizada  --------")
        print("\n     ------- Etapa 1 Finalizada -------   ")

    except:
        print(f"Algo de errado na conexão com o banco de dados {database}!")

    finally:
        if engine:
            del engine
            print("     ------- Conexão Encerrada -------   ")
        

