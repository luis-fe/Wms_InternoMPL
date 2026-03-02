import gc
import os

import psycopg2
from sqlalchemy import create_engine
from models.configuracoes import  empresaConfigurada
from dotenv import load_dotenv, dotenv_values


def conexao():
    load_dotenv('/home/grupompl/Wms_InternoMPL/ambiente.env')

    db_name = os.getenv('POSTGRE_NAME')
    db_user = os.getenv('POSTGRE_USER')
    db_password = os.getenv('POSTGRE_PASSWORD')

    if empresaConfigurada.EmpresaEscolhida() == '1':
        db_host = os.getenv('POSTGRE_HOST')
    else:
        db_host = "localhost"

    portbanco = "5432"


    return psycopg2.connect(dbname=db_name, user=db_user, password=db_password, host=db_host, port=portbanco)

def Funcao_Inserir (df_tags, tamanho,tabela, metodo):
    load_dotenv('/home/grupompl/Wms_InternoMPL/ambiente.env')

    # Configurações de conexão ao banco de dados
    database = os.getenv('POSTGRE_NAME')
    user = os.getenv('POSTGRE_USER')
    password = os.getenv('POSTGRE_PASSWORD')

    if empresaConfigurada.EmpresaEscolhida() == '1':
        host = "localhost"
    else:
        host = "localhost"

    port = "5432"

# Cria conexão ao banco de dados usando SQLAlchemy
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')

    # Inserir dados em lotes
    chunksize = tamanho
    for i in range(0, len(df_tags), chunksize):
        df_tags.iloc[i:i + chunksize].to_sql(tabela, engine, if_exists=metodo, index=False , schema='Reposicao')

def Funcao_InserirOFF (df_tags, tamanho,tabela, metodo):
    load_dotenv('/home/grupompl/Wms_InternoMPL/ambiente.env')

    # Configurações de conexão ao banco de dados
    database = os.getenv('POSTGRE_NAME')
    user = os.getenv('POSTGRE_USER')
    password = os.getenv('POSTGRE_PASSWORD')
    if empresaConfigurada.EmpresaEscolhida() == '1':
        host = "localhost"
    else:
        host = "localhost"

    port = "5432"

# Cria conexão ao banco de dados usando SQLAlchemy
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')

    # Inserir dados em lotes
    chunksize = tamanho
    for i in range(0, len(df_tags), chunksize):
        df_tags.iloc[i:i + chunksize].to_sql(tabela, engine, if_exists=metodo, index=False , schema='off')
def conexaoEngine():
    load_dotenv('/home/grupompl/Wms_InternoMPL/ambiente.env')

    db_name = os.getenv('POSTGRE_NAME')
    db_user = os.getenv('POSTGRE_USER')
    db_password = os.getenv('POSTGRE_PASSWORD')
    if empresaConfigurada.EmpresaEscolhida() == '1':
        host = "localhost"
    else:
        host = "localhost"
    portbanco = "5432"

    connection_string = f"postgresql://{db_user}:{db_password}@{host}:{portbanco}/{db_name}"
    return create_engine(connection_string)

def conexaoPCP():
    load_dotenv('/home/grupompl/Wms_InternoMPL/ambiente.env')

    db_name = "PCP"
    db_user = os.getenv('POSTGRE_USER')
    db_password = "postgres"
    db_host = "10.162.0.53"
    portbanco = "5432"

    return psycopg2.connect(dbname=db_name, user=db_user, password=db_password, host=db_host, port=portbanco)

def Funcao_InserirPCP (df_tags, tamanho,tabela, metodo):
    load_dotenv('/home/grupompl/Wms_InternoMPL/ambiente.env')

    # Configurações de conexão ao banco de dados
    database = "PCP"
    user = os.getenv('POSTGRE_USER')
    password = os.getenv('POSTGRE_PASSWORD')
    host = "localhost"
    port = "5432"

# Cria conexão ao banco de dados usando SQLAlchemy
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')

    # Inserir dados em lotes
    chunksize = tamanho
    for i in range(0, len(df_tags), chunksize):
        df_tags.iloc[i:i + chunksize].to_sql(tabela, engine, if_exists=metodo, index=False , schema='pcp')
    gc.collect()
