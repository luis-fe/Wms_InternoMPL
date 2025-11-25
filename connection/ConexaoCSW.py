import os

import jaydebeapi
import pandas as pd
import models.configuracoes.empresaConfigurada
from dotenv import load_dotenv, dotenv_values

empresa = models.configuracoes.empresaConfigurada.EmpresaEscolhida()
print(empresa)

def Conexao():
    empresa = models.configuracoes.empresaConfigurada.EmpresaEscolhida()

    if empresa == '1':
        x1 = ConexaoInternoMPL()
        return x1
    else:
        x4 = ConexaoCianorte()
        return x4


def ConexaoCianorte():
        load_dotenv('/home/grupompl/Wms_InternoMPL/ambiente.env')
        host = os.getenv('CSW_HOST')
        senha = os.getenv('CSW_PASSWORD')
        user = os.getenv('CSW_USER')

    # try:
        conn = jaydebeapi.connect(
    'com.intersys.jdbc.CacheDriver',
    f'jdbc:Cache://{host}/CONSISTEM',
    {'user': f'{user}', 'password': f'{senha}'},
    'CacheDB_root.jar'
    )
        return conn



# Função de conectar com o CSW, com 2 opções de conexao:
def ConexaoInternoMPL():
        load_dotenv('/home/grupompl/Wms_InternoMPL/ambiente.env')
        host = os.getenv('CSW_HOST')
        senha = os.getenv('CSW_PASSWORD')
        user = os.getenv('CSW_USER')


        conn = jaydebeapi.connect(
            'com.intersys.jdbc.CacheDriver',
            f'jdbc:Cache://{host}/CONSISTEM',
            {'user': f'{user}', 'password': f'{senha}'},
            'CacheDB_root.jar'
        )
        return conn

def Conexao2():
    load_dotenv('/home/grupompl/Wms_InternoMPL/ambiente.env')
    host = os.getenv('CSW_HOST')
    senha = os.getenv('CSW_PASSWORD')
    user = os.getenv('CSW_USER')

    conn = jaydebeapi.connect(
        'com.intersys.jdbc.CacheDriver',
        f'jdbc:Cache://{host}/CONSISTEM',
        {'user': f'{user}', 'password': f'{senha}'},
        'CacheDB_root.jar'
    )
    return conn
def ConexaoExterna2():
    load_dotenv('/home/grupompl/Wms_InternoMPL/ambiente.env')
    host = os.getenv('CSW_HOST')
    senha = os.getenv('CSW_PASSWORD')
    user = os.getenv('CSW_USER')

    conn = jaydebeapi.connect(
        'com.intersys.jdbc.CacheDriver',
        f'jdbc:Cache://{host}/CONSISTEM',
        {'user': f'{user}', 'password': f'{senha}'},
        'CacheDB_root.jar'
    )
    return conn

def obter_notaCsw():
    conn = Conexao()
    data = pd.read_sql(" select t.codigo ,t.descricao  from Fat.TipoDeNotaPadrao t ", conn)
    conn.close()

    return data

def VerificarConexao():
    load_dotenv('/home/grupompl/Wms_InternoMPL/ambiente.env')
    host = os.getenv('CSW_HOST')
    senha = os.getenv('CSW_PASSWORD')
    user = os.getenv('CSW_USER')

    try:
        connPrincipal = jaydebeapi.connect(
        'com.intersys.jdbc.CacheDriver',
        f'jdbc:Cache://{host}/CONSISTEM',
        {'user': f'{user}', 'password': f'{senha}'},
        'CacheDB_root.jar'
    )
        teste = pd.read_sql(" select t.codigo ,t.descricao  from Fat.TipoDeNotaPadrao t ", connPrincipal)
        data = pd.DataFrame([{'Mensagem':'Conexao com CSW normal com o servidor 187.32.10.129:1972 _system ','teste':'csw'}])
    except:
        data = pd.DataFrame([{'Mensagem': 'falha na conexao com o servidor 187.32.10.129:1972 _system ','teste':'csw'}])

    try:
        connContigencia = Conexao2()
        teste2 = pd.read_sql(" select t.codigo ,t.descricao  from Fat.TipoDeNotaPadrao t ", connContigencia)
        data2 = pd.DataFrame([{'Mensagem2':'Conexao com CSW normal com o servidor 187.32.10.129:1972:1972 root ','teste':'csw'}])
    except:
        data2 = pd.DataFrame([{'Mensagem2': 'falha na conexao com o servidor 187.32.10.129:1972:1972 root','teste':'csw'}])



    data = pd.merge(data,data2,on='teste')

    return data

def pesquisaTagCSW(codbarras):
    try:
        emp = models.configuracoes.empresaConfigurada.EmpresaEscolhida()
        codbarras = "'"+codbarras+"'"
        conn = Conexao()
        data = pd.read_sql(" select codBarrasTag , codNaturezaAtual , situacao  FROM Tcr.TagBarrasProduto p"
                           " WHERE p.codBarrasTag = "+ codbarras +' and codEmpresa = '+emp, conn)
        conn.close()
        if not data.empty:
            data['stauts conexao'] = True
            return data
        else:
            data = pd.DataFrame([{'situacao':0}])
           # print('etapa ok')

            return data
    except:
        return pd.DataFrame([{'stautus': False, 'situacao':999}])


####### TESTE NO INICIO DA APLICACAO,
"""
load_dotenv('/home/grupompl/Wms_InternoMPL/ambiente.env')
host = os.getenv('CSW_HOST')
senha = os.getenv('CSW_PASSWORD')
user = os.getenv('CSW_USER')
print(f'Teste variavel {host}')
try:
    teste = obter_notaCsw()
    print(f' {teste}')
except:
    print('caiu a conexao')
    """
