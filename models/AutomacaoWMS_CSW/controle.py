### Esse arquivo contem as funcoes de salvar as utimas consulta no banco de dados do POSTGRE , com o
#objetivo especifico de controlar as requisicoes
import gc

import ConexaoPostgreMPL
from datetime import datetime
import pytz
import pandas as pd
import psutil

# Funcao Para obter a Data e Hora
def obterHoraAtual():
    fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
    agora = datetime.now(fuso_horario)
    agora = agora.strftime('%Y-%m-%d  %H:%M:%S.%f')[:-3]
    return agora

def salvar(rotina, ip,datahoraInicio):
    datahorafinal = obterHoraAtual()

    # Converte as strings para objetos datetime
    data1_obj = datetime.strptime(datahoraInicio, "%Y-%m-%d %H:%M:%S.%f")
    data2_obj = datetime.strptime(datahorafinal,  "%Y-%m-%d %H:%M:%S.%f")

    # Calcula a diferença entre as datas
    diferenca = data1_obj - data2_obj

    # Obtém a diferença em dias como um número inteiro
    diferenca_em_dias = diferenca.days

    # Obtém a diferença total em segundos
    diferenca_total_segundos = diferenca.total_seconds()
    tempoProcessamento = float(diferenca_total_segundos)


    conn = ConexaoPostgreMPL.conexao()

    consulta = 'insert into "Reposicao".configuracoes.controle_requisicao_csw (rotina, fim, inicio, ip_origem, "tempo_processamento(s)") ' \
          'values (%s , %s , %s , %s, %s )'

    cursor = conn.cursor()

    cursor.execute(consulta,(rotina,datahorafinal, datahoraInicio, ip, tempoProcessamento ))
    conn.commit()
    cursor.close()

    conn.close()

# Funcao que retorna a utima atualizacao
def UltimaAtualizacao(classe, dataInicial):

    conn = ConexaoPostgreMPL.conexao()

    consulta = pd.read_sql('Select max(datahora_final) as ultimo from "Reposicao".automacao_csw.atualizacoes where classe = %s ', conn, params=(classe,))

    conn.close()

    datafinal = consulta['ultimo'][0]

    # Converte as strings para objetos datetime
    data1_obj = datetime.strptime(dataInicial, "%Y-%m-%d %H:%M:%S.%f")
    data2_obj = datetime.strptime(datafinal, "%Y-%m-%d %H:%M:%S.%f")

    # Calcula a diferença entre as datas
    diferenca = data1_obj - data2_obj

    # Obtém a diferença em dias como um número inteiro
    diferenca_em_dias = diferenca.days

    # Obtém a diferença total em segundos
    diferenca_total_segundos = diferenca.total_seconds()


    return float(diferenca_total_segundos)



def ExcluirHistorico(diasDesejados):
    conn = ConexaoPostgreMPL.conexao()

    deletar = "DELETE FROM pcp.controle_requisicao_csw crc " \
              "WHERE rotina = 'Portal Consulta OP' " \
              "AND ((SUBSTRING(fim, 7, 4)||'-'||SUBSTRING(fim, 4, 2)||'-'||SUBSTRING(fim, 1, 2))::date - now()::date) < -%s"

    cursor = conn.cursor()

    cursor.execute(deletar, (diasDesejados,))
    conn.commit()
    cursor.close()
    conn.close()


def TempoUltimaAtualizacao(dataHoraAtual, rotina):
    conn = ConexaoPostgreMPL.conexaoEngine()

    consulta = pd.read_sql('select max(fim) as "ultimaData" from "Reposicao".configuracoes.controle_requisicao_csw crc '
                          "where rotina = %s ", conn, params=(rotina,) )

    utimaAtualizacao = consulta['ultimaData'][0]
    if utimaAtualizacao != None:

        if len(utimaAtualizacao) < 23:
            print(utimaAtualizacao)
            utimaAtualizacao = utimaAtualizacao + '.001'
        else:
            utimaAtualizacao = utimaAtualizacao

    else:
        print('segue o baile')


    if utimaAtualizacao != None:

        # Converte as strings para objetos datetime
        data1_obj = datetime.strptime(dataHoraAtual, "%Y-%m-%d %H:%M:%S.%f")
        data2_obj = datetime.strptime(utimaAtualizacao, "%Y-%m-%d %H:%M:%S.%f")

        # Calcula a diferença entre as datas
        diferenca = data1_obj - data2_obj

        # Obtém a diferença em dias como um número inteiro
        diferenca_em_dias = diferenca.days

        # Obtém a diferença total em segundos
        diferenca_total_segundos = diferenca.total_seconds()

        return diferenca_total_segundos


    else:
        diferenca_total_segundos = 9999
        return diferenca_total_segundos


def conversaoData(data):
    data1_obj = datetime.strptime(data, "%Y-%m-%d %H:%M:%S.%f")

    return data1_obj

def InserindoStatus(rotina, ip,datahoraInicio):
    datahorafinal = obterHoraAtual()

    # Converte as strings para objetos datetime
    data1_obj = datetime.strptime(datahoraInicio, "%Y-%m-%d %H:%M:%S.%f")
    data2_obj = datetime.strptime(datahorafinal,  "%Y-%m-%d %H:%M:%S.%f")

    # Calcula a diferença entre as datas
    diferenca = data1_obj - data2_obj

    # Obtém a diferença em dias como um número inteiro
    diferenca_em_dias = diferenca.days

    # Obtém a diferença total em segundos
    diferenca_total_segundos = diferenca.total_seconds()
    tempoProcessamento = float(diferenca_total_segundos)


    conn = ConexaoPostgreMPL.conexao()

    consulta = 'insert into "Reposicao".configuracoes.controle_requisicao_csw (rotina, fim, inicio, ip_origem, status, "tempo_processamento(s)" )' \
          ' values (%s , %s , %s , %s, %s , %s )'

    cursor = conn.cursor()

    cursor.execute(consulta,(rotina,datahorafinal, datahoraInicio, ip,'em andamento', tempoProcessamento ))
    conn.commit()
    cursor.close()

    conn.close()

def salvarStatus(rotina, ip,datahoraInicio):
    datahorafinal = obterHoraAtual()

    # Converte as strings para objetos datetime
    data1_obj = datetime.strptime(datahoraInicio, "%Y-%m-%d %H:%M:%S.%f")
    data2_obj = datetime.strptime(datahorafinal,  "%Y-%m-%d %H:%M:%S.%f")

    # Calcula a diferença entre as datas
    diferenca = data1_obj - data2_obj

    # Obtém a diferença em dias como um número inteiro
    diferenca_em_dias = diferenca.days

    # Obtém a diferença total em segundos
    diferenca_total_segundos = diferenca.total_seconds()
    tempoProcessamento = float(diferenca_total_segundos)


    conn = ConexaoPostgreMPL.conexao()

    consulta = 'update "Reposicao".configuracoes.controle_requisicao_csw set fim = %s, "tempo_processamento(s)" = %s , status = %s' \
               ' where  rotina = %s and inicio = %s and ip_origem = %s '

    cursor = conn.cursor()

    cursor.execute(consulta,(datahorafinal, tempoProcessamento,'concluido',rotina,datahoraInicio, ip,  ))
    conn.commit()
    cursor.close()

    conn.close()

def distinctStatus(rotina):
    conn = ConexaoPostgreMPL.conexao()
    consulta = pd.read_sql('select distinct status from "Reposicao".configuracoes.controle_requisicao_csw'
               ' where rotina = %s ',conn,params=(rotina,))


    conn.close()

    if not consulta.empty:
        return 'em andamento'
    else:
        return 'nao iniciado'

def salvarStatus_Etapa1(rotina, ip,datahoraInicio,etapa):
    datahorafinal = obterHoraAtual()

    # Converte as strings para objetos datetime
    print('etapa 1salvar status')
    print(datahoraInicio)
    data1_obj = datetime.strptime(datahoraInicio, "%Y-%m-%d %H:%M:%S.%f")
    data2_obj = datetime.strptime(datahorafinal,  "%Y-%m-%d %H:%M:%S.%f")

    # Calcula a diferença entre as datas
    diferenca = data1_obj - data2_obj

    # Obtém a diferença em dias como um número inteiro
    diferenca_em_dias = diferenca.days

    # Obtém a diferença total em segundos
    diferenca_total_segundos = diferenca.total_seconds()
    tempoProcessamento = float(diferenca_total_segundos)

    cpu_percent = psutil.cpu_percent()

    conn = ConexaoPostgreMPL.conexao()

    consulta = """
                    update 
                        "Reposicao".configuracoes.controle_requisicao_csw 
                    set 
                        etapa1 = %s, "etapa1_tempo" = %s, "tempo_processamento(s)" = %s , "usoCpu1" = %s
                    where  
                        rotina = %s 
                        and inicio = %s 
                        and ip_origem = %s 
                """

    cursor = conn.cursor()

    cursor.execute(consulta,(etapa, tempoProcessamento,tempoProcessamento,cpu_percent, rotina,datahoraInicio, ip,  ))
    conn.commit()
    cursor.close()

    conn.close()
    gc.collect()

    return datahorafinal

def salvarStatus_Etapa2(rotina, ip,datahoraInicio,etapa):
    datahorafinal = obterHoraAtual()

    # Converte as strings para objetos datetime
    data1_obj = datetime.strptime(datahoraInicio, "%Y-%m-%d %H:%M:%S.%f")
    data2_obj = datetime.strptime(datahorafinal,  "%Y-%m-%d %H:%M:%S.%f")

    # Calcula a diferença entre as datas
    diferenca = data1_obj - data2_obj

    # Obtém a diferença em dias como um número inteiro
    diferenca_em_dias = diferenca.days

    # Obtém a diferença total em segundos
    diferenca_total_segundos = diferenca.total_seconds()
    tempoProcessamento = float(diferenca_total_segundos)


    conn = ConexaoPostgreMPL.conexao()

    consulta = 'update "Reposicao".configuracoes.controle_requisicao_csw set etapa2 = %s, "etapa2_tempo" = %s, "tempo_processamento(s)" = ( %s + "tempo_processamento(s)" ), "usoCpu2" = %s ' \
               ' where  rotina = %s and status = %s and ip_origem = %s '

    cursor = conn.cursor()

    cpu_percent = psutil.cpu_percent()

    cursor.execute(consulta,(etapa, tempoProcessamento,tempoProcessamento,cpu_percent, rotina,'em andamento', ip,  ))
    conn.commit()
    cursor.close()

    conn.close()

    return datahorafinal

def salvarStatus_Etapa3(rotina, ip,datahoraInicio,etapa):
    datahorafinal = obterHoraAtual()

    # Converte as strings para objetos datetime
    data1_obj = datetime.strptime(datahoraInicio, "%Y-%m-%d %H:%M:%S.%f")
    data2_obj = datetime.strptime(datahorafinal,  "%Y-%m-%d %H:%M:%S.%f")

    # Calcula a diferença entre as datas
    diferenca = data1_obj - data2_obj

    # Obtém a diferença em dias como um número inteiro
    diferenca_em_dias = diferenca.days

    # Obtém a diferença total em segundos
    diferenca_total_segundos = diferenca.total_seconds()
    tempoProcessamento = float(diferenca_total_segundos)


    conn = ConexaoPostgreMPL.conexao()

    consulta = 'update "Reposicao".configuracoes.controle_requisicao_csw set etapa3 = %s, "etapa3_tempo" = %s, "tempo_processamento(s)" = ( %s + "tempo_processamento(s)" ) ' \
               ' where  rotina = %s and status = %s and ip_origem = %s '

    cursor = conn.cursor()

    cursor.execute(consulta,(etapa, tempoProcessamento,tempoProcessamento,rotina,'em andamento', ip,  ))
    conn.commit()
    cursor.close()

    conn.close()

    return datahorafinal


def salvarStatus_Etapa4(rotina, ip,datahoraInicio,etapa):
    datahorafinal = obterHoraAtual()

    # Converte as strings para objetos datetime
    data1_obj = datetime.strptime(datahoraInicio, "%Y-%m-%d %H:%M:%S.%f")
    data2_obj = datetime.strptime(datahorafinal,  "%Y-%m-%d %H:%M:%S.%f")

    # Calcula a diferença entre as datas
    diferenca = data1_obj - data2_obj

    # Obtém a diferença em dias como um número inteiro
    diferenca_em_dias = diferenca.days

    # Obtém a diferença total em segundos
    diferenca_total_segundos = diferenca.total_seconds()
    tempoProcessamento = float(diferenca_total_segundos)


    conn = ConexaoPostgreMPL.conexao()

    consulta = 'update "Reposicao".configuracoes.controle_requisicao_csw set etapa4 = %s, "etapa4_tempo" = %s, "tempo_processamento(s)" = ( %s + "tempo_processamento(s)" ) ' \
               ' where  rotina = %s and status = %s and ip_origem = %s '

    cursor = conn.cursor()

    cursor.execute(consulta,(etapa, tempoProcessamento,tempoProcessamento,rotina,'em andamento', ip,  ))
    conn.commit()
    cursor.close()

    conn.close()

    return datahorafinal

def salvarStatus_Etapa5(rotina, ip,datahoraInicio,etapa):
    datahorafinal = obterHoraAtual()

    # Converte as strings para objetos datetime
    data1_obj = datetime.strptime(datahoraInicio, "%Y-%m-%d %H:%M:%S.%f")
    data2_obj = datetime.strptime(datahorafinal,  "%Y-%m-%d %H:%M:%S.%f")

    # Calcula a diferença entre as datas
    diferenca = data1_obj - data2_obj

    # Obtém a diferença em dias como um número inteiro
    diferenca_em_dias = diferenca.days

    # Obtém a diferença total em segundos
    diferenca_total_segundos = diferenca.total_seconds()
    tempoProcessamento = float(diferenca_total_segundos)


    conn = ConexaoPostgreMPL.conexao()

    consulta = 'update "Reposicao".configuracoes.controle_requisicao_csw set etapa5 = %s, "etapa5_tempo" = %s, "tempo_processamento(s)" = ( %s + "tempo_processamento(s)" ) ' \
               ' where  rotina = %s and status = %s and ip_origem = %s '

    cursor = conn.cursor()

    cursor.execute(consulta,(etapa, tempoProcessamento,tempoProcessamento,rotina,'em andamento', ip,  ))
    conn.commit()
    cursor.close()

    conn.close()

    return datahorafinal