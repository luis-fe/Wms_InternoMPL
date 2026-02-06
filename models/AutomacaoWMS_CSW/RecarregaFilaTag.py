import gc
from psycopg2 import sql

from Service.GeracaoTags import Tag_Service
from connection import ConexaoCSW
import pandas as pd
import numpy
import datetime
import pytz

import ConexaoPostgreMPL
from models.AutomacaoWMS_CSW import controle
from models.configuracoes import empresaConfigurada


def obterHoraAtual():
    fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
    agora = datetime.datetime.now(fuso_horario)
    hora_str = agora.strftime('%d/%m/%Y %H:%M')
    return hora_str
def RecarregarTagFila(codbarras):
    valor = ConexaoCSW.pesquisaTagCSW(codbarras)

    if valor['stauts conexao'][0]==True:
        conn = ConexaoCSW.Conexao() # Abrir conexao com o csw
        codbarras = "'" + codbarras + "'"

        df_tags = pd.read_sql(
            "SELECT  codBarrasTag as codbarrastag, codNaturezaAtual as codnaturezaatual , codEngenharia , codReduzido as codreduzido,(SELECT i.nome  FROM cgi.Item i WHERE i.codigo = t.codReduzido) as descricao , numeroop as numeroop,"
            " (SELECT i2.codCor||'-'  FROM cgi.Item2  i2 WHERE i2.Empresa = 1 and  i2.codItem  = t.codReduzido) || "
            "(SELECT i2.descricao  FROM tcp.SortimentosProduto  i2 WHERE i2.codEmpresa = 1 and  i2.codProduto  = t.codEngenharia  and t.codSortimento  = i2.codSortimento) as cor,"
            " (SELECT tam.descricao  FROM cgi.Item2  i2 join tcp.Tamanhos tam on tam.codEmpresa = i2.Empresa and tam.sequencia = i2.codSeqTamanho  WHERE i2.Empresa = 1 and  i2.codItem  = t.codReduzido) as tamanho, codEmpresa as codempresa "
            " from tcr.TagBarrasProduto t WHERE situacao = 3 and codBarrasTag = "+ codbarras,
            conn)

        numeroOP = df_tags['numeroop'][0]
        numeroOP = "'%" + numeroOP + "%'"

        df_opstotal = pd.read_sql('SELECT top 200000 numeroOP as numeroop , totPecasOPBaixadas as totalop  '
                                  'from tco.MovimentacaoOPFase WHERE codEmpresa = 1 and codFase = 236  '
                                  'and numeroOP like '+ numeroOP, conn)
        conn.close()# Encerrar conexao com o csw
        df_tags = pd.merge(df_tags, df_opstotal, on='numeroop', how='left')
        df_tags['totalop'] = df_tags['totalop'].replace('', numpy.nan).fillna('0')
        df_tags['codnaturezaatual'] = df_tags['codnaturezaatual'].astype(str)
        df_tags['totalop'] = df_tags['totalop'].astype(int)
        epc = LerEPC(codbarras)
        df_tags = pd.merge(df_tags, epc, on='codbarrastag', how='left')
        df_tags.rename(columns={'codbarrastag': 'codbarrastag', 'codEngenharia': 'engenharia'
            , 'numeroop': 'numeroop'}, inplace=True)
        df_tags['epc'] = df_tags['epc'].str.extract('\|\|(.*)').squeeze()
        dataHora = obterHoraAtual()
        df_tags['DataHora'] = dataHora

        return df_tags
    else:
        return pd.DataFrame([{'mensagem':False}])



def LerEPC(codbarras):
    conn = ConexaoCSW.Conexao()#abrir conexao o csw
    codbarras = codbarras

    consulta = pd.read_sql('select epc.id as epc, t.codBarrasTag as codbarrastag from tcr.SeqLeituraFase  t '
                           'join Tcr_Rfid.NumeroSerieTagEPC epc on epc.codTag = t.codBarrasTag '
                           'WHERE codBarrasTag = '+ codbarras ,conn)
    conn.close()#encerrar conexao com o csw

    return consulta


def InserirTagAvulsa(codbarras, codnaturezaatual, engenharia, codreduzido, descricao,
                     numeroop, cor , tamanho, epc, DataHora, totalop, codempresa):
    conn = ConexaoPostgreMPL.conexao()# Abrir conexao com o Postgre

    insert = 'insert into "Reposicao".filareposicaoportag f ' \
             '(codbarras, codnaturezaatual, engenharia, codreduzido, descricao, numeroop, cor , tamanho, epc, DataHora, totalop, codempresa) ' \
             'values ' \
             '(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'

    cursor = conn.cursor()  # Crie um cursor para executar a consulta SQL
    cursor.execute(insert, (
    codbarras, codnaturezaatual, engenharia, codreduzido, descricao, numeroop, cor, tamanho, epc, DataHora, totalop,
    codempresa))
    conn.commit()  # Faça o commit da transação
    cursor.close()  # Feche o cursor

    conn.close()  # Feche a conexão com o banco de dados




    conn.close()
    return pd.DataFrame([{'Mensagem': 'A Tag Foi inserida no WMS'}])

def BuscarTagsGarantia(rotina, ip, datahoraInicio, emp):

    query = """
    SELECT 
        p.codBarrasTag as codbarrastag, 
        p.codReduzido as codreduzido, 
        p.codEngenharia as engenharia,
        (SELECT i.nome FROM cgi.Item i WHERE i.codigo = p.codReduzido) as descricao, 
        situacao, 
        codNaturezaAtual as natureza, 
        codEmpresa as codempresa,
        (SELECT s.corbase || '-' || s.nomecorbase FROM tcp.SortimentosProduto s WHERE s.codempresa = 1 AND s.codproduto = p.codEngenharia AND s.codsortimento = p.codSortimento) as cor, 
        (SELECT t.descricao FROM tcp.Tamanhos t WHERE t.codempresa = 1 AND t.sequencia = p.seqTamanho) as tamanho, 
        p.numeroOP as numeroop
    FROM 
        Tcr.TagBarrasProduto p 
    WHERE 
        p.codEmpresa = ? AND 
        p.numeroOP IN (
            SELECT numeroOP  
            FROM tco.OrdemProd o 
            WHERE codEmpresa = ? AND codFaseAtual IN (210, 320, 56, 432, 441, 452, 423, 433, 437, 429, 365) AND situacao = 3
        )
    """

    with ConexaoCSW.Conexao() as conn:
        with conn.cursor() as cursor_csw:
            # Executa a consulta e armazena os resultados
            cursor_csw.execute(query, (emp, emp))
            colunas = [desc[0] for desc in cursor_csw.description]
            rows = cursor_csw.fetchall()
            consulta = pd.DataFrame(rows, columns=colunas)
            del rows

    # Salva o status da etapa 1
    etapa1 = controle.salvarStatus_Etapa1(rotina, ip, datahoraInicio, 'etapa csw Tcr.TagBarrasProduto p')

    # Busca restrições e substitutos
    restringe = BuscaResticaoSubstitutos()

    if restringe['numeroop'][0] != 'vazio':
        consulta.fillna('-', inplace=True)
        consulta = pd.merge(consulta, restringe, on=['numeroop', 'cor'], how='left')
        consulta['resticao'].fillna('-', inplace=True)
    else:
        consulta['resticao'] = '-'
        consulta['considera'] = '-'

    # Salva o status da etapa 2
    etapa2 = controle.salvarStatus_Etapa2(rotina, ip, etapa1, 'Adicionando os substitutos selecionados no wms')

    return consulta, etapa2

def SalvarTagsNoBancoPostgre(rotina, ip, datahoraInicio, empresa):
    consulta, etapa2 = BuscarTagsGarantia(rotina, ip, datahoraInicio, empresa)
    ConexaoPostgreMPL.Funcao_InserirOFF(consulta, consulta.size, 'filareposicaoof', 'replace')
    etapa3 = controle.salvarStatus_Etapa3(rotina, ip, etapa2, 'Adicionar as tags ao wms')
    del consulta, etapa2
    gc.collect()

def BuscaResticaoSubstitutos():
    conn = ConexaoPostgreMPL.conexao()

    consulta = pd.read_sql("select numeroop , codproduto||'||'||cor||'||'||numeroop  as resticao,  "
                            'cor, considera  from "Reposicao"."Reposicao"."SubstitutosSkuOP"  '
                           "sso where sso.considera = 'sim' ",conn)

    conn.close()

    if consulta.empty:

        return pd.DataFrame([{'numeroop':'vazio','cor':'vazio','resticao':'vazio','considera':'vazio'}])

    else:

        return consulta

def FilaTags(rotina, datainico ,empresa, n_epc_atualizar :int = 50):

    with ConexaoCSW.Conexao() as conn:
        with conn.cursor() as cursor_csw:
            sql = f"""
                    SELECT 
                        codBarrasTag AS codbarrastag, 
                        codNaturezaAtual AS codnaturezaatual, 
                        codEngenharia, 
                        codReduzido AS codreduzido,
                        (SELECT i.nome FROM cgi.Item i WHERE i.codigo = t.codReduzido) AS descricao, 
                        numeroop AS numeroop,
                        (
                            SELECT 
                                i2.codCor || '-' 
                            FROM 
                                cgi.Item2 i2 
                            WHERE 
                                i2.Empresa = {str(empresa)} 
                                AND i2.codItem = t.codReduzido) || 
                        (SELECT i2.descricao 
                         FROM tcp.SortimentosProduto i2 
                         WHERE i2.codEmpresa = 1 AND i2.codProduto = t.codEngenharia AND t.codSortimento = i2.codSortimento) AS cor,
                        (SELECT tam.descricao 
                         FROM cgi.Item2 i2 
                         JOIN tcp.Tamanhos tam ON tam.codEmpresa = i2.Empresa AND tam.sequencia = i2.codSeqTamanho 
                         WHERE i2.Empresa = 1 AND i2.codItem = t.codReduzido) AS tamanho, 
                        codEmpresa AS codempresa 
                    FROM 
                        tcr.TagBarrasProduto t 
                    WHERE 
                        codEmpresa IN ({str(empresa)}) AND 
                        codNaturezaAtual IN (5, 7, 54) AND 
                        situacao IN (3, 8)
                    """

            cursor_csw.execute(sql)
            colunas = [desc[0] for desc in cursor_csw.description]
            # Busca todos os dados
            rows = cursor_csw.fetchall()
            # Cria o DataFrame com as colunas
            df_tags = pd.DataFrame(rows, columns=colunas)

            meuSql2 =f"""
                        SELECT 
                            top 200000 
                                numeroOP as numeroop , 
                                totPecasOPBaixadas as totalop 
                        from 
                            tco.MovimentacaoOPFase 
                        WHERE 
                            codEmpresa = {str(empresa)}
                            and codFase = 449 
                        order by 
                            numeroOP desc 
                        """
            cursor_csw.execute(meuSql2)
            colunas = [desc[0] for desc in cursor_csw.description]
            # Busca todos os dados
            rows = cursor_csw.fetchall()
            # Cria o DataFrame com as colunas
            df_opstotal = pd.DataFrame(rows, columns=colunas)




    conn2 = ConexaoPostgreMPL.conexao()


    etapa1 = controle.salvarStatus_Etapa1(rotina,'automacao', datainico,'from tcr.TagBarrasProduto t')



    etapa2 = controle.salvarStatus_Etapa2(rotina,'automacao', etapa1,'from tco.MovimentacaoOPFase ')


    df_tags = pd.merge(df_tags, df_opstotal, on='numeroop', how='left')
    df_tags['totalop'] = df_tags['totalop'].replace('', numpy.nan).fillna('0')
    df_tags['codnaturezaatual'] = df_tags['codnaturezaatual'].astype(str)
    df_tags['totalop'] = df_tags['totalop'].astype(int)

    # CRIANDO O DATAFRAME DO QUE JA FOI REPOSTO E USANDO MERGE
       # Verificando as tag's que ja foram repostas
    TagsRepostas = pd.read_sql('select "codbarrastag" as codbarrastag, "usuario" as usuario_  from "Reposicao"."tagsreposicao" tr '
                               ' union select "codbarrastag" as codbarrastag, "usuario" as usuario_ from "Reposicao"."Reposicao".tagsreposicao_inventario ti ',conn2)

    df_tags = pd.merge(df_tags, TagsRepostas, on='codbarrastag', how='left')
    df_tags = df_tags.loc[df_tags['usuario_'].isnull()]
    df_tags.drop('usuario_', axis=1, inplace=True)
    etapa3 = controle.salvarStatus_Etapa3(rotina,'automacao', etapa2,'WMS: "Reposicao"."tagsreposicao"   ')


        # Verificando as tag's que ja estam na fila
    ESTOQUE = pd.read_sql('select "usuario", "codbarrastag" as codbarrastag, "Situacao" as sti_aterior  from "Reposicao"."filareposicaoportag" ',conn2)
    df_tags = pd.merge(df_tags,ESTOQUE,on='codbarrastag',how='left')
    df_tags['Situacao'] = df_tags.apply(lambda row: 'Reposto' if not pd.isnull(row['usuario']) else 'Reposição não Iniciada', axis=1)
    etapa4 = controle.salvarStatus_Etapa4(rotina,'automacao', etapa3,'WMS: "Reposicao"."filareposicaoportag"   ')

    #epc = LerEPC2(empresa)
    etapa5 = controle.salvarStatus_Etapa5(rotina,'automacao', etapa4,'csw: ler os EPCS  ')

    #df_tags = pd.merge(df_tags, epc, on='codbarrastag', how='left')
    df_tags.rename(columns={'codbarrastag': 'codbarrastag','codEngenharia':'engenharia'
                            , 'numeroop':'numeroop'}, inplace=True)
    conn2.close()
    df_tags = df_tags.loc[df_tags['sti_aterior'].isnull()]
    df_tags.drop_duplicates(subset='codbarrastag', inplace=True)
    # Excluir a coluna 'B' inplace
    df_tags.drop('sti_aterior', axis=1, inplace=True)
    df_tags.drop_duplicates(subset='codbarrastag', inplace=True)
    #df_tags['epc'] = df_tags['epc'].str.extract('\|\|(.*)').squeeze()

    tamanho = df_tags['codbarrastag'].size
    dataHora = obterHoraAtual()
    df_tags['DataHora'] = dataHora

    if empresa == '1':
        restringe = BuscaResticaoSubstitutos()
        if not restringe.empty:

            df_tags = pd.merge(df_tags,restringe,on=['numeroop','cor'],how='left')
            df_tags['resticao'].fillna('-', inplace=True)
            df_tags['considera'].fillna('-', inplace=True)
    else:
        print('empresa 4')


    print(df_tags)
    #try:
    if tamanho > 0:
        ConexaoPostgreMPL.Funcao_Inserir(df_tags, tamanho,'filareposicaoportag', 'append')
        Tag_Service.Tag_service(str(empresa)).atualizar_EPC_WMs_CSW(n_epc_atualizar)

    else:
        Tag_Service.Tag_service(str(empresa)).atualizar_EPC_WMs_CSW(n_epc_atualizar)

    AtualizarStatusFila()




def LerEPC2(empresa):

    sql = f"""
                select 
                    epc.id as epc, 
                    t.codBarrasTag as codbarrastag 
                from 
                    tcr.SeqLeituraFase  t
                join 
                    Tcr_Rfid.NumeroSerieTagEPC epc 
                    on epc.codTag = t.codBarrasTag
                WHERE 
                    t.codEmpresa = {str(empresa)} 
                    and (t.codTransacao = 3500 or t.codTransacao = 501)
                    and (
                            codLote like '24%' 
                            or codLote like '25%' 
                            or codLote like '26%'
                            or codLote like '27%' 
                        )
    """

    with ConexaoCSW.Conexao() as conn:

        consulta = pd.read_sql(sql,conn)

    return consulta


def AtualizarStatusFila():
    sql = """
    update "Reposicao"."Reposicao".filareposicaoportag
set status_fila = 'Tag Separado'
where codbarrastag in (
select codbarrastag  from "Reposicao"."Reposicao".tags_separacao ts 
where ts.codbarrastag in (select codbarrastag  from "Reposicao"."Reposicao".filareposicaoportag f))
    """

    conn = ConexaoPostgreMPL.conexao()
    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()
    cursor.close()
    conn.close()

def avaliacaoFila(rotina):
    xemp = empresaConfigurada.EmpresaEscolhida()
    xemp = "'"+xemp+"'"
    with ConexaoCSW.Conexao() as conn:
        with conn.cursor() as cursor_csw:

            sql1 = """select br.codBarrasTag as codbarrastag , 'estoque' as estoque  from Tcr.TagBarrasProduto br 
    WHERE br.codEmpresa in (%s) and br.situacao in (3, 8) and codNaturezaAtual in (5, 7, 54, 8) """%xemp
            cursor_csw.execute(sql1)
            colunas = [desc[0] for desc in cursor_csw.description]
            # Busca todos os dados
            rows = cursor_csw.fetchall()
            # Cria o DataFrame com as colunas
            SugestoesAbertos = pd.DataFrame(rows, columns=colunas)

    datahoraInicio = controle.obterHoraAtual()
    etapa1 = controle.salvarStatus_Etapa1(rotina, 'automacao',datahoraInicio,'etapa csw Tcr.TagBarrasProduto p')

    conn2 = ConexaoPostgreMPL.conexao()

    tagWms = pd.read_sql('select * from "Reposicao".filareposicaoportag t ', conn2)
    tagWms2 = pd.read_sql('select * from "Reposicao".tagsreposicao t ', conn2)

    tagWms = pd.merge(tagWms,SugestoesAbertos, on='codbarrastag', how='left')
    tagWms2 = pd.merge(tagWms2,SugestoesAbertos, on='codbarrastag', how='left')

    etapa2 = controle.salvarStatus_Etapa2(rotina, 'automacao',etapa1,'etapa merge filatagsWMS+tagsProdutoCSW')


    tagWms = tagWms[tagWms['estoque']!='estoque']
    tagWms2 = tagWms2[tagWms2['estoque']!='estoque']


    tamanho =tagWms['codbarrastag'].size
    tamanho2 =tagWms2['codbarrastag'].size

    # Obter os valores para a cláusula WHERE do DataFrame
    lista = tagWms['codbarrastag'].tolist()
    lista2 = tagWms2['codbarrastag'].tolist()

    # Construir a consulta DELETE usando a cláusula WHERE com os valores do DataFrame


    #bACKUP DAS TAGS QUE TIVERAM SAIDA FORA DO WMS NA FILA

    if tamanho != 0:
        query = sql.SQL('DELETE FROM "Reposicao"."filareposicaoportag" WHERE codbarrastag IN ({})').format(
            sql.SQL(',').join(map(sql.Literal, lista))
        )

        query2 = sql.SQL('DELETE FROM "Reposicao"."tagsreposicao" WHERE codbarrastag IN ({})').format(
            sql.SQL(',').join(map(sql.Literal, lista2))
        )

        # Executar a consulta DELETE
        with conn2.cursor() as cursor:
            cursor.execute(query)
            conn2.commit()

            try:
                cursor.execute(query2)
                conn2.commit()
            except:
                print('sem tags para limpar na fila')


    else:
        print('2.1.1 sem tags para ser eliminadas na Fila Tags Reposicao')
    etapa3 = controle.salvarStatus_Etapa3(rotina, 'automacao',etapa2,'deletando saidas fora do WMS')