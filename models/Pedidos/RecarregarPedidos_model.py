'''Arquivo .py que Representa a Carga de pedidos que vem do ERP para o WMS'''

from connection import ConexaoCSW
import ConexaoPostgreMPL
import pandas as pd
import pytz
import datetime
from psycopg2 import sql
from models import PedidosClass
from models import Pedidos_Csw

'''Função para obter a Data e Hora e Minuto atual do sistema'''
def obterHoraAtual():
    fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
    agora = datetime.datetime.now(fuso_horario)
    hora_str = agora.strftime('%d/%m/%Y %H:%M')
    return hora_str

'''Função GENERICA para criar agrupamento de pedidos'''
def criar_agrupamentos(grupo):
    return '/'.join(sorted(set(grupo)))

'''Funcao Para acessar o ERP atual e obter os tipos de Notas'''
def obter_notaCsw():
    conn = ConexaoCSW.Conexao() # Abrir conexao com o csw
    data = pd.read_sql(" select t.codigo ,t.descricao  from Fat.TipoDeNotaPadrao t ", conn)
    conn.close() # Encerrar conexao com o csw

    return data


def RecarregarPedidos(empresa):
        '''Metodo utilizado para recarregar os pedidos no WMS '''
        pedidos_Csw = Pedidos_Csw.Pedidos_Csw(empresa)

        conn = ConexaoCSW.Conexao()


        # 2 - Acionando a funcao de Excluir os pedidos nao encontrados :
        tamanhoExclusao = ExcuindoPedidosNaoEncontrados(empresa)

        # 3 - Obtendo os pedidos do CSW, que sao  "sugestoes" + "pedidos mkt" :
        SugestoesAbertos = PedidosClass.Pedido(empresa).get_SugestoesPedidosGeral()

        PedidosSituacao = pd.read_sql(
            "select DISTINCT p.codPedido||'-'||p.codSequencia as codPedido , 'Em Conferencia' as situacaopedido  FROM ped.SugestaoPedItem p "
            'join ped.SugestaoPed s on s.codEmpresa = p.codEmpresa and s.codPedido = p.codPedido and s.codsequencia = p.codSequencia '
            'WHERE p.codEmpresa =' + empresa +
            ' and s.situacaoSugestao = 2', conn)

        PedidosSituacaoMkt = pd.read_sql("""SELECT codPedido||'-Mkt' as codPedido,
            'Em Conferencia' as situacaopedido 
            FROM ped.Pedido e
            WHERE e.codtiponota in (1001 , 38) and situacao = 0 and codEmpresa = """ +str(empresa)+""" and dataEmissao > DATEADD(DAY, -120, GETDATE()) """,conn)

        PedidosSituacao = pd.concat([PedidosSituacao, PedidosSituacaoMkt])

        SugestoesAbertos = pd.merge(SugestoesAbertos, PedidosSituacao, on='codPedido', how='left')

        # Nessa Etapa é realizado uma consuta Sql para obter os pedidos que estão  prontos para faturar , conhecido popularmente como "RETORNA".
        CapaPedido = pedidos_Csw.obter_fila_pedidos_nivel_capa()
        obsCapaPedido = pedidos_Csw.get_clientes_Pedidos_revisar()
        CapaPedido = pd.merge(CapaPedido,obsCapaPedido,on= 'codCliente', how = 'left')
        CapaPedido['obs'].fillna('-',inplace=True)

        SugestoesAbertos = pd.merge(SugestoesAbertos,CapaPedido,on= 'codPedido2', how = 'left')
        SugestoesAbertos.rename(columns={'codPedido': 'codigopedido', 'vlrSugestao': 'vlrsugestao'
            , 'dataGeracao': 'datageracao', 'situacaoSugestao': 'situacaosugestao',
                                         'dataFaturamentoPrevisto': 'datafaturamentoprevisto',
                                         'codCliente': 'codcliente', 'codRepresentante': 'codrepresentante',
                                         'codTipoNota': 'codtiponota'}, inplace=True)
        tiponota = obter_notaCsw()
        tiponota['codigo'] = tiponota['codigo'].astype(str)
        tiponota.rename(columns={'codigo': 'codtiponota', 'descricao': 'desc_tiponota'}, inplace=True)

        tiponota['desc_tiponota'] = tiponota['codtiponota'] + '-' + tiponota['desc_tiponota']
        SugestoesAbertos = pd.merge(SugestoesAbertos, tiponota, on='codtiponota', how='left')
        condicaopgto = pd.read_sql("SELECT v.codEmpresa||'||'||codigo as condvenda, descricao as  condicaopgto FROM cad.CondicaoDeVenda v",conn)
        SugestoesAbertos = pd.merge(SugestoesAbertos, condicaopgto, on='condvenda', how='left')

        SugestoesAbertos.fillna('-', inplace=True)


        conn.close() # Encerrando conexao com o csw
        # Procurando somente pedidos novos a incrementar
        conn2 = ConexaoPostgreMPL.conexao()
        validacao = pd.read_sql('select codigopedido, '+"'ok'"+' as "validador"  from "Reposicao".filaseparacaopedidos f ', conn2)

        SugestoesAbertos2 = pd.merge(SugestoesAbertos, validacao, on='codigopedido', how='left')

        SugestoesAbertos2 = SugestoesAbertos2.loc[SugestoesAbertos2['validador'].isnull()]
        SugestoesAbertos2.drop('validador', axis=1, inplace=True)


        tamanho = SugestoesAbertos2['codigopedido'].size
        dataHora = obterHoraAtual()
        SugestoesAbertos2['datahora'] = dataHora
        # Contar o número de ocorrências de cada valor na coluna 'coluna'
        contagem = SugestoesAbertos2['codcliente'].value_counts()

        # Criar uma nova coluna 'contagem' no DataFrame com os valores contados
        SugestoesAbertos2['contagem'] = SugestoesAbertos2['codcliente'].map(contagem)
        # Aplicar a função de agrupamento usando o método groupby
        SugestoesAbertos2['agrupamentopedido'] = SugestoesAbertos2.groupby('codcliente')['codigopedido'].transform(
            criar_agrupamentos)
        SugestoesAbertos2.drop('codPedido2', axis=1, inplace=True)

        if tamanho >= 1:
            ConexaoPostgreMPL.Funcao_Inserir(SugestoesAbertos2, tamanho, 'filaseparacaopedidos', 'append')

            SugestoesAbertos2 = SugestoesAbertos2.reset_index()
            SugestoesAbertos2 = SugestoesAbertos2.drop_duplicates()

            for i in range(tamanho):
                pedidox = SugestoesAbertos2['codigopedido'][i]

                DetalhandoPedidoSku(empresa, pedidox)


            status = Verificando_RetornaxConferido(empresa)
            print(SugestoesAbertos2.columns)
            PedidosClass.Pedido(empresa).agrupar_pedidos()

            return pd.DataFrame([{'Mensagem:':f'foram inseridos {tamanho} pedidos!','Excluido':f'{tamanhoExclusao} pedidos removidos pois ja foram faturados ',
                                  'Pedidos Atualizados para Retorna':f'{status}'}])
        else:
            status = Verificando_RetornaxConferido(empresa)
            return pd.DataFrame([{'Mensagem:':f'nenhum pedido atualizado','Excluido':f'{tamanhoExclusao} pedidos removidos pois ja foram faturados ',
                                  'Pedidos Atualizados para Retorna':f'{status}'}])





'''
################  MODULO PEDIDOS ##############################
Funcao de Excluir os Pedidos nao Encotrados
'''
def ExcuindoPedidosNaoEncontrados(empresa):

    # 1 - Encontrar os pedidos atuais no retorna e os pedidos de MKT

    conn = ConexaoCSW.Conexao() # Estabelecer a Conexao com o CSW
    # Obter os pedidos do Retorna
    retornaCsw = pd.read_sql("""SELECT  codPedido||'-'||codsequencia as codigopedido,
        (SELECT codTipoNota  FROM ped.Pedido p WHERE p.codEmpresa = e.codEmpresa and p.codpedido = e.codPedido) as codtiponota,
        'ok' as valida 
        FROM ped.SugestaoPed e WHERE e.codEmpresa = """+ str(empresa) +
        """ and e.dataGeracao > DATEADD(DAY, -120, GETDATE()) and situacaoSugestao = 2""", conn)

    pedidosMKT = pd.read_sql("""SELECT codPedido||'-Mkt' as codigopedido,
        (SELECT p.codTipoNota  FROM ped.Pedido p WHERE p.codEmpresa = e.codEmpresa and p.codpedido = e.codPedido) as codtiponota,
        'ok' as valida 
        FROM ped.Pedido e
        WHERE e.codtiponota in (1001 , 38) and situacao = 0 and codEmpresa = """+str(empresa)+""" and dataEmissao > DATEADD(DAY, -120, GETDATE())""",conn)
    retornaCsw = pd.concat([retornaCsw,pedidosMKT])

    conn.close() # Encerrar a Conexao com o CSW


    # 2 - Obtendo a TABELA "filaseparacaopedidos" e "PEDIDOSSKU"  do WMS

    conn2 = ConexaoPostgreMPL.conexao()
    validacao = pd.read_sql(
        "select codigopedido, codtiponota " 
                                              ' from "Reposicao".filaseparacaopedidos f ', conn2)

    validacaoPedidossku = pd.read_sql(
        "select codpedido as codigopedido " 
                                              ' from "Reposicao".pedidossku f ', conn2)

    validacao = pd.merge(validacao, retornaCsw, on=['codigopedido','codtiponota'], how='left') #Merge entre as tabelas
    validacao.fillna('-', inplace=True)
    validacao = validacao[validacao['valida'] == '-'].reset_index()

    tamanho = validacao['codigopedido'].size

    validacao2 = pd.merge(validacaoPedidossku, retornaCsw, on=['codigopedido'], how='left')
    validacao2.fillna('-', inplace=True)

    validacao2 = validacao2[validacao2['valida'] == '-']
    validacao2 = validacao2.reset_index()
    tamanho2 = validacao2['codigopedido'].size


    for i in range(tamanho):

        pedido = validacao['codigopedido'][i]
        tiponota = validacao['codtiponota'][i]

        # Acessando os pedidos com enderecos reservados
        queue = """Delete from "Reposicao".filaseparacaopedidos
                            where codigopedido = %s and codtiponota = %s """


        cursor = conn2.cursor()
        cursor.execute(queue,(pedido,tiponota))
        conn2.commit()

    for i in range(tamanho2):
        pedido = validacao2['codigopedido'][i]

        # Acessando os pedidos com enderecos reservados
        queue = 'Delete from "Reposicao".pedidossku ' \
                " where codpedido = %s "

        cursor = conn2.cursor()
        cursor.execute(queue, (pedido,))
        conn2.commit()




    conn2.close()



    return tamanho




'''FUNCAO DETALHA PEDIDO SKU'''
def DetalhandoPedidoSku(empresa, pedido):
    conncsw = ConexaoCSW.Conexao() #ConexaoCsw
    pedido2 = pedido.split('-')[0] +'|'+ pedido.split('-')[1]

    if pedido.split('-')[1] == 'Mkt':
        SugestoesAbertos = pd.read_sql("""select pg.codPedido as codpedido, 'Mkt' as codSequencia, 
        pg.codProduto as produto, pg.qtdePedida as qtdesugerida, 0 as qtdepecasconf  FROM ped.PedidoItemGrade pg
        WHERE pg.codEmpresa = """+str(empresa)+""" and pg.codPedido = """+pedido.split('-')[0],conncsw)

    else:
        SugestoesAbertos = pd.read_sql(
            'select s.codPedido as codpedido, s.codSequencia , s.produto, s.qtdeSugerida as qtdesugerida , s.qtdePecasConf as qtdepecasconf  '
            'from ped.SugestaoPedItem s  '
            'WHERE s.codEmpresa =' + empresa +
            " and s.codPedido||'|'||s.codSequencia = "+"'" + pedido2 + "'", conncsw)

    conncsw.close()#FecharConexaoCsw

    SugestoesAbertos['necessidade'] = SugestoesAbertos['qtdesugerida'] - SugestoesAbertos['qtdepecasconf']
    SugestoesAbertos['codpedido'] = SugestoesAbertos['codpedido'] + '-' + SugestoesAbertos['codSequencia']
    dataHora = obterHoraAtual()
    SugestoesAbertos['datahora'] = dataHora
    SugestoesAbertos['reservado'] = 'nao'
    SugestoesAbertos.drop('codSequencia', axis=1, inplace=True)
    SugestoesAbertos['endereco'] = 'Não Reposto'
    #SugestoesAbertos = SugestoesAbertos.drop_duplicates()




    query =  'Insert into "Reposicao".pedidossku (codpedido, produto, qtdesugerida, qtdepecasconf, endereco,' \
             ' necessidade, datahora, reservado ) values (%s, %s, %s, %s, %s, %s, %s, %s )'

    conn_pg = ConexaoPostgreMPL.conexao()

    # Pesquisar se em pedidossku ja existe o item

    consulta = pd.read_sql('select * from "Reposicao".pedidossku '
                           'where codpedido = %s ', conn_pg, params=(pedido,))


    if consulta.empty:
        cursor = conn_pg.cursor()

        for _, row in SugestoesAbertos.iterrows():
            cursor.execute(query, (
                row['codpedido'], row['produto'], row['qtdesugerida'], row['qtdepecasconf'],
                row['endereco'], row['necessidade'], row['datahora'], row['reservado']
            ))
            conn_pg.commit()

        cursor.close()
        conn_pg.close()
    else:
        SugestoesAbertos = pd.DataFrame([{'mensagem':'Ja existe na tabela pedidossku'}])

    return SugestoesAbertos


"""FUNCAO VERIFICAR SE O PEDIDO ESTA CONFERIDO NO RETORNA"""
def Verificando_RetornaxConferido(empresa):
    conn = ConexaoCSW.Conexao()

    retornaCsw = pd.read_sql(
        "SELECT  i.codPedido, sum(i.qtdePecasConf) as conf , i.codSequencia  "
        " from ped.SugestaoPedItem i  "
        ' WHERE i.codEmpresa =' + empresa +
        ' group by i.codPedido, i.codSequencia', conn)

    pedidosMkt = pd.read_sql("""select p.codPedido, sum(p.qtdPecasFaturadas) as conf, 'Mkt' as codSequencia
                            from ped.pedido p
                            WHERE p.codEmpresa = """+str(empresa) +""" and dataEmissao > DATEADD(DAY, -120, GETDATE()) and situacao = 0 and codtiponota in (1001 , 38)
                            GROUP BY p.codPedido """,conn)

    retornaCsw = pd.concat([retornaCsw,pedidosMkt])

    retornaCsw['codPedido'] = retornaCsw['codPedido'] + '-' + retornaCsw['codSequencia']
    retornaCsw = retornaCsw[retornaCsw['conf'] == 0]

    # Transformando a coluna 'codPedido' em uma lista separada por vírgulas
    codPedido_lista = retornaCsw['codPedido'].str.cat(sep=',')

    conn.close()

    # Conectar ao banco de dados PostgreSQL
    conn_pg = ConexaoPostgreMPL.conexao()

    # Construir a consulta SQL parametrizada com psycopg2.sql

    values = sql.SQL(',').join(map(sql.Literal, codPedido_lista.split(',')))
    query = sql.SQL('UPDATE "Reposicao".filaseparacaopedidos SET situacaopedido = '
                    "'No Retorna' WHERE situacaopedido <> 'No Retorna' and codigopedido IN ({})").format(values)

    # Executar a consulta SQL
    cursor = conn_pg.cursor()

    cursor.execute(query)
    # Obter o número de linhas afetadas
    num_linhas_afetadas = cursor.rowcount
    conn_pg.commit()
    cursor.close()
    query2 = sql.SQL('UPDATE "Reposicao".filaseparacaopedidos SET situacaopedido = '
                     "'Em Conferencia' WHERE  codigopedido not IN ({})").format(values)
    cursor2 = conn_pg.cursor()

    cursor2.execute(query2)

    conn_pg.commit()
    cursor2.close()

    conn_pg.close()

    return num_linhas_afetadas