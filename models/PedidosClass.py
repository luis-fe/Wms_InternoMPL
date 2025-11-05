import gc
import pandas as pd
from sqlalchemy import text
import ConexaoPostgreMPL
from connection import ConexaoCSW
import models.configuracoes.empresaConfigurada


class Pedido():
    '''Classe que gerencia os pedidos enviado ao WMS'''

    def __init__(self, codEmpresa = None, codPedido = None):

        self.codEmpresa = str(codEmpresa)
        self.codPedido = codPedido

    def DetalhaPedido(self):
        '''Metodo que detalha o pedido no WMS'''

        # 1- Filtrando o Pedido na tabela de pedidosSku
        conn = ConexaoPostgreMPL.conexaoEngine()


        sqlFilaPedidos = f"""
                    select 
                        codigopedido, 
                        desc_tiponota  , 
                        codcliente ||'-'|| desc_cliente as cliente,
                        codrepresentante  ||'-'|| desc_representante  as repres,
                        agrupamentopedido,
                        cod_usuario as usuario
                    from 
                        "Reposicao".filaseparacaopedidos f 
                    where 
                        codigopedido = '{self.codPedido}'
        """

        skus1 = pd.read_sql(sqlFilaPedidos, conn)

        if skus1.empty:
            # Olha Para os Pedidos de Transferencia
            skus = pd.read_sql("select descricaopedido as codigopedido, 'transferencia' as desc_tiponota,"
                               " 'transferencia de Naturezas' as cliente  "
                               ",'transferencia de Naturezas' as repres, "
                               'codigopedido as agrupamentopedido '
                               'from "Reposicao"."pedidosTransferecia" f  '
                               "where situacao = 'aberto'"
                               ' and descricaopedido= ' + "'" + self.codPedido + "'"
                               , conn)
        else:

            skus = skus1

        grupo = pd.read_sql('select agrupamentopedido '
                            'from "Reposicao".filaseparacaopedidos f  where codigopedido= ' + "'" + self.codPedido + "'"
                            , conn)
        DetalhaSku = pd.read_sql(
            'select  produto as reduzido, qtdesugerida , endereco as endereco, necessidade as a_concluir , '
            'qtdesugerida as total, (qtdesugerida - necessidade) as qtdrealizado'
            ' from "Reposicao".pedidossku p  where codpedido= ' + "'" + self.codPedido + "'"
                                                                                    " order by endereco asc", conn)

        # Validando as descricoes + cor + tamanho dos produtos para nao ser null

        descricaoSku = pd.read_sql(
            """
            select engenharia as referencia, codreduzido as reduzido, descricao, cor ,tamanho from "Reposicao"."Tabela_Sku"
            where codreduzido in
            (select  produto as reduzido
            from "Reposicao".pedidossku p  where codpedido = """ + "'" + self.codPedido + "') """, conn)
        itensMkt = DescricaoCswItensMKT()
        descricaoSku = pd.concat([descricaoSku, itensMkt])

        descricaoSku.drop_duplicates(subset='reduzido', keep='first', inplace=True)

        DetalhaSku = pd.merge(DetalhaSku, descricaoSku, on='reduzido', how='left')

        # Agrupar os valores da col2 por col1 e concatenar em uma nova coluna
        DetalhaSku['endereco'] = DetalhaSku.groupby(['reduzido'])['endereco'].transform(lambda x: ', '.join(x))
        # Remover as linhas duplicadas
        DetalhaSku['total'] = DetalhaSku.groupby('reduzido')['total'].transform('sum')
        DetalhaSku['qtdesugerida'] = DetalhaSku.groupby('reduzido')['qtdesugerida'].transform('sum')

        DetalhaSku['qtdrealizado'] = DetalhaSku.groupby('reduzido')['qtdrealizado'].transform('sum')
        DetalhaSku['a_concluir'] = DetalhaSku.groupby('reduzido')['a_concluir'].transform('sum')
        DetalhaSku['qtdesugerida'] = DetalhaSku['qtdesugerida'].astype(int)
        DetalhaSku['qtdrealizado'] = DetalhaSku['qtdrealizado'].astype(int)
        DetalhaSku['concluido_X_total'] = DetalhaSku['qtdrealizado'].astype(str) + '/' + DetalhaSku[
            'qtdesugerida'].astype(str)
        DetalhaSku = DetalhaSku.drop_duplicates()
        DetalhaSku.fillna('nao localizado', inplace=True)

        # finalizacaoPedidoModel.VerificarExisteApontamento(codPedido, skus['usuario'][0])

        data = {
            '1 - codpedido': f'{skus["codigopedido"][0]} ',
            '2 - Tiponota': f'{skus["desc_tiponota"][0]} ',
            '3 - Cliente': f'{skus["cliente"][0]} ',
            '4- Repres.': f'{skus["repres"][0]} ',
            # '4.1- Grupo.': f'{skus["grupo"][0]} ',
            '5- Detalhamento dos Sku:': DetalhaSku.to_dict(orient='records')
        }

        return [data]

    def consultaERPCSW_TipoNota(self):
        '''Metodo  que busca no ERP do CSW os tipo de Notas '''

        sql = """
        select
            n.codigo ,
            n.descricao, 
            t.codNatureza1 as natureza
        from
            Fat.TipoDeNotaPadrao n
        left join
            est.Transacao t
            on t.codEmpresa = 1 
            and t.codTransacao = n.codTransacaoEstoque 
        """

        with ConexaoCSW.Conexao() as conn:
            with conn.cursor() as cursor_csw:
                cursor_csw.execute(sql)
                colunas = [desc[0] for desc in cursor_csw.description]
                # Busca todos os dados
                rows = cursor_csw.fetchall()
                # Cria o DataFrame com as colunas
                consulta = pd.DataFrame(rows, columns=colunas)
                del rows
                gc.collect()
        return  consulta

    def __sugestoesPedidosAberto_ErpCsw(self):
        '''Metodo PRIVADO que busca no ERP do CSW as sugestoes de pedidos'''

        SugestoesAbertos = """
                SELECT 
                    codPedido||'-'||codsequencia as codPedido, 
                    codPedido as codPedido2, 
                    dataGeracao, 
                    priorizar, 
                    vlrSugestao,
                    situacaoSugestao, 
                    dataFaturamentoPrevisto  
                from 
                    ped.SugestaoPed
                WHERE 
                    codEmpresa =""" + self.codEmpresa +""" and situacaoSugestao =2 """


        with ConexaoCSW.Conexao() as conn:
            with conn.cursor() as cursor_csw:
                cursor_csw.execute(SugestoesAbertos)
                colunas = [desc[0] for desc in cursor_csw.description]
                # Busca todos os dados
                rows = cursor_csw.fetchall()
                # Cria o DataFrame com as colunas
                SugestoesAbertos = pd.DataFrame(rows, columns=colunas)
                del rows
                gc.collect()
        return  SugestoesAbertos


    def __sugestoesPedidosMKTo_ErpCsw(self):
        '''Metodo PRIVADO que busca no ERP do CSW as Pedidos de Marketing '''


        PedidosMkt = """
        SELECT 
            codPedido||'-Mkt' as codPedido,
            codPedido as codPedido2,
            dataemissao as dataGeracao,
            '0' as priorizar, 
            vlrPedido as vlrSugestao, 
            '2' as situacaoSugestao,
            dataPrevFat as dataFaturamentoPrevisto
        FROM 
            ped.Pedido e
        WHERE 
            e.codtiponota in (1001 , 38) 
            and situacao = 0 and codEmpresa = """+str(self.codEmpresa)+"""
        and dataEmissao > DATEADD(DAY, -120, GETDATE())"""



        with ConexaoCSW.Conexao() as conn:
            with conn.cursor() as cursor_csw:
                cursor_csw.execute(PedidosMkt)
                colunas = [desc[0] for desc in cursor_csw.description]
                # Busca todos os dados
                rows = cursor_csw.fetchall()
                # Cria o DataFrame com as colunas
                SugestoesAbertos = pd.DataFrame(rows, columns=colunas)
                del rows
                gc.collect()
        return  SugestoesAbertos


    def get_SugestoesPedidosGeral(self):
        '''Metodo que unifica as sugestoes + pedidos de marketing'''

        sugestoes = self.__sugestoesPedidosAberto_ErpCsw()
        sugestoesMkt = self.__sugestoesPedidosMKTo_ErpCsw()

        PedidosSituacao = pd.concat([sugestoes, sugestoesMkt])

        return PedidosSituacao

    def __situacaoPedidoGeral(self):


        PedidosSituacao = """
                select 
                    DISTINCT p.codPedido||'-'||p.codSequencia as codPedido , 
                    'Em Conferencia' as situacaopedido  
                FROM 
                    ped.SugestaoPedItem p
                join 
                    ped.SugestaoPed s 
                    on s.codEmpresa = p.codEmpresa 
                    and s.codPedido = p.codPedido 
                    and s.codsequencia = p.codSequencia 
                WHERE 
                    p.codEmpresa ="""+self.codEmpresa +"""and s.situacaoSugestao = 2
            UNION
                SELECT 
                    codPedido||'-Mkt' as codPedido,
                    'Em Conferencia' as situacaopedido 
                FROM 
                    ped.Pedido e
                WHERE 
                    e.codtiponota in (1001 , 38) 
                    and situacao = 0 
                    and codEmpresa = """ +str(self.codEmpresa)+""" 
                    and dataEmissao > DATEADD(DAY, -120, GETDATE()) """


        with ConexaoCSW.Conexao() as conn:
            with conn.cursor() as cursor_csw:
                cursor_csw.execute(PedidosSituacao)
                colunas = [desc[0] for desc in cursor_csw.description]
                # Busca todos os dados
                rows = cursor_csw.fetchall()
                # Cria o DataFrame com as colunas
                PedidosSituacao = pd.DataFrame(rows, columns=colunas)
                del rows
                gc.collect()
        return  PedidosSituacao

    def agrupar_pedidos(self):
        """Método público que agrupa os pedidos por cliente"""

        sql_pedidos = """
            SELECT
                f.codigopedido,
                f.codcliente
            FROM
                "Reposicao"."Reposicao".filaseparacaopedidos f
        """

        conn = ConexaoPostgreMPL.conexaoEngine()
        consulta = pd.read_sql(sql_pedidos, conn)

        # Cria a coluna agrupando todos os pedidos do mesmo cliente
        consulta['agrupamentopedido'] = consulta.groupby('codcliente')['codigopedido'].transform(
            self.__criar_agrupamentos
        )

        with ConexaoPostgreMPL.conexao() as conn:
            trans = conn.begin()
            for _, row in consulta.iterrows():
                    sql_update = """
                        UPDATE "Reposicao"."Reposicao".filaseparacaopedidos
                        SET agrupamentopedido = :agrupamento
                        WHERE codigopedido = :pedido
                    """
                    conn.execute(
                        text(sql_update),
                        {"agrupamento": row['agrupamentopedido'], "pedido": row['codigopedido']}
                    )

            # Confirma todas as alterações
            trans.commit()



        return consulta  # (opcional, mas útil pra devolver o resultado)

    def __criar_agrupamentos(self, grupo):
        """Método privado auxiliar para concatenar os pedidos únicos"""
        return '/'.join(sorted(set(grupo)))





