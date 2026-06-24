'''ARQUIVO .PY COM AS INFORMAÇOES REFERENTE AO MODULO DE PEDIDOS - FILA PEDIDOS , que representa os pedidos que estao em fila para ser separado'''


import ConexaoPostgreMPL
from connection import ConexaoCSW
import pandas as pd
import numpy as np
import datetime
import pytz


#import locale
#from models import finalizacaoPedidoModel


def obterHoraAtual():
    fuso_horario = pytz.timezone('America/Sao_Paulo')
    agora = datetime.datetime.now(fuso_horario)
    hora_str = agora.strftime('%Y-%m-%d %H:%M:%S')
    return hora_str

def FilaPedidos(empresa):
    conn = ConexaoPostgreMPL.conexaoEngine()
    
    
    
    
    
    
    
    pedido = pd.read_sql(
        """
        SELECT 
            f.codigopedido, 
            f.vlrsugestao, 
            f.codcliente, 
            f.desc_cliente, 
            f.cod_usuario, 
            f.cidade, 
            f.estado, 
            f.datageracao, 
            f.codrepresentante, 
            f.desc_representante, 
            f.desc_tiponota, 
            f.condicaopgto, 
            f.agrupamentopedido, 
            f.situacaopedido, 
            CASE 
                WHEN f.obs = 'REVISAR' THEN COALESCE(f.prioridade, '') || '-' || f.obs
                ELSE f.prioridade 
            END AS prioridade, 
            f.obs 
        FROM 
            "Reposicao".filaseparacaopedidos f;
        """
         , conn)
    naturezaPedido = pd.read_sql(
        """
            select 
                desc_tiponota, 
                natureza 
            from 
                configuracoes.tiponota_nat 
                """, conn)
    pedido = pd.merge(pedido, naturezaPedido, on="desc_tiponota", how='left')

    pedidosku = pd.read_sql('select codpedido, sum(qtdesugerida) as qtdesugerida, sum(necessidade) as necessidade   from "Reposicao".pedidossku p  '
                            'group by codpedido ', conn)
    pedidosku.rename(columns={'codpedido': '01-CodPedido', 'qtdesugerida': '15-qtdesugerida','necessidade': '19-necessidade'}, inplace=True)

    usuarios = pd.read_sql(
        'select codigo as cod_usuario , nome as nomeusuario_atribuido  from "Reposicao".cadusuarios c ', conn)
    usuarios['cod_usuario'] = usuarios['cod_usuario'].astype(str)
    pedido = pd.merge(pedido, usuarios, on='cod_usuario', how='left')

    try:

        transporta = """SELECT  t.cidade , t.siglaEstado as estado, f.fantasia as transportadora  FROM Asgo_Trb.TransPreferencia t
                                  join cad.Transportador  f on  f.codigo  = t.Transportador
                                  WHERE t.Empresa ="""+str(empresa)

        with ConexaoCSW.Conexao() as conn2:
            with conn2.cursor() as cursor:
                cursor.execute(transporta)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                transporta = pd.DataFrame(rows, columns=colunas)


        pedido = pd.merge(pedido, transporta, on=["cidade", "estado"], how='left')

       # pedido['transportadora'] = 'Perdeu Conexao Csw'
    except:

        pedido['transportadora'] = 'Perdeu Conexao Csw'



    pedido.rename(
        columns={'codigopedido': '01-CodPedido', 'datageracao': '02- Data Sugestao', 'desc_tiponota': '03-TipoNota',
                 'codcliente': '04-codcliente',
                 'desc_cliente': '05-desc_cliente', 'cidade': '06-cidade', 'estado': '07-estado',
                 'codrepresentante': '08-codrepresentante', 'desc_representante': '09-Repesentante',
                 'cod_usuario': '10-codUsuarioAtribuido',
                 'nomeusuario_atribuido': '11-NomeUsuarioAtribuido', 'vlrsugestao': '12-vlrsugestao',
                 'condicaopgto': '13-CondPgto', 'agrupamentopedido': '14-AgrupamentosPedido','situacaopedido': '22- situacaopedido',"transportadora":"23-transportadora"}, inplace=True)
    pedido['12-vlrsugestao'] = 'R$ ' + pedido['12-vlrsugestao']
    pedido = pd.merge(pedido, pedidosku, on='01-CodPedido', how='left')
    pedido['15-qtdesugerida'] = pedido['15-qtdesugerida'].fillna(0)
    pedidoskuReposto = pd.read_sql('select codpedido, sum(necessidade) as reposto  from "Reposicao".pedidossku p '
                                   "where endereco <> 'Não Reposto' "
                                   'group by codpedido ', conn)
    pedidoskuReposto.rename(columns={'codpedido': '01-CodPedido', 'reposto': '16-Endereco Reposto'}, inplace=True)
    pedido = pd.merge(pedido, pedidoskuReposto, on='01-CodPedido', how='left')
    pedidoskuReposto2 = pd.read_sql('select codpedido, sum(necessidade) as naoreposto  from "Reposicao".pedidossku p '
                                    "where endereco = 'Não Reposto' "
                                    'group by codpedido ', conn)
    pedidoskuReposto2.rename(columns={'codpedido': '01-CodPedido', 'naoreposto': '17-Endereco NaoReposto'},
                             inplace=True)
    pedido = pd.merge(pedido, pedidoskuReposto2, on='01-CodPedido', how='left')
    pedido['16-Endereco Reposto'] = pedido['16-Endereco Reposto'].fillna(0)
    pedido['17-Endereco NaoReposto'] = pedido['17-Endereco NaoReposto'].fillna(0)
    pedido['19-necessidade'] = pedido['19-necessidade'].fillna(0)

    pedido['18-%Reposto'] = pedido['17-Endereco NaoReposto'] + pedido['16-Endereco Reposto']


    pedido['18-%Reposto'] = pedido['16-Endereco Reposto'] / pedido['18-%Reposto']


    pedido['20-Separado%'] = 1 - (pedido['19-necessidade'] / pedido['15-qtdesugerida'])
    pedido['18-%Reposto'] = (pedido['18-%Reposto'] * 100).round(2)
    pedido['18-%Reposto'] = pedido['18-%Reposto'].fillna(0)
    pedido['20-Separado%'] = (pedido['20-Separado%'] * 100).round(2)
    pedido['20-Separado%'] = pedido['20-Separado%'].fillna(0)
    # obtendo a Marca do Pedido
    marca = pd.read_sql('select codpedido ,  t.engenharia   from "Reposicao".pedidossku p '
                        'join "Reposicao".tagsreposicao t on t.codreduzido = p.produto  '
                        ' group by codpedido, t.engenharia ',conn)
    marca['engenharia'] = marca['engenharia'].str.slice(1)
    if empresa == '4':

        marca['21-MARCA'] =np.where((marca['engenharia'].str[:3] == '302') | (marca['engenharia'].str[:3] == '302') , 'M.POLLO', 'PACO')
    else:
        marca['21-MARCA'] =np.where((marca['engenharia'].str[:3] == '102') | (marca['engenharia'].str[:3] == '202') , 'M.POLLO', 'PACO')

    marca.drop('engenharia', axis=1, inplace=True)
    marca.drop_duplicates(subset='codpedido', inplace=True)
    marca.rename(columns={'codpedido': '01-CodPedido'}, inplace=True)
    pedido = pd.merge(pedido, marca, on='01-CodPedido', how='left')
    pedido['21-MARCA'].fillna('-', inplace=True)
    pedido['22- situacaopedido'].fillna('No Retorna', inplace=True)
    pedido.fillna('-', inplace=True)
    pedido.replace([np.inf, -np.inf], 0, inplace=True)

    return pedido

def FilaAtribuidaUsuario(codUsuario, empresa):
    x = FilaPedidos(empresa)
    codUsuario = str(codUsuario)
    x = x[x['10-codUsuarioAtribuido'] == codUsuario]
    x = x.sort_values(by=['prioridade',"04-codcliente"], ascending=False)

    return x



def ClassificarFila(coluna, tipo, empresa):
    fila = FilaPedidos(empresa)
    fila['12-vlrsugestao'] = fila['12-vlrsugestao'].str.replace("R\$", "").astype(float)

    if tipo == 'desc':
        fila = fila.sort_values(by=coluna, ascending=False)
        fila['12-vlrsugestao'] = fila['12-vlrsugestao'] .astype(str)
        fila['12-vlrsugestao'] = 'R$ ' + fila['12-vlrsugestao']

        return fila

    else:
        fila['12-vlrsugestao'] = fila['12-vlrsugestao'] .astype(str)
        fila['12-vlrsugestao'] = 'R$ ' + fila['12-vlrsugestao']
        fila = fila.sort_values(by=coluna, ascending=True)
        return fila