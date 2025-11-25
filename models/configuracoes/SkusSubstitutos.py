import ConexaoPostgreMPL
import pandas as pd


def SubstitutosPorOP(filtro = ''):
   if  filtro == '':
        conn = ConexaoPostgreMPL.conexao()

        consultar = pd.read_sql('Select categoria as "1-categoria", numeroop as "2-numeroOP", codproduto as "3-codProduto", cor as "4-cor", databaixa_req as "5-databaixa", '
                                '"coodigoPrincipal" as "6-codigoPrinc", '
                                'nomecompontente as "7-nomePrinc",'
                                '"coodigoSubs" as "8-codigoSub",'
                                'nomesub as "9-nomeSubst", aplicacao as "10-aplicacao", considera from "Reposicao"."SubstitutosSkuOP" ', conn)

        conn.close()

        consultar.fillna('-',inplace=True)

        # Fazer a ordenacao
        consultar = consultar.sort_values(by=['considera','5-databaixa'], ascending=False)  # escolher como deseja classificar
        consultar = consultar.drop_duplicates()

        return consultar
   else:
       conn = ConexaoPostgreMPL.conexao()

       consultar = pd.read_sql('Select categoria as "1-categoria", numeroop as "2-numeroOP", codproduto as "3-codProduto", cor as "4-cor", databaixa_req as "5-databaixa", '
                               '"coodigoPrincipal" as "6-codigoPrinc", '
                               'nomecompontente as "7-nomePrinc",'
                               '"coodigoSubs" as "8-codigoSub",'
                               'nomesub as "9-nomeSubst",aplicacao as "10-aplicacao",  considera from "Reposicao"."SubstitutosSkuOP" where categoria = %s ', conn, params=(filtro,))

       conn.close()

       # Fazer a ordenacao
       consultar = consultar.sort_values(by=['considera', '5-databaixa'],
                                         ascending=False)  # escolher como deseja classificar
       consultar.fillna('-', inplace=True)

       consultar = consultar.drop_duplicates()

       return consultar

def ObterCategorias():
    conn = ConexaoPostgreMPL.conexao()

    consultar = pd.read_sql('Select distinct categoria from "Reposicao"."SubstitutosSkuOP" ', conn)

    conn.close()


    return consultar

def UpdetaConsidera(arrayOP , arraycor, arraydesconsidera):
    conn = ConexaoPostgreMPL.conexao()

    indice = 0
    for i in range(len(arrayOP)):
        indice = 1 + indice
        op = arrayOP[i]
        cor = arraycor[i]
        considera = arraydesconsidera[i]

        update = 'update "Reposicao"."SubstitutosSkuOP" set considera = %s where numeroop = %s and "cor" = %s'

        cursor = conn.cursor()
        cursor.execute(update,(considera, op, cor,))
        conn.commit()
        cursor.close()


    conn.close()
    return pd.DataFrame([{'Mensagem':'Salvo com sucesso'}])


def PesquisaEnderecoSubstitutoVazio():
    conn = ConexaoPostgreMPL.conexao()

    consulta = pd.read_sql('select c.codendereco , saldo from "Reposicao"."Reposicao".enderecoporsku sle '
                        'right join "Reposicao"."Reposicao".cadendereco c on c.codendereco = sle.codendereco '
                            "where c.endereco_subst = 'sim' and saldo is null and pre_reserva is null", conn)

    conn.close()

    consulta['saldo'] = 0

    return consulta


def SugerirEnderecoRestrito(numeroop,SKU ):

    validador = PesquisarSKUOP(numeroop, SKU)

    if validador['status'][0] == 'False':

        return pd.DataFrame([{'mensagem':'sem restricao de Substituto segue fluxo !', 'status':False}])

    else:


        sugestaoEndereco = PesquisaEnderecoSubstitutoVazio()

    if sugestaoEndereco.empty:

        return pd.DataFrame([{'mensagem':'Atencao! OP selecionada  como SUBSTUICAO. ',
                            'EnderecoRepor':'Solicitar para Supervisor os endereco de SKU DE SUBSTITUICAO ','status':False}])
    else:
        endereco = sugestaoEndereco['codendereco'][0]


        #Atualizar endereco com a informacao
        #PreReservarEndereco(endereco, validador['status'][0])

        return pd.DataFrame([{'mensagem':'Atencao! OP selecionada  como SKU DE SUBSTUICAO, repor nos enderecos reservados ',
                            'status':True}])


def PesquisarSKUOP(numeroop,SKU):
    conn = ConexaoPostgreMPL.conexaoEngine()

    consulta = pd.read_sql("""select resticao from "Reposicao".filareposicaoof x
                           where numeroop = %s and codreduzido = %s """, conn ,params=(numeroop,SKU))


    if consulta.empty:

        return pd.DataFrame([{'status':'False'}])
    else:

        resticao = consulta['resticao'][0]

        if resticao != '-':
            return pd.DataFrame([{'status': resticao}])

        else:
            return pd.DataFrame([{'status': 'False'}])


def PreReservarEndereco(endereco, restricao):
    conn = ConexaoPostgreMPL.conexao()

    update = 'update "Reposicao"."Reposicao".cadendereco ' \
             'set pre_reserva = %s ' \
             'where codendereco = %s '

    cursor = conn.cursor()
    cursor.execute(update,(restricao, endereco))
    conn.commit()

    cursor.close()

    conn.close()

def EnderecoPropostoSubtituicao(restricao):
    conn = ConexaoPostgreMPL.conexao()

    consulta = pd.read_sql('select codendereco from "Reposicao"."Reposicao".cadendereco '
                           'where pre_reserva = %s', conn,params=(restricao))
    conn.close()

    return consulta['codendereco'][0]

def LimprandoPréReserva(endereco):
    conn = ConexaoPostgreMPL.conexao()

    update = 'update "Reposicao"."Reposicao".cadendereco ' \
             'set reservado = pre_reserva , pre_reserva = null  ' \
             'where codendereco = %s '

    cursor = conn.cursor()
    cursor.execute(update,(endereco,))
    conn.commit()

    cursor.close()

    conn.close()


def AtualizarReservadoLiberados():
    conn = ConexaoPostgreMPL.conexao()


    update = 'update  "Reposicao"."Reposicao".cadendereco c'\
' set reservado = null '\
' where c.codendereco in ('\
' select c.codendereco from "Reposicao"."Reposicao".enderecoporsku sle' \
' right join "Reposicao"."Reposicao".cadendereco c on c.codendereco = sle.codendereco'\
" where c.endereco_subst = %s and saldo is null and reservado is not null)"

    cursor = conn.cursor()
    cursor.execute(update, ('sim'))
    conn.commit()

    cursor.close()

    conn.close()


def PesquisaEnderecoEspecial(endereco):
    conn = ConexaoPostgreMPL.conexao()

    consulta = """
    select c.endereco_subst  from "Reposicao"."Reposicao".cadendereco c 
    where c.codendereco = %s
 """

    consulta = pd.read_sql(consulta,conn,params=(endereco,))

    conn.close()


    if consulta['endereco_subst'][0] == 'sim':

        return True
    else:
        return False


def RelacaoPedidosEntregues(dataInicio, dataFinal):

    query="""
select dataseparacao::date, codpedido , cor, engenharia, resticao as "OrigemSubst" from "Reposicao"."Reposicao".tags_separacao ts2 
where ts2.codpedido|| engenharia ||cor in (
 select codpedido||engenharia||cor  from "Reposicao"."Reposicao".tags_separacao ts 
 where numeroop||cor in (select sso.numeroop||sso.cor from "Reposicao"."Reposicao"."SubstitutosSkuOP" sso where sso.considera = 'sim'))
 and dataseparacao::date >= %s and dataseparacao::date <= %s
order by codpedido , engenharia , cor 
    """
    conn = ConexaoPostgreMPL.conexao()
    consultar = pd.read_sql(query,conn,params=(dataInicio,dataFinal,))
    conn.close()
    consultar = consultar.sort_values(by=['codpedido', 'engenharia', 'cor'],
                                      ascending=False)  # escolher como deseja classificar
    consultar.fillna('-',inplace=True)

    def avaliar_grupo(df_grupo):
        return len(set(df_grupo)) == 1

    df_resultado = consultar.groupby(['codpedido', 'engenharia', 'cor'])['OrigemSubst'].apply(avaliar_grupo).reset_index()
    df_resultado.columns = ['codpedido', 'engenharia', 'cor', 'Resultado']

    consulta = pd.merge(consultar, df_resultado, on=['codpedido', 'engenharia', 'cor'], how='left')

    consultar1 = consulta[consulta['Resultado'] == False]
    consultar2 = consulta[consulta['Resultado'] == True]

    consultar1 = consultar1.drop_duplicates()  ## Elimando as possiveis duplicatas
    consultar2 = consultar2.drop_duplicates()  ## Elimando as possiveis duplicatas

    NPedidos = consultar1.loc[:, ['codpedido']]
    NPedidos = NPedidos.drop_duplicates()
    NPedidos = NPedidos['codpedido'].count()

    NPedidosok = consultar2.loc[:, ['codpedido']]
    NPedidosok = NPedidosok.drop_duplicates()
    NPedidosok = NPedidosok['codpedido'].count()


    consultar1['dataseparacao']= pd.to_datetime(consultar1['dataseparacao'],errors='coerce', infer_datetime_format=True)
    consultar1['dataseparacao'] = consultar1['dataseparacao'].dt.strftime('%d/%m/%Y')

    consultar1.drop(['Resultado',],axis=1, inplace=True)

    dados = {
        '0-Intervalo': f'{dataInicio} À {dataFinal}',
        '1-Qtd Pedidos Entregues com Divergencia': f'{NPedidos} Pedidos (Pedidos que deram certos {NPedidosok})',
        '6 -Detalhamento': consultar.to_dict(orient='records')

    }
    return pd.DataFrame([dados])


