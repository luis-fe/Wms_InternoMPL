import gc
from datetime import datetime
import pandas as pd
import pytz
from connection import ConexaoCSW
import ConexaoPostgreMPL


class ReposicaoViaOFF():
    """Classe do WMS responsavel pela reposicao via OFF (antes das tag entrar em estoque), atribuindo tag a um Ncaixa e a NCarrinho """

    def __init__(self, codbarrastag, Ncaixa=None, empresa=None, usuario=None, natureza=None, estornar=False,
                 Ncarrinho='', numeroOP=None, codreduzido=None):
        self.codbarrastag = str(codbarrastag)
        self.codbarrasPesquisa = "'" + self.codbarrastag + "'"

        self.Ncaixa = Ncaixa
        self.Ncarrinho = Ncarrinho
        self.empresa = str(empresa)
        self.usuario = usuario
        self.natureza = natureza
        self.estornar = estornar
        self.numeroOP = numeroOP
        self.codreduzido = codreduzido

    def apontarTagCaixa(self):
        '''Metodo criado para apontar a tag x Caixa x Ncarrinho '''

        usuario = self.usuario.strip()

        # 2 - Validar se a Tag ja está sincronizada com o banco WMS
        pesquisa = self.consultaTagOFFWMS()

        # 3 - retornando if de acordo com a respostas:

        ## Caso nao for encontrado tag, é feito uma pesquisada direto do CSW para recuperar a tag , porem ela deve estar nas situacoes 0 ou 9:
        if pesquisa.empty and self.estornar == False:
            conn2 = ConexaoCSW.Conexao()
            consultaCsw = self.buscarTagCsw()

            if not consultaCsw.empty:

                consultaCsw['usuario'] = usuario
                consultaCsw['caixa'] = self.Ncaixa
                consultaCsw['natureza'] = self.natureza
                consultaCsw['DataReposicao'] = self.dataHora()
                consultaCsw['resticao'] = 'veio csw'
                consultaCsw['Ncarrinho'] = self.Ncarrinho

                self.InculirTagCaixa(consultaCsw)
                conn2.close()

                return pd.DataFrame([{'status': True, 'Mensagem': 'tag inserido !'}])

            else:

                conn2.close()
                print('tag nao existe na tablea filareposicaooff ')

                return pd.DataFrame([{'status': False, 'Mensagem': f'tag {self.codbarrastag} nao encontrada !'}])

        else:
            # Caso a tag for encontrada na fila de reposicao da qualidade:

            ## Aqui complementamos no DataFrame "pesquisa" as informacoes de usuario, Ncaixa, caixa e Data e Hora
            pesquisa['usuario'] = usuario
            pesquisa['caixa'] = self.Ncaixa
            pesquisa['natureza'] = self.natureza
            pesquisa['DataReposicao'] = self.dataHora()
            pesquisa['Ncarrinho'] = self.Ncarrinho

            VerificandoExitenciaCaixa = self.PesquisarSeTagJaFoiBipada()  # Nessa etapa é conferida se a Tag ja foi ou nao bipada

            # Caso a tag ainda nao esteja bipada, aprova a insercao !:
            if VerificandoExitenciaCaixa == 1 and self.estornar == False:
                self.InculirTagCaixa(pesquisa)  #
                return pd.DataFrame([{'status': True, 'Mensagem': 'tag inserido !'}])

            # Caso a tag ja tenha sido bipado, avisa ao usuario :
            elif VerificandoExitenciaCaixa == 2 and self.estornar == False:
                return pd.DataFrame(
                    [{'status': False,
                      'Mensagem': f'tag {self.codbarrastag} ja bipado nessa caixa, deseja estornar ?'}])
            elif self.estornar == False and VerificandoExitenciaCaixa != 2:
                return pd.DataFrame(
                    [{'status': False,
                      'Mensagem': f'tag {self.codbarrastag} ja bipado em outra  caixa de n°{VerificandoExitenciaCaixa}, deseja estornar ?'}])
            else:
                estorno = self.EstornarTag()
                return estorno

    def consultaTagOFFWMS(self):
        # Estabelece a conexão com o banco de dados
        engine = ConexaoPostgreMPL.conexaoEngine()

        # Realiza a consulta SQL de maneira segura, usando parâmetros para evitar SQL Injection
        query = '''
               SELECT * 
               FROM "Reposicao".off.filareposicaoof 
               WHERE codbarrastag = %s
               AND codempresa = %s
           '''
        pesquisa = pd.read_sql(query, engine, params=(self.codbarrastag, self.empresa))

        # Retorna os dados consultados
        return pesquisa

    def buscarTagCsw(self):
        '''Metodo utilizado para buscar a tag direto do Csw'''

        consulta = """
            SELECT 
                p.codBarrasTag as codbarrastag , 
                p.codReduzido as codreduzido, 
                p.codEngenharia as engenharia,
                (select i.nome from cgi.Item i WHERE i.codigo = p.codReduzido) as descricao, situacao, codNaturezaAtual as natureza, 
                codEmpresa as codempresa,
                (select s.corbase||'-'||s.nomecorbase  from tcp.SortimentosProduto s WHERE s.codempresa = 1 and s.codproduto = p.codEngenharia and s.codsortimento = p.codSortimento)
                as cor, 
                (select t.descricao from tcp.Tamanhos t WHERE t.codempresa = 1 and t.sequencia = p.seqTamanho ) as tamanho, p.numeroOP as numeroop
            from 
                Tcr.TagBarrasProduto p 
            WHERE 
                p.codEmpresa = '""" + self.empresa + """' and situacao in (0, 9) and codbarrastag = """ + self.codbarrasPesquisa

        with ConexaoCSW.Conexao2() as conn:
            with conn.cursor() as cursor:
                cursor.execute(consulta)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)
        del rows
        gc.collect()

        return consulta

    def dataHora(self):
        '''Metodo que retorna a data e hora atual'''

        fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
        agora = datetime.now(fuso_horario)
        hora_str = agora.strftime('%Y-%m-%d %H:%M:%S')

        return hora_str

    ## Funcao que insere os dados na tabela "Reposicao".off.reposicao_qualidade , persistindo os dados com as tags bipada na caixa
    def InculirTagCaixa(self, dataframe):

        ## Removendo duplicatas do dataframe:
        dataframe = dataframe.drop_duplicates(subset=['codbarrastag'])  ## Elimando as possiveis duplicatas

        conn = ConexaoPostgreMPL.conexao()

        cursor = conn.cursor()  # Crie um cursor para executar a consulta SQL
        insert = """
                    insert into off.reposicao_qualidade 
                        (
                        codbarrastag, 
                        codreduzido, 
                        engenharia, 
                        descricao, 
                        natureza, 
                        codempresa, 
                        cor, 
                        tamanho, 
                        numeroop, 
                        caixa, 
                        usuario, 
                        "DataReposicao", 
                        resticao, 
                        "Ncarrinho")
                     values 
                        ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )"""

        values = [(row['codbarrastag'], row['codreduzido'], row['engenharia'], row['descricao']
                   , row['natureza'], row['codempresa'], row['cor'], row['tamanho'], row['numeroop'], row['caixa'],
                   row['usuario'], row['DataReposicao'], row['resticao'], row['Ncarrinho']) for index, row in
                  dataframe.iterrows()]
        cursor.executemany(insert, values)
        conn.commit()  # Faça o commit da transação
        cursor.close()  # Feche o cursor

        conn.close()

    def PesquisarSeTagJaFoiBipada(self):
        '''Funcao que retorna se a tag ja foi ou não bipada em alguma caixa'''

        conn = ConexaoPostgreMPL.conexao()
        consulta = pd.read_sql('select caixa  from "off".reposicao_qualidade rq'
                               ' where rq.codbarrastag = ' + self.codbarrasPesquisa, conn)
        conn.close()

        if consulta.empty:
            return 1  # Optei por retorna valores ao inves de booleano
        else:
            caixaAntes = consulta['caixa'][0]  # Aqui retornamos o numero da caixa que essa tag foi bipada

            # Caso 1 : a caixa anterior é a mesma caixa que ele está tendando bipar
            if caixaAntes == str(self.Ncaixa):
                return 2  # Optei por retorna valores ao inves de booleano
            # Caso 2 : a caixa anterior nao é a mesma que o usuario está tentando bipar:
            else:
                return consulta['caixa'][0]  # Retorna a Nova Caixa

    def EstornarTag(self):
        conn = ConexaoPostgreMPL.conexao()
        delete = 'delete from "off".reposicao_qualidade ' \
                 'where codbarrastag  = ' + self.codbarrasPesquisa
        cursor = conn.cursor()
        cursor.execute(delete, )
        conn.commit()
        cursor.close()
        conn.close()

        return pd.DataFrame([{'status': True, 'Mensagem': 'tag estornada! '}])

    def consultaCaixa(self):
        '''Metodo utilizado para detalhar uma caixa em especifico '''

        conn = ConexaoPostgreMPL.conexaoEngine()
        consultarCAIXA = pd.read_sql(
            'select rq.codbarrastag , rq.codreduzido, rq.engenharia, rq.descricao, rq.natureza, rq."Ncarrinho", '
            'rq.codempresa, rq.cor, rq.tamanho, rq.numeroop, rq.usuario, rq."DataReposicao"  from "off".reposicao_qualidade rq  '
            " where rq.caixa = %s ", conn, params=(self.Ncaixa,))
        if consultarCAIXA.empty:
            return pd.DataFrame({'mensagem': ['caixa vazia'], 'codbarrastag': '', 'numeroop': '', 'status': True})
        else:

            # Obtenod a quantidade de peças por reduzido e o Total Geral
            consultarCAIXA, totalOP = self.get_quantidadeOP_Sku(consultarCAIXA)

            # Organizando as informacoes
            self.numeroOP = consultarCAIXA['numeroop'][0]
            codempresa = consultarCAIXA['codempresa'][0]
            self.codreduzido = consultarCAIXA['codreduzido'][0]
            descricao = consultarCAIXA['descricao'][0]
            self.Ncarrinho = consultarCAIXA['Ncarrinho'][0]
            cor = consultarCAIXA['cor'][0]
            eng = consultarCAIXA['engenharia'][0]
            tam = consultarCAIXA['tamanho'][0]
            consultarCAIXA.fillna('Nao Iniciado', inplace=True)
            totalPcSku = consultarCAIXA['total_pcs'][0]

            consultarCAIXA.drop(['numeroop', 'codempresa', 'codreduzido', 'descricao', 'cor', 'engenharia', 'tamanho',
                                 'total_pcs']
                                , axis=1, inplace=True)

            totalbipagemOP, totalbipagemSku = self.totalBipado(True)

            data = {

                '0- mensagem ': 'Caixa Cheia',
                '001-NCarrinho': str(self.Ncarrinho),
                '01- status': False,
                '02- Empresa': codempresa,
                '03- numeroOP': self.numeroOP,
                '04- totalOP': totalOP,
                '05- totalOPBipado': totalbipagemOP,
                '06- engenharia': eng,
                '07- codreduzido': self.codreduzido,
                '08- descricao': descricao,
                '09- cor': cor,
                '10- tamanho': tam,
                '11- totalpçsSKU': totalPcSku,
                '12- totalpcsSkuBipado': totalbipagemSku,
                '13- Tags da Caixa ': consultarCAIXA.to_dict(orient='records')
            }
            return [data]

    def get_quantidadeOP_Sku(self, dataFrame):

        # 1 - Especifica no DataFrame a coluna numeroOP e em seguida remover as duplicadas
        novo = dataFrame[['numeroop']]
        novo = novo.drop_duplicates(subset=['numeroop'])

        # 2: Transformar o dataFrame em lista
        resultado = '({})'.format(', '.join(["'{}'".format(valor) for valor in novo['numeroop']]))

        # 3 filtrar as OPs especificadas para obter a quantidade:
        conn = ConexaoPostgreMPL.conexaoPCP()
        get = pd.read_sql('SELECT  codreduzido, total_pcs '
                          'FROM "PCP".pcp.ordemprod o '
                          "WHERE numeroop IN " + resultado, conn)
        conn.close()

        totalGeral = get["total_pcs"].sum()
        totalGeral = int(totalGeral)
        get = pd.merge(dataFrame, get, on='codreduzido', how='left')

        return get, totalGeral

    def totalBipado(self, agrupado):

        conn = ConexaoPostgreMPL.conexao()
        consulta = pd.read_sql(
            'select numeroop, rq.codreduzido, rq.cor as  "codSortimento", tamanho, count(codreduzido) as "Qtbipado"  from "Reposicao"."off".reposicao_qualidade rq '
            'where rq.codempresa  = %s and numeroop = %s group by numeroop, codreduzido, cor, tamanho',
            conn, params=(self.empresa, self.numeroOP,))
        conn.close()

        # Totaliza o total de OPs bipada
        totalBipadoOP = consulta['numeroop'].count()

        if agrupado == True:
            totalSku = consulta[consulta['codreduzido'] == self.codreduzido]
            totalSku = totalSku['Qtbipado'].sum()

            return totalBipadoOP, totalSku
        else:

            consulta['codSortimento'] = consulta['codSortimento'].str.split('-').str[0]
            consulta['sortimentosCores'] = consulta['codSortimento']
            consulta.drop('codSortimento'
                          , axis=1, inplace=True)

            return consulta, totalBipadoOP

    def qtdCaixaPorCarrinho(self):
        '''Metodo que resume a quantidade de Caixa x NCarrinho'''

        sql = """
        select
            "Ncarrinho",
            count(DISTINCT caixa) as QtdCaixa
        from
	        "off".reposicao_qualidade rq
	    where 
	        rq.codempresa  = '1' and (rq."statusNCarrinho" <> 'liberado' or rq."statusNCarrinho" is null)
        group by 
            "Ncarrinho"
        order by 
            "Ncarrinho" asc
        """

        conn = ConexaoPostgreMPL.conexaoEngine()
        consulta = pd.read_sql(sql, conn, params=(self.empresa,))
        consulta.fillna('-', inplace=True)
        nomeCarrinho = self.nomeUsuarioCarrinho()
        consulta = pd.merge(consulta, nomeCarrinho, on='Ncarrinho', how='left')
        consulta.fillna('-', inplace=True)

        return consulta

    def consulaDetalharCarrinho(self):
        '''Metodo que detalha um NCarrinho:
        NCarrinho , [caixas: OP : qtdPcas]
        total caixas
        total OP
        total pecas
        '''

        sql = """
        select
            "Ncarrinho" ,
            caixa,
            numeroop,
            codreduzido as SKU,
            count(codbarrastag)as "qtdPcas"
        from
            "off".reposicao_qualidade rq
        where
            rq."Ncarrinho" = %s and rq.codempresa = %s and (rq."statusNCarrinho" <> 'liberado' or rq."statusNCarrinho" is null)
        group by
            "Ncarrinho" ,
            caixa,
            numeroop,
            codreduzido
        """

        conn = ConexaoPostgreMPL.conexaoEngine()
        consulta = pd.read_sql(sql, conn, params=(self.Ncarrinho, self.empresa))
        consulta.fillna('-', inplace=True)

        nomeCarrinho = self.nomeUsuarioCarrinho()
        consulta = pd.merge(consulta, nomeCarrinho, on='Ncarrinho', how='left')
        consulta.fillna('-', inplace=True)

        if not consulta.empty:

            TotalCaixa = consulta['caixa'].nunique()
            TotalOps = consulta['numeroop'].nunique()
            TotalPcs = consulta['qtdPcas'].sum()

            dados = {
                '1-TotalCaixas': TotalCaixa,
                '2-Total Ops': TotalOps,
                '3-Total Pcs': TotalPcs,
                '4 -Detalhamento': consulta.to_dict(orient='records')

            }
            return pd.DataFrame([dados])
        else:
            dados = {
                '1-TotalCaixas': '-',
                '2-Total Ops': '-',
                '3-Total Pcs': '-',
                '4 -Detalhamento': consulta.to_dict(orient='records')

            }
            return pd.DataFrame([dados])

    def liberarCarrinho(self):
        '''Metodo utilizado para liberar o carrinho'''

        update = """
        update 
            "off".reposicao_qualidade 
        set 
            "statusNCarrinho" = 'liberado'
        where
            "Ncarrinho" = %s and codempresa = %s 
        """

        with ConexaoPostgreMPL.conexao() as conn:
            with conn.cursor() as curr:
                curr.execute(update, (self.Ncarrinho, self.empresa))
                conn.commit()

                return pd.DataFrame([{'status': True, 'mensagem': 'Carrinho liberado com sucesso'}])

    def nomeUsuarioCarrinho(self):
        '''Metodo que verifica o usuario do carrinho'''

        sql = '''
        select
            "Ncarrinho",
            c.nome
        from
	        "off".reposicao_qualidade rq
	    INNER JOIN 
	        "Reposicao"."Reposicao".cadusuarios c on C.codigo::VARCHAR = RQ.usuario 
	    where 
	        rq.codempresa  = '1' 
	        and (rq."statusNCarrinho" <> 'liberado' or rq."statusNCarrinho" is null) 
	        and ("Ncarrinho" is not null and "Ncarrinho" <> '')
        group by 
            "Ncarrinho" ,
            c.nome
        order by 
            "Ncarrinho" asc
        '''

        conn = ConexaoPostgreMPL.conexaoEngine()
        consulta = pd.read_sql(sql, conn, params=(self.Ncarrinho, self.empresa))

        # Mantendo apenas a primeira ocorrência de cada valor em col1
        consulta = consulta.drop_duplicates(subset="Ncarrinho", keep="first").reset_index(drop=True)
        consulta.fillna('-', inplace=True)

        return consulta

    def registrarTagsOFArray(self, arrayTags):
        '''Metodo para registrar na Reposicao OFF as tags via Array'''

        try:
            pesquisa = pd.DataFrame(arrayTags, columns=['codbarrastag'])  # Define o nome da coluna como 'Tag'

            pesquisa['usuario'] = self.usuario
            pesquisa['caixa'] = self.Ncaixa
            pesquisa['natureza'] = self.natureza
            pesquisa['DataReposicao'] = self.dataHora()
            pesquisa['Ncarrinho'] = self.Ncarrinho
            pesquisa['codempresa'] = self.empresa

            ## Removendo duplicatas do dataframe:
            pesquisa = pesquisa.drop_duplicates(subset=['codbarrastag'])  ## Elimando as possiveis duplicatas

            conn = ConexaoPostgreMPL.conexao()

            cursor = conn.cursor()  # Crie um cursor para executar a consulta SQL
            insert = """
                        insert into off.reposicao_qualidade 
                            (
                            codbarrastag, 
                            natureza, 
                            codempresa, 
                            caixa, 
                            usuario, 
                            "DataReposicao", 
                            "Ncarrinho")
                         values 
                            (  %s, %s, %s, %s, %s, %s, %s )"""

            values = [(row['codbarrastag']
                       , row['natureza'], row['codempresa'], row['caixa'],
                       row['usuario'], row['DataReposicao'], row['Ncarrinho']) for index, row in
                      pesquisa.iterrows()]
            cursor.executemany(insert, values)
            conn.commit()  # Faça o commit da transação
            cursor.close()  # Feche o cursor

            conn.close()

            return pd.DataFrame([{'Mensagem': 'Tags Registradas com sucesso', 'status': True}])
        except:

            query = f"""
            SELECT codbarrastag
            FROM off.reposicao_qualidade 
            WHERE codbarrastag = ANY(ARRAY{arrayTags}::text[]);
            """
            conn = ConexaoPostgreMPL.conexaoEngine()
            consulta = pd.read_sql(query,conn)

            tag_array = consulta['codbarrastag'].to_list()

            return pd.DataFrame([{'Mensagem':f'Tags{tag_array} ja possuem registro','status':False}])

    def consultarTags_OP_rdz(self):
        '''Metodo utilizado para obter as tags e o reduzido '''

        sql = """
        select
            f.codbarrastag
        from
            "off".filareposicaoof f
        where
            f.numeroop = %s
            and codempresa = %s
            and codreduzido = %s
        """

        conn = ConexaoPostgreMPL.conexaoEngine()
        consulta = pd.read_sql(sql, conn, params=(self.numeroOP, self.empresa, self.codreduzido,))

        return consulta

    def obterOPReduzido(self):
        '''Metodo utilizado para consultar o reduzido e a OP apartir da primeira tag'''

        sql = """
        select
            f.codreduzido,
            f.engenharia,
            f.cor,
            f.tamanho,
            f.numeroop ,
            f.descricao  
        from
            "off".filareposicaoof f
        where
            f.codbarrastag = %s
        """

        conn = ConexaoPostgreMPL.conexaoEngine()
        consulta = pd.read_sql(sql, conn, params=(self.codbarrastag,))

        return consulta

    def informcaoCaixaDetalhado(self):
        conn1 = ConexaoPostgreMPL.conexaoEngine()  # Abrindo a Conexao com o Postgre WMS
        consulta = pd.read_sql(
            'select rq.caixa, rq.codbarrastag , rq.codreduzido, rq.engenharia, rq.descricao, rq.natureza'
            ', rq.codempresa, rq.cor, rq.tamanho, rq.numeroop, rq.usuario, rq."DataReposicao", resticao as restricao  from "off".reposicao_qualidade rq  '
            "where rq.caixa = %s and rq.empresa = %s ", conn1, params=(self.Ncaixa, self.empresa))

        if consulta.empty:
            return pd.DataFrame([{'caixa': 'vazia', 'codreduzido': '-'}])

        else:

            return consulta  # NumeroCaixa, codbarras, codreduzido, engenharia, descricao, natureza, emoresa, cor , tamanho , OP , usuario , DataReposicao, restricao









