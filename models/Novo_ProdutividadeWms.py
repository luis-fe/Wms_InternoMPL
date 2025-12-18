import pandas as pd
import pytz
from datetime import datetime

import ConexaoPostgreMPL
from connection import WmsConnectionClass


class ProdutividadeWms:
    '''Classe responsável pela interação com a produtividade do WMS, com as regras e consultas '''

    def __init__(self, codEmpresa=None, codUsuarioCargaEndereco=None,
                 endereco=None, qtdPcs=0, codNatureza=None,
                 dataInicio='', dataFim = '', tempoAtualizacao = None , nome = ''):
        self.codEmpresa = codEmpresa
        self.codUsuarioCargaEndereco = codUsuarioCargaEndereco
        self.endereco = endereco
        self.qtdPcs = qtdPcs
        self.codNatureza = codNatureza
        self.dataHora = self.__obterDataHoraSystem()
        self.dataInicio = dataInicio
        self.dataFim = dataFim
        self.tempoAtualizacao = tempoAtualizacao
        self.nome = nome

    def inserirProducaoCarregarEndereco(self):
        '''Método que insere a produtividade na ação de recarregar endereço do WMS'''

        sql = """
            INSERT INTO 
                "Reposicao"."ProducaoRecarregarEndereco"
                ("codEmpresa", "usuario_carga", "dataHoraCarga", "endereco", "qtdPcs", "codNatureza")
            VALUES 
                (%s, %s, %s, %s, %s, %s)
        """

        with WmsConnectionClass.WmsConnectionClass(self.codEmpresa).conexao() as conn:
            with conn.cursor() as curr:
                curr.execute(
                    sql,
                    (self.codEmpresa, self.codUsuarioCargaEndereco, self.dataHora, self.endereco, self.qtdPcs,
                     self.codNatureza)
                )
                conn.commit()

        return {'Status': True, 'Mensagem': 'Produtividade inserida com sucesso!'}

    def __obterDataHoraSystem(self):
        '''Método privado que obtém a data e hora do sistema com fuso horário'''
        fuso_horario = pytz.timezone('America/Sao_Paulo')
        agora = datetime.now(fuso_horario)
        return agora.strftime('%Y-%m-%d %H:%M:%S')


    def consultaProd_CarregarCaixas(self):
        '''Método que consulta a produtividade da atividade de Carregar Caixa no endereco '''


        sql = """
            select
                e."codEmpresa",
                e."usuario_carga",
                c.nome ,
                count("endereco") as "qtdCaixas",
                sum("qtdPcs") as "qtdPcs"
            from
                "Reposicao"."ProducaoRecarregarEndereco" e
            inner join
                "Reposicao"."Reposicao".cadusuarios c 
                on c.codigo::Varchar = e."usuario_carga"
            where 
                e."dataHoraCarga"::Date >= %s
                and e."dataHoraCarga"::Date <= %s
                and e."codEmpresa" = %s
            group by 
                e."codEmpresa", e."usuario_carga", c.nome
            order by 
                "qtdCaixas" desc 
        """


        conn = WmsConnectionClass.WmsConnectionClass(self.codEmpresa).conexaoEngine()

        consulta = pd.read_sql(sql, conn, params=(self.dataInicio, self.dataFim, self.codEmpresa))

        Atualizado = self.__obterDataHoraSystem()

        record = self.__recordHistorico_CarregarEndereco()
        record1 = record["qtdCaixas"][0]
        total = consulta['qtdCaixas'].sum()
        totalPcs = consulta['qtdPcs'].sum()

        self.tempoAtualizacao = 5 * 60
        self.temporizadorConsultaProdutividadeRepositorTagCaixa()

        data = {
            '0- Atualizado:':f'{Atualizado}',
            '1- Record': f'{record["nome"][0]}',
            '1.1- Record qtdCaixas': f'{record1}',
            '1.2- Record data': f'{record["dataRecord"][0]}',
            '2 Total Caixas':f'{total}',
            '2.1 Total Pcs': f'{totalPcs}',
            '3- Ranking Carregar Endereco': consulta.to_dict(orient='records')
        }
        return pd.DataFrame([data])




    def __recordHistorico_CarregarEndereco(self):
        '''Método que busca o record Historico da Atividade de Carregar Endereco'''

        sql = """
            select 
                e."codEmpresa",
                e."usuario_carga",
                c.nome ,
                count("endereco") as "qtdCaixas",
                sum("qtdPcs") as "qtdPcs",
                e."dataHoraCarga"::Date as "dataRecord"
            from
                "Reposicao"."ProducaoRecarregarEndereco" e
            inner join
                "Reposicao"."Reposicao".cadusuarios c 
                on c.codigo::Varchar = e."usuario_carga"
            where 
                 e."codEmpresa" = %s
            group by 
                e."codEmpresa", e."usuario_carga", c.nome, e."dataHoraCarga"::Date
            order by 
                "qtdCaixas" desc 
           	limit 1
        """

        conn = WmsConnectionClass.WmsConnectionClass(self.codEmpresa).conexaoEngine()

        consulta = pd.read_sql(sql, conn, params=(self.codEmpresa, ))

        return consulta


    def __inserir_produtividadeRepositorTagCaixa(self):
        '''Metodo que armazena a produtividade do repositor de tags inserida na caixa '''


        sql = """
            INSERT INTO 
                "Reposicao"."ProducaoeRepositorTagCaixa"
                ("codEmpresa", "usuario_repositorTAG", "dataHora", "NCaixa", "qtdPcs", "codNatureza")
            VALUES 
                (%s, %s, %s, %s, %s, %s)
        """

        with WmsConnectionClass.WmsConnectionClass(self.codEmpresa).conexao() as conn:
            with conn.cursor() as curr:
                curr.execute(
                    sql,
                    (self.codEmpresa, self.codUsuarioCargaEndereco, self.dataHora, self.endereco, self.qtdPcs,
                     self.codNatureza)
                )
                conn.commit()

        return {'Status': True, 'Mensagem': 'Produtividade inserida com sucesso!'}

    def temporizadorConsultaProdutividadeRepositorTagCaixa(self):
        '''Metodo que carrega e insere na tabela a Produtividade RepositorTagCaixa a cada nSegundo '''

        self.tempoAtualizacao = 60 * 5
        verificaAtualizacao = self.__atualizaInformacaoAtualizacao('temporizadorConsultaProdutividadeRepositorTagCaixa')
        print(f'status {verificaAtualizacao}')

        if verificaAtualizacao == True:
            self.__exclusaoDadosProdutividadeBiparTagCaixa()
            self.__atualizandoTagRepostasNaTabelaSeparacao()

            sql = """
                       SELECT
        rq.usuario,
        rq."Ncarrinho",
        rq.caixa,
        rq."DataReposicao"::date AS data,
        date_trunc('hour', rq."DataReposicao"::timestamp) + 
            INTERVAL '1 minute' * floor(date_part('minute', rq."DataReposicao"::timestamp) / 5) * 5 AS hora_intervalo,
        count(rq.codbarrastag) AS "qtdPcs"
    FROM
        "Reposicao"."off".reposicao_qualidade rq
    WHERE 
        rq."DataReposicao"::date = CURRENT_DATE
    GROUP BY
        rq.usuario,
        rq."Ncarrinho",
        rq.caixa,
        rq."DataReposicao"::date,
        hora_intervalo
    UNION
    SELECT
        usuario,
        'veioCaixa' AS "Ncarrinho",
        "numeroop" AS "caixa",
        "DataReposicao"::date AS data,
        date_trunc('hour', "DataReposicao"::timestamp) + 
            INTERVAL '1 minute' * floor(date_part('minute', "DataReposicao"::timestamp) / 5) * 5 AS hora_intervalo,
        count(codbarrastag) AS "qtdPcs"
    FROM
        "Reposicao"."Reposicao".tagsreposicao t 
    WHERE
        (proveniencia  LIKE '%%Veio%%' OR proveniencia IS NULL)
        AND "DataReposicao"::date = CURRENT_DATE
    GROUP BY
        t.usuario,
        "Ncarrinho",
        caixa,
        t."DataReposicao"::date,
        hora_intervalo
    UNION
    SELECT
        usuario,
        'inventario/transf' AS "Ncarrinho",
        "numeroop" AS "caixa",
        "DataReposicao"::date AS data,
        date_trunc('hour', "DataReposicao"::timestamp) + 
            INTERVAL '1 minute' * floor(date_part('minute', "DataReposicao"::timestamp) / 5) * 5 AS hora_intervalo,
        count(codbarrastag) AS "qtdPcs"
    FROM
        "Reposicao"."Reposicao".tagsreposicao t 
    WHERE
        (proveniencia NOT LIKE '%%Veio%%' OR proveniencia IS NULL)
        AND "DataReposicao"::date = CURRENT_DATE
    GROUP BY
        t.usuario,
        "Ncarrinho",
        caixa,
        t."DataReposicao"::date,
        hora_intervalo
    ORDER BY
        data, hora_intervalo
                                    """

            conn = ConexaoPostgreMPL.conexaoEngine()

            consulta = pd.read_sql(sql,conn)
            consulta['data'] =consulta['data'].astype(str)
            consulta['id'] = (consulta.groupby('data').cumcount() + 1).astype(str) + '|'+consulta['data']



            ConexaoPostgreMPL.Funcao_Inserir(consulta,consulta['Ncarrinho'].size,'ProdutividadeBiparTagCaixa','append')


    def __exclusaoDadosProdutividadeBiparTagCaixa(self):
        '''Metodo que exclui os dados do dia na tabela ProdutividadeBiparTagCaixa '''


        delete = """
        delete 
            FROM "Reposicao"."Reposicao"."ProdutividadeBiparTagCaixa" pbtc
            WHERE pbtc."data"::date = CURRENT_DATE
            and "Ncarrinho" <> 'separadoDia'
            ;
        """

        with ConexaoPostgreMPL.conexao() as conn2:
            with conn2.cursor() as curr:
                curr.execute(delete,)
                conn2.commit()




    def __atualizaInformacaoAtualizacao(self, nomeRotina = ''):
        '''Metodo que atualiza no banco de Dados Postgres a data da atualizacao '''

        sqlConsulta = """
        select 
            * 
        from 
            "Produtividade"."ControleAutomacaoProdutividade"
        where
            "Rotina" = %s
        """

        sqlInsert = """
        update 
            "Produtividade"."ControleAutomacaoProdutividade" 
        set
             "DataHora" = %s
        where
            "Rotina" = %s
        """

        conn = ConexaoPostgreMPL.conexaoEngine()
        consultaSql1 = pd.read_sql(sqlConsulta, conn ,params=(nomeRotina,))
        print(consultaSql1)
        data_hora_atual = self.__obterHoraAtual()

        if not consultaSql1.empty:
            utimaAtualizacao = consultaSql1['DataHora'][0]

            # Converte as strings para objetos datetime
            data1_obj = datetime.strptime(data_hora_atual, "%Y-%m-%d  %H:%M:%S")
            data2_obj = datetime.strptime(utimaAtualizacao, "%Y-%m-%d  %H:%M:%S")

            # Calcula a diferença entre as datas
            diferenca = data1_obj - data2_obj

            # Obtém a diferença em dias como um número inteiro
            diferenca_em_dias = diferenca.days

            # Obtém a diferença total em segundos
            diferenca_total_segundos = diferenca.total_seconds()

            if diferenca_total_segundos >= self.tempoAtualizacao:

                with ConexaoPostgreMPL.conexao() as conn2:
                    with conn2.cursor() as curr:

                        curr.execute(sqlInsert,(data_hora_atual, nomeRotina))
                        conn2.commit()

                return True

            else:
                return False


        else :

            with ConexaoPostgreMPL.conexao() as conn2:
                with conn2.cursor() as curr:
                    curr.execute(sqlInsert, (data_hora_atual, nomeRotina))
                    conn2.commit()

            return True



    def __obterHoraAtual(self):
        '''Metodo Privado que retorna a Data Hora do Sistema Operacional'''
        fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
        agora = datetime.now(fuso_horario)
        agora = agora.strftime('%Y-%m-%d %H:%M:%S')
        return agora


    def consultaConsultaProdutividadeRepositorTagCaixa(self):
        '''Método que consulta a Produtivdade de RepositorTag'''


        self.tempoAtualizacao = 5 * 60
        self.temporizadorConsultaProdutividadeRepositorTagCaixa()

        sqlMax = """
        select
	        max(hora_intervalo) as "Atualizado"
        from
	        "Reposicao"."Reposicao"."ProdutividadeBiparTagCaixa" pbtc
	        """

        conn = ConexaoPostgreMPL.conexaoEngine()
        max = pd.read_sql(sqlMax,conn)

        if max.empty:
            Atualizado = self.__obterHoraAtual()
        else:
            Atualizado = max['Atualizado'][0]

        sqlConsultaRecord= f"""
        select
	        pbtc."data", 
	        "usuario", 
	        sum("qtdPcs")as producao, 
	        c.nome
        from
	        "Reposicao"."Reposicao"."ProdutividadeBiparTagCaixa" pbtc
	    join 	
	    	"Reposicao"."Reposicao".cadusuarios c 
	    	on c.codigo::varchar = pbtc.usuario 
	    group by 
	    	pbtc."data", "usuario", c.nome
	    order by 
	    	producao desc limit 1
        """

        consultaRecord = pd.read_sql(sqlConsultaRecord,conn)


        sql = """
                   select
								distinct 
                    usuario,
                    caixa,
                    "data",
                    hora_intervalo,
                    "qtdPcs",
                    "nome"
			from
				"Reposicao"."Reposicao"."ProdutividadeBiparTagCaixa" pbtc
			join 	
                "Reposicao"."Reposicao".cadusuarios c 
                on c.codigo::varchar = pbtc.usuario 
			where
				pbtc."data" >= %s
				and pbtc."data" <= %s
        """

        consulta = pd.read_sql(sql,conn, params=(self.dataInicio, self.dataFim,))
        total = consulta['qtdPcs'].sum()
        if not consulta.empty:

            consulta['intervalo_10min'] = consulta['hora_intervalo'].dt.floor('10min')


            consulta = consulta.groupby(['nome','usuario','intervalo_10min']).agg({
                'qtdPcs':"sum"
            }).reset_index()

            consulta['ritmo'] = round(((60 * 10) / consulta['qtdPcs']), 2)
            consulta['ritimoAcum'] = consulta.groupby('usuario')['ritmo'].cumsum()

            consulta['parcial'] = consulta.groupby(['usuario']).cumcount() + 1

            # ritmoApurado: média parcial acumulada do ritmo
            consulta['ritmoApurado'] = consulta['ritimoAcum'] / consulta['parcial']
            # print(consulta)
            # Criar coluna com "bloco de 10 minutos"
            print(consulta[consulta['usuario'] == '2323'])

            # Primeiro, crie uma cópia da coluna com NaN onde ritmo >= 150
            consulta['ritmo_valido'] = consulta['ritmo'].where(consulta['ritmo'] < 150)

            # Agora calcule a média apenas com os valores válidos
            media_geral = round(
                consulta.groupby('usuario')['ritmo_valido'].transform('mean'),
                2
            )

            # apuradoGeral: média final do ritmo por usuário
            consulta['Ritmo'] = media_geral
            consulta['ritmo2'] = (
                    consulta.groupby('usuario')['ritimoAcum'].transform('max') /
                    consulta.groupby('usuario')['parcial'].transform('max')
            )

            consulta = consulta.groupby(['nome', 'usuario']).agg({
                'qtdPcs': "sum",
                'Ritmo': "first",
                'ritmo2': "first"
            }).reset_index()
            consulta = consulta.sort_values(by=['qtdPcs'],
                                            ascending=False)  # escolher como deseja classificar

            consulta.rename(columns={'qtdPcs': 'qtde', "Ritmo": "ritmo"},
                            inplace=True)

            consulta.fillna('-', inplace=True)
        else:
            consulta = consulta.groupby(['nome','usuario','hora_intervalo']).agg({
                'qtdPcs':"sum"
            }).reset_index()




        data = {
            '0- Atualizado:':f'{Atualizado}',
            '1- Record Repositor': f'{consultaRecord["nome"][0]}',
            '1.1- Record qtd': f'{consultaRecord["producao"][0]}',
            '1.2- Record data': f'{consultaRecord["data"][0]}',
            '2 Total Periodo':f'{total}',
            '3- Ranking Repositores': consulta.to_dict(orient='records')
        }

        return pd.DataFrame([data])




    def __atualizandoTagRepostasNaTabelaSeparacao(self):



        sql = """
          SELECT
        usuario,
        'separadoDia' AS "Ncarrinho",
        "numeroop" AS "caixa",
        "DataReposicao"::date AS data,
        date_trunc('hour', "DataReposicao"::timestamp) + 
            INTERVAL '1 minute' * floor(date_part('minute', "DataReposicao"::timestamp) / 5) * 5 AS hora_intervalo,
        count(codbarrastag) AS "qtdPcs"
    FROM
        "Reposicao"."Reposicao".tags_separacao  t 
    WHERE
         "DataReposicao"::date = CURRENT_DATE
          AND "DataReposicao"::timestamp > CURRENT_DATE - INTERVAL '1 day'
    GROUP BY
        t.usuario,
        "Ncarrinho",
        caixa,
        t."DataReposicao"::date,
        hora_intervalo
    ORDER BY
        data, hora_intervalo
        """



        conn = ConexaoPostgreMPL.conexaoEngine()
        consulta = pd.read_sql(sql, conn)

        if not consulta.empty :
            self.__exclussao_TagRepostasNaTabelaSeparacao()
            consulta['data'] = consulta['data'].astype(str)
            consulta['id'] = (consulta.groupby('data').cumcount() + 1).astype(str) + '|' + consulta['data']
            self.__sinalizando_N_linhasInserido_TagRepostasNaTabelaSeparacao(consulta['Ncarrinho'].size)
            ConexaoPostgreMPL.Funcao_Inserir(consulta, consulta['Ncarrinho'].size, 'ProdutividadeBiparTagCaixa', 'append')



    def __exclussao_TagRepostasNaTabelaSeparacao(self):
        '''Metodo que realiza a exclusao TEMPORARIA  do bipado do dia registrado na tabela Separacao'''


        delete = """
        delete 
            FROM "Reposicao"."Reposicao"."ProdutividadeBiparTagCaixa" pbtc
            WHERE 
                pbtc."data"::date = CURRENT_DATE
                 and "Ncarrinho" = 'separadoDia'  ;
        """

        with ConexaoPostgreMPL.conexao() as conn2:
            with conn2.cursor() as curr:
                curr.execute(delete,)
                conn2.commit()


    def __sinalizando_N_linhasInserido_TagRepostasNaTabelaSeparacao(self, Nlinhas):

        update = """
                update 
                    "Reposicao"."Produtividade"."ControleAutomacaoProdutividade" 
                set 
                    "NLinhas_BipagemSep" = %s
                where
                    "Rotina" = 'temporizadorConsultaProdutividadeRepositorTagCaixa'
        """


        with ConexaoPostgreMPL.conexao() as conn2:
            with conn2.cursor() as curr:
                curr.execute(update, (Nlinhas,))
                conn2.commit()


    def consultaSeparacaoDiariaPorUsuario(self):
        '''Metodo que consulta a separacao diaria por usuario'''

        self.tempoAtualizacao = 60 * 5
        verificaAtualizacao = self.__atualizaInformacaoAtualizacao('temporizadorConsultaProdutividadeSeparacao')


        if verificaAtualizacao == True:
            self.__exclusaoDadosProdutividadeSepararTag()


            sql = """
            SELECT
                usuario,
                "codpedido" AS "codPedido",
                "dataseparacao"::date AS data,
                date_trunc('hour', "dataseparacao"::timestamp) + 
                    INTERVAL '1 minute' * floor(date_part('minute', "dataseparacao"::timestamp) / 5) * 5 AS hora_intervalo,
                count(codbarrastag) AS "qtdPcs"
            FROM
                "Reposicao"."Reposicao".tags_separacao  t 
            WHERE
                 "dataseparacao"::date = CURRENT_DATE
                  AND "dataseparacao"::timestamp > CURRENT_DATE - INTERVAL '1 day'
            GROUP BY
                t.usuario,
                codPedido,
                t."dataseparacao"::date,
                hora_intervalo
            ORDER BY
                data, hora_intervalo
            """

            conn = ConexaoPostgreMPL.conexaoEngine()
            consulta = pd.read_sql(sql, conn)

            if not consulta.empty:

                consulta['data'] = consulta['data'].astype(str)
                consulta['id'] = (consulta.groupby('data').cumcount() + 1).astype(str) + '|' + consulta['data']
                ConexaoPostgreMPL.Funcao_Inserir(consulta, consulta['codPedido'].size, 'ProdutividadeBiparTagSeparacao',
                                                 'append')

    def consultaConsultaProdutividadeSeparadorTag(self):
        '''Método que consulta a Produtivdade de RepositorTag'''


        self.consultaSeparacaoDiariaPorUsuario()

        sqlMax = """
        select
	        max(hora_intervalo) as "Atualizado"
        from
	        "Reposicao"."Reposicao"."ProdutividadeBiparTagSeparacao" pbtc
	        """

        conn = ConexaoPostgreMPL.conexaoEngine()
        max = pd.read_sql(sqlMax,conn)

        if max.empty:
            Atualizado = self.__obterHoraAtual()
        else:
            Atualizado = max['Atualizado'][0]

        sqlConsultaRecord= f"""
        select
	        pbtc."data", 
	        "usuario", 
	        sum("qtdPcs")as producao, 
	        c.nome
        from
	        "Reposicao"."Reposicao"."ProdutividadeBiparTagSeparacao" pbtc
	    join 	
	    	"Reposicao"."Reposicao".cadusuarios c 
	    	on c.codigo::varchar = pbtc.usuario 
	    group by 
	    	pbtc."data", "usuario", c.nome
	    order by 
	    	producao desc limit 1
        """

        consultaRecord = pd.read_sql(sqlConsultaRecord,conn)


        sql = """
                   select
				*
			from
				"Reposicao"."Reposicao"."ProdutividadeBiparTagSeparacao" pbtc
			join 	
                "Reposicao"."Reposicao".cadusuarios c 
                on c.codigo::varchar = pbtc.usuario 
			where
				pbtc."data" >= %s
				and pbtc."data" <= %s
        """

        consulta = pd.read_sql(sql,conn, params=(self.dataInicio, self.dataFim,))
        total = consulta['qtdPcs'].sum()
        if not consulta.empty:

            consulta['intervalo_10min'] = consulta['hora_intervalo'].dt.floor('10min')


            consulta = consulta.groupby(['nome','usuario','intervalo_10min']).agg({
                'qtdPcs':"sum"
            }).reset_index()

            consulta['ritmo'] = round(((60 * 10) / consulta['qtdPcs']), 2)
            consulta['ritimoAcum'] = consulta.groupby('usuario')['ritmo'].cumsum()

            consulta['parcial'] = consulta.groupby(['usuario']).cumcount() + 1

            # ritmoApurado: média parcial acumulada do ritmo
            consulta['ritmoApurado'] = consulta['ritimoAcum'] / consulta['parcial']
            # print(consulta)
            # Criar coluna com "bloco de 10 minutos"
            print(consulta[consulta['usuario'] == '2323'])

            # Primeiro, crie uma cópia da coluna com NaN onde ritmo >= 150
            consulta['ritmo_valido'] = consulta['ritmo'].where(consulta['ritmo'] < 150)

            # Agora calcule a média apenas com os valores válidos
            media_geral = round(
                consulta.groupby('usuario')['ritmo_valido'].transform('mean'),
                2
            )

            # apuradoGeral: média final do ritmo por usuário
            consulta['Ritmo'] = media_geral
            consulta['ritmo2'] = (
                    consulta.groupby('usuario')['ritimoAcum'].transform('max') /
                    consulta.groupby('usuario')['parcial'].transform('max')
            )

            consulta = consulta.groupby(['nome', 'usuario']).agg({
                'qtdPcs': "sum",
                'Ritmo': "first",
                'ritmo2': "first"
            }).reset_index()
            consulta = consulta.sort_values(by=['qtdPcs'],
                                            ascending=False)  # escolher como deseja classificar

            consulta.rename(columns={'qtdPcs': 'qtde', "Ritmo": "ritmo"},
                            inplace=True)



            sqlQTDPedidos = """
            select
				usuario, 
                COUNT(DISTINCT "codPedido") AS "Qtd Pedido"
			from
				"Reposicao"."Reposicao"."ProdutividadeBiparTagSeparacao" pbtc
			join 	
                "Reposicao"."Reposicao".cadusuarios c 
                on c.codigo::varchar = pbtc.usuario 
			where
				pbtc."data" >= %s
				and pbtc."data" <= %s
			group by 
			usuario
            """

            sqlQTDPedidos = pd.read_sql(sqlQTDPedidos,conn, params=(self.dataInicio, self.dataFim,))

            consulta = pd.merge(consulta,sqlQTDPedidos,on='usuario',how='left')

            consulta['Méd pçs/ped.'] = round(consulta['qtde'] /consulta['Qtd Pedido'])

            consulta.fillna('-', inplace=True)





        else:
            consulta = consulta.groupby(['nome','usuario','hora_intervalo']).agg({
                'qtdPcs':"sum"
            }).reset_index()




        data = {
            '0- Atualizado:':f'{Atualizado}',
            '1- Record Repositor': f'{consultaRecord["nome"][0]}',
            '1.1- Record qtd': f'{consultaRecord["producao"][0]}',
            '1.2- Record data': f'{consultaRecord["data"][0]}',
            '2 Total Periodo':f'{total}',
            '3- Ranking Repositores': consulta.to_dict(orient='records')
        }

        return pd.DataFrame([data])

    def __exclusaoDadosProdutividadeSepararTag(self):
        '''Metodo que exclui os dados do dia na tabela ProdutividadeBiparTagCaixa '''


        delete = """
        delete 
            FROM "Reposicao"."Reposicao"."ProdutividadeBiparTagSeparacao" pbtc
            WHERE pbtc."data"::date = CURRENT_DATE
            ;
        """

        with ConexaoPostgreMPL.conexao() as conn2:
            with conn2.cursor() as curr:
                curr.execute(delete,)
                conn2.commit()

    def produtividade_peloHorario_colaborador(self, faixaTemporal):
        '''Metodo que desdobra a produtividade do colaborador ao longo do tempo'''

        conn = ConexaoPostgreMPL.conexaoEngine()

        sql = """
                           select
        				*
        			from
        				"Reposicao"."Reposicao"."ProdutividadeBiparTagSeparacao" pbtc
        			join 	
                        "Reposicao"."Reposicao".cadusuarios c 
                        on c.codigo::varchar = pbtc.usuario 
        			where
        				pbtc."data" >= %s
        				and pbtc."data" <= %s
        				and nome = %s
                """

        consulta = pd.read_sql(sql, conn, params=(self.dataInicio, self.dataFim,self.nome))
        print(consulta)
        print(self.nome)

        consulta['hora_intervalo'] = pd.to_datetime(consulta['hora_intervalo'], errors='coerce')
        consulta['intervalo'] = consulta['hora_intervalo'].dt.floor(f'{faixaTemporal}min')
        consulta = consulta.groupby(['nome', 'usuario', 'intervalo']).agg({
            'qtdPcs': "sum"
        }).reset_index()
        consulta['ritmo'] = round(((60 * int(faixaTemporal)) / consulta['qtdPcs']), 2)
        consulta['ritmo'] = pd.to_numeric(consulta['ritmo'], errors='coerce')

        consulta.fillna('-', inplace=True)

        return consulta


























