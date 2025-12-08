import pytz
import locale

from connection import ConexaoCSW
import ConexaoPostgreMPL
import pandas as pd
import datetime


def obterHoraAtual():
    fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horÃ¡rio do Brasil
    agora = datetime.datetime.now(fuso_horario)
    hora_str = agora.strftime('%Y-%m-%d %H:%M:%S')
    return hora_str

def RegistrarInventario(usuario, data, endereco):
    conn = ConexaoPostgreMPL.conexao()
    # VERIFICANDO SE EXISTE CODIGO DE BARRAS DUPLICADOS NA FILA
    deletar = 'delete from "Reposicao".registroinventario ' \
              " where endereco = %s and situacao = 'iniciado'"
    cursor = conn.cursor()
    cursor.execute(deletar
                   , (endereco,))
    conn.commit()

    inserir = 'insert into "Reposicao".registroinventario ("usuario","data","endereco", situacao)  '\
                        ' values(%s, %s, %s, %s) '
    cursor.execute(inserir
                   , (
                   usuario, data, endereco, 'iniciado'))

    conn.commit()
    cursor.close()
    conn.close()


def ApontarTagInventario(codbarra, endereco, usuario, padrao=False):
    conn = ConexaoPostgreMPL.conexao()

    validador, colu1, colu_epc, colu_tamanho, colu_cor, colu_eng, colu_red, colu_desc, colu_numeroop, colu_totalop, natureza, usuario_inv   = PesquisarTagPrateleira(codbarra, endereco)
    # Caso a tag estiver em inventario
    if validador == 1:
        query = 'update "Reposicao".tagsreposicao_inventario '\
            'set situacaoinventario  = '+"'OK', "+ \
            'usuario = %s, "Endereco"= %s, "usuario_inv"= %s  '\
            'where codbarrastag = %s'
        cursor = conn.cursor()
        cursor.execute(query
                       , (
                           usuario, endereco,usuario_inv,codbarra,))

        # Obter o nÃºmero de linhas afetadas
        numero_linhas_afetadas = cursor.rowcount
        conn.commit()
        cursor.close()
        conn.close()
        return pd.DataFrame({'Status Conferencia': [True], 'Mensagem': [f'tag: {codbarra} conferida!']})


    if validador == 11:
        query = 'update "Reposicao".tagsreposicao_inventario '\
            'set situacaoinventario  = '+"'OK', "+ \
            'usuario = %s, "Endereco" = %s  '\
            'where codbarrastag = %s'
        cursor = conn.cursor()
        cursor.execute(query
                       , (
                           usuario,endereco, codbarra,))

        # Obter o nÃºmero de linhas afetadas
        numero_linhas_afetadas = cursor.rowcount
        conn.commit()
        cursor.close()
        conn.close()
        return pd.DataFrame({'Status Conferencia': [True], 'Mensagem': [f'tag: {codbarra} mudado para {endereco}!']})


    if validador == False:
        conn.close()
        return pd.DataFrame({'Status Conferencia': [False], 'Mensagem': [f'tag: {codbarra} nÃ£o exite no estoque! ']})
    # caso ache na fila
    if validador ==3:
        query = 'insert into  "Reposicao".tagsreposicao_inventario ' \
                '("codbarrastag","Endereco","situacaoinventario","epc","tamanho","cor","engenharia","codreduzido","descricao","numeroop","totalop","usuario","natureza") ' \
                'values(%s,%s,'+"'adicionado do fila'"+',%s,%s,%s,%s,%s,%s,%s,%s,%s, %s)'
        cursor = conn.cursor()
        cursor.execute(query
                       , (
                           codbarra,endereco,colu_epc, colu_tamanho,colu_cor,colu_eng,colu_red,colu_desc,colu_numeroop,colu_totalop, usuario, natureza))

        # Obter o nÃºmero de linhas afetadas
        numero_linhas_afetadas = cursor.rowcount
        conn.commit()
        cursor.close()
        delete = 'Delete from "Reposicao"."filareposicaoportag"  ' \
                 'where "codbarrastag" = %s;'
        cursor = conn.cursor()
        cursor.execute(delete
                       , (
                           codbarra,))
        conn.commit()
        cursor.close()


        conn.close()
        return pd.DataFrame({'Status Conferencia': [True], 'Mensagem': [f'tag: {codbarra} veio da FilaReposicao, serÃ¡ listado ao salvar ']})
    if validador == 2 and padrao == False:

        return pd.DataFrame({'Status Conferencia': [False],
                             'Mensagem': [f'tag: {codbarra} veio de outro endereÃ§o: {colu1} , deseja prosseguir?']})
    if validador == 2 and padrao == True:
        insert = 'INSERT INTO "Reposicao".tagsreposicao_inventario ("usuario", "codbarrastag", "codreduzido", "Endereco", ' \
                 '"engenharia", "DataReposicao", "descricao", "epc", "StatusEndereco", ' \
                 '"numeroop", "cor", "tamanho", "totalop", "situacaoinventario", natureza) ' \
                 'SELECT %s, "codbarrastag", "codreduzido", %s, "engenharia", ' \
                 '"DataReposicao", "descricao", "epc", "StatusEndereco", "numeroop", "cor", "tamanho", "totalop", ' \
                 "'endereco migrado', natureza" \
                 ' FROM "Reposicao".tagsreposicao t ' \
                 ' WHERE "codbarrastag" = %s;'
        cursor = conn.cursor()
        cursor.execute(insert, (usuario, endereco, codbarra))
        conn.commit()
        cursor.close()
        delete = 'Delete from "Reposicao"."tagsreposicao"  ' \
                 'where "codbarrastag" = %s;'
        cursor = conn.cursor()
        cursor.execute(delete
                       , (
                           codbarra,))
        conn.commit()
        cursor.close()
        conn.close()
        return pd.DataFrame({'Status Conferencia': [True], 'Mensagem': [f'tag: {codbarra} veio de outro endereÃ§o, serÃ¡ listadado ao salvar']})
    else:
        return pd.DataFrame({'Status Conferencia': [False], 'Mensagem': [f'tag: {codbarra} nÃ£o exite no estoque! ']})



def PesquisarTagPrateleira(codbarra, endereco):
    conn = ConexaoPostgreMPL.conexao()

    #verificar se a tag esta na tabela de inventario
    query1 = pd.read_sql('SELECT "codbarrastag", "Endereco", usuario from "Reposicao".tagsreposicao_inventario t '
            'where codbarrastag = '+"'"+codbarra+"'",conn )
    #enderecoNovo = query1["Endereco"][0]
    if not query1.empty  :
        usuario_inv = query1['usuario'][0]
        conn.close()
        return 1, 2, 3, 4, 5, 6 ,7 ,8 , 9 , 10,11, usuario_inv

    else:
        # Procurar a tag em outras prateleiras
        query2 = pd.read_sql('select codbarrastag, "Endereco", natureza   from "Reposicao".tagsreposicao f  '
                             'where codbarrastag = ' + "'" + codbarra + "'", conn)
        if not query2.empty: #caso ache em outra prateleira

            conn.close()
            return 2, query2["Endereco"][0], 2, 2,2,2,2,2,2,2,2,''
        else:
            # procurar na fila
            query3 = pd.read_sql('select "codbarrastag","epc", "tamanho", "cor", "engenharia" , "codreduzido",  '
                                 '"descricao" ,"numeroop", "totalop", "codnaturezaatual" from "Reposicao".filareposicaoportag f  '
                                 'where codbarrastag = %s ', conn,params=(codbarra,))

            if not query3.empty: #Caso ache na fila
                conn.close()
                return 3, query3["codbarrastag"][0],query3["epc"][0],query3["tamanho"][0],query3["cor"][0],query3["engenharia"][0],query3["codreduzido"][0], \
                    query3["descricao"][0],query3["numeroop"][0],query3["totalop"][0], query3["codnaturezaatual"][0],''

            else:
                # procurar
                query4 = pd.read_sql('select "Endereco"  from "Reposicao".tagsreposicao t  '
                                     'where codbarrastag = ' + "'" + codbarra + "'", conn)
                if not query3.empty:
                    conn.close()
                    return query4["Endereco"][0], 4, 4, 4,4,4,4,4,4,4,4,''
                else:
                    conn.close()
                    return False, False, False, False, False, False, False, False, False, False, False,False
def SituacaoEndereco(endereco,usuario, data):
    conn = ConexaoPostgreMPL.conexao()
    select = 'select * from "Reposicao"."cadendereco" ce ' \
             'where codendereco = %s'
    cursor = conn.cursor()
    cursor.execute(select, (endereco, ))
    resultado = cursor.fetchall()
    cursor.close()
    if not resultado:
        conn.close()
        return pd.DataFrame({'Status Endereco': [False], 'Mensagem': [f'endereco {endereco} nao existe!']})
    else:
        saldo = Estoque_endereco(endereco)
        if saldo == 0:
            conn.close()
            RegistrarInventario(usuario, data, endereco)
            return pd.DataFrame({'Status Endereco': [True], 'Mensagem': [f'endereco {endereco} existe!'],
                                 'Status do Saldo': ['Vazio, preparado para o INVENTARIO !']})
        else:
            skus = pd.read_sql('select "codreduzido", count(codbarrastag)as Saldo  from "Reposicao".tagsreposicao t  '
                                    'where "Endereco"='+" '"+endereco+"'"+' group by "Endereco" , "codreduzido" ',conn)

            skus['enderco'] = endereco
            skus['Status Endereco'] = True
            skus['Mensagem'] = f'EndereÃ§o {endereco} existe!'
            skus['Status do Saldo']='Cheio, serÃ¡ esvaziado para o INVENTARIO'

            DetalhaSku =pd.read_sql('select "codreduzido", "codbarrastag" ,"epc"  from "Reposicao".tagsreposicao t  '
                                    'where "Endereco"='+" '"+endereco+"'",conn)

            conn.close()
            RegistrarInventario(usuario, data, endereco)

            data = {
                '2 - Endereco': f'{skus["enderco"][0]} ',
                '3 - Status Endereco': f'{skus["Status Endereco"][0]}',
                '1 - Mensagem': f'{skus["Mensagem"][0]} ',
                '4- Suituacao':f'{skus["Status do Saldo"][0]} ',
                '5- Detalhamento dos Tags:':DetalhaSku.to_dict(orient='records')
            }
            return [data]


def Estoque_endereco(endereco):
    conn = ConexaoPostgreMPL.conexao()
    consultaSql = 'select count(codbarrastag)as Saldo  from "Reposicao".tagsreposicao t  ' \
                  'where "Endereco" = %s '\
                    'group by "Endereco" '

    cursor = conn.cursor()
    cursor.execute(consultaSql, (endereco,))
    resultado = cursor.fetchall()
    cursor.close()
    if not resultado:
        return 0
    else:
        return resultado[0][0]

def SalvarInventario(endereco):
    conn = ConexaoPostgreMPL.conexao()
    DataReposicao = obterHoraAtual()



    # Avisar sobre as Tags migradas
    Aviso = pd.read_sql('SELECT * FROM "Reposicao".tagsreposicao_inventario t '
             'WHERE "Endereco" = '+ "'"+endereco+"'"+' and "situacaoinventario" is not null ;', conn)
    #Autorizar migracao

    numero_tagsMigradas = Aviso["Endereco"].size


    # Inserir de volta as tags que deram certo
    insert = 'INSERT INTO "Reposicao".tagsreposicao ("usuario", "codbarrastag", "codreduzido", "Endereco", ' \
             '"engenharia", "DataReposicao", "descricao", "epc", "StatusEndereco", ' \
             '"numeroop", "cor", "tamanho", "totalop","natureza","usuario_inv") ' \
             'SELECT distinct "usuario", "codbarrastag", "codreduzido", "Endereco", "engenharia", ' \
             ' %s ,  "descricao", "epc", "StatusEndereco", "numeroop", "cor", "tamanho", "totalop", "natureza", "usuario_inv" ' \
             'FROM "Reposicao".tagsreposicao_inventario t ' \
             'WHERE "Endereco" = %s and "situacaoinventario" = %s ;'
    cursor = conn.cursor()
    cursor.execute(insert, (DataReposicao,endereco,'OK'))
    conn.commit()
    cursor.close()

    #deletar as tag's ok

    delete = 'Delete FROM "Reposicao".tagsreposicao_inventario t ' \
             'WHERE "Endereco" = %s and "situacaoinventario" = %s;'
    cursor = conn.cursor()
    cursor.execute(delete, (endereco,'OK'))
    conn.commit()
    cursor.close()

    cursor.close()

    # inserir as tag que foram encontradas proveniente de outro lugar
    datahora = obterHoraAtual()
    insert = 'INSERT INTO "Reposicao".tagsreposicao ("usuario", "codbarrastag", "codreduzido", "Endereco", ' \
             '"engenharia", "DataReposicao", "descricao", "epc", "StatusEndereco", ' \
             '"numeroop", "cor", "tamanho", "totalop", "natureza") ' \
             'SELECT "usuario", "codbarrastag", "codreduzido", "Endereco", "engenharia", ' \
             '%s , "descricao", "epc", "StatusEndereco", "numeroop", "cor", "tamanho", "totalop", "natureza" ' \
             'FROM "Reposicao".tagsreposicao_inventario t ' \
             'WHERE "Endereco" = %s and "situacaoinventario" is not null ;'
    cursor = conn.cursor()
    cursor.execute(insert, (datahora, endereco))
    numero_linhas_afetadas = cursor.rowcount
    conn.commit()




    # Tags nao encontradas , avisar e trazer a lista de codigo barras e epc para o usuario tomar decisao
    Aviso2 = pd.read_sql('SELECT "codbarrastag", "epc" FROM "Reposicao".tagsreposicao_inventario t '
                         'WHERE "Endereco" = ' + "'" + endereco + "'" + ' and "situacaoinventario" is null;', conn)

    numero_tagsNaoEncontradas = Aviso2["codbarrastag"].size

    # deletar as tag's MIGRADAS

    deleteMigradas = 'Delete FROM "Reposicao".tagsreposicao_inventario t ' \
             'WHERE "Endereco" = %s and "situacaoinventario" is not null ;'
    cursor = conn.cursor()
    cursor.execute(deleteMigradas, (endereco,))
    conn.commit()
    cursor.close()
    cursor = conn.cursor()

    salvarRegistro = 'update "Reposicao".registroinventario ' \
                     "set situacao = 'finalizado', datafinalizacao = %s where endereco = %s and situacao = 'iniciado'"
    cursor.execute(salvarRegistro, (datahora,endereco,))
    conn.commit()
    cursor.close()
    data = {
        '1 - Tags Encontradas': f'{numero_linhas_afetadas} foram encontradas e inventariadas com sucesso',
        '2 - Tags Migradas de endereÃ§o':
            f'{numero_tagsMigradas} foram migradas para o endereÃ§o {endereco} e inventariadas com sucesso',
        '3 - Tags Nao encontradas': f'{numero_tagsNaoEncontradas} nÃ£o foram encontradas no endereÃ§o {endereco}',
        '3.1 - Listagem Tags Nao encontradas [Codigo Barras, EPC]': Aviso2.to_dict(orient='records')
    }

    return [data]

#Funcao para excluir eventuais tags que ao ser inventaria ainda conste na tabela reposicao
def ExcluirTagsDuplicadas(endereco):
    conn = ConexaoPostgreMPL.conexao()

    delete = 'delete  from "Reposicao"."Reposicao".tagsreposicao t ' \
             ' where t.codbarrastag in (' \
             ' select codbarrastag from "Reposicao"."Reposicao".tagsreposicao_inventario ti ' \
             ' where "Endereco" = %s and ti.situacaoinventario is not null )'

    cursor = conn.cursor()
    cursor.execute(delete, (endereco,))
    conn.commit()
    cursor.close()


######### FUNCAO PARA EMITIR O RELATÃ“RIO DE INVENTARIO REALIZADO ENTRE ALGUM PERÃODO

    # Parametros: 1: Data Inicio do Inventario
    # Parametro: 2: DataFinal do Inventario
    # Parametro: 3: Natureza a ser analizada
    # Parametro: 4: Empresa
    # Parametro 5: Se deseja ou Nao emitir o relatÃ³rio completo de Tag's (TRUE OR FALSE)
def RelatorioInventario(dataInicio, dataFim, natureza, empresa, emtirRelatorio):
    conn = ConexaoPostgreMPL.conexao() # Ligando a Conexao
    natureza = str(natureza) # Obtendo a natureza e transformando para string

    ## OBTENDO O TOTAL DE PEÃ‡AS NO ESTOUE :
        # 1 Consulta Sql das tags que estao repostas
    TotalPcs = pd.read_sql('select natureza, count(codbarrastag) as "totalReposicao" from "Reposicao"."Reposicao".tagsreposicao t '
                           'group by natureza ',conn)
        # 2 Consulta Sql das tags que estao em inventario
    TotalPcs_EmINVENTARIO = pd.read_sql('select codnaturezaatual as natureza, count(codbarrastag) as "totalReposicao"  from "Reposicao"."Reposicao".filareposicaoportag  ti '
                                        ' where codempresa =  %s'
                                        'group by natureza ',conn,params=(empresa,))

        # 3 Consulta Sql das tags que estao em fila
    TotalPcs_EmFila = pd.read_sql('select natureza, count(codbarrastag) as "totalReposicao" from "Reposicao"."Reposicao".tagsreposicao_inventario t '
                           'group by natureza ',conn)
        # 4 UNINDO AS CONSULTAS PARA FORMAR O TOTALPEÃ‡AS
    TotalPcs = pd.concat([TotalPcs, TotalPcs_EmINVENTARIO, TotalPcs_EmFila],ignore_index=True)

    ## Obtendo os Enderecos que foram inventariados no periodo
    inventariado = pd.read_sql('select "Endereco", natureza, codbarrastag, codreduzido  from "Reposicao"."Reposicao".tagsreposicao t '
                               'where "Endereco" in ( '
                               'select codendereco from ('
                               'select usuario , "data"::date as datainicio ,endereco as  codendereco ,situacao ,"datafinalizacao"::date as datafinalizacao  from "Reposicao"."Reposicao".registroinventario r ) as df'
                               ' where df.datainicio >= %s and df.datainicio <= %s )', conn,
                               params=(dataInicio, dataFim,))

    ## O codigo IF abaixo Ã© para validar as NATUREZAS:
    if natureza == '': # Caso a natureza for ocultada na API - TRAZ TODAS AS NATUREZAS
        sql1 = pd.read_sql('select codendereco  from "Reposicao"."Reposicao".cadendereco c  '
                      'order by codendereco ',conn)


    else: # Caso a API venha com alguma natureza no parametro
        sql1 = pd.read_sql('select codendereco  from "Reposicao"."Reposicao".cadendereco c  '
                      'where c.natureza = %s '
                      'order by codendereco ',conn,params=(natureza,))
        TotalPcs['natureza'] = TotalPcs['natureza'].astype(str)
        TotalPcs = TotalPcs[TotalPcs['natureza'] == natureza]
        inventariado = inventariado[inventariado['natureza']==natureza]



    if emtirRelatorio == True: # Caso a API solicite um relatorio GERAL
        return inventariado

    else: # Caso a API seja NORMAL : sem emissao de relatorio:



        # Obtendo o total de PEÃ‡AS e formatando o numero para tornar apresentavel
        TotalPecas = TotalPcs['totalReposicao'].sum()
        TotalPecas = "{:,.0f}".format(TotalPecas)
        TotalPecas = str(TotalPecas).replace(',', '.')

        # Obtendo o total de PEÃ‡AS_INVENTARIADAS e formatando o numero para tornar apresentavel
        invetariadoPecas = inventariado['Endereco'].count()
        invetariadoPecas = "{:,.0f}".format(invetariadoPecas)
        invetariadoPecas = str(invetariadoPecas).replace(',', '.')

        #STATUS de peÃ§as Inventariadas:
        statusGeral = invetariadoPecas+'/'+TotalPecas

        sql1['rua'] = sql1['codendereco'].str.split('-').str[0]

        sql2 = pd.read_sql('select * from ('
                           'select usuario , "data"::date as datainicio ,endereco as  codendereco ,situacao ,"datafinalizacao"::date as datafinalizacao  from "Reposicao"."Reposicao".registroinventario r ) as df'
                           ' where df.datainicio >= %s and df.datainicio <= %s ',conn,params=(dataInicio,dataFim,))



        sql2['ocorrencia'] = sql2.groupby('codendereco').cumcount() + 1
        sql2 = sql2[sql2['ocorrencia'] ==1]




        sql= pd.merge(sql1, sql2, on='codendereco', how='left')
        sql.fillna('-', inplace=True)


        sql['finalizado'] = sql.apply(lambda row: 1 if row['situacao'] == 'finalizado' else 0 , axis=1)

        sql = sql.groupby(['rua']).agg({
                'rua':'first',
                'codendereco': 'count',
            'finalizado':'sum'
            })

        sql.rename(
            columns={'codendereco': 'Qtd Prat.','finalizado':'status','rua':'Rua'},
            inplace=True)

        sql['% Realizado'] = sql['status']/sql['Qtd Prat.']
        sql['% Realizado'] = sql['% Realizado']  *100
        # Funcao utilizada para formatar numero no PANDAS
        def format_with_separator(value):
            return locale.format('%0.2f', value, grouping=True)
        sql['% Realizado'] = sql['% Realizado'].apply(format_with_separator)
        sql['% Realizado'] = sql['% Realizado'] .astype(str) + '%'

        def format_with_separator2(value):
            return locale.format('%0.0f', value, grouping=True)



        # Obtendo o total GERAL de endereÃ§os e formatando o numero para tornar apresentavel
        totalEnderecos = sql['Qtd Prat.'].sum()
        sql['Qtd Prat.'] = sql['Qtd Prat.'].apply(format_with_separator2).str.replace(',','.')

        totalEnderecos = "{:,.0f}".format(totalEnderecos)
        totalEnderecos = str(totalEnderecos).replace(',','.')

        # Obtendo o total de endereÃ§os INVENTARIADOS e formatando o numero para tornar apresentavel
        Enderecos_inv = sql['status'].sum()
        Enderecos_inv = "{:,.0f}".format(Enderecos_inv)
        Enderecos_inv = str(Enderecos_inv).replace(',', '.')

        # Obtendo o % de peÃ§as  Inventariadas e formatando o numero para tornar apresentavel
        PerInventarioPecas = (inventariado['Endereco'].count() / TotalPcs['totalReposicao'].sum())*100
        PerInventarioPecas = "{:,.0f}".format(PerInventarioPecas)
        PerInventarioPecas = str(PerInventarioPecas).replace(',', '.')+'%'

        sql['status'] = sql['status'].astype(str) + '/' + sql['Qtd Prat.'].astype(str)


        data = {
            '3 - Total Enderecos': f'{totalEnderecos} ',
            '4- Enderecos Inventariados':f'{Enderecos_inv}',
            '1: Total de PeÃ§as':f'{TotalPecas}',
            '2- PÃ§s Inventariadas':f'{invetariadoPecas}',
            '2.1- Status Getal Inventario': f'{statusGeral}',
            '2.2- % PÃ§s Inventariadas': f'{PerInventarioPecas}',
            '5- Detalhamento Ruas:': sql.to_dict(orient='records')
        }
        return pd.DataFrame([data])

### FUNCAO PARA LIMPAR AS TAGS DO ESTOQUE

def LimparTagsSaidaForaWms(situacao, empresa, natureza):


    # Tratando a situacao da tag para acrescentar na consulta SQL
    if situacao == '3' or situacao == '8':
        situacao = '('+"'"+situacao+"'"+')'

    elif situacao == '3,8':
        situacao = "('3','8')"
    else:
        situacao = "('3','8')"

    empresa2 = "'"+empresa+"'"
    natureza2 = "'"+natureza+"'"


    # Estabelece uma Conexao com o CSW
    conn = ConexaoCSW.Conexao()

    consultar = pd.read_sql('SELECT t.codBarrasTag as  codbarrastag, situacao as situacao_csw FROM Tcr.TagBarrasProduto t '
                            'where t.situacao in '+situacao + ' and t.codempresa = '+ empresa2+" and  codNaturezaAtual = "+natureza2 ,conn)
    consultar['saida'] = 'nao'

    conn.close()


    # Fazer a consulta comparativa com as tags do Wms
    conn2 = ConexaoPostgreMPL.conexao()

    FILA = pd.read_sql('select codbarrastag from "Reposicao".filareposicaoportag '
                       'where codempresa = %s and codnaturezaatual = %s ',conn2,params=(empresa,natureza,))
    FILA['situacao'] = 'na fila'

    REPOSICAO = pd.read_sql('select codbarrastag, usuario, "DataReposicao" as datareposicao from "Reposicao".tagsreposicao t '
                            'where t.natureza = %s ', conn2,params=(natureza,))
    REPOSICAO['situacao'] = 'reposicao'
    INVENTARIO = pd.read_sql('select codbarrastag, usuario, "DataReposicao" as datareposicao from "Reposicao".tagsreposicao_inventario t '
                            'where t.natureza = %s ', conn2,params=(natureza,))
    INVENTARIO['situacao'] = 'inventario'



    FILA = pd.concat([FILA,REPOSICAO, INVENTARIO],ignore_index=True)



    consultar = pd.merge(consultar, FILA, on ='codbarrastag', how='right')
    consultar.fillna('-', inplace=True)
    consultar = consultar[consultar['saida'] == '-']
    dataHota = obterHoraAtual()
    consultar['data_saida'] =  dataHota
    consultar['natureza_saida'] = natureza

    #Inserindo as Tags com Saida AVULSA no WMS
    tamanho =  consultar['saida'].count()
    if tamanho > 0:
        ConexaoPostgreMPL.Funcao_Inserir(consultar, tamanho, 'saida_avulsa', 'append')
    else:
        print('sem tags')



    #Deletando tag da Tabela Inventario
    inventario_deletar = consultar[consultar['situacao']=='inventario']
    if not inventario_deletar.empty:
        novo = inventario_deletar[['codbarrastag']]
        novo = novo.drop_duplicates(subset=['codbarrastag'])

        # Passo 3: Transformar o dataFrame em lista
        resultado = '({})'.format(', '.join(["'{}'".format(valor) for valor in novo['codbarrastag']]))

        deletar = 'delete from  "Reposicao".tagsreposicao_inventario ' \
                  'where codbarrastag in '+resultado

        cursor = conn2.cursor()

        cursor.execute(deletar
                       , ())

        conn2.commit()
        cursor.close()
    else:
        print('sem tag em inventario para deletar')

    #Deletando tag da Tabela reposicao
    reposicao_deletar = consultar[consultar['situacao']=='reposicao']
    if not reposicao_deletar.empty:
        novo = reposicao_deletar[['codbarrastag']]
        novo = novo.drop_duplicates(subset=['codbarrastag'])

        # Passo 3: Transformar o dataFrame em lista
        resultado = '({})'.format(', '.join(["'{}'".format(valor) for valor in novo['codbarrastag']]))

        deletar = 'delete from  "Reposicao".tagsreposicao ' \
                  'where codbarrastag in '+resultado

        cursor = conn2.cursor()

        cursor.execute(deletar
                       , ())

        conn2.commit()
        cursor.close()
    else:
        print('sem tag em REPOSICAO para deletar')

    #Deletando tag da Tabela filareposicao
    fila_deletar = consultar[consultar['situacao']=='na fila']
    if not fila_deletar.empty:
        novo = fila_deletar[['codbarrastag']]
        novo = novo.drop_duplicates(subset=['codbarrastag'])

        # Passo 3: Transformar o dataFrame em lista
        resultado = '({})'.format(', '.join(["'{}'".format(valor) for valor in novo['codbarrastag']]))

        deletar = 'delete from  "Reposicao".filareposicaoportag ' \
                  'where codbarrastag in '+resultado

        cursor = conn2.cursor()

        cursor.execute(deletar
                       , ())

        conn2.commit()
        cursor.close()
    else:
        print('sem tag em FILA para deletar')

    return consultar










