import pandas as pd
import ConexaoPostgreMPL


# CLASSE COM AS FUNÇOES PARA INTERAGIR COM AS APIS DE ACESSO DA "REPOSICAO"
def ApontarTagReduzido(codbarra,endereco,usuario,dthora, Prosseguir = 0, empresa = '1', natureza = '5'):

    pesquisa, epc, colu_tamanho, colu_cor, colu_eng,colu_red, colu_desc,\
                           colu_numeroop, colu_totalop, enderecoAntes = PesquisarTagPrateleira(codbarra)
    if pesquisa == 1 and Prosseguir ==0:
        conn = ConexaoPostgreMPL.conexao()
        query = 'insert into  "Reposicao".tagsreposicao ' \
                '("codbarrastag","Endereco","epc","tamanho","cor","Engenharia","codreduzido","descricao","numeroop","totalop","usuario",proveniencia, natureza) ' \
                'values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
        cursor = conn.cursor()
        cursor.execute(query
                       , (
                           codbarra, endereco, epc, colu_tamanho, colu_cor, colu_eng, colu_red, colu_desc,
                           colu_numeroop, colu_totalop, usuario,'veio da fila: reposicao por SKU',natureza))

        # Obter o número de linhas afetadas
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
        return pd.DataFrame({'Status': [True], 'Mensagem': [f'tag: {codbarra} veio da FilaReposicao, e foi salvo endereço {endereco}']})
    if pesquisa == 2 and Prosseguir ==0:
        conn = ConexaoPostgreMPL.conexao()
        query = 'insert into  "Reposicao".tagsreposicao ' \
                '("codbarrastag","Endereco","situacaoinventario","epc","tamanho","cor","Engenharia","codreduzido","descricao","numeroop","totalop","usuario", natureza, proveniencia) ' \
                'values(%s,%s,' + "'adicionado do fila'" + ',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
        cursor = conn.cursor()
        cursor.execute(query
                       , (
                           codbarra, endereco, epc, colu_tamanho, colu_cor, colu_eng, colu_red, colu_desc,
                           colu_numeroop, colu_totalop, usuario, natureza, 'veio do Inventario'))

        # Obter o número de linhas afetadas
        numero_linhas_afetadas = cursor.rowcount
        conn.commit()
        cursor.close()
        delete = 'Delete from "Reposicao"."tagsreposicao_inventario"  ' \
                 'where "codbarrastag" = %s;'
        cursor = conn.cursor()
        cursor.execute(delete
                       , (
                           codbarra,))
        conn.commit()
        cursor.close()

        conn.close()
        return pd.DataFrame(
            {'Status': [True], 'Mensagem': [f'tag: {codbarra} veio de um inventario aberto, e foi salvo endereço {endereco}']})
    if pesquisa == 3 and Prosseguir ==0:
        return pd.DataFrame({'Status': [False], 'Mensagem': [
            f'tag: {codbarra} tag ja esta endereçada no endereço {enderecoAntes} deseja estornar ?']})
    if Prosseguir ==True and pesquisa == 3:
        EstornoApontamento(codbarra,usuario)
        return pd.DataFrame({'Status': [False], 'Mensagem': [f'tag: {codbarra} ,nao esta localizada no estoque, conferir com o supervisor.']})
    else:
        return pd.DataFrame({'Status': [False], 'Mensagem': [f'tag: {codbarra} ,nao esta localizada no estoque, conferir com o supervisor.']})
def PesquisarTagPrateleira(codbarra):
    conn = ConexaoPostgreMPL.conexao()

    query3 = pd.read_sql('select "codbarrastag","epc", "tamanho", "cor", "engenharia" , "codreduzido",  '
                                 '"descricao" ,"numeroop", "totalop" from "Reposicao".filareposicaoportag f  '
                                 'where codbarrastag = ' + "'" + codbarra + "'", conn)
    if not query3.empty :
        conn.close()
        return 1, query3['epc'][0],query3['tamanho'][0],query3['cor'][0],query3['engenharia'][0],query3['codreduzido'][0],query3['descricao'][0],\
            query3['numeroop'][0],query3['totalop'][0],1
    else:

        query2 = pd.read_sql('SELECT "usuario", "codbarrastag", "codreduzido", "Endereco", '
                 '"Engenharia", "DataReposicao", "descricao", "epc", "StatusEndereco", '
                 '"numeroop", "cor", "tamanho", "totalop" from "Reposicao".tagsreposicao_inventario t '
                 'where codbarrastag = ' + "'" + codbarra + "'", conn)

        if not query2.empty:
            conn.close()
            return 2, query2['epc'][0],query2['tamanho'][0],query2['cor'][0],query2['Engenharia'][0],\
                    query2['codreduzido'][0], query2['descricao'][0], query2['numeroop'][0],query2['totalop'][0],2
        else:
            query3 = pd.read_sql('SELECT * from "Reposicao".tagsreposicao t '
                 'where codbarrastag = ' + "'" + codbarra + "'", conn)
            if not query3.empty:
                conn.close()
                return 3, query3['epc'][0], query3['tamanho'][0], query3['cor'][0], query3['Engenharia'][0], \
                        query3['codreduzido'][0], query3['descricao'][0], query3['numeroop'][0], query3['totalop'][0],query3['Endereco'][0]
            else:
                return 4,4,4,4,4,4,4,4,4,4

def EstornoApontamento(codbarra, usuario):
    conn = ConexaoPostgreMPL.conexao()
    usuario = str(usuario)
    pesquisa, epc, tam, colu_cor, engenharia, reduzido, descricao, \
        colu_numeroop, colu_totalop, enderecoAntes = PesquisarTagPrateleira(codbarra)
    Insert = 'INSERT INTO  "Reposicao"."filareposicaoportag" ("codreduzido", "engenharia","codbarrastag","numeroop", "descricao", "cor", "epc", "tamanho", "totalop", "Situacao", "usuario") ' \
             'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'+"'Reposição não Iniciada'"+',%s);'
    cursor = conn.cursor()
    cursor.execute(Insert
                   , (reduzido, engenharia, codbarra, colu_numeroop, descricao, colu_cor, epc, tam, colu_totalop, usuario))
    # Obter o número de linhas afetadas
    numero_linhas_afetadas = cursor.rowcount
    conn.commit()
    cursor.close()
    # Apagando a Tag estorna
    cursor = conn.cursor()
    delete = 'Delete from "Reposicao"."tagsreposicao"  ' \
             'where "codbarrastag" = %s;'
    cursor.execute(delete
                   , (codbarra,))
    conn.commit()
    cursor.close()
    conn.close()

    return True


