import pandas as pd
from connection import WmsConnectionClass as conexao
import ConexaoPostgreMPL
from models import Perfil


class Usuario:
    """
    Classe que representa os usuários do sistema WMS.

    Atributos:
        codigo (str): Código ou matrícula do usuário.
        login (str): Login de acesso do usuário.
        nome (str): Nome completo do usuário.
        situacao (str): Situação atual do usuário (Ativo/Inativo).
        funcaoWMS (str): Função desempenhada no sistema WMS.
        senha (str): Senha de acesso do usuário.
    """

    def __init__(self, codigo=None, login=None, nome=None, situacao=None, funcaoWMS=None, senha=None, perfil=None):
        """
        Construtor da classe Usuario.

        Args:
            codigo (str, opcional): Código ou matrícula do usuário.
            login (str, opcional): Login do usuário.
            nome (str, opcional): Nome completo do usuário.
            situacao (str, opcional): Situação do usuário.
            funcaoWMS (str, opcional): Função do usuário no sistema WMS.
            senha (str, opcional): Senha de acesso do usuário.
        """
        self.codigo = codigo
        self.login = login
        self.nome = nome
        self.situacao = situacao
        self.funcaoWMS = funcaoWMS
        self.senha = senha
        self.perfil = perfil

    def getUsuarios(self):
        """
        Obtém todos os usuários cadastrados na plataforma.

        Returns:
            pd.DataFrame: DataFrame com os dados dos usuários.
        """
        sqlGetUsuarios = """
            SELECT
                *
            FROM
                "Reposicao"."cadusuarios"
            order by nome
        """
        try:
            with ConexaoPostgreMPL.conexao() as conn:
                with conn.cursor() as curr:
                    curr.execute(sqlGetUsuarios)
                    usuarios = curr.fetchall()
                    colunas = [desc[0] for desc in curr.description]
                    dataframe = pd.DataFrame(usuarios, columns=colunas)
                    dataframe.fillna('NAO', inplace=True)
            return dataframe
        except Exception as e:
            print(f"Erro ao obter usuários: {e}")
            return pd.DataFrame()

    def inserirUsuario(self):
        """
        Insere um novo usuário na plataforma WMS.

        Returns:
            bool: True se a inserção for bem-sucedida, False caso contrário.
        """
        insert = """
        INSERT INTO "Reposicao"."Reposicao".cadusuarios
            (codigo, funcao, nome, login, situacao, perfil, senha)
        VALUES
            (%s, %s, %s, %s, %s, %s, %s)
        """
        with ConexaoPostgreMPL.conexao() as conn:
                with conn.cursor() as curr:
                    curr.execute(insert,
                                 (int(self.codigo), self.funcaoWMS, self.nome, self.login, 'ATIVO', self.perfil, self.senha))
                    conn.commit()
        return True

    def consultaUsuarioSenha(self):
        """
        Consulta a existência de um usuário com base no código e senha.

        Returns:
            bool: True se o usuário e senha corresponderem, False caso contrário.
        """
        query = """
        SELECT 
            COUNT(*)
        FROM 
            "Reposicao"."Reposicao".cadusuarios us
        WHERE 
            codigo = %s AND senha = %s
        """
        try:
            with ConexaoPostgreMPL.conexao() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, (self.codigo, self.senha))
                    result = cursor.fetchone()[0]
            return result == 1
        except Exception as e:
            print(f"Erro ao consultar usuário e senha: {e}")
            return False

    def consultaUsuario(self):
        conn = ConexaoPostgreMPL.conexao()
        cursor = conn.cursor()
        codigo = int(self.codigo)
        cursor.execute("""
                select
                       codigo, 
                       nome, 
                       funcao, 
                       situacao, 
                       empresa, 
                       perfil, 
                       login 
                from 
                    "Reposicao"."cadusuarios" c
                where 
                    codigo = %s
                       """, (codigo,))
        usuarios = cursor.fetchall()
        cursor.close()
        conn.close()
        if not usuarios:
            return 0, 0, 0, 0, 0, 0, 0
        else:
            self.perfil = usuarios[0][6]
            return usuarios[0][0], usuarios[0][1], usuarios[0][2], usuarios[0][3], usuarios[0][4], usuarios[0][5], \
            usuarios[0][6]

    def PesquisarSenha(self):
        '''Api usada para restricao de pesquisa de senha dos usuarios '''

        conn = ConexaoPostgreMPL.conexao()
        cursor = conn.cursor()
        cursor.execute('select codigo, nome, senha from "Reposicao"."cadusuarios" c')
        usuarios = cursor.fetchall()
        cursor.close()
        conn.close()

        return usuarios

    def inserirPerfilUsuario(self, nomePerfil):
        ''' metodo construido para inserir o Pefil ao usuario '''

        update = """
                update 
                    "Reposicao"."cadusuarios"
                set
                    perfil = %s
                where 
                    codigo = %s
        """

        perfil = Perfil.Perfil('', nomePerfil)
        self.perfil = perfil.descobrircodPerfil()
        validador = self.perfil
        if validador != None:

            with ConexaoPostgreMPL.conexao() as conn:
                with conn.cursor() as curr:
                    curr.execute(update, (self.perfil, self.codigo))
                    conn.commit()

            return pd.DataFrame([{'status': True, 'Mensagem': 'Salvo com sucesso'}])

        else:
            return pd.DataFrame([{'status': False, 'Mensagem': f'{self.perfil}-{nomePerfil} nao encontrado '}])

    def rotasAutorizadasUsuarios(self):
        '''Metodo que retorna as rotas altorizadas para os usuarios '''

        sql1 = """
        select  
            codigo, 
            nome, 
            perfil as "codPerfil",
            p."nomePerfil"
        from 
            "Reposicao"."cadusuarios" c
        left join 
            "Reposicao"."Pefil" p 
        on perfil::varchar = "codPerfil"
        order by c.nome asc

        """

        sql2 = """
        select
                "codPerfil",
                tp."nomeTela",
                t.menu,
                t."urlTela" as "urlTela"
            from 
                "Reposicao"."TelaAcessoPerfil" tp
            inner join 
                "Reposicao"."TelaAcesso" t
                on tp."nomeTela" = t."nomeTela"
        """

        conn = ConexaoPostgreMPL.conexaoEngine()
        consulta1 = pd.read_sql(sql1, conn)
        consulta2 = pd.read_sql(sql2, conn)

        agrupamento2 = consulta2.groupby(['codPerfil', 'menu']).agg({
            'urlTela': lambda x: list(x.dropna().astype(str).unique())
        }).reset_index()

        # Transformação para o formato desejado
        agrupamento2 = (
            agrupamento2.groupby("codPerfil")
            .apply(lambda x: list(zip(x["menu"], x["urlTela"])))
            .reset_index(name="urlTela")
        )


        consulta = pd.merge(consulta1, agrupamento2, on='codPerfil', how='left')

        consulta.fillna('-', inplace=True)



        consulta = consulta.sort_values(by='nome', ascending=True,
                                              ignore_index=True)  # escolher como deseja classificar

        return consulta


    def rotasAutorizadasPORUsuario(self):
        '''Metodo que retorna as rotas altorizadas para o usuario em especifico '''

        todos = self.rotasAutorizadasUsuarios()

        usuario = todos[todos['codigo'] ==int(self.codigo)].reset_index()

        return usuario

    def inserirArrayPefilUsuario(self, arrayUsuario, arrayNomePerfil):
        '''Metodo para inserir via array os perfil aos usuarios'''

        # Correção do for utilizando zip para iterar sobre ambas as listas
        for a, p in zip(arrayUsuario, arrayNomePerfil):
            self.codigo = a
            self.inserirPerfilUsuario(p)

        return pd.DataFrame([{'status': True, 'Mensagem': 'Perfis inseridos com sucesso'}])

    def atualizarInformUsuario(self):
        conn = ConexaoPostgreMPL.conexao()
        cursor = conn.cursor()
        cursor.execute("""
                        UPDATE "Reposicao"."cadusuarios" 
                            SET nome=%s, funcao=%s, situacao= %s, login = %s, perfil = %s
                        WHERE codigo=%s """,
                       (self.nome, self.funcaoWMS, self.situacao, self.login, self.perfil, self.codigo))
        conn.commit()
        cursor.close()
        conn.close()

        return self.nome

















