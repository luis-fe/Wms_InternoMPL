import ConexaoPostgreMPL
import pandas as pd

class Enderecos():

    def __init__(self, codEmpresa = None, codNatureza = None):

        self.codEmpresa = codEmpresa
        self.codNatureza = codNatureza


    def get_enderecos(self):
        '''Metodo que levanta a informacoes dos enderecos '''

        conn = ConexaoPostgreMPL.conexaoEngine()

        sql_consultar_enderecos = f"""
            select
	            codempresa,
	            codendereco as "codEndereco",
	            natureza as "codNatureza",
                split_part(c.codendereco , '-', 1) AS rua,
                split_part(c.codendereco, '-', 2) AS modulo,
                split_part(c.codendereco, '-', 3) AS posicao            
            from
	            "Reposicao"."Reposicao".cadendereco c
            where 
               codempresa = '{str(self.codEmpresa)}' 
            order by 
                substring(codendereco,1,2)::int ,
                split_part(c.codendereco, '-', 2)::int,
                split_part(c.codendereco, '-', 3)::int
        """

        consulta = pd.read_sql(sql_consultar_enderecos,conn)
        return consulta

