import pandas as pd
from models import Endereco

'''Service que faz o Gerenciamento dos Enderecos do WMS'''


class Gestao_Endereco():

    def __init__(self, codEmpresa = '1'):

        self.codEmpresa = codEmpresa
        self.enderos_models = Endereco.Enderecos(self.codEmpresa)

    def get_informacoes_enderecos(self):
        '''Metodo que levanta a informacoes dos enderecos '''


        consulta = self.enderos_models.get_enderecos()

        total_enderecos = consulta['codendereco'].count()
        ruas = consulta.groupby('rua').agg({'rua': 'first'}).reset_index()

        data = {

            '1- total_enderecos ': f'{total_enderecos}',
            '2.1- Rua ': ruas.to_dict(orient='records'),
            '3- Detalhamento ': consulta.to_dict(orient='records')
        }
        return pd.DataFrame([data])

