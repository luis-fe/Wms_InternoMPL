from models.GeracaoTags.Tag import Tag
class Tag_service:
    """Responsável pelo gerenciamento e consulta de Tags no WMS."""

    def __init__(self, cod_empresa: str = '1'):
        self.cod_empresa = cod_empresa

        self.tag_model = Tag(self.cod_empresa)


    def atualizar_EPC_WMs(self):
        '''Metodo responsavel por atualizar os EPCs do WMS'''

        # 1. Buscar Tags com Epc Vazio

        dF_tags_sem_epc = self.tag_model.obter_lista_tags_sem_epc()

        #1.1 - Transformar no padrao
        print(dF_tags_sem_epc)





