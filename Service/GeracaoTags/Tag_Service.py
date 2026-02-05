from models.GeracaoTags.Tag import Tag
class Tag_service:
    """Responsável pelo gerenciamento e consulta de Tags no WMS."""

    def __init__(self, cod_empresa: str = '1'):
        self.cod_empresa = cod_empresa

        self.tag_model = Tag(self.cod_empresa)


    def atualizar_EPC_WMs_CSW(self):
        '''Metodo responsavel por atualizar os EPCs do WMS'''

        # 1. Buscar Tags com Epc Vazio

        dF_tags_sem_epc = self.tag_model.obter_lista_tags_sem_epc()
        #1.1 - Transformar no padrao

        # Criamos as cláusulas individuais e as juntamos com o OR
        sql_clause = "\n   OR ".join([f"t.id [ '||{item}'" for item in dF_tags_sem_epc])

        # Adicionamos o início da string
        resultado = f"   {sql_clause}"

        print(resultado)


        #2 - Buscar EPC das tags






