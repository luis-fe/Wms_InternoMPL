from models.GeracaoTags.Tag import Tag
from models.GeracaoTags.Tag_Csw import Tag_Csw

class Tag_service:
    """Responsável pelo gerenciamento e consulta de Tags no WMS."""

    def __init__(self, cod_empresa: str = '1'):
        self.cod_empresa = cod_empresa

        self.tag_model = Tag(self.cod_empresa)
        self.tag_Csw_service = Tag_Csw(self.cod_empresa)

    def atualizar_EPC_WMs_CSW(self):
        '''Metodo responsavel por atualizar os EPCs do WMS'''

        # 1. Buscar Tags com Epc Vazio (Supondo que retorne uma lista de strings)
        dF_tags_sem_epc = self.tag_model.obter_lista_tags_sem_epc()

        if not dF_tags_sem_epc:
            print("Nenhuma tag sem EPC encontrada.")
            return

        # 1.1 - Montar a cláusula SQL corretamente
        # Usamos o primeiro elemento para iniciar e os demais com OR
        primeira_tag = dF_tags_sem_epc[0]
        outras_tags = dF_tags_sem_epc[1:]

        # Começamos a string sem o OR no início
        clausula_formatada = f"t.id [ '||{primeira_tag}'"

        # Se houver mais tags, adicionamos os ORs
        if outras_tags:
            sql_or = "".join([f" OR t.id [ '||{item}'" for item in outras_tags[:50]])
            clausula_formatada += sql_or

        # 2. Buscar os EPC no CSW
        # Passamos apenas a parte do filtro. O seu service deve colocar isso dentro do WHERE
        dF_tags_EPC_csw = self.tag_Csw_service.filtar_epc_csw(clausula_formatada)

        print(dF_tags_EPC_csw)


        #2 - Buscar EPC das tags






