from models.GeracaoTags import Tag


class Tag_Csw():
    '''Classe responsavel pelo gerenciamento das Tags no csw'''

    def __init__(self, arrayTags = '', empresa = '1'):

        self.arrayTags = arrayTags
        self.empresa = empresa
        # Importacao das Tag do WMS para integracao com CSW
        self.tagWMS = Tag.Tag(self.empresa)




    def filtar_epc_csw(self):
        '''Metodo que obtem do ERP CSW os Epc's das tags '''

        listaTAGs_semEMP = self.tagWMS.obter_lista_tags_sem_epc()

