import psycopg2

class WmsConnectionClass():
    '''Class que faz a conexao com o banco WMS'''
    def __init__(self, empresa = None):
        self.empresa = empresa

    def conectar(self):

        db_name = "WMS"
        db_user = "postgres"
        db_password = "Master100"

        if self.empresa == None:
            host = '10.162.0.190'
        else:
            host = ''

        portbanco = "5432"

        return psycopg2.connect(dbname=db_name, user=db_user, password=db_password, host=host, port=portbanco)