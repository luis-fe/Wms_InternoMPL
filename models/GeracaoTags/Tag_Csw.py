import gc
import pandas as pd
from connection import ConexaoCSW

class Tag_Csw():
    '''Classe responsavel pelo gerenciamento das Tags no csw'''

    def __init__(self, empresa = '1'):

        self.empresa = empresa


    def filtar_epc_csw(self, clausulaTags : str ):
        '''Metodo que obtem do ERP CSW os Epc's das tags '''

        sql = f"""
        SELECT 
            SUBSTRING(t.id, CHARINDEX('||', t.id) + 2, 299) AS codtag,
            SUBSTRING(t.id, 1, CHARINDEX('||', t.id) - 1) AS epc
        FROM 
            Tcr_Rfid.NumeroSerieEPCTag t
        WHERE 
            t.id [ '||99999999999999999999' {clausulaTags}
        """.strip()  # O .strip() remove espaços em branco extras no fim que podem confundir o JDBC
        print(sql)


        with ConexaoCSW.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)

        # Libera memória manualmente
        del rows
        gc.collect()

        return consulta


