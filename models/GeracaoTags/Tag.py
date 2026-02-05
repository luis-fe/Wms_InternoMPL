import pandas as pd
import logging
from typing import Optional
from psycopg2.extras import execute_batch
import ConexaoPostgreMPL
from connection.WmsConnectionClass import WmsConnectionClass

# Configuração básica de log para capturar erros de consulta
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Tag:
    """Responsável pelo gerenciamento da Tags no WMS."""

    def __init__(self, cod_empresa: str = '1'):
        self.cod_empresa = cod_empresa
        self.wms_connection = WmsConnectionClass(self.cod_empresa)

    def obter_tags_sem_epc(self) -> pd.DataFrame:
        """
        Consulta na Fila do WMS as tags que possuem o campo EPC como nulo.

        Returns:
            pd.DataFrame: DataFrame contendo a coluna 'codbarrastag'.
        """
        query = """
            SELECT 
                codbarrastag
            FROM 
                "Reposicao"."Reposicao".filareposicaoportag
            WHERE 
                epc IS NULL
        """

        try:
            engine = self.wms_connection.conexaoEngine()

            df = pd.read_sql(query, engine)

            logger.info(f"{len(df)} tags encontradas com EPC nulo.")
            return df

        except Exception as e:
            logger.error(f"Erro ao consultar tags no WMS: {e}")
            # Retorna um DataFrame vazio em caso de erro para não quebrar o fluxo
            return pd.DataFrame(columns=['codbarrastag'])

    def obter_lista_tags_sem_epc(self) -> list:
        """Retorna apenas uma lista simples de códigos de barras (útil para loops)."""
        df = self.obter_tags_sem_epc()
        return df['codbarrastag'].tolist()

    def update_epc_fila(self, df_resultado):
        '''Metodo que recebe o DataFrame e atualiza o PostgreSQL em lote'''

        if df_resultado is None or df_resultado.empty:
            print("DataFrame vazio. Nada para atualizar.")
            return

        query = """
        UPDATE 
            "Reposicao"."Reposicao".filareposicaoportag
        SET
            epc = %s
        WHERE codbarrastag = %s
        """

        # Preparamos os dados: uma lista de tuplas [(epc, codtag), (epc, codtag), ...]
        # O DataFrame do Caché retorna 'codtag' e 'epc'
        dados_para_update = list(df_resultado[['epc', 'codtag']].itertuples(index=False, name=None))

        conn = ConexaoPostgreMPL.conexao()
        cursor = conn.cursor()

        try:
            # execute_batch é muito mais rápido que um loop manual
            execute_batch(cursor, query, dados_para_update)
            conn.commit()
            print(f"Sucesso! {len(dados_para_update)} registros atualizados.")
        except Exception as e:
            conn.rollback()
            print(f"Erro ao atualizar PostgreSQL: {e}")
        finally:
            cursor.close()
            conn.close()





''