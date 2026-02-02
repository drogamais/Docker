import os
import sys
import pandas as pd
import pymysql
import duckdb
from datetime import date
from typing import List, Dict

# 1. Ajuste de PATH primeiro para garantir que os imports funcionem
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(os.path.dirname(current_dir)) 
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

# 2. Agora realizamos os imports locais
from settings.config import MINIO_CONFIG, setup_minio_env, DUCKDB_SECRET_SQL
from flows_prefect.utils import marcar_sucesso_bronze 

DB_ACODE = {
    "username": "egtec_xml_rd_15",
    "password": "32XD#bdCA5R15dm",
    "host": "db-xml-rd.acode.com.br",
    "port": 3306,
    "database": "acode_master_redes",
    "cursorclass": pymysql.cursors.DictCursor
}

BUCKET_BRONZE = "s3://bronze/acode_compras_produto_comercial"

class AcodeBronzeETL:
    def __init__(self):
        setup_minio_env()
        self.con_duck = None

    def get_remote_conn(self):
        conn_params = DB_ACODE.copy()
        if "drivername" in conn_params: del conn_params["drivername"]
        return pymysql.connect(**conn_params)

    def _init_duckdb(self):
        if not self.con_duck:
            self.con_duck = duckdb.connect()
            self.con_duck.execute("INSTALL httpfs; LOAD httpfs;")
            self.con_duck.execute(DUCKDB_SECRET_SQL)

    def obter_totalizadores_remotos(self) -> List[Dict]:
        print("🔍 Consultando totalizadores remotos...")
        sql = """
        SELECT data_proc, SUM(Registros) as qtd_esperada
        FROM si_15_cubo_xml_analitico_diario_totalizador
        WHERE data_proc IS NOT NULL
        GROUP BY data_proc
        ORDER BY data_proc DESC
        """
        try:
            conn = self.get_remote_conn()
            with conn.cursor() as cursor:
                cursor.execute(sql)
                result = cursor.fetchall()
            conn.close()
            return result
        except Exception as e:
            print(f"❌ Erro ao buscar totalizadores remotos: {e}")
            return []

    def verificar_total_s3(self, data_proc) -> int:
        self._init_duckdb()
        s3_path = f"{BUCKET_BRONZE}/data_proc={data_proc}/*.parquet"
        try:
            query = f"SELECT COUNT(*) as qtd FROM read_parquet('{s3_path}')"
            return self.con_duck.execute(query).fetchone()[0]
        except:
            return 0

    def extrair_e_salvar(self, data_proc, qtd_esperada):
        print(f"⬇️ Baixando dados de {data_proc}...")
        # Adicione aqui o UNION com retroativo se necessário
        sql = f"SELECT * FROM si_15_cubo_xml_analitico_diario WHERE data_proc = '{data_proc}'"
        
        try:
            conn = self.get_remote_conn()
            df = pd.read_sql(sql, conn)
            conn.close()

            if df.empty: return

            s3_output = f"{BUCKET_BRONZE}/data_proc={data_proc}/part-0.parquet"
            storage_options = {
                "key": MINIO_CONFIG["access_key"],
                "secret": MINIO_CONFIG["secret_key"],
                "client_kwargs": {
                    "endpoint_url": f"http://{MINIO_CONFIG['endpoint']}",
                    "region_name": MINIO_CONFIG['region']
                }
            }
            
            if 'data_proc' in df.columns:
                df = df.drop(columns=['data_proc'])

            df.to_parquet(s3_output, index=False, storage_options=storage_options, compression='snappy')
            print(f"✅ Dia {data_proc} salvo.")
        except Exception as e:
            print(f"❌ Erro em {data_proc}: {e}")

    def run(self, sistema_flag: str):
        print(f"🚀 Iniciando Pipeline Bronze Acode para {sistema_flag}...")
        totalizadores = self.obter_totalizadores_remotos()
        if not totalizadores: return

        for item in totalizadores:
            str_dia = item['data_proc'].strftime('%Y-%m-%d')
            qtd_remota = int(item['qtd_esperada'])
            
            if self.verificar_total_s3(str_dia) == qtd_remota:
                print(f"👍 {str_dia}: OK.")
            else:
                self.extrair_e_salvar(str_dia, qtd_remota)
        
        print(f"\n🚩 Sinalizando sucesso para o sistema: {sistema_flag}")
        marcar_sucesso_bronze(sistema=sistema_flag)
        print("🏁 Fim.")

if __name__ == "__main__":
    etl = AcodeBronzeETL()
    # Use o nome que o seu flow_comercial_sell_in.py está esperando
    etl.run(sistema_flag="ACODE_COMPRAS")