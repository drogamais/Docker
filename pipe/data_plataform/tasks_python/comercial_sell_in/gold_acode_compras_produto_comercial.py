import duckdb
import pymysql
import os
import tempfile
import sys

# Para enxergar um diretório acima
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from utils.monitor import DBMonitor
from settings.config import DB_CONFIG, DUCKDB_SECRET_SQL, setup_minio_env, get_temp_csv_caminho

# 1. Configura o ambiente (MinIO) automaticamente
setup_minio_env()

# 2. Define o caminho do CSV usando a função padronizada
CSV_PATH = get_temp_csv_caminho("carga_gold_final.csv")

# Caminho do arquivo definido globalmente para todas as funções verem
TEMP_DIR = tempfile.gettempdir()

def duckdb_csv():
    print(f"📂 [1/3] Arquivo temporário definido: {CSV_PATH}")
    
    con = duckdb.connect()
    try:
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute(DUCKDB_SECRET_SQL)

        print("🦆 DuckDB: Extraindo dados e gerando CSV...")
        
        # --- MUDANÇA PRINCIPAL: IDs convertidos para VARCHAR (Texto) ---
        query = f"""
        COPY (
            SELECT 
                CAST(hash(concat(EAN, Produto)) AS VARCHAR) AS id_produto,
                CAST(hash(Desc_Marca) AS VARCHAR) AS id_marca,
                CAST(hash(Fornecedor) AS VARCHAR) AS id_fornecedor,
                CAST(hash(Fabricante) AS VARCHAR) AS id_fabricante,
                CAST(hash(concat(Grupo, Sub_Classe)) AS VARCHAR) AS id_grupo_subclasse,
                
                CAST(Loja_CNPJ AS VARCHAR) AS loja_cnpj,
                CAST(data_emissao AS DATE) AS data_emissao,
                
                CAST(Val_Prod_sem_STRet AS DECIMAL(15,4)) AS val_prod_sem_stret,
                CAST(ACODE_Val_Total AS DECIMAL(15,4)) AS acode_val_total,
                CAST(Qtd_Trib AS INT) AS qtd_trib,
                
                now() AS data_atualizacao
            FROM read_parquet('s3://silver/silver_acode_compras_produto_comercial/**/*.parquet')
        ) TO '{CSV_PATH}' (FORMAT CSV, DELIMITER ';', HEADER FALSE);
        """
        con.execute(query)
        print("✅ CSV gerado com sucesso.")
    except Exception as e:
        print(f"❌ Erro no DuckDB: {e}")
        sys.exit(1) # Para o script se falhar aqui
    finally:
        con.close()

def csv_mariadb():
    if not os.path.exists(CSV_PATH):
        print("❌ Erro: Arquivo CSV não encontrado. Pulo etapa.")
        return

    tamanho = os.path.getsize(CSV_PATH)
    print(f"🐬 [2/3] Iniciando carga no MariaDB: {tamanho / 1e6:.2f} MB")

    conn = None
    try:
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor()

        table_prod = "gold_acode_compras_produto_comercial"
        table_new = f"{table_prod}_new"
        table_old = f"{table_prod}_old"

        # Limpeza preventiva
        cursor.execute(f"DROP TABLE IF EXISTS {table_new}")
        cursor.execute(f"DROP TABLE IF EXISTS {table_old}")

        print(f"🔨 Criando tabela staging: {table_new}")
        
        # --- MUDANÇA NO DDL: IDs agora são VARCHAR(50) ---
        ddl = f"""
        CREATE TABLE {table_new} (
            id_fato INT AUTO_INCREMENT PRIMARY KEY,
            id_produto VARCHAR(50), 
            id_marca VARCHAR(50), 
            id_fornecedor VARCHAR(50),
            id_fabricante VARCHAR(50), 
            id_grupo_subclasse VARCHAR(50),
            loja_cnpj VARCHAR(20), 
            data_emissao DATE,
            val_prod_sem_stret DECIMAL(15,4), 
            acode_val_total DECIMAL(15,4),
            qtd_trib INT, 
            data_atualizacao DATETIME
        ) ENGINE=Aria TRANSACTIONAL=0 ROW_FORMAT=PAGE;
        """
        cursor.execute(ddl)

        # Monitoramento
        # (Atenção: removi os ** do DB_CONFIG se você ajustou a classe Monitor como conversamos antes. 
        # Se não ajustou, mantenha os **). Assumindo a versão corrigida:
        monitor = DBMonitor(DB_CONFIG)
        monitor.start(table_name=table_new, total_bytes_csv=tamanho)

        # Carga
        print("🚚 Carregando dados...")
        sql_load = f"""
        LOAD DATA LOCAL INFILE '{CSV_PATH}'
        INTO TABLE {table_new}
        FIELDS TERMINATED BY ';' LINES TERMINATED BY '\\n'
        (id_produto, id_marca, id_fornecedor, id_fabricante, id_grupo_subclasse, 
         loja_cnpj, data_emissao, val_prod_sem_stret, acode_val_total, qtd_trib, data_atualizacao)
        """
        cursor.execute(sql_load)
        conn.commit()

        monitor.stop()
        
        # Índices
        print("⚙️ Criando índices...")
        # Índices em colunas de texto curtas (50 chars) são muito rápidos
        indices = [
            f"CREATE INDEX idx_produto ON {table_new} (id_produto)",
            f"CREATE INDEX idx_marca ON {table_new} (id_marca)",
            f"CREATE INDEX idx_fornecedor ON {table_new} (id_fornecedor)",
            f"CREATE INDEX idx_fabricante ON {table_new} (id_fabricante)",
            f"CREATE INDEX idx_grupo_subclasse ON {table_new} (id_grupo_subclasse)",
            f"CREATE INDEX idx_loja_cnpj ON {table_new} (loja_cnpj)",
            f"CREATE INDEX idx_data_emissao ON {table_new} (data_emissao)"
        ]
        for sql in indices: cursor.execute(sql)
        print("✅ Índices criados.")

        # Swap
        print("🔄 Trocando tabelas (Blue-Green Deployment)...")
        cursor.execute(f"SHOW TABLES LIKE '{table_prod}'")
        if cursor.fetchone():
            cursor.execute(f"RENAME TABLE {table_prod} TO {table_old}, {table_new} TO {table_prod}")
        else:
            cursor.execute(f"RENAME TABLE {table_new} TO {table_prod}")
        
        cursor.execute(f"DROP TABLE IF EXISTS {table_old}")
        conn.commit()
        print("🏁 Carga finalizada!")

    except Exception as e:
        print(f"❌ Erro no MariaDB: {e}")
        if conn: conn.rollback()
        sys.exit(1)
    finally:
        if conn: conn.close()

def limpar_temp():
    print(f"🧹 [3/3] Limpeza de arquivos temporários...")
    if os.path.exists(CSV_PATH):
        try:
            os.remove(CSV_PATH)
            print("✅ Arquivo removido com sucesso.")
        except Exception as e:
            print(f"⚠️ Aviso: Não foi possível remover o arquivo: {e}")
    else:
        print("ℹ️ Nenhum arquivo para limpar.")

# --- ORQUESTRAÇÃO PRINCIPAL ---
if __name__ == "__main__":
    duckdb_csv()
    csv_mariadb()
    limpar_temp()