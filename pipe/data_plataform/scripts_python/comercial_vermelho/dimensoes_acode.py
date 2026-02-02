import duckdb
import pymysql
import os
import sys

# Ajuste de PATH para encontrar módulos pai (config, utils)
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from settings.config import DB_CONFIG, DUCKDB_SECRET_SQL, setup_minio_env, get_temp_csv_caminho

# 1. CONFIGURAÇÕES GLOBAIS
setup_minio_env()
S3_BASE = "s3://silver/silver_acode_compras_produto_comercial/**/*.parquet"

# --- CONFIGURAÇÃO ---
DIMENSOES_CONFIG = [
    {
        "tabela": "dim_fabricante_acode",
        "s3_path": S3_BASE,
        "colunas_origem": ["Fabricante"],
        "hash_cols": ["Fabricante"],
        "id_col": "id_fabricante",
        "tipos": {"Fabricante": "VARCHAR(120)"}
    },
    {
        "tabela": "dim_fornecedor_acode",
        "s3_path": S3_BASE,
        "colunas_origem": ["Fornecedor"],
        "hash_cols": ["Fornecedor"],
        "id_col": "id_fornecedor",
        "tipos": {"Fornecedor": "VARCHAR(150)"}
    },
    {
        "tabela": "dim_marca_acode",
        "s3_path": S3_BASE,
        "colunas_origem": ["Desc_Marca"],
        "hash_cols": ["Desc_Marca"],
        "id_col": "id_marca",
        "renames": {"Desc_Marca": "nome_marca"},
        "tipos": {"Desc_Marca": "VARCHAR(50)"}
    },
    {
        "tabela": "dim_grupo_subclasse_acode",
        "s3_path": S3_BASE,
        "colunas_origem": ["Grupo", "Sub_Classe"],
        "hash_cols": ["Grupo", "Sub_Classe"],
        "id_col": "id_grupo_subclasse",
    },
    {
        "tabela": "dim_produto_acode", 
        "s3_path": S3_BASE,
        "colunas_origem": ["EAN", "Produto"],
        "hash_cols": ["EAN", "Produto"],
        "id_col": "id_produto",
        "tipos": {"EAN": "VARCHAR(20)", "Produto": "VARCHAR(255)"}
    }
]

# --- 2. FUNÇÕES DO PIPELINE ---

def duckdb_csv(config):
    """Gera o CSV com agregação de MAX(data_emissao)"""
    table_name = config["tabela"]
    csv_filename = f"{table_name}.csv"
    csv_path_local = get_temp_csv_caminho(csv_filename)
    
    print(f"\n📦 [1/3] DuckDB: Processando {table_name}...")
    
    con = duckdb.connect()
    try:
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute(DUCKDB_SECRET_SQL)

        # Monta colunas dinamicamente
        hash_sql = f"hash(concat({', '.join(config['hash_cols'])}))"
        
        select_cols = []
        for col in config["colunas_origem"]:
            novo_nome = config.get("renames", {}).get(col, col)
            select_cols.append(f"CAST(regexp_replace({col}, '[\n\r;]', '', 'g') AS VARCHAR) AS {novo_nome}")
            
        select_str = ", ".join(select_cols)

        # --- LÓGICA DO GROUP BY ---
        # Para usar MAX(), precisamos agrupar por todas as outras colunas.
        # Coluna 1 é o ID (Hash), Colunas 2 até N são as descrições.
        num_cols_dimensao = len(config["colunas_origem"])
        indices_group_by = ", ".join([str(i) for i in range(1, num_cols_dimensao + 2)]) 
        # range(1, N+2) porque SQL começa em 1, e temos Hash + N colunas

        query = f"""
        COPY (
            SELECT
                {hash_sql} AS {config['id_col']},   -- Coluna 1
                {select_str},                       -- Colunas 2 até N
                MAX(data_emissao) AS ultima_emissao, -- NOVA COLUNA
                now() AS data_atualizacao
            FROM read_parquet('{config['s3_path']}')
            WHERE {config['hash_cols'][0]} IS NOT NULL
            GROUP BY {indices_group_by}
        ) TO '{csv_path_local}' (FORMAT CSV, DELIMITER ';', HEADER FALSE);
        """
        con.execute(query)
        print("   ✅ CSV gerado (com Max Data).")
        return csv_path_local

    except Exception as e:
        print(f"   ❌ Erro no DuckDB: {e}")
        sys.exit(1)
    finally:
        con.close()

def csv_mariadb(config, csv_path_local):
    if not csv_path_local or not os.path.exists(csv_path_local):
        print("   ⚠️ CSV não encontrado. Pulando carga.")
        return

    table_prod = config["tabela"]
    table_new = f"{table_prod}_new"
    table_old = f"{table_prod}_old"

    print(f"   🐬 [2/3] MariaDB: Carregando {table_prod}...")
    
    conn = None
    try:
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        cursor.execute(f"DROP TABLE IF EXISTS {table_new}")
        cursor.execute(f"DROP TABLE IF EXISTS {table_old}")
        
        # 1. Monta DDL
        colunas_ddl = [f"{config['id_col']} BIGINT PRIMARY KEY"]
        colunas_load = [config['id_col']]
        
        for col in config["colunas_origem"]:
            novo_nome = config.get("renames", {}).get(col, col)
            tipo_coluna = config.get("tipos", {}).get(col, "VARCHAR(255)")
            
            colunas_ddl.append(f"`{novo_nome}` {tipo_coluna}")
            colunas_load.append(novo_nome)
            
        # --- ADICIONADO: Coluna de Data ---
        colunas_ddl.append("ultima_emissao DATE")
        colunas_load.append("ultima_emissao")
        # ----------------------------------

        colunas_ddl.append("data_atualizacao DATETIME")
        colunas_load.append("data_atualizacao")
        
        ddl = f"""
        CREATE TABLE {table_new} (
            {', '.join(colunas_ddl)}
        ) ENGINE=Aria TRANSACTIONAL=0 ROW_FORMAT=PAGE;
        """
        cursor.execute(ddl)
        
        # 2. Load Data
        sql_load = f"""
        LOAD DATA LOCAL INFILE '{csv_path_local}'
        INTO TABLE {table_new}
        FIELDS TERMINATED BY ';' LINES TERMINATED BY '\\n'
        ({', '.join(colunas_load)})
        """
        cursor.execute(sql_load)
        conn.commit()

        # 3. Índices Extras
        if len(config["colunas_origem"]) > 0:
            col_nome = config.get("renames", {}).get(config["colunas_origem"][0], config["colunas_origem"][0])
            try:
                cursor.execute(f"CREATE INDEX idx_busca_{col_nome} ON {table_new} (`{col_nome}`(50))")
                # Índice útil para a data também
                cursor.execute(f"CREATE INDEX idx_ultima_emissao ON {table_new} (ultima_emissao)")
            except: pass

        # 4. Swap
        cursor.execute(f"SHOW TABLES LIKE '{table_prod}'")
        if cursor.fetchone():
            cursor.execute(f"RENAME TABLE {table_prod} TO {table_old}, {table_new} TO {table_prod}")
        else:
            cursor.execute(f"RENAME TABLE {table_new} TO {table_prod}")
            
        cursor.execute(f"DROP TABLE IF EXISTS {table_old}")
        conn.commit()
        print(f"   ✅ {table_prod} atualizada com sucesso!")

    except Exception as e:
        print(f"   ❌ Erro no MariaDB: {e}")
        if conn: conn.rollback()
    finally:
        if conn: conn.close()

def limpar_temp(csv_path_local):
    if csv_path_local and os.path.exists(csv_path_local):
        try: os.remove(csv_path_local)
        except: pass

# --- 3. ORQUESTRAÇÃO PRINCIPAL ---
if __name__ == "__main__":
    print("🚀 Iniciando Pipeline de Dimensões (Com Max Data)...")
    
    for dim in DIMENSOES_CONFIG:
        caminho_arquivo = duckdb_csv(dim)
        if caminho_arquivo:
            csv_mariadb(dim, caminho_arquivo)
            limpar_temp(caminho_arquivo)
            
    print("\n🏁 Processo finalizado.")