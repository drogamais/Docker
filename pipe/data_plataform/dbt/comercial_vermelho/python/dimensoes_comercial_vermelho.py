import polars as pl
import sqlalchemy
import urllib.parse

def carregar_dimensoes_polars():
    # 1. Configurações de conexão
    storage_options = {
        "key": "minioadmin",
        "secret": "minioadmin",
        "endpoint_url": "http://192.168.21.251:9000",
    }
    s3_path = "s3://silver/silver_acode_compras_produto_comercial/**/*.parquet"

    user = "drogamais"
    password = urllib.parse.quote_plus("dB$MYSql@2119")
    host = "10.48.12.20"
    database = "dbDrogamais"
    connection_uri = f"mysql+pymysql://{user}:{password}@{host}:3306/{database}"

    print(" Iniciando processamento das Dimensões...")

    # Lazy frame para otimização
    lf = pl.scan_parquet(s3_path, storage_options=storage_options)

    # Dicionário definindo como cada dimensão deve ser processada
    config_dimensoes = {
        "dim_fabricante_acode": {
            "colunas": ["Fabricante"],
            "id_col": "id_fabricante",
            "hash_cols": ["Fabricante"]
        },
        "dim_fornecedor_acode": {
            "colunas": ["Fornecedor"],
            "id_col": "id_fornecedor",
            "hash_cols": ["Fornecedor"]
        },
        "dim_marca_acode": {
            "colunas": ["Desc_Marca"],
            "id_col": "id_marca",
            "hash_cols": ["Desc_Marca"],
            "renames": {"Desc_Marca": "nome_marca"}
        },
        "dim_grupo_subclasse_acode": {
            "colunas": ["Grupo", "Sub_Classe"],
            "id_col": "id_grupo_subclasse",
            "hash_cols": ["Grupo", "Sub_Classe"]
        },
        "dim_produto_acode": {
            "colunas": ["EAN", "Produto"],
            "id_col": "id_produto",
            "hash_cols": ["EAN", "Produto"]
        }
    }

    for table_name, cfg in config_dimensoes.items():
        print(f"📦 Processando {table_name}...")
        
        # Filtra nulos e remove duplicatas
        df_dim = lf.select(cfg["colunas"]).drop_nulls().unique()

        # Gera o ID numérico (Hash)
        df_dim = df_dim.with_columns(
            pl.concat_str(cfg["hash_cols"]).hash().alias(cfg["id_col"])
        )

        # Renomeia colunas se necessário (ex: Desc_Marca -> nome_marca)
        if "renames" in cfg:
            df_dim = df_dim.rename(cfg["renames"])

        # Executa e escreve no MariaDB
        df_final = df_dim.collect()
        df_final.write_database(
            table_name=table_name,
            connection=connection_uri,
            if_table_exists="replace",
            engine="sqlalchemy"
        )
        print(f" {table_name} carregada ({len(df_final)} linhas).")

if __name__ == "__main__":
    carregar_dimensoes_polars()