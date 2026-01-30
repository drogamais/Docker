import polars as pl
import sqlalchemy
import urllib.parse

def carregar_gold_polars():
    # 1. Configurações de conexão
    # S3 / MinIO
    storage_options = {
        "key": "minioadmin",
        "secret": "minioadmin",
        "endpoint_url": "http://192.168.21.251:9000",
    }
    s3_path = "s3://silver/silver_acode_compras_produto_comercial/**/*.parquet"

    # MariaDB
    user = "drogamais"
    password = urllib.parse.quote_plus("dB$MYSql@2119")
    host = "10.48.12.20"
    database = "dbDrogamais"
    connection_uri = f"mysql+pymysql://{user}:{password}@{host}:3306/{database}"

    print(f" Iniciando leitura da Silver no MinIO...")

    # 2. Leitura e Processamento com Polars
    # scan_parquet permite que o Polars otimize a consulta antes de carregar os dados
    df = pl.scan_parquet(s3_path, storage_options=storage_options)

    # Transformações e geração de IDs (Hash Inteiro de 64 bits)
    df_processed = df.with_columns([
        # Criando Surrogate Keys numéricas (mais rápidas para o Power BI)
        pl.concat_str(["EAN", "Produto"]).hash().alias("id_produto"),
        pl.col("Desc_Marca").hash().alias("id_marca"),
        pl.col("Fornecedor").hash().alias("id_fornecedor"),
        pl.col("Fabricante").hash().alias("id_fabricante"),
        pl.concat_str(["Grupo", "Sub_Classe"]).hash().alias("id_grupo_subclasse"),
        
        # Garantindo tipos de dados corretos
        pl.col("Loja_CNPJ").cast(pl.Utf8),
        pl.col("data_emissao").cast(pl.Date),
        pl.col("Val_Prod_sem_STRet").cast(pl.Float64),
        pl.col("ACODE_Val_Total").cast(pl.Float64),
        pl.col("Qtd_Trib").cast(pl.Float64),
        
        # Timestamp de auditoria
        pl.lit(pl.datetime.now()).alias("data_atualizacao")
    ]).collect() # Executa o plano de processamento

    print(f"✅ Processamento concluído. Linhas para carregar: {len(df_processed)}")

    # 3. Escrita no MariaDB
    # O Polars utiliza o SQLAlchemy internamente para write_database
    df_processed.write_database(
        table_name="gold_acode_compras_produto_comercial",
        connection=connection_uri,
        if_table_exists="replace", # Recria a tabela (equivalente ao seu pre-hook de DROP)
        engine="sqlalchemy"
    )

    print(f" Carga da camada Gold finalizada com sucesso no MariaDB.")

if __name__ == "__main__":
    carregar_gold_polars()