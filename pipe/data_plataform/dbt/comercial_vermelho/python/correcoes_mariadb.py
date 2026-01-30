import urllib.parse
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

def executar_correcoes():
    # Dados de conexão
    user = "drogamais"
    password = "dB$MYSql@2119"
    host = "10.48.12.20"
    database = "dbDrogamais"

    safe_password = urllib.parse.quote_plus(password)
    url = f"mysql+pymysql://{user}:{safe_password}@{host}:3306/{database}"
    engine = create_engine(url)

    # Lista de comandos DDL para tipos e índices
    comandos = [
        # 1. Ajuste de Tipos de Coluna para garantir precisão e performance
        """
        ALTER TABLE gold_acode_compras_produto_comercial 
        MODIFY COLUMN id_produto VARCHAR(32) NOT NULL,
        MODIFY COLUMN id_marca VARCHAR(32),
        MODIFY COLUMN id_fornecedor VARCHAR(32),
        MODIFY COLUMN id_fabricante VARCHAR(32),
        MODIFY COLUMN id_grupo_subclasse VARCHAR(32),
        MODIFY COLUMN Loja_CNPJ VARCHAR(20),
        MODIFY COLUMN Loja VARCHAR(150),
        MODIFY COLUMN data_emissao DATE,
        MODIFY COLUMN Val_Prod_sem_STRet DECIMAL(15,4),
        MODIFY COLUMN ACODE_Val_Total DECIMAL(15,4),
        MODIFY COLUMN Qtd_Trib DECIMAL(10,4),
        MODIFY COLUMN data_atualizacao DATETIME
        """,
        # 2. Criação de Índices (Idempotente com IF NOT EXISTS)
        "CREATE INDEX IF NOT EXISTS idx_produto ON gold_acode_compras_produto_comercial (id_produto)",
        "CREATE INDEX IF NOT EXISTS idx_marca ON gold_acode_compras_produto_comercial (id_marca)",
        "CREATE INDEX IF NOT EXISTS idx_fornecedor ON gold_acode_compras_produto_comercial (id_fornecedor)",
        "CREATE INDEX IF NOT EXISTS idx_fabricante ON gold_acode_compras_produto_comercial (id_fabricante)",
        "CREATE INDEX IF NOT EXISTS idx_grupo_subclasse ON gold_acode_compras_produto_comercial (id_grupo_subclasse)",
        "CREATE INDEX IF NOT EXISTS idx_loja_cnpj ON gold_acode_compras_produto_comercial (Loja_CNPJ)",
        "CREATE INDEX IF NOT EXISTS idx_data_emissao ON gold_acode_compras_produto_comercial (data_emissao)"
    ]

    try:
        with engine.connect() as connection:
            print(f"Conectado ao MariaDB em {host} via SQLAlchemy.")
            for comando in comandos:
                connection.execute(text(comando))
            connection.commit()
            print("Sucesso: Tipos e índices atualizados na camada Gold.")
    except SQLAlchemyError as e:
        print(f"Erro ao aplicar correções no MariaDB: {e}")
    finally:
        engine.dispose()

if __name__ == "__main__":
    executar_correcoes()