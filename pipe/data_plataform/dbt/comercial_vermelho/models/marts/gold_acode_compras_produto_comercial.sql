{{ config(
    materialized='table',
    database='mariadb_end',
    schema='dbDrogamais',
    alias='gold_acode_compras_produto_comercial',

    pre_hook=["DROP TABLE IF EXISTS mariadb_end.dbDrogamais.gold_acode_compras_produto_comercial"]
) }}



SELECT
    -- Chaves (Surrogate Keys)
    MD5(CAST(EAN AS VARCHAR) || CAST(Produto AS VARCHAR)) AS id_produto,
    MD5(CAST(Desc_Marca AS VARCHAR)) AS id_marca,
    MD5(CAST(Fornecedor AS VARCHAR)) AS id_fornecedor,
    MD5(CAST(Fabricante AS VARCHAR)) as id_fabricante,
    MD5(CAST(Grupo AS VARCHAR) || CAST(Sub_Classe AS VARCHAR)) AS id_grupo_subclasse,
    
    -- Contexto
    CAST(Loja_CNPJ AS VARCHAR(18)) AS Loja_CNPJ,
    CAST(Loja AS VARCHAR(100)) AS Loja,
    CAST(data_emissao AS DATE) AS data_emissao,
    
    -- Métricas
    Val_Prod_sem_STRet,
    ACODE_Val_Total,
    Qtd_Trib,
    
    -- Auditoria
    current_timestamp AS data_atualizacao

-- FROM {{ ref('silver_acode_compras_produto_comercial') }}
FROM read_parquet('s3://silver/silver_acode_compras_produto_comercial/**/*.parquet')