{{ config(
    materialized='table',
    database='mariadb_end',
    schema='dbDrogamais',
    alias='dim_produto_acode',
    pre_hook=["DROP TABLE IF EXISTS mariadb_end.dbDrogamais.dim_produto_acode"]
) }}



SELECT 
    MD5(CAST(EAN AS VARCHAR) || CAST(Produto AS VARCHAR)) as id_produto,
    CAST(EAN AS VARCHAR) as gtin,
    CAST(Produto AS VARCHAR) as Produto,
    CONCAT(CAST(EAN AS VARCHAR), ' - ', CAST(Produto AS VARCHAR)) as Produto_completo

-- FROM {{ ref('silver_acode_compras_produto_comercial') }}
FROM read_parquet('s3://silver/silver_acode_compras_produto_comercial/**/*.parquet')

WHERE EAN IS NOT NULL
GROUP BY EAN, Produto