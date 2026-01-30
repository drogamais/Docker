{{ config(
    materialized='table',
    database='mariadb_end',
    schema='dbDrogamais',
    alias='dim_fornecedor_acode',
    pre_hook=["DROP TABLE IF EXISTS mariadb_end.dbDrogamais.dim_fornecedor_acode"]
) }}



SELECT 
    MD5(CAST(Fornecedor AS VARCHAR)) as id_fornecedor,
    CAST(Fornecedor AS VARCHAR) as Fornecedor

-- FROM {{ ref('silver_acode_compras_produto_comercial') }}
FROM read_parquet('s3://silver/silver_acode_compras_produto_comercial/**/*.parquet')

WHERE Fornecedor IS NOT NULL

GROUP BY Fornecedor