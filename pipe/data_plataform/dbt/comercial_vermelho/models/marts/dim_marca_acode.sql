{{ config(
    materialized='table',
    database='mariadb_end',
    schema='dbDrogamais',
    alias='dim_marca_acode',
    pre_hook=["DROP TABLE IF EXISTS mariadb_end.dbDrogamais.dim_marca_acode"]
) }}



SELECT 
    MD5(CAST(Desc_Marca AS VARCHAR)) as id_marca,
    CAST(Desc_Marca AS VARCHAR) as nome_marca

-- FROM {{ ref('silver_acode_compras_produto_comercial') }}
FROM read_parquet('s3://silver/silver_acode_compras_produto_comercial/**/*.parquet')

WHERE Desc_Marca IS NOT NULL
GROUP BY Desc_Marca