{{ config(
    materialized='table',
    database='mariadb_end',
    schema='dbDrogamais',
    alias='dim_fabricante_acode',
    pre_hook=["DROP TABLE IF EXISTS mariadb_end.dbDrogamais.dim_fabricante_acode"]
) }}



SELECT 
    hash(CAST(Fabricante AS VARCHAR)) as id_fabricante,
    CAST(Fabricante AS VARCHAR) as Fabricante

-- FROM {{ ref('silver_acode_compras_produto_comercial') }}
FROM read_parquet('s3://silver/silver_acode_compras_produto_comercial/**/*.parquet')

WHERE Fabricante IS NOT NULL
GROUP BY Fabricante