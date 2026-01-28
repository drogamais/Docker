{{ config(
    materialized='view',
    database='nessie',
    schema='comercial_vermelho',
    alias='dim_produto'
) }}
SELECT 
    MD5(CAST(EAN AS VARCHAR) || CAST(Produto AS VARCHAR)) as id_produto,
    CAST(EAN AS VARCHAR) as gtin,
    CAST(Produto AS VARCHAR) as Produto,
    CONCAT(CAST(EAN AS VARCHAR), ' - ', CAST(Produto AS VARCHAR)) as Produto_completo
FROM {{ ref('silver_acode_compras_produto_comercial') }} AT BRANCH main
WHERE EAN IS NOT NULL
GROUP BY EAN, Produto