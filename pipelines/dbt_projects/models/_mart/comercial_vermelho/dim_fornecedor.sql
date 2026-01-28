{{ config(
    materialized='view',
    database='nessie',
    schema='comercial_vermelho',
    alias='dim_fornecedor'
) }}

SELECT 
    MD5(CAST(Fornecedor AS VARCHAR)) as id_fornecedor,
    CAST(Fornecedor AS VARCHAR) as Fornecedor
FROM {{ ref('silver_acode_compras_produto_comercial') }} AT BRANCH main
WHERE Fornecedor IS NOT NULL
GROUP BY Fornecedor