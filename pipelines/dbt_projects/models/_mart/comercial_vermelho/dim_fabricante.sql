{{ config(
    materialized='view',
    database='nessie',
    schema='comercial_vermelho',
    alias='dim_fabricante'
) }}

SELECT 
    MD5(CAST(Fabricante AS VARCHAR)) as id_Fabricante,
    CAST(Fabricante AS VARCHAR) as Fabricante
FROM {{ ref('silver_acode_compras_produto_comercial') }} AT BRANCH main
WHERE Fabricante IS NOT NULL
GROUP BY Fabricante