{{ config(
    materialized='view',
    database='nessie',
    schema='comercial_vermelho',
    alias='dim_marca'
) }}

SELECT 
    MD5(CAST(Desc_Marca AS VARCHAR)) as id_marca,
    CAST(Desc_Marca AS VARCHAR) as nome_marca
FROM {{ ref('silver_acode_compras_produto_comercial') }} AT BRANCH main
WHERE Desc_Marca IS NOT NULL
GROUP BY Desc_Marca