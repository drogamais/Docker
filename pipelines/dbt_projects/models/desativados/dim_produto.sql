{{ config(materialized='view') }}

SELECT 
    MD5(CAST(EAN AS VARCHAR) || CAST(Produto AS VARCHAR)) as id_produto,
    CAST(EAN AS VARCHAR) as gtin,
    CAST(Produto AS VARCHAR) as Produto,
    CONCAT(CAST(EAN AS VARCHAR), ' - ', CAST(Produto AS VARCHAR)) as Produto_completo
FROM {{ source('drogamais', 'dw_tb_acode_temp') }}
WHERE EAN IS NOT NULL
GROUP BY EAN, Produto