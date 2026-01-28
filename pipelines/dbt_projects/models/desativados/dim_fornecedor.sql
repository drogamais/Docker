{{ config(materialized='view') }}

SELECT 
    MD5(CAST(Fornecedor AS VARCHAR) || '') as id_fornecedor,
    CAST(Fornecedor AS VARCHAR) as Fornecedor
FROM {{ source('drogamais', 'dw_tb_acode_temp') }}
WHERE Fornecedor IS NOT NULL
GROUP BY Fornecedor