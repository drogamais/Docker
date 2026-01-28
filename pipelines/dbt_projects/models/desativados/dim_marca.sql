{{ config(materialized='view') }}

SELECT 
    MD5(CAST(Desc_Marca AS VARCHAR) || '') as id_marca,
    CAST(Desc_Marca AS VARCHAR) as nome_marca
FROM {{ source('drogamais', 'dw_tb_acode_temp') }}
WHERE Desc_Marca IS NOT NULL
GROUP BY Desc_Marca