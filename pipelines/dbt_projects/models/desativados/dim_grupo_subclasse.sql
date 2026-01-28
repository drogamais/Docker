{{ config(materialized='view') }}

SELECT 
    -- MD5 gera um hash de 32 caracteres (hex) que serve como UUID determinístico
    -- O "|| ''" força o Dremio a tratar como texto e não tentar converter para número
    MD5(CAST(Grupo AS VARCHAR) || CAST(Sub_Classe AS VARCHAR)) as id_grupo_subclasse, 
    CAST(Grupo AS VARCHAR) as Grupo, 
    CAST(Sub_Classe AS VARCHAR) as Sub_Classe
FROM {{ source('drogamais', 'dw_tb_acode_temp') }}
WHERE Sub_Classe IS NOT NULL
GROUP BY Grupo, Sub_Classe