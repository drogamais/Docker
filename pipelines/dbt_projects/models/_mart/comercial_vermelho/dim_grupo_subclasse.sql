{{ config(
    materialized='view',
    database='nessie',
    schema='comercial_vermelho',
    alias='dim_grupo_subclasse'
) }}

SELECT 
    -- MD5 gera um hash de 32 caracteres (hex) que serve como UUID determinístico
    -- O "|| ''" força o Dremio a tratar como texto e não tentar converter para número
    MD5(CAST(Grupo AS VARCHAR) || CAST(Sub_Classe AS VARCHAR)) as id_grupo_subclasse, 
    CAST(Grupo AS VARCHAR) as Grupo, 
    CAST(Sub_Classe AS VARCHAR) as Sub_Classe
FROM {{ ref('silver_acode_compras_produto_comercial') }} AT BRANCH main
WHERE Sub_Classe IS NOT NULL
GROUP BY Grupo, Sub_Classe