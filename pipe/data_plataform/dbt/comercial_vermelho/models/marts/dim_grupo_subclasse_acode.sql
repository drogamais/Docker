{{ config(
    materialized='table',
    database='mariadb_end',
    schema='dbDrogamais',
    alias='dim_grupo_subclasse_acode',
    pre_hook=["DROP TABLE IF EXISTS mariadb_end.dbDrogamais.dim_grupo_subclasse_acode"]
) }}



SELECT 
    -- MD5 gera um hash de 32 caracteres (hex) que serve como UUID determinístico
    -- O "|| ''" força o Dremio a tratar como texto e não tentar converter para número
    hash(CAST(Grupo AS VARCHAR) || CAST(Sub_Classe AS VARCHAR)) as id_grupo_subclasse, 
    CAST(Grupo AS VARCHAR) as Grupo, 
    CAST(Sub_Classe AS VARCHAR) as Sub_Classe

-- FROM {{ ref('silver_acode_compras_produto_comercial') }}
FROM read_parquet('s3://silver/silver_acode_compras_produto_comercial/**/*.parquet')

WHERE Sub_Classe IS NOT NULL
GROUP BY Grupo, Sub_Classe