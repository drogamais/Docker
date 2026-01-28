{{ config(
    materialized='view',
    database='nessie',
    schema='comercial_vermelho',
    alias='gold_acode_compras_produto_comercial'
) }}

SELECT
    -- Criando os mesmos IDs das dimensões para servirem de chave estrangeira (FK)
    MD5(CAST(EAN AS VARCHAR) || CAST(Produto AS VARCHAR)) AS id_produto,
    MD5(CAST(Desc_Marca AS VARCHAR)) AS id_marca,
    MD5(CAST(Fornecedor AS VARCHAR)) AS id_fornecedor,
    MD5(CAST(Grupo AS VARCHAR) || CAST(Sub_Classe AS VARCHAR)) AS id_grupo_subclasse,
    
    -- Colunas de Contexto e Tempo
    CAST(Loja_CNPJ AS VARCHAR(18)) AS Loja_CNPJ,
    CAST(Loja AS VARCHAR(100)) AS Loja,
    CAST(data_emissao AS DATE) AS data_emissao,
    
    -- Métricas (O que será somado no Power BI)
    Val_Prod_sem_STRet,
    ACODE_Val_Total,
    Qtd_Trib,
    
    -- Auditoria
    data_atualizacao

FROM {{ ref('silver_acode_compras_produto_comercial') }} AT BRANCH main