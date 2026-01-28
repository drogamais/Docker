{{ config(
    materialized='table',
    datalake='nessie',
    schema='silver'
    partition_by=[month('data_emissao')],
    alias='silver_acode_compras_produto_comercial'
) }}

SELECT 
    CAST(EAN AS VARCHAR(20)) AS EAN,
    CAST(Loja_CNPJ AS VARCHAR(18)) AS Loja_CNPJ,
    CAST(MAX(Loja) AS VARCHAR(100)) AS Loja,
    CAST(MAX(Produto) AS VARCHAR(255)) AS Produto,
    CAST(data_emissao AS DATE) AS data_emissao,
    CAST(Fabricante AS VARCHAR(50)) AS Fabricante,
    CAST(Fornecedor AS VARCHAR(120)) AS Fornecedor,
    CAST(Grupo AS VARCHAR(50)) AS Grupo,
    CAST(Sub_Classe AS VARCHAR(80)) AS Sub_Classe,
    CAST(Desc_Marca AS VARCHAR(50)) AS Desc_Marca,
    CAST(SUM(Val_Prod_sem_STRet) AS DECIMAL(15,4)) AS Val_Prod_sem_STRet,
    CAST(SUM(ACODE_Val_Total) AS DECIMAL(15,4)) AS ACODE_Val_Total,
    CAST(SUM(Qtd_Trib) AS DECIMAL(10,4)) AS Qtd_Trib,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS data_atualizacao

FROM minio.bronze."compras-acode"

WHERE data_emissao >= ADD_MONTHS(DATE_TRUNC('MONTH', CURRENT_DATE), -24)

GROUP BY 
    EAN, Loja_CNPJ, data_emissao, Fabricante, Fornecedor, Grupo, Sub_Classe, Desc_Marca