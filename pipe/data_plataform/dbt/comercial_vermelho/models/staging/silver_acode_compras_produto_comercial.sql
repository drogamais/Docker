{{ config(
    materialized='external',
    location='s3://silver/silver_acode_compras_produto_comercial',
    alias='silver_acode_compras_produto_comercial',
    options={
        'format': 'parquet',
        'partition_by': 'ano',
        'overwrite_or_ignore': 'true'
    }
) }}

SELECT 
    CAST(EAN AS VARCHAR) AS EAN,
    CAST(Loja_CNPJ AS VARCHAR) AS Loja_CNPJ,
    CAST(MAX(Loja) AS VARCHAR) AS Loja,
    CAST(MAX(Produto) AS VARCHAR) AS Produto,
    -- DuckDB lida bem com conversão direta de Date
    CAST(data_emissao AS DATE) AS data_emissao,
    YEAR(CAST(data_emissao AS DATE)) AS ano,

    CAST(Fabricante AS VARCHAR) AS Fabricante,
    CAST(Fornecedor AS VARCHAR) AS Fornecedor,
    CAST(Grupo AS VARCHAR) AS Grupo,
    CAST(Sub_Classe AS VARCHAR) AS Sub_Classe,
    CAST(Desc_Marca AS VARCHAR) AS Desc_Marca,
    -- Decimal precisa de precisão
    CAST(SUM(Val_Prod_sem_STRet) AS DECIMAL(15,4)) AS Val_Prod_sem_STRet,
    CAST(SUM(ACODE_Val_Total) AS DECIMAL(15,4)) AS ACODE_Val_Total,
    CAST(SUM(Qtd_Trib) AS DECIMAL(10,4)) AS Qtd_Trib,
    current_timestamp AS data_atualizacao

-- O '**' garante que ele leia recursivamente todas as subpastas do bucket
-- CORREÇÃO: Voltamos para o comando direto que força a leitura do arquivo S3
FROM read_parquet('s3://bronze/compras-acode/**/*.parquet')

-- DuckDB usa sintaxe padrão SQL para intervalos
WHERE data_emissao >= (current_date - INTERVAL 24 MONTH)

GROUP BY 
    EAN, Loja_CNPJ, data_emissao, Fabricante, Fornecedor, Grupo, Sub_Classe, Desc_Marca