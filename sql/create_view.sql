CREATE VIEW IF NOT EXISTS sku_price AS
SELECT
    JSONExtractString(dado_linha, 'cod_prod') AS cod_prod,
    JSONExtractString(dado_linha, 'data_inicio') AS data_inicio,
    JSONExtractString(dado_linha, 'data_fim') AS data_fim,
    JSONExtractFloat(dado_linha, 'preco') AS preco,
    data_ingestao,
    tag
FROM
    working_data;
