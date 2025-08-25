# Pacote de Ingestão de Dados

Este projeto demonstra um pipeline de ingestão de dados, desde a leitura de arquivos locais até a disponibilização em um banco de dados analítico.

## Objetivo

O principal objetivo deste projeto é automatizar o processo de coleta, processamento e armazenamento de dados de vendas (SKU e preços) para análises futuras. Ele serve como um exemplo prático de um pipeline de dados utilizando ferramentas como Docker, MinIO e ClickHouse.

## Fluxo Funcional

O pipeline executa os seguintes passos de forma automatizada:

1.  **Leitura dos Dados**: O processo inicia com a leitura de dados de um arquivo CSV contendo informações sobre SKUs e seus respectivos preços.
2.  **Processamento e Enriquecimento**: Os dados lidos são processados, limpos e transformados para um formato mais eficiente e padronizado (Parquet).
3.  **Armazenamento Intermediário**: O arquivo Parquet processado é enviado para um ambiente de armazenamento de objetos (MinIO), garantindo que os dados brutos e processados sejam mantidos em um local seguro e acessível.
4.  **Carga no Banco de Dados**: Finalmente, os dados são carregados do arquivo Parquet para o banco de dados ClickHouse, onde ficam disponíveis para consultas e análises de alta performance.
5.  **Visualização Simplificada**: Uma view (visão) é criada no ClickHouse para simplificar o acesso e a consulta aos dados mais recentes.

## Como Executar

Para executar o projeto e colocar o pipeline em funcionamento, siga os passos abaixo:

1.  **Iniciar a Infraestrutura**:
    Os serviços de apoio (MinIO e ClickHouse) são gerenciados com Docker. Para iniciá-los, execute o comando na raiz do projeto:
    ```bash
    docker-compose up -d
    ```

2.  **Executar o Pipeline de Ingestão**:
    Com a infraestrutura no ar, execute o script principal da aplicação para iniciar o processo de ingestão de dados:
    ```bash
    python app.py
    ```

Ao final da execução, os dados do arquivo `sku_price.csv` estarão processados e disponíveis para consulta na tabela `working_data` e na view `latest_sku_prices` dentro do ClickHouse.

Executar pacote: Para executar o pacote é necessário dar o comando 'poetry run python app.py' na pasta 'data_pipeline'
Executar testes: Dentro da pasta 'data_pipeline' entrar na pasta 'tests' e executar o comando 'pytest'
