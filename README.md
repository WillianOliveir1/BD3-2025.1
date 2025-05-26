# Pipeline ETL - Preços de Combustíveis no Brasil

## Sobre o Projeto
Pipeline ETL para processamento de dados históricos de preços de combustíveis no Brasil, usando dados do Kaggle para criar um modelo dimensional no formato Star Schema.


# Análise de Preços de Combustíveis no Brasil

Este projeto implementa um pipeline ETL (Extract, Transform, Load) para análise de preços de combustíveis no Brasil, utilizando dados do Kaggle.

## Pré-requisitos

- Python 3.11 ou superior
- pip (gerenciador de pacotes Python)
- Conta no Kaggle (para acesso aos dados)
- Git

## Configuração do Ambiente

1. Clone o repositório:
```bash
git clone https://github.com/WillianOliveir1/BD3-2025.1.git
cd BD3-2025.1
```

2. Crie e ative um ambiente virtual:
```bash
python -m venv venv
.\venv\Scripts\activate
```

3. Instale as dependências:
```bash
pip install -r requirements.txt
```

4. Configure as credenciais do Kaggle:
   - Acesse [kaggle.com](https://www.kaggle.com/)
   - Vá em "Account" > "API" > "Create New API Token"
   - O navegador irá baixar automaticamente o arquivo `kaggle.json`
   - Crie a pasta `.kaggle` no seu diretório home, se ela não existir:
     ```bash
     mkdir %USERPROFILE%\.kaggle
     ```
   - Mova o arquivo `kaggle.json` baixado para a pasta `.kaggle`:
     ```bash
     move %USERPROFILE%\Downloads\kaggle.json %USERPROFILE%\.kaggle\
     ```
   - **Importante**: 
     - Nunca compartilhe o arquivo `kaggle.json` ou adicione-o ao controle de versão
     - O arquivo deve ficar apenas em `%USERPROFILE%\.kaggle\kaggle.json`
     - Não coloque o arquivo em nenhum outro local do projeto

## Configuração das Variáveis de Ambiente

O projeto utiliza um arquivo `.env` para configurar variáveis de ambiente importantes. Copie o arquivo `.env.example` para `.env` e configure as seguintes variáveis:

### SPARK_HOME
- **O que é**: Caminho para a instalação do Apache Spark
- **Como configurar**: 
  1. Baixe o Apache Spark da [página oficial](https://spark.apache.org/downloads.html)
  2. Descompacte em um diretório (ex: `C:/spark-3.5.0-bin-hadoop3`)
  3. Defina `SPARK_HOME` com o caminho completo para este diretório

### KAGGLE_USERNAME e KAGGLE_KEY
- **O que é**: Suas credenciais do Kaggle
- **Como configurar**:
  1. Acesse [kaggle.com/account](https://www.kaggle.com/account)
  2. Role até a seção "API"
  3. Clique em "Create New API Token"
  4. Abra o arquivo `kaggle.json` baixado
  5. Copie o valor de "username" para `KAGGLE_USERNAME`
  6. Copie o valor de "key" para `KAGGLE_KEY`

### JAVA_HOME
- **O que é**: Caminho para a instalação do Java JDK
- **Como configurar**:
  1. Instale o Java JDK 11 ou superior
  2. Encontre o diretório de instalação (geralmente em `C:/Program Files/Java/jdk-11.x.x`)
  3. Defina `JAVA_HOME` com o caminho completo para este diretório

### DATA_PATH
- **O que é**: Caminho para o diretório onde os dados serão armazenados
- **Valor padrão**: `./data`
- **Como configurar**: 
  - Mantenha o valor padrão `./data` para usar o diretório local
  - Ou defina um caminho absoluto para armazenar os dados em outro local

### KAGGLE_DATASET
- **O que é**: Identificador do dataset do Kaggle que será usado
- **Valor padrão**: `matheusfreitag/gas-prices-in-brazil`
- **Observação**: Não altere este valor, ele é específico para este projeto

## Estrutura do Projeto


O projeto segue uma arquitetura em camadas para processamento de dados:
- **Bronze**: Dados brutos extraídos do Kaggle
- **Silver**: Dados limpos e transformados
- **Gold**: Dados agregados e prontos para análise

```
.
├── data/                      # Dados organizados em camadas
│   ├── bronze/               # Dados brutos do Kaggle
│   │   └── gas_prices_in_brazil/
│   ├── silver/              # Modelo Star Schema
│   │   └── gas_prices_in_brazil/
│   │       ├── dim_regiao.parquet
│   │       ├── dim_estado.parquet
│   │       ├── dim_produto.parquet
│   │       ├── dim_tempo.parquet
│   │       └── fact_precos.parquet
│   └── gold/                # Visualizações e análises
│       └── gas_prices_in_brazil/
├── src/                      # Código fonte
│   ├── infrastructure/      # Componentes compartilhados
│   │   ├── data_lake_manager.py
│   │   └── spark_manager_v2.py
│   └── pipeline/
│       └── gas_prices_in_brazil/          # Pipeline específico Kaggle
│           ├── upstream/    # Extração (E)
│           ├── midstream/   # Transformação (T)
│           ├── downstream/  # Load (L)
│           ├── etl_pipeline.py
│           └── run_pipeline.py
└── tests/                    # Testes automatizados
    ├── test_extract.py
    └── test_reorganized_pipeline.py
```

## Modelo de Dados

### Star Schema (Silver)
- **Fact_Precos**: Preços dos combustíveis
- **Dim_Regiao**: Regiões do Brasil
- **Dim_Estado**: Estados brasileiros
- **Dim_Produto**: Tipos de combustível
- **Dim_Tempo**: Dimensão temporal

## Notas de Implementação

### Camadas de Dados
- **Bronze**: Dados brutos do Kaggle
- **Silver**: Modelo dimensional otimizado
- **Gold**: Visualizações e análises prontas

### Spark SQL
- Dados em Silver salvos em Parquet
- Otimizado para consultas analíticas
- Suporte a joins eficientes

### Qualidade de Dados
- Validações na extração
- Transformações documentadas
- Testes automatizados

## Execução do Pipeline

### Pipeline Completo
```bash
python src/pipeline/gas_prices_in_brazil/run_pipeline.py
```

### Execução por Etapas
```bash
# Extração (Bronze)
python src/pipeline/gas_prices_in_brazil/run_stages.py --stage extract

# Transformação (Silver)
python src/pipeline/gas_prices_in_brazil/run_stages.py --stage transform

# Carregamento (Gold)
python src/pipeline/gas_prices_in_brazil/run_stages.py --stage load
```

## Testes

### Executar Todos os Testes
```bash
python -m pytest tests/
```

### Executar Testes Específicos
```bash
# Teste de extração
python -m pytest tests/test_extract.py

# Teste do pipeline reorganizado
python -m pytest tests/test_reorganized_pipeline.py
```

## Estrutura dos Dados

### Camada Bronze
- Dataset: `gas_prices_in_brazil`
- Arquivo: `2004-2021.tsv`
- Fonte: [Kaggle - Gas Prices in Brazil](https://www.kaggle.com/matheusfreitag/gas-prices-in-brazil)

## Contribuindo

1. Crie um branch para sua feature:
```bash
git checkout -b feature/nova-funcionalidade
```

2. Faça suas alterações e commit:
```bash
git add .
git commit -m "feat: descrição da sua funcionalidade"
```

3. Push para o repositório:
```bash
git push origin feature/nova-funcionalidade
```

4. Abra um Pull Request

## 📁 Estrutura do Projeto Organizada

O projeto foi reorganizado seguindo as melhores práticas de engenharia de dados:

- **`src/`** - Código fonte principal
  - `upstream/` - Módulos de extração de dados (ingestão)
  - `midstream/` - Transformações Bronze → Silver (processamento)
  - `downstream/` - Módulos de carregamento e entrega
  - `pipeline/` - Orquestração e coordenação do pipeline ETL
  - `infrastructure/` - Gerenciamento de infraestrutura (DataLake, Spark)
  - `analysis/` - Análises e consultas avançadas
