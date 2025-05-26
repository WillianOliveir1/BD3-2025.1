# 🚗⛽ Pipeline ETL - Análise de Preços de Combustíveis no Brasil

## 📊 Sobre o Projeto
Este projeto implementa um pipeline ETL (Extract, Transform, Load) para análise avançada de preços de combustíveis no Brasil, utilizando dados históricos do Kaggle (2004-2021). O projeto emprega tecnologias de Big Data como **Apache Spark** e conceitos de **Data Lake** para processar e analisar grandes volumes de dados de forma eficiente.

### 🎯 Objetivo
Construir um sistema robusto de análise de dados que permite:
- Extração automatizada de dados do Kaggle
- Transformação dos dados em um modelo dimensional (Star Schema)
- Análises avançadas com Spark SQL para responder questões de negócio
- Consultas otimizadas para grandes volumes de dados

### 🏗️ Arquitetura
O projeto utiliza uma arquitetura de **Data Lake** com três camadas:
- **Bronze** (Raw Data): Dados brutos extraídos do Kaggle
- **Silver** (Curated Data): Dados limpos e transformados em modelo dimensional
- **Gold** (Business Data): Dados agregados e otimizados para análises específicas

## 🛠️ Tecnologias Utilizadas

### Core Technologies
- **Apache Spark 3.5+**: Motor de processamento distribuído para transformações e análises
- **Python 3.11+**: Linguagem principal do projeto
- **PyArrow**: Interface para formatos de dados colunares
- **Parquet**: Formato de armazenamento colunar otimizado

### Data Engineering Stack
- **Kaggle API**: Extração automatizada de dados
- **Data Lake Architecture**: Organização em camadas Bronze/Silver/Gold
- **Star Schema**: Modelagem dimensional para análises OLAP
- **Spark SQL**: Engine de consultas para análises complexas

### Infrastructure & Tools
- **pytest**: Framework de testes
- **logging**: Sistema de logs estruturado
- **pathlib**: Gerenciamento de caminhos de arquivos
- **typing**: Type hints para melhor qualidade de código

## 📋 Pré-requisitos

- Python 3.11 ou superior
- pip (gerenciador de pacotes Python)
- Conta no Kaggle (para acesso aos dados)
- Git

## 🚀 Configuração do Ambiente

1. **Clone o repositório**:
```cmd
git clone https://github.com/WillianOliveir1/BD3-2025.1.git
cd BD3-2025.1
```

2. **Crie e ative um ambiente virtual**:
```cmd
python -m venv venv
venv\Scripts\activate
```

3. **Instale as dependências**:
```cmd
pip install -r requirements.txt
```

4. **Configure as credenciais do Kaggle**:
   - Acesse [kaggle.com](https://www.kaggle.com/)
   - Vá em "Account" > "API" > "Create New API Token"
   - O navegador irá baixar automaticamente o arquivo `kaggle.json`
   - Crie a pasta `.kaggle` no seu diretório home, se ela não existir:
     ```cmd
     mkdir %USERPROFILE%\.kaggle
     ```
   - Mova o arquivo `kaggle.json` baixado para a pasta `.kaggle`:
     ```cmd
     move %USERPROFILE%\Downloads\kaggle.json %USERPROFILE%\.kaggle\
     ```
   - **⚠️ Importante**: 
     - Nunca compartilhe o arquivo `kaggle.json` ou adicione-o ao controle de versão
     - O arquivo deve ficar apenas em `%USERPROFILE%\.kaggle\kaggle.json`

## ⚙️ Configuração das Variáveis de Ambiente

O projeto utiliza um arquivo `.env` para configurar variáveis de ambiente importantes. 

### 🔥 SPARK_HOME
- **O que é**: Caminho para a instalação do Apache Spark
- **Como configurar**: 
  1. Baixe o Apache Spark da [página oficial](https://spark.apache.org/downloads.html)
  2. Descompacte em um diretório (ex: `C:\spark-3.5.0-bin-hadoop3`)
  3. Defina `SPARK_HOME` com o caminho completo para este diretório

### 🔑 KAGGLE_USERNAME e KAGGLE_KEY
- **O que é**: Suas credenciais do Kaggle
- **Como configurar**:
  1. Acesse [kaggle.com/account](https://www.kaggle.com/account)
  2. Role até a seção "API"
  3. Clique em "Create New API Token"
  4. Abra o arquivo `kaggle.json` baixado
  5. Copie o valor de "username" para `KAGGLE_USERNAME`
  6. Copie o valor de "key" para `KAGGLE_KEY`

### ☕ JAVA_HOME
- **O que é**: Caminho para a instalação do Java JDK
- **Como configurar**:
  1. Instale o Java JDK 11 ou superior
  2. Encontre o diretório de instalação (geralmente em `C:\Program Files\Java\jdk-11.x.x`)
  3. Defina `JAVA_HOME` com o caminho completo para este diretório

### 📂 DATA_PATH
- **O que é**: Caminho para o diretório onde os dados serão armazenados
- **Valor padrão**: `./data`
- **Como configurar**: 
  - Mantenha o valor padrão `./data` para usar o diretório local
  - Ou defina um caminho absoluto para armazenar os dados em outro local

### 📊 KAGGLE_DATASET
- **O que é**: Identificador do dataset do Kaggle que será usado
- **Valor padrão**: `matheusfreitag/gas-prices-in-brazil`
- **Observação**: Não altere este valor, ele é específico para este projeto

## 📁 Estrutura do Projeto

O projeto segue uma arquitetura de **Data Lake** moderna com separação clara de responsabilidades:

```
📦 BD3-2025.1/
├── 📂 data/                           # 🏗️ Data Lake - Armazenamento em Camadas
│   ├── 🥉 bronze/                     # Dados brutos (Raw Data)
│   │   └── kaggle/
│   │       └── gas_prices_in_brazil/
│   │           ├── YYYYMMDD/
│   │               └── 2004-2021.tsv
│   │           
│   ├── 🥈 silver/                     # Dados curados (Star Schema)
│   │   └── kaggle/
│   │       └── gas_prices_in_brazil/
│   │           ├── dim_estado.parquet/     # Dimensão Estados
│   │           ├── dim_produto.parquet/    # Dimensão Produtos
│   │           ├── dim_regiao.parquet/     # Dimensão Regiões
│   │           ├── dim_tempo.parquet/      # Dimensão Tempo
│   │           └── fact_precos.parquet/    # Tabela Fato - Preços
│   └── 🥇 gold/                       # Dados para negócio (Analytics Ready)
├── 📂 src/                            # 💻 Código Fonte
│   ├── 📂 analysis/                   # 📊 Módulos de Análise
│   │   ├── fuel_price_analyzer.py     # Analisador principal
│   │   └── run_analysis.py            # Runner de análises
│   ├── 📂 infrastructure/             # 🏗️ Infraestrutura e Configuração
│   │   ├── config.py                  # Configurações globais
│   │   ├── data_lake_manager.py       # Gerenciador do Data Lake
│   │   ├── environment_manager.py     # Gerenciador de ambiente
│   │   ├── hadoop_setup.py            # Configuração Hadoop
│   │   ├── logging_config.py          # Sistema de logs
│   │   └── spark_manager.py           # Gerenciador Spark
│   └── 📂 pipeline/                   # 🔄 Pipeline ETL
│       └── kaggle/                    # Pipeline específico Kaggle
│           ├── upstream/              # ⬇️ Extração (Extract)
│           │   └── extractor.py
│           ├── midstream/             # 🔄 Transformação (Transform)
│           │   └── transformer.py
│           ├── downstream/            # ⬆️ Carregamento (Load)
│           │   └── loader.py
│           ├── etl_pipeline.py        # 🚀 Pipeline principal
│           └── run_pipeline.py        # Runner do pipeline
├── 📂 tests/                          # 🧪 Testes Automatizados
├── 📄 requirements.txt                # 📦 Dependências Python
├── 📄 setup.py                        # ⚙️ Configuração do projeto
└── 📖 README.md                       # Documentação
```

### 🏗️ Arquitetura em Camadas

#### 🥉 Bronze Layer (Raw Data)
- **Propósito**: Armazenamento de dados brutos extraídos do Kaggle
- **Formato**: TSV (Tab-Separated Values)
- **Organização**: Particionado por data de extração
- **Características**: Dados imutáveis, backup histórico

#### 🥈 Silver Layer (Curated Data)
- **Propósito**: Dados limpos e transformados em modelo dimensional
- **Formato**: Parquet (otimizado para consultas analíticas)
- **Modelo**: Star Schema com dimensões e tabela fato
- **Características**: Dados validados, tipados e otimizados

#### 🥇 Gold Layer (Business Data)
- **Propósito**: Dados agregados e otimizados para casos de uso específicos
- **Formato**: Parquet com particionamento estratégico
- **Características**: Dados prontos para visualização e relatórios

## 🗄️ Modelo de Dados - Star Schema

### 📊 Arquitetura Dimensional

O projeto implementa um **Star Schema** otimizado para análises OLAP (Online Analytical Processing):

```
                    ┌─────────────────┐
                    │   dim_tempo     │
                    │ ─────────────── │
                    │ id (PK)         │
                    │ inicio          │
                    │ fim             │
                    │ semana          │
                    │ mes             │
                    │ ano             │
                    └─────────┬───────┘
                              │
            ┌─────────────────┐│┌─────────────────┐
            │  dim_produto    │││   dim_regiao    │
            │ ─────────────── │││ ─────────────── │
            │ id (PK)         │││ id (PK)         │
            │ nome            │││ nome            │
            │ unidade         │││ codigo          │
            └─────────┬───────┘│└─────────┬───────┘
                      │        │          │
                      │    ┌───▼──────────▼───┐
                      │    │   fact_precos    │
                      │    │ ─────────────────│
                      └────┤ produtoId (FK)   │
                           │ estadoId (FK)    │
                           │ regiaoId (FK)    │
                           │ tempoId (FK)     │
                           │ pMed             │
                           │ pMin             │
                           │ pMax             │
                           │ desvio           │
                           │ coef_var         │
                           │ registro_count   │
                           └───▲──────────────┘
                               │
                    ┌─────────┴───────┐
                    │   dim_estado    │
                    │ ─────────────── │
                    │ id (PK)         │
                    │ sigla           │
                    │ nome            │
                    │ regiaoId (FK)   │
                    └─────────────────┘
```

### 📋 Descrição das Tabelas

#### 🎯 **fact_precos** (Tabela Fato)
- **Propósito**: Armazena os preços dos combustíveis com métricas agregadas
- **Granularidade**: Por produto, estado, região e período
- **Métricas**:
  - `pMed`: Preço médio de revenda
  - `pMin`: Preço mínimo de revenda  
  - `pMax`: Preço máximo de revenda
  - `desvio`: Desvio padrão dos preços
  - `coef_var`: Coeficiente de variação
  - `registro_count`: Quantidade de registros agregados

#### 🏷️ **dim_produto** (Dimensão Produto)
- **Propósito**: Tipos de combustíveis disponíveis
- **Exemplos**: Gasolina Comum, Etanol Hidratado, Diesel S-10, GNV

#### 🌍 **dim_regiao** (Dimensão Região)
- **Propósito**: Regiões geográficas do Brasil
- **Valores**: Norte, Nordeste, Centro-Oeste, Sudeste, Sul

#### 🏛️ **dim_estado** (Dimensão Estado)
- **Propósito**: Estados brasileiros com relacionamento hierárquico
- **Características**: Vinculado à região correspondente

#### 📅 **dim_tempo** (Dimensão Tempo)
- **Propósito**: Períodos de análise com hierarquia temporal
- **Granularidade**: Períodos semanais com componentes de data
- **Componentes**: Ano, mês, semana para drill-down/roll-up

## 🚀 Execução do Pipeline

### 🔄 Pipeline Completo (Recomendado)
Execute todo o processo ETL de uma vez:
```cmd
python src/pipeline/kaggle/run_pipeline.py
```

### ⚙️ Execução por Etapas
Para desenvolvimento e debugging, execute etapas específicas:

```cmd
# 1️⃣ Extração (Bronze) - Dados do Kaggle
python src/pipeline/kaggle/run_stages.py --stage extract

# 2️⃣ Transformação (Silver) - Modelo Dimensional
python src/pipeline/kaggle/run_stages.py --stage transform

# 3️⃣ Carregamento (Gold) - Dados Agregados
python src/pipeline/kaggle/run_stages.py --stage load
```

### 📊 Executar Análises
Execute análises específicas sobre os dados processados:
```cmd
python src/analysis/run_analysis.py
```

## 🧪 Testes e Validação

### 🏃‍♂️ Executar Todos os Testes
```cmd
python -m pytest tests/ -v
```

### 🎯 Executar Testes Específicos
```cmd
# Teste de extração de dados
python -m pytest tests/test_extract.py -v

# Teste do pipeline ETL
python -m pytest tests/test_etl_pipeline.py -v

# Teste do gerenciador Spark
python -m pytest tests/test_spark_manager.py -v

# Teste de transformações
python -m pytest tests/test_transform.py -v
```

### 📈 Cobertura de Testes
```cmd
python -m pytest tests/ --cov=src --cov-report=html
```

## 📊 Estrutura dos Dados e Fonte

### 🥉 Camada Bronze
- **Dataset**: `gas_prices_in_brazil`
- **Arquivo**: `2004-2021.tsv` (125MB+)
- **Fonte**: [Kaggle - Gas Prices in Brazil](https://www.kaggle.com/matheusfreitag/gas-prices-in-brazil)
- **Período**: 2004-2021 (18 anos de dados históricos)
- **Registros**: ~2.8 milhões de registros
- **Atualização**: Particionado por data de extração para versionamento

### 🔍 Dicionário de Dados
### 🗂️ Dados Origem (Kaggle)
- **Arquivo**: `2004-2021.tsv`
- **Formato**: Tab-Separated Values (TSV)

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `DATA INICIAL` | Date | Data de início da coleta |
| `DATA FINAL` | Date | Data final da coleta |
| `REGIÃO` | String | Região geográfica (N, NE, CO, SE, S) |
| `ESTADO` | String | Estado brasileiro (sigla) |
| `PRODUTO` | String | Tipo de combustível |
| `NÚMERO DE POSTOS PESQUISADOS` | Integer | Quantidade de postos na amostra |
| `UNIDADE DE MEDIDA` | String | Medida do produto (R$/l, R$/m³) |
| `PREÇO MÉDIO REVENDA` | Double | Média dos preços coletados |
| `DESVIO PADRÃO REVENDA` | Double | Desvio padrão dos preços |
| `PREÇO MÍNIMO REVENDA` | Double | Menor preço encontrado |
| `PREÇO MÁXIMO REVENDA` | Double | Maior preço encontrado |
| `COEF DE VARIAÇÃO REVENDA` | Double | Coeficiente de variação dos preços |

#### 📊 Tabela Fato (fact_precos)
| Campo | Tipo | Descrição |
|-------|------|-----------|
| `produtoId` | Long | ID do produto (FK) |
| `estadoId` | Long | ID do estado (FK) |
| `regiaoId` | Long | ID da região (FK) |
| `tempoId` | Long | ID do período (FK) |
| `pMed` | Double | Preço médio de revenda (R$) |
| `pMin` | Double | Preço mínimo de revenda (R$) |
| `pMax` | Double | Preço máximo de revenda (R$) |
| `desvio` | Double | Desvio padrão dos preços |
| `coef_var` | Double | Coeficiente de variação |
| `registro_count` | Long | Quantidade de registros agregados |

#### 🏷️ Dimensão Produto (dim_produto)
| Campo | Tipo | Descrição |
|-------|------|-----------|
| `id` | Long | ID único do produto (PK) |
| `nome` | String | Nome do combustível |
| `unidade` | String | Unidade de medida |

#### 🌍 Dimensão Região (dim_regiao)
| Campo | Tipo | Descrição |
|-------|------|-----------|
| `id` | Long | ID único da região (PK) |
| `nome` | String | Nome da região |
| `codigo` | String | Sigla da região |

#### 🏛️ Dimensão Estado (dim_estado)
| Campo | Tipo | Descrição |
|-------|------|-----------|
| `id` | Long | ID único do estado (PK) |
| `sigla` | String | Sigla do estado |
| `nome` | String | Nome do estado |
| `regiaoId` | Long | ID da região (FK) |

#### 📅 Dimensão Tempo (dim_tempo)
| Campo | Tipo | Descrição |
|-------|------|-----------|
| `id` | Long | ID único do período (PK) |
| `inicio` | Date | Data de início |
| `fim` | Date | Data de fim |
| `semana` | Int | Número da semana |
| `mes` | Int | Número do mês |
| `ano` | Int | Ano |

## 💡 Funcionalidades Implementadas

### 🔧 Pipeline ETL Robusto
- ✅ Extração automatizada com retry e validação
- ✅ Transformações usando Apache Spark
- ✅ Modelo dimensional com Star Schema
- ✅ Particionamento inteligente dos dados
- ✅ Sistema de logs estruturado
- ✅ Testes automatizados

### 📈 Análises Avançadas
- ✅ Consultas Spark SQL otimizadas
- ✅ Agregações por múltiplas dimensões
- ✅ Análise temporal com drill-down
- ✅ Comparações regionais e por produto
- ✅ Métricas estatísticas (média, min, max, desvio)

### 🏗️ Infraestrutura Moderna
- ✅ Configuração automática do ambiente Spark
- ✅ Gerenciamento de Data Lake
- ✅ Suporte a Hadoop (opcional)
- ✅ Arquitetura escalável e modular

## 🎯 Casos de Uso de Análise

Este projeto permite responder questões como:

### 📊 Análises de Preço
- Maiores e menores preços por combustível/região/período
- Evolução temporal dos preços
- Comparação entre estados e regiões
- Volatilidade dos preços (coeficiente de variação)

### ⛽ Análises Comparativas
- Combustível mais econômico por região/período
- Análise de competitividade Etanol vs Gasolina
- Ranking de estados por preço médio
- Sazonalidade dos preços

### 🏭 Análises de Mercado
- Participação por tipo de combustível
- Concentração regional de preços
- Análise de outliers e anomalias
- Tendências de longo prazo

## 🤝 Contribuindo

1. **Crie um branch para sua feature**:
```cmd
git checkout -b feature/nova-funcionalidade
```

2. **Faça suas alterações e commit**:
```cmd
git add .
git commit -m "feat: descrição da sua funcionalidade"
```

3. **Execute os testes**:
```cmd
python -m pytest tests/ -v
```

4. **Push para o repositório**:
```cmd
git push origin feature/nova-funcionalidade
```

5. **Abra um Pull Request**

## 📚 Documentação Adicional

### 🔗 Links Úteis
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Kaggle API Documentation](https://github.com/Kaggle/kaggle-api)
- [PyArrow Documentation](https://arrow.apache.org/docs/python/)
- [Dataset Original](https://www.kaggle.com/datasets/matheusfreitag/gas-prices-in-brazil)

### 🏆 Melhores Práticas Implementadas
- **Arquitetura em Camadas**: Separação clara Bronze/Silver/Gold
- **Modelo Dimensional**: Star Schema para consultas eficientes
- **Formato Colunar**: Parquet para performance analítica
- **Particionamento**: Organização otimizada dos dados
- **Testes Automatizados**: Garantia de qualidade
- **Logs Estruturados**: Monitoramento e debugging
- **Type Hints**: Código mais legível e manutenível

## 🚀 Próximos Passos

### 📈 Melhorias Planejadas
- [ ] Implementação de Hadoop distribuído
- [ ] Dashboard interativo com visualizações
- [ ] API REST para consultas
- [ ] Pipeline CI/CD automatizado
- [ ] Monitoramento com métricas
- [ ] Algoritmos de Machine Learning para previsão de preços

### 🎯 Extensões Possíveis
- [ ] Integração com outras fontes de dados (ANP, IBGE)
- [ ] Análises de correlação com indicadores econômicos
- [ ] Implementação de Data Quality checks
- [ ] Streaming de dados em tempo real
- [ ] Deploy em cloud (AWS, Azure, GCP)

---

## 📝 Licença

Este projeto é desenvolvido para fins educacionais como parte do curso de Banco de Dados 3 (BD3-2025.1).

## 👥 Autores

- **Willian Oliveira** - Desenvolvimento e arquitetura

---

**⭐ Se este projeto foi útil para você, considere dar uma estrela no repositório!**
