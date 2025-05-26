"""
Configurações centralizadas do projeto
"""
import os
from pathlib import Path
from dotenv import load_dotenv

def get_project_root() -> Path:
    """Retorna o diretório raiz do projeto"""
    return Path(__file__).parent.parent.parent

def validate_config(config: dict):
    """Valida se todas as variáveis de ambiente necessárias estão configuradas"""
    required_vars = [
        'SPARK_HOME',
        'KAGGLE_USERNAME',
        'KAGGLE_KEY',
        'JAVA_HOME',
        'KAGGLE_DATASET'
    ]
    
    missing = []
    for var in required_vars:
        if not config.get(var):
            missing.append(var)
            
    if missing:
        raise ValueError(f"Variáveis de ambiente necessárias não encontradas: {', '.join(missing)}")

def load_config():
    """Carrega e retorna as configurações do projeto"""
    # Carrega variáveis do arquivo .env no diretório raiz
    env_path = get_project_root() / '.env'
    load_dotenv(str(env_path))
    
    config = {
        'SPARK_HOME': os.getenv('SPARK_HOME'),
        'KAGGLE_USERNAME': os.getenv('KAGGLE_USERNAME'),
        'KAGGLE_KEY': os.getenv('KAGGLE_KEY'),
        'JAVA_HOME': os.getenv('JAVA_HOME'),
        'DATA_PATH': os.path.join(str(get_project_root()), os.getenv('DATA_PATH', 'data')),
        'KAGGLE_DATASET': os.getenv('KAGGLE_DATASET'),
        # Caminhos padrão do projeto
        'BRONZE_PATH': 'bronze',
        'SILVER_PATH': 'silver',
        'GOLD_PATH': 'gold',
        # Constantes específicas do dataset
        'DATASET_SOURCE': 'kaggle',
        'DATASET_NAME': 'gas_prices_in_brazil',
        'RAW_DATASET_FILE': '2004-2021.tsv',
        # Nomes das tabelas dimensão e fato
        'DIMENSION_TABLES': [
            'dim_regiao.parquet',
            'dim_estado.parquet',
            'dim_produto.parquet',
            'dim_tempo.parquet'
        ],
        'FACT_TABLE': 'fact_precos.parquet'
    }

    validate_config(config)
    return config

# Initialize CONFIG at module level
CONFIG = load_config()
