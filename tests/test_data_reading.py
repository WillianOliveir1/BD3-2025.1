"""
Testes para leitura de dados usando Spark
"""
import pytest
from pathlib import Path
from src.infrastructure.logging_config import setup_logger, LOG_EMOJIS
from src.infrastructure.spark_manager import SparkManager
from src.infrastructure.data_lake_manager import DataLakeManager
from src.infrastructure.config import CONFIG

# Configurar logger para testes
logger = setup_logger('TestDataReading')

@pytest.fixture(scope="module")
def spark_manager():
    """Fixture que fornece um gerenciador Spark para todos os testes"""
    logger.info("%s Configurando ambiente Spark para testes", LOG_EMOJIS['START'])
    
    # Criar e configurar SparkManager
    manager = SparkManager(
        app_name="TestDataReading",
        config={
            "spark.driver.host": "localhost",
            "spark.driver.bindAddress": "127.0.0.1",
            "spark.master": "local[*]",
            "spark.executor.memory": "2g",
            "spark.driver.memory": "2g",
            "spark.sql.shuffle.partitions": "2",
            "spark.default.parallelism": "2"
        }
    )
    
    # Iniciar sessão
    logger.info("%s Iniciando sessão Spark", LOG_EMOJIS['PROCESS'])
    yield manager
    
    # Limpar após os testes
    logger.info("%s Finalizando sessão Spark", LOG_EMOJIS['PROCESS'])
    manager.stop_session()
    logger.info("%s Sessão Spark finalizada", LOG_EMOJIS['END'])

def get_most_recent_data_path(base_path: Path) -> Path:
    """Retorna o caminho da pasta de dados mais recente"""
    logger.info("%s Procurando pasta de dados mais recente em: %s", LOG_EMOJIS['PROCESS'], base_path)
    
    # Listar todas as pastas que começam com uma data (YYYY-MM)
    data_folders = [f for f in base_path.iterdir() if f.is_dir() and len(f.name) >= 7 and f.name[:7].replace("-", "").isdigit()]
    
    if not data_folders:
        error_msg = f"Nenhuma pasta com data encontrada em {base_path}"
        logger.error("%s %s", LOG_EMOJIS['ERROR'], error_msg)
        raise FileNotFoundError(error_msg)
    
    # Ordenar por nome (que é a data) e pegar o mais recente
    latest_folder = sorted(data_folders)[-1]
    logger.info("%s Pasta mais recente encontrada: %s", LOG_EMOJIS['SUCCESS'], latest_folder)
    return latest_folder

def test_read_gas_prices_tsv(spark_manager):
    """Testa a leitura do arquivo TSV de preços de combustíveis"""
    # Iniciar sessão Spark
    spark = spark_manager.start_session()
    assert spark is not None, "Falha ao iniciar sessão Spark"
    
    logger.info("%s Iniciando teste de leitura de dados", LOG_EMOJIS['START'])
    
    # Inicializar DataLakeManager
    data_lake = DataLakeManager()
    
    # Encontrar o caminho do arquivo usando DataLakeManager
    data_path = data_lake.get_bronze_path(
        source=CONFIG.get('DATASET_SOURCE', 'kaggle'),
        dataset=CONFIG.get('DATASET_NAME', 'gas_prices_in_brazil')
    )
    
    # Verificar se o diretório base existe
    assert data_path.exists(), f"Diretório base não encontrado: {data_path}"
    
    # Encontrar a pasta mais recente
    latest_data_path = get_most_recent_data_path(data_path)
    
    # Listar arquivos TSV na pasta mais recente
    tsv_files = list(latest_data_path.glob("*.tsv"))
    logger.info("\nArquivos TSV encontrados: %s", [f.name for f in tsv_files])
    
    # Verificar se existem arquivos TSV
    assert len(tsv_files) > 0, f"Nenhum arquivo .tsv encontrado em {latest_data_path}"
    
    # Pegar o primeiro arquivo TSV para teste
    tsv_file = str(tsv_files[0])
    logger.info("\nLendo arquivo: %s", tsv_file)
    
    # Ler o arquivo TSV
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("delimiter", "\t") \
        .csv(tsv_file)
    
    # Verificações básicas
    assert df is not None, "DataFrame não foi criado"
    row_count = df.count()
    assert row_count > 0, "DataFrame está vazio"
    
    # Verificar colunas necessárias para o modelo dimensional
    required_columns = {
        # Dimensão Região/Estado
        'REGIÃO': str,
        'ESTADO': str,
        
        # Dimensão Produto
        'PRODUTO': str,
        'UNIDADE DE MEDIDA': str,
        
        # Dimensão Tempo (aceitando date type)
        'DATA INICIAL': 'date',
        'DATA FINAL': 'date',
        
        # Métricas (aceitando integer para contagem)
        'PREÇO MÉDIO REVENDA': float,
        'NÚMERO DE POSTOS PESQUISADOS': 'integer'
    }
    
    # Verificar schema
    schema = {field.name: field.dataType.typeName() for field in df.schema.fields}
    logger.info("\nSchema do DataFrame:")
    for col, type_name in schema.items():
        logger.info("%s: %s", col, type_name)
    
    missing_columns = []
    wrong_types = []
    
    for col, expected_type in required_columns.items():
        if col not in schema:
            missing_columns.append(col)
        else:
            # Verificar tipo de dados (simplificado)
            current_type = schema[col].lower()
            if expected_type == str and 'string' not in current_type:
                wrong_types.append(f"{col} deveria ser string, mas é {current_type}")
            elif expected_type == float and 'double' not in current_type and 'decimal' not in current_type:
                wrong_types.append(f"{col} deveria ser número, mas é {current_type}")
            elif expected_type == 'date' and 'date' not in current_type:
                wrong_types.append(f"{col} deveria ser date, mas é {current_type}")
            elif expected_type == 'integer' and 'integer' not in current_type:
                wrong_types.append(f"{col} deveria ser integer, mas é {current_type}")
    
    assert not missing_columns, f"Colunas faltando: {missing_columns}"
    assert not wrong_types, f"Tipos incorretos: {wrong_types}"
    
    # Verificar qualidade dos dados
    # 1. Contagem de valores nulos
    null_counts = {col: df.filter(df[col].isNull()).count() for col in required_columns.keys()}
    logger.info("\nContagem de valores nulos por coluna:")
    for col, count in null_counts.items():
        logger.info("%s: %d", col, count)
        
    # 2. Verificar valores únicos para dimensões
    logger.info("\nValores únicos por dimensão:")
    logger.info("Regiões: %d", df.select('REGIÃO').distinct().count())
    logger.info("Estados: %d", df.select('ESTADO').distinct().count())
    logger.info("Produtos: %d", df.select('PRODUTO').distinct().count())
    logger.info("Unidades de Medida: %d", df.select('UNIDADE DE MEDIDA').distinct().count())
    
    # Mostrar amostra dos dados
    logger.info("\nAmostra dos dados:")
    df.show(5, truncate=False)
    logger.info("%s Teste de leitura de dados concluído", LOG_EMOJIS['END'])
