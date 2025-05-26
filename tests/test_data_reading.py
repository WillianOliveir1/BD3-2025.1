"""
Testes para leitura de dados
"""
import os
import pytest
from pathlib import Path
from pyspark.sql import SparkSession

@pytest.fixture(scope="module")
def spark():
    """Fixture que fornece uma sessão Spark para todos os testes"""
    # Configurar ambiente
    hadoop_home = os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(__file__)), "hadoop"))
    os.environ["HADOOP_HOME"] = hadoop_home
    os.environ["PATH"] = f"{os.path.join(hadoop_home, 'bin')};{os.environ.get('PATH', '')}"
    
    # Verificar se winutils.exe existe
    winutils_path = Path(hadoop_home) / "bin" / "winutils.exe"
    assert winutils_path.exists(), f"winutils.exe não encontrado em {winutils_path}"
    
    # Iniciar sessão Spark com configurações específicas para Windows
    spark = SparkSession.builder \
        .appName("TestDataReading") \
        .config("spark.driver.host", "localhost") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.master", "local[*]") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()
    
    yield spark
    
    # Limpar após os testes
    spark.stop()

def get_most_recent_data_path(base_path: Path) -> Path:
    """Retorna o caminho da pasta de dados mais recente"""
    # Listar todas as pastas que começam com uma data (YYYY-MM)
    data_folders = [f for f in base_path.iterdir() if f.is_dir() and len(f.name) >= 7 and f.name[:7].replace("-", "").isdigit()]
    
    if not data_folders:
        raise FileNotFoundError(f"Nenhuma pasta com data encontrada em {base_path}")
    
    # Ordenar por nome (que é a data) e pegar o mais recente
    latest_folder = sorted(data_folders)[-1]
    print(f"\nUsando pasta mais recente: {latest_folder}")
    return latest_folder

def test_read_gas_prices_tsv(spark):
    """Testa a leitura do arquivo TSV de preços de combustíveis"""
    # Encontrar o caminho do arquivo
    base_path = Path(__file__).parent.parent
    data_path = base_path / "data" / "bronze" / "kaggle" / "gas_prices_in_brazil"
    
    # Verificar se o diretório base existe
    assert data_path.exists(), f"Diretório base não encontrado: {data_path}"
    
    # Encontrar a pasta mais recente
    latest_data_path = get_most_recent_data_path(data_path)
    
    # Listar arquivos TSV na pasta mais recente
    tsv_files = list(latest_data_path.glob("*.tsv"))
    print(f"\nArquivos TSV encontrados: {[f.name for f in tsv_files]}")
    
    # Verificar se existem arquivos TSV
    assert len(tsv_files) > 0, f"Nenhum arquivo .tsv encontrado em {latest_data_path}"
    
    # Pegar o primeiro arquivo TSV para teste
    tsv_file = str(tsv_files[0])
    print(f"\nLendo arquivo: {tsv_file}")
    
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
        # Dimensão Tempo
        'DATA INICIAL': str,
        'DATA FINAL': str,
        # Dimensão Produto
        'PRODUTO': str,
        'UNIDADE DE MEDIDA': str,
        # Fatos
        'NÚMERO DE POSTOS PESQUISADOS': float,
        'PREÇO MÉDIO REVENDA': float,
        'DESVIO PADRÃO REVENDA': float,
        'PREÇO MÍNIMO REVENDA': float,
        'PREÇO MÁXIMO REVENDA': float,
        'COEF DE VARIAÇÃO REVENDA': float,
        'MARGEM MÉDIA REVENDA': float
    }
    
    # Verificar presença e tipo de cada coluna
    schema = dict(df.dtypes)
    print("\nEsquema do DataFrame:")
    for col, type_name in schema.items():
        print(f"{col}: {type_name}")
    
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
    
    assert not missing_columns, f"Colunas faltando: {missing_columns}"
    assert not wrong_types, f"Tipos incorretos: {wrong_types}"
    
    # Verificar qualidade dos dados
    # 1. Contagem de valores nulos
    null_counts = {col: df.filter(df[col].isNull()).count() for col in required_columns.keys()}
    print("\nContagem de valores nulos por coluna:")
    for col, count in null_counts.items():
        print(f"{col}: {count}")
        
    # 2. Verificar valores únicos para dimensões
    print("\nValores únicos por dimensão:")
    print(f"Regiões: {df.select('REGIÃO').distinct().count()}")
    print(f"Estados: {df.select('ESTADO').distinct().count()}")
    print(f"Produtos: {df.select('PRODUTO').distinct().count()}")
    print(f"Unidades de Medida: {df.select('UNIDADE DE MEDIDA').distinct().count()}")
    
    # Mostrar amostra dos dados
    print("\nAmostra dos dados:")
    df.show(5, truncate=False)
