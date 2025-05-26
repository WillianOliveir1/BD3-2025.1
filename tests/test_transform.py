"""
Testes para o transformador multidimensional usando pytest
"""
import logging
from pathlib import Path
import pytest
from pyspark.sql import DataFrame
from src.pipeline.kaggle.midstream.transformer import MultidimensionalTransformer
import os

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@pytest.fixture
def transformer():
    """Fixture que fornece uma instância configurada do transformador"""
    base_path = Path(__file__).resolve().parent.parent
    silver_path = base_path / "data/silver"
    return MultidimensionalTransformer(output_base_path=str(silver_path))

@pytest.fixture
def bronze_file():
    """Fixture que fornece o caminho para o arquivo bronze mais recente"""
    base_path = Path(__file__).resolve().parent.parent    
    bronze_path = base_path / "data/bronze/kaggle/gas_prices_in_brazil"
    # Encontrar a pasta mais recente
    latest_date = sorted(path.name for path in bronze_path.iterdir() if path.is_dir())[-1]
    files = list((bronze_path / latest_date).glob("*.tsv"))
    if not files:
        pytest.skip("Arquivo TSV não encontrado na camada bronze")
    return str(files[0])

@pytest.fixture(autouse=True)
def cleanup_spark():
    """Fixture para garantir limpeza do Spark após cada teste"""
    yield
    # Força limpeza após cada teste
    transformer = MultidimensionalTransformer()
    if hasattr(transformer, 'spark_manager') and transformer.spark_manager:
        transformer.spark_manager.stop_session()

def test_transform_to_star_schema_basic(transformer, bronze_file):
    """Testa a transformação básica para modelo Star Schema"""
    # Executar transformação
    result = transformer.transform_to_star_schema(bronze_file)
    
    # Verificar sucesso
    assert result['success'], f"Transformação falhou: {result.get('error')}"
    
    # Verificar arquivos gerados
    output_path = Path(result['output_path'])
    expected_files = [
        'dim_regiao.parquet',
        'dim_estado.parquet',
        'dim_produto.parquet',
        'dim_tempo.parquet',
        'fact_precos.parquet'
    ]
    
    for file in expected_files:
        assert (output_path / file).exists(), f"Arquivo {file} não foi criado"

def test_dimension_table_structures(transformer, bronze_file):
    """Testa a estrutura das tabelas dimensão"""
    result = transformer.transform_to_star_schema(bronze_file)
    output_path = Path(result['output_path'])
    
    # Iniciar nova sessão Spark para leitura
    transformer._setup_spark()
    spark = transformer.spark
    
    # Verificar dim_regiao
    dim_regiao = spark.read.parquet(str(output_path / "dim_regiao.parquet"))
    assert set(dim_regiao.columns) == {"id", "nome"}, "Estrutura incorreta da dim_regiao"
    
    # Verificar dim_estado
    dim_estado = spark.read.parquet(str(output_path / "dim_estado.parquet"))
    assert set(dim_estado.columns) == {"id", "nome", "regiaoId"}, "Estrutura incorreta da dim_estado"
    
    # Verificar dim_produto
    dim_produto = spark.read.parquet(str(output_path / "dim_produto.parquet"))
    assert set(dim_produto.columns) == {"id", "nome", "unidade"}, "Estrutura incorreta da dim_produto"
    
    # Verificar dim_tempo
    dim_tempo = spark.read.parquet(str(output_path / "dim_tempo.parquet"))
    expected_cols = {"id", "inicio", "fim", "semana", "mes", "ano"}
    assert set(dim_tempo.columns) == expected_cols, "Estrutura incorreta da dim_tempo"

def test_fact_table_structure(transformer, bronze_file):
    """Testa a estrutura da tabela fato"""
    result = transformer.transform_to_star_schema(bronze_file)
    output_path = Path(result['output_path'])
    
    # Iniciar nova sessão Spark para leitura
    transformer._setup_spark()
    spark = transformer.spark
    
    fact = spark.read.parquet(str(output_path / "fact_precos.parquet"))
    expected_cols = {
        "id", "tempoId", "estadoId", "produtoId",
        "numPostoPesq", "pMed", "desvioPadrao",
        "pMin", "pMax", "coefVariacao", "margemMediaRevenda"
    }
    assert set(fact.columns) == expected_cols, "Estrutura incorreta da fact_precos"

def test_referential_integrity(transformer, bronze_file):
    """Testa a integridade referencial entre fato e dimensões"""
    result = transformer.transform_to_star_schema(bronze_file)
    output_path = Path(result['output_path'])
    
    # Iniciar nova sessão Spark para leitura
    transformer._setup_spark()
    spark = transformer.spark
    
    # Carregar tabelas
    fact = spark.read.parquet(str(output_path / "fact_precos.parquet"))
    dim_estado = spark.read.parquet(str(output_path / "dim_estado.parquet"))
    dim_produto = spark.read.parquet(str(output_path / "dim_produto.parquet"))
    dim_tempo = spark.read.parquet(str(output_path / "dim_tempo.parquet"))
    
    # Verificar integridade referencial estado
    estado_ids_fact = set(row['estadoId'] for row in fact.select('estadoId').distinct().collect())
    estado_ids_dim = set(row['id'] for row in dim_estado.select('id').collect())
    assert estado_ids_fact.issubset(estado_ids_dim), "Violação de integridade referencial em estado"
    
    # Verificar integridade referencial produto
    produto_ids_fact = set(row['produtoId'] for row in fact.select('produtoId').distinct().collect())
    produto_ids_dim = set(row['id'] for row in dim_produto.select('id').collect())
    assert produto_ids_fact.issubset(produto_ids_dim), "Violação de integridade referencial em produto"
    
    # Verificar integridade referencial tempo
    tempo_ids_fact = set(row['tempoId'] for row in fact.select('tempoId').distinct().collect())
    tempo_ids_dim = set(row['id'] for row in dim_tempo.select('id').collect())
    assert tempo_ids_fact.issubset(tempo_ids_dim), "Violação de integridade referencial em tempo"

def test_data_quality(transformer, bronze_file):
    """Testa a qualidade dos dados transformados"""
    result = transformer.transform_to_star_schema(bronze_file)
    output_path = Path(result['output_path'])
    
    # Iniciar nova sessão Spark para leitura
    transformer._setup_spark()
    spark = transformer.spark
    
    # Verificar dimensão região
    dim_regiao = spark.read.parquet(str(output_path / "dim_regiao.parquet"))
    assert dim_regiao.filter("nome is null").count() == 0, "Encontradas regiões sem nome"
    assert dim_regiao.count() == 5, "Número incorreto de regiões"
    
    # Verificar dimensão estado
    dim_estado = spark.read.parquet(str(output_path / "dim_estado.parquet"))
    assert dim_estado.filter("nome is null").count() == 0, "Encontrados estados sem nome"
    assert dim_estado.count() == 27, "Número incorreto de estados"
    
    # Verificar dimensão produto
    dim_produto = spark.read.parquet(str(output_path / "dim_produto.parquet"))
    assert dim_produto.filter("nome is null").count() == 0, "Encontrados produtos sem nome"
    assert dim_produto.filter("unidade is null").count() == 0, "Produtos sem unidade de medida"
    
    # Verificar tabela fato
    fact = spark.read.parquet(str(output_path / "fact_precos.parquet"))
    assert fact.filter("pMed is null").count() == 0, "Encontrados registros sem preço médio"
    assert fact.filter("pMed < 0").count() == 0, "Encontrados preços negativos"
