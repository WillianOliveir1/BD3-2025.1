"""
Testes para o transformador multidimensional
"""

# Imports da biblioteca padrão
import sys
from pathlib import Path

# Imports de bibliotecas de terceiros
import pytest
from pyspark.sql import DataFrame

# Imports do projeto
from src.pipeline.kaggle.midstream.transformer import MultidimensionalTransformer
from src.infrastructure.logging_config import setup_logger, LOG_EMOJIS
from src.infrastructure.config import CONFIG
from src.infrastructure.data_lake_manager import DataLakeManager

# Configurar caminhos
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

# Configurar logger para testes
logger = setup_logger('TestTransform')

# Inicializar o DataLakeManager
data_lake = DataLakeManager()

@pytest.fixture
def transformer():
    """Fixture que fornece uma instância configurada do transformador"""
    silver_path = data_lake.silver_dir
    logger.info("%s Inicializando transformador com caminho: %s", LOG_EMOJIS['PROCESS'], silver_path)
    return MultidimensionalTransformer(str(silver_path))

@pytest.fixture
def bronze_file():
    """Fixture que fornece o caminho para o arquivo bronze mais recente"""
    bronze_path = data_lake.bronze_dir / CONFIG['DATASET_SOURCE'] / CONFIG['DATASET_NAME']
    logger.info("%s Procurando arquivo bronze mais recente em: %s", LOG_EMOJIS['PROCESS'], bronze_path)
    
    # Encontrar a pasta mais recente
    try:
        latest_date = sorted(path.name for path in bronze_path.iterdir() if path.is_dir())[-1]
        files = list((bronze_path / latest_date).glob("*.tsv"))
        if not files:
            logger.error("%s Arquivo TSV não encontrado na camada bronze", LOG_EMOJIS['ERROR'])
            pytest.skip("Arquivo TSV não encontrado na camada bronze")
        logger.info("%s Arquivo bronze encontrado: %s", LOG_EMOJIS['SUCCESS'], files[0])
        return str(files[0])
    except Exception as e:
        logger.error("%s Erro ao procurar arquivo bronze: %s", LOG_EMOJIS['ERROR'], str(e))
        pytest.skip("Erro ao procurar arquivo bronze: %s" % str(e))

def test_transform_to_star_schema_basic(transformer, bronze_file):
    """Testa a transformação básica para modelo Star Schema"""
    logger.info("="*70)
    logger.info("%s TESTE DE TRANSFORMAÇÃO PARA STAR SCHEMA", LOG_EMOJIS['START'])
    logger.info("="*70)
    
    try:
        # Executar transformação
        logger.info("\n%s Executando transformação do arquivo: %s", LOG_EMOJIS['TRANSFORM'], bronze_file)
        result = transformer.transform_to_star_schema(bronze_file)
        
        # Verificar sucesso
        assert result['success'], f"{LOG_EMOJIS['ERROR']} Transformação falhou: {result.get('error')}"
        logger.info("%s Transformação concluída com sucesso", LOG_EMOJIS['SUCCESS'])
        
        # Verificar arquivos gerados
        output_path = Path(result['output_path'])
        expected_files = CONFIG['DIMENSION_TABLES'] + [CONFIG['FACT_TABLE']]
        
        logger.info("\n%s Verificando arquivos gerados em: %s", LOG_EMOJIS['FILE'], output_path)
        for file_name in expected_files:
            file_path = output_path / file_name
            folder_exists = file_path.is_dir()
            has_success = (file_path / "_SUCCESS").exists() if folder_exists else False
            has_parquet = any(f.suffix == '.parquet' for f in file_path.glob('part-*.parquet')) if folder_exists else False
            
            assert folder_exists, f"{LOG_EMOJIS['ERROR']} Diretório ausente: {file_name}"
            assert has_success, f"{LOG_EMOJIS['ERROR']} Arquivo _SUCCESS ausente em: {file_name}"
            assert has_parquet, f"{LOG_EMOJIS['ERROR']} Arquivos parquet ausentes em: {file_name}"
            
            size_mb = sum(f.stat().st_size for f in file_path.glob('*') if f.is_file()) / (1024 * 1024)
            logger.info("   %s %s (%.2f MB)", LOG_EMOJIS['SUCCESS'], file_name, size_mb)
        
        logger.info("\n%s TESTE DE TRANSFORMAÇÃO CONCLUÍDO COM SUCESSO!", LOG_EMOJIS['SUCCESS'])
        
    except Exception as e:
        logger.error("%s ERRO NO TESTE: %s", LOG_EMOJIS['ERROR'], str(e))
        raise

def test_dimension_table_structures(transformer, bronze_file):
    """Testa a estrutura das tabelas dimensão"""
    logger.info("="*70)
    logger.info("%s TESTE DE ESTRUTURA DAS DIMENSÕES", LOG_EMOJIS['START'])
    logger.info("="*70)
    
    try:
        # Executar transformação
        logger.info("\n%s Executando transformação do arquivo: %s", LOG_EMOJIS['TRANSFORM'], bronze_file)
        result = transformer.transform_to_star_schema(bronze_file)
        
        # Verificar sucesso
        assert result['success'], f"{LOG_EMOJIS['ERROR']} Transformação falhou: {result.get('error')}"
        output_path = Path(result['output_path'])
        
        # Obter sessão Spark para leitura
        logger.info("\n%s Obtendo sessão Spark para validação", LOG_EMOJIS['PROCESS'])
        spark = transformer.get_spark_session()
        
        # Estrutura esperada das dimensões
        expected_structures = {
            'dim_regiao.parquet': {
                'description': 'Dimensão de Regiões',
                'columns': {"id", "nome"}
            },
            'dim_estado.parquet': {
                'description': 'Dimensão de Estados',
                'columns': {"id", "nome", "regiaoId"}
            },
            'dim_produto.parquet': {
                'description': 'Dimensão de Produtos',
                'columns': {"id", "nome", "unidade"}
            },
            'dim_tempo.parquet': {
                'description': 'Dimensão de Tempo',
                'columns': {"id", "inicio", "fim", "semana", "mes", "ano"}
            }
        }
        
        logger.info("\n%s Verificando estrutura das dimensões:", LOG_EMOJIS['DATA'])
        for file_name, structure in expected_structures.items():
            df = spark.read.parquet(str(output_path / file_name))
            actual_columns = set(df.columns)        
            logger.info("\n%s %s", LOG_EMOJIS['DATA'], structure['description'])
            logger.info("   %s Arquivo: %s", LOG_EMOJIS['FILE'], file_name)
            logger.info("   %s Colunas encontradas: %s", LOG_EMOJIS['PROCESS'], list(actual_columns))
            
            assert actual_columns == structure['columns'], \
                f"{LOG_EMOJIS['ERROR']} Estrutura incorreta em {structure['description']}"
            logger.info("   %s Estrutura validada com sucesso", LOG_EMOJIS['SUCCESS'])
        
        logger.info("\n%s TESTE DE ESTRUTURA CONCLUÍDO COM SUCESSO!", LOG_EMOJIS['SUCCESS'])
        
    except Exception as e:
        logger.error("%s ERRO NO TESTE: %s", LOG_EMOJIS['ERROR'], str(e))
        raise

def test_fact_table_structure(transformer, bronze_file):
    """Testa a estrutura da tabela fato"""
    logger.info("="*70)
    logger.info("%s TESTE DE ESTRUTURA DA TABELA FATO", LOG_EMOJIS['START'])
    logger.info("="*70)
    
    try:
        # Executar transformação
        logger.info("\n%s Executando transformação do arquivo: %s", LOG_EMOJIS['TRANSFORM'], bronze_file)
        result = transformer.transform_to_star_schema(bronze_file)
        
        # Verificar sucesso
        assert result['success'], f"{LOG_EMOJIS['ERROR']} Transformação falhou: {result.get('error')}"
        output_path = Path(result['output_path'])
        
        # Obter sessão Spark para leitura
        logger.info("\n%s Obtendo sessão Spark para validação", LOG_EMOJIS['PROCESS'])
        spark = transformer.get_spark_session()
        
        # Estrutura esperada da tabela fato
        fact_file = "fact_precos.parquet"
        expected_cols = {
            # Chaves
            "id": "ID único do registro",
            "tempoId": "Chave estrangeira para dimensão tempo",
            "estadoId": "Chave estrangeira para dimensão estado",
            "produtoId": "Chave estrangeira para dimensão produto",
            # Métricas
            "numPostoPesq": "Número de postos pesquisados",
            "pMed": "Preço médio de revenda",
            "desvioPadrao": "Desvio padrão do preço",
            "pMin": "Preço mínimo encontrado",
            "pMax": "Preço máximo encontrado",
            "coefVariacao": "Coeficiente de variação",
            "margemMediaRevenda": "Margem média de revenda"
        }
        
        # Verificar estrutura
        logger.info("\n%s Verificando estrutura da tabela fato:", LOG_EMOJIS['DATA'])
        logger.info("   %s Arquivo: %s", LOG_EMOJIS['FILE'], fact_file)
        
        fact = spark.read.parquet(str(output_path / fact_file))
        actual_columns = set(fact.columns)
        logger.info("\n%s Colunas encontradas:", LOG_EMOJIS['DATA'])
        for col in sorted(actual_columns):
            desc = expected_cols.get(col, "Coluna não esperada")
            logger.info("   • %s: %s", col, desc)
        
        assert actual_columns == set(expected_cols.keys()), \
            f"{LOG_EMOJIS['ERROR']} Estrutura incorreta da tabela fato"
        
        logger.info("\n%s Estrutura da tabela fato validada com sucesso", LOG_EMOJIS['SUCCESS'])
        
    except Exception as e:
        logger.error("%s ERRO NO TESTE: %s", LOG_EMOJIS['ERROR'], str(e))
        raise

def test_referential_integrity(transformer, bronze_file):
    """Testa a integridade referencial entre fato e dimensões"""
    result = transformer.transform_to_star_schema(bronze_file)
    output_path = Path(result['output_path'])
    
    # Obter sessão Spark para leitura
    spark = transformer.get_spark_session()
    
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
    logger.info("="*70)
    logger.info("%s TESTE DE QUALIDADE DOS DADOS", LOG_EMOJIS['START'])
    logger.info("="*70)
    
    try:
        # Executar transformação
        logger.info("\n%s Executando transformação do arquivo: %s", LOG_EMOJIS['TRANSFORM'], bronze_file)
        result = transformer.transform_to_star_schema(bronze_file)
        
        # Verificar sucesso
        assert result['success'], f"{LOG_EMOJIS['ERROR']} Transformação falhou: {result.get('error')}"
        output_path = Path(result['output_path'])
        
        # Obter sessão Spark para leitura
        logger.info("\n%s Obtendo sessão Spark para validação", LOG_EMOJIS['PROCESS'])
        spark = transformer.get_spark_session()
        
        # Critérios de qualidade
        quality_checks = [
            {
                'file': 'dim_regiao.parquet',
                'description': 'Dimensão de Regiões',
                'checks': [
                    ('nome is null', 0, 'Encontradas regiões sem nome'),
                    ('true', 5, 'Número incorreto de regiões')
                ]
            },
            {
                'file': 'dim_estado.parquet',
                'description': 'Dimensão de Estados',
                'checks': [
                    ('nome is null', 0, 'Encontrados estados sem nome'),
                    ('true', 27, 'Número incorreto de estados')
                ]
            },
            {
                'file': 'dim_produto.parquet',
                'description': 'Dimensão de Produtos',
                'checks': [
                    ('nome is null', 0, 'Encontrados produtos sem nome'),
                    ('unidade is null', 0, 'Produtos sem unidade de medida')
                ]
            },
            {
                'file': 'fact_precos.parquet',
                'description': 'Tabela Fato de Preços',
                'checks': [
                    ('pMed is null', 0, 'Encontrados registros sem preço médio'),
                    ('pMed < 0', 0, 'Encontrados preços negativos')
                ]
            }
        ]
        
        logger.info("\n%s Executando validações de qualidade:", LOG_EMOJIS['DATA'])
        for table in quality_checks:            
            logger.info("\n%s %s", LOG_EMOJIS['DATA'], table['description'])
            logger.info("   %s Arquivo: %s", LOG_EMOJIS['FILE'], table['file'])
            
            df = spark.read.parquet(str(output_path / table['file']))
            total_records = df.count()
            logger.info("   %s Total de registros: %d", LOG_EMOJIS['DATA'], total_records)
            
            for condition, expected, message in table['checks']:
                if condition == 'true':
                    actual = total_records
                else:
                    actual = df.filter(condition).count()
                
                assert actual == expected, \
                    f"{LOG_EMOJIS['ERROR']} {message} (encontrado: {actual}, esperado: {expected})"
                
                if condition == 'true':
                    logger.info("   %s Contagem total validada", LOG_EMOJIS['SUCCESS'])
                else:
                    logger.info("   %s Validação [%s] OK", LOG_EMOJIS['SUCCESS'], condition)
            
        logger.info("\n%s TESTE DE QUALIDADE CONCLUÍDO COM SUCESSO!", LOG_EMOJIS['SUCCESS'])
        
    except Exception as e:
        logger.error("%s ERRO NO TESTE: %s", LOG_EMOJIS['ERROR'], str(e))
        raise
