"""
Script de teste para verificar a transformação de dados do pipeline ETL
"""

# Imports da biblioteca padrão
import sys
from pathlib import Path
import traceback
from typing import Tuple

# Imports de bibliotecas de terceiros
import pandas as pd

# Imports do projeto
from src.pipeline.kaggle.etl_pipeline import FuelPriceETL, PipelineStage
from src.infrastructure.config import CONFIG
from src.infrastructure.data_lake_manager import DataLakeManager
from src.infrastructure.logging_config import setup_logger, LOG_EMOJIS

# Configurar caminhos
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

# Configurar logger para testes
logger = setup_logger('TestETL')

def test_transform_stage():
    """
    Testa o estágio de transformação do pipeline ETL.
    Verifica se os dados da camada bronze são corretamente transformados
    em um modelo dimensional na camada silver.
    """
    logger.info("="*70)
    logger.info("%s TESTE DE TRANSFORMAÇÃO DO PIPELINE ETL", LOG_EMOJIS['START'])
    logger.info("="*70)
    
    try:
        # Inicializar pipeline e data lake manager
        logger.info("\n%s Inicializando pipeline e data lake manager...", LOG_EMOJIS['PROCESS'])
        data_lake_manager = DataLakeManager(str(project_root / "data"))
        etl = FuelPriceETL()
        
        # Encontrar arquivo Bronze mais recente para teste
        dataset_source = CONFIG['DATASET_SOURCE']
        dataset_name = CONFIG['DATASET_NAME']
        bronze_path = data_lake_manager.get_bronze_path(dataset_source, dataset_name)
        
        latest_bronze = None
        if bronze_path.exists():
            for date_dir in sorted(bronze_path.iterdir(), reverse=True):
                if date_dir.is_dir():                    
                    tsv_file = date_dir / CONFIG['RAW_DATASET_FILE']
                    if tsv_file.exists():
                        latest_bronze = tsv_file
                        break
                        
        # Verificar se temos dados para testar
        assert latest_bronze is not None, f"{LOG_EMOJIS['ERROR']} Nenhum arquivo Bronze encontrado para teste"
            
        logger.info("%s Usando arquivo Bronze: %s", LOG_EMOJIS['FILE'], latest_bronze)
        
        # Testar apenas transformação (dados já extraídos)
        logger.info("\n%s Testando etapa de transformação...", LOG_EMOJIS['TRANSFORM'])
        result = etl.transform(input_file=latest_bronze)

        # Verificar resultado
        assert result is not None, f"{LOG_EMOJIS['ERROR']} A transformação não retornou resultado"
        assert result.get('success') is True, f"{LOG_EMOJIS['ERROR']} {result.get('error', 'Erro desconhecido')}"
        
        logger.info("\n%s TESTE CONCLUÍDO COM SUCESSO!", LOG_EMOJIS['SUCCESS'])
        
        # Verificar estrutura Silver
        silver_path = Path(result['output_path'])  # Usar o caminho retornado pelo transformer
        logger.info("\n%s Verificando estrutura Silver em: %s", LOG_EMOJIS['DATA'], silver_path)
        
        expected_files = CONFIG['DIMENSION_TABLES'] + [CONFIG['FACT_TABLE']]
        
        # Verificar se todos os arquivos existem e têm dados
        for file_name in expected_files:
            file_path = silver_path / file_name
            folder_exists = file_path.is_dir()
            has_success = (file_path / "_SUCCESS").exists() if folder_exists else False
            has_parquet = any(f.suffix == '.parquet' for f in file_path.glob('part-*.parquet')) if folder_exists else False
            
            assert folder_exists, f"{LOG_EMOJIS['ERROR']} Diretório ausente: {file_name}"
            assert has_success, f"{LOG_EMOJIS['ERROR']} Arquivo _SUCCESS ausente em: {file_name}"
            assert has_parquet, f"{LOG_EMOJIS['ERROR']} Arquivos parquet ausentes em: {file_name}"
            
            size_mb = sum(f.stat().st_size for f in file_path.glob('*') if f.is_file()) / (1024 * 1024)
            logger.info("   %s %s (%.2f MB)", LOG_EMOJIS['SUCCESS'], file_name, size_mb)
            
    except Exception as e:
        logger.error("%s ERRO NO TESTE: %s", LOG_EMOJIS['ERROR'], str(e))
        logger.error(traceback.format_exc())
        raise

def verify_star_schema_structure():
    """
    Verifica se a estrutura star schema está correta e contém todos os arquivos
    e dados necessários.
    """
    logger.info("\n" + "="*70)
    logger.info("%s VERIFICAÇÃO DA ESTRUTURA STAR SCHEMA", LOG_EMOJIS['DATA'])
    logger.info("="*70)
    
    data_lake_manager = DataLakeManager(str(project_root / "data"))
    silver_path = data_lake_manager.get_silver_path(CONFIG['DATASET_NAME'])
    
    assert silver_path.exists(), f"{LOG_EMOJIS['ERROR']} Diretório Silver não encontrado!"
    
    try:
        # Usar as constantes do projeto para nomes das tabelas
        tables = {
            CONFIG['DIMENSION_TABLES'][0]: "Dimensão de Regiões",
            CONFIG['DIMENSION_TABLES'][1]: "Dimensão de Estados",
            CONFIG['DIMENSION_TABLES'][2]: "Dimensão de Produtos",
            CONFIG['DIMENSION_TABLES'][3]: "Dimensão de Tempo",
            CONFIG['FACT_TABLE']: "Tabela Fato de Preços"
        }
        
        total_size = 0
        
        for file_name, description in tables.items():
            file_path = silver_path / file_name
            assert file_path.exists(), f"{LOG_EMOJIS['ERROR']} {description} ({file_name}) - AUSENTE"
            
            try:
                df = pd.read_parquet(file_path)                    
                size_mb = file_path.stat().st_size / (1024 * 1024)
                total_size += size_mb
                
                # Verificar se o DataFrame tem dados
                assert len(df) > 0, f"{LOG_EMOJIS['ERROR']} {description} ({file_name}) está vazio"
                
                logger.info("%s %s", LOG_EMOJIS['SUCCESS'], description)
                logger.info("   %s %s", LOG_EMOJIS['FILE'], file_name)
                logger.info("   %s Registros: %s", LOG_EMOJIS['DATA'], format(len(df), ','))
                logger.info("   %s Tamanho: %.2f MB", LOG_EMOJIS['DATA'], size_mb)
                logger.info("   %s Colunas: %s", LOG_EMOJIS['PROCESS'], list(df.columns))
                logger.info("")
            except Exception as e:
                raise AssertionError(f"{LOG_EMOJIS['ERROR']} Erro ao ler {file_name}: {str(e)}")
                
        logger.info("%s TOTAL: %.2f MB em estrutura star schema", LOG_EMOJIS['DATA'], total_size)
        logger.info("%s Estrutura otimizada para queries Spark SQL!", LOG_EMOJIS['SUCCESS'])
        
    except Exception as e:
        logger.error("%s Erro na verificação: %s", LOG_EMOJIS['ERROR'], str(e))
        logger.error(traceback.format_exc())
        raise

def main():
    """
    Função principal que executa os testes de transformação e validação do schema
    """
    logger.info("%s Iniciando testes do pipeline ETL...", LOG_EMOJIS['START'])
    
    try:
        # Testar estágio de transformação
        test_transform_stage()
        
        # Verificar estrutura
        verify_star_schema_structure()
        
        logger.info("\n" + "="*70)
        logger.info("%s RESULTADO FINAL DOS TESTES", LOG_EMOJIS['DATA'])
        logger.info("="*70)
        logger.info("Transformação: %s OK", LOG_EMOJIS['SUCCESS'])
        logger.info("Star Schema: %s OK", LOG_EMOJIS['SUCCESS'])
        
        logger.info("\n%s TESTES CONCLUÍDOS COM SUCESSO!", LOG_EMOJIS['SUCCESS'])
        logger.info("   • Transformação de dados funcionando corretamente")
        logger.info("   • Estrutura Silver otimizada para Spark SQL")
        logger.info("   • Dados em formato Parquet apenas")
        logger.info("   • Star schema multidimensional implementado")
        
    except AssertionError as e:
        logger.error(str(e))
        logger.warning("\n%s PROBLEMAS DETECTADOS - Verifique logs acima", LOG_EMOJIS['WARNING'])
        raise
        
    except Exception as e:
        logger.error("%s Erro inesperado: %s", LOG_EMOJIS['ERROR'], str(e))
        logger.error(traceback.format_exc())
        logger.warning("\n%s PROBLEMAS DETECTADOS - Verifique logs acima", LOG_EMOJIS['WARNING'])
        raise

if __name__ == "__main__":
    main()
