"""
Testes para o processo de extração de dados do Kaggle
"""

# Imports da biblioteca padrão
import os
import sys
from pathlib import Path
from datetime import datetime

# Imports de bibliotecas de terceiros
import pytest

# Imports do projeto
from src.pipeline.kaggle.upstream.extractor import KaggleExtractor
from src.infrastructure.data_lake_manager import DataLakeManager
from src.infrastructure.logging_config import setup_logger, LOG_EMOJIS

# Configurar caminhos
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

# Configurar logger para testes
logger = setup_logger('TestExtract')

@pytest.fixture
def data_lake():
    """Fixture que fornece uma instância do DataLakeManager"""
    return DataLakeManager()

@pytest.fixture
def extractor(data_lake):
    """Fixture que fornece uma instância do KaggleExtractor"""
    return KaggleExtractor(data_lake_manager=data_lake)

def test_extraction(data_lake, extractor):
    """Testa o processo completo de extração de dados do Kaggle"""
    logger.info("="*70)
    logger.info("%s TESTE DE EXTRAÇÃO DO KAGGLE", LOG_EMOJIS['START'])
    logger.info("="*70)
    
    try:
        # Verificar credenciais no local padrão
        kaggle_creds_path = os.path.join(os.path.expanduser('~'), '.kaggle', 'kaggle.json')
        assert os.path.exists(kaggle_creds_path), \
            f"{LOG_EMOJIS['ERROR']} Arquivo de credenciais do Kaggle não encontrado em {kaggle_creds_path}"
        logger.info("%s Credenciais do Kaggle encontradas", LOG_EMOJIS['SUCCESS'])
        
        # Executar extração
        logger.info("\n%s Iniciando extração do dataset...", LOG_EMOJIS['EXTRACT'])
        result = extractor.extract_dataset()
        assert result is not None, f"{LOG_EMOJIS['ERROR']} A extração falhou"
        logger.info("%s Dataset extraído com sucesso", LOG_EMOJIS['SUCCESS'])
        
        # Verificar estrutura de diretórios
        bronze_path = data_lake.get_bronze_path(
            source='kaggle',
            dataset='gas_prices_in_brazil',
            date=datetime.now()
        )
        assert bronze_path.exists(), \
            f"{LOG_EMOJIS['ERROR']} Diretório bronze não encontrado: {bronze_path}"
        logger.info("%s Estrutura de diretórios verificada", LOG_EMOJIS['SUCCESS'])
        
        # Verificar arquivo
        tsv_files = list(bronze_path.glob('**/*.tsv'))
        assert tsv_files, f"{LOG_EMOJIS['ERROR']} Nenhum arquivo TSV encontrado em {bronze_path}"
        logger.info("%s Arquivos TSV encontrados: %d", LOG_EMOJIS['FILE'], len(tsv_files))
        
        # Verificar tamanho do arquivo
        file_size = os.path.getsize(tsv_files[0])
        file_size_mb = file_size / (1024 * 1024)
        assert file_size > 0, f"{LOG_EMOJIS['ERROR']} Arquivo vazio: {tsv_files[0]}"
        logger.info("%s Arquivo validado: %s (%.2f MB)", LOG_EMOJIS['SUCCESS'], tsv_files[0], file_size_mb)
        
        logger.info("\n%s TESTE DE EXTRAÇÃO CONCLUÍDO COM SUCESSO!", LOG_EMOJIS['SUCCESS'])
        
    except Exception as e:
        logger.error("%s ERRO NO TESTE: %s", LOG_EMOJIS['ERROR'], str(e))
        raise
