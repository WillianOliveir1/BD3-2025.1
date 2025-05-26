"""
Módulo responsável pela extração de dados do Kaggle.
"""
import sys
import os
from datetime import datetime
from pathlib import Path
from kaggle.api.kaggle_api_extended import KaggleApi
from typing import Optional

from src.infrastructure.config import CONFIG
from src.infrastructure.data_lake_manager import DataLakeManager
from src.infrastructure.logging_config import setup_logger, LOG_EMOJIS

class KaggleExtractor:
    """
    Responsável por extrair dados do Kaggle e armazenar na camada bronze.
    """
    
    def __init__(self, data_lake_manager: Optional[DataLakeManager] = None):
        """
        Inicializa o extrator do Kaggle.
        
        Args:
            data_lake_manager: Instância do DataLakeManager. Se não fornecida, cria uma nova instância.
        """
        self.logger = setup_logger('KaggleExtractor')
        self.data_lake = data_lake_manager or DataLakeManager()
        
        # Configurar API do Kaggle
        self.kaggle_dataset = CONFIG['KAGGLE_DATASET']
        os.environ['KAGGLE_USERNAME'] = CONFIG['KAGGLE_USERNAME']
        os.environ['KAGGLE_KEY'] = CONFIG['KAGGLE_KEY']
        # Inicializar API do Kaggle
        self.api = KaggleApi()
        self.api.authenticate()
        
        # Configurar dataset padrão
        self.dataset = os.getenv('KAGGLE_DATASET', "matheusfreitag/gas-prices-in-brazil")
        self.logger.info("%s Dataset configurado: %s", LOG_EMOJIS['CONFIG'], self.dataset)
    
    def extract_dataset(self, dataset_ref: str = None, filename: str = None, force_download: bool = False) -> Path:
        """
        Extrai um dataset do Kaggle e salva na camada bronze.
        
        Args:
            dataset_ref: Referência do dataset no formato "owner/dataset-name". Se não fornecido, usa o dataset padrão configurado
            filename: Nome do arquivo específico para baixar (opcional)
            
        Returns:
            Path para o arquivo salvo ou diretório contendo os arquivos
        """
        dataset_ref = dataset_ref or self.dataset
        self.logger.info("%s Iniciando extração do dataset: %s", LOG_EMOJIS['START'], dataset_ref)
        
        try:
            # Processar referência do dataset
            dataset_owner, dataset_name = dataset_ref.split('/')
            dataset_name = dataset_name.replace('-', '_')  # Convertendo hífens para underscores
            
            # Configurar diretórios
            processing_date = datetime.now()
            bronze_path = self.data_lake.get_bronze_path(
                source='kaggle',
                dataset=dataset_name,
                date=processing_date
            )
            # Verificar se já existe o arquivo
            existing_files = list(bronze_path.parent.glob('*/*/*.tsv'))  # Busca em todas as pastas de data
            if existing_files and not force_download:
                self.logger.info("%s Arquivo já existe em: %s", LOG_EMOJIS['FILE'], existing_files[0])
                return existing_files[0]
            bronze_path.mkdir(parents=True, exist_ok=True)
            
            # Baixar dataset
            self.logger.info("%s Baixando dataset %s", LOG_EMOJIS['DOWNLOAD'], dataset_ref)
            self.api.dataset_download_files(
                dataset_ref,
                path=bronze_path,
                unzip=True
            )
            if filename:
                # Retorna o arquivo específico
                target_file = bronze_path / filename
                if not target_file.exists():
                    self.logger.error("%s Arquivo %s não encontrado no dataset", LOG_EMOJIS['ERROR'], filename)
                    raise FileNotFoundError(f"Arquivo {filename} não encontrado no dataset")
                self.logger.info("%s Arquivo extraído com sucesso: %s", LOG_EMOJIS['SUCCESS'], target_file)
                return target_file
            
            # Se não especificou arquivo, retorna o primeiro TSV encontrado
            tsv_files = list(bronze_path.glob('*.tsv'))
            if not tsv_files:
                self.logger.error("%s Nenhum arquivo TSV encontrado no dataset", LOG_EMOJIS['ERROR'])
                raise FileNotFoundError("Nenhum arquivo TSV encontrado no dataset")
                
            self.logger.info("%s Dataset extraído com sucesso: %s", LOG_EMOJIS['SUCCESS'], tsv_files[0])
            return tsv_files[0]
        
        except Exception as e:
            self.logger.error("%s Erro na extração do dataset: %s", LOG_EMOJIS['ERROR'], str(e))
            raise
            
if __name__ == "__main__":
    # Test the extractor
    extractor = KaggleExtractor()
    file_path = extractor.extract_dataset()