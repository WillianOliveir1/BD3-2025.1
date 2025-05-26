"""
Módulo responsável pela extração de dados do Kaggle.
"""
import sys
import os
import logging
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from kaggle.api.kaggle_api_extended import KaggleApi
from typing import Optional

# Adiciona o diretório raiz ao PYTHONPATH
root_dir = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(root_dir))

from src.infrastructure.data_lake_manager import DataLakeManager

def setup_logger(name: str) -> logging.Logger:
    """Configura um logger com formato específico"""
    logger = logging.getLogger(name)
    
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    
    return logger

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
        
        # Carregar configurações
        self.logger.info("Carregando configurações do Kaggle")
        load_dotenv()
        
        # Configurar credenciais do Kaggle
        os.environ['KAGGLE_USERNAME'] = os.getenv('KAGGLE_USERNAME')
        os.environ['KAGGLE_KEY'] = os.getenv('KAGGLE_KEY')
        
        # Inicializar API do Kaggle
        self.api = KaggleApi()
        self.api.authenticate()        # Configurar dataset padrão
        self.dataset = os.getenv('KAGGLE_DATASET', "matheusfreitag/gas-prices-in-brazil")
        self.logger.info(f"Dataset configurado: {self.dataset}")
    
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
        self.logger.info(f"Iniciando extração do dataset: {dataset_ref}")
        
        try:            # Processar referência do dataset
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
                self.logger.info(f"Arquivo já existe em: {existing_files[0]}")
                return existing_files[0]
            bronze_path.mkdir(parents=True, exist_ok=True)
            
            # Baixar dataset
            self.logger.info(f"Baixando dataset {dataset_ref}")
            self.api.dataset_download_files(
                dataset_ref,
                path=bronze_path,
                unzip=True
            )
            
            if filename:
                # Retorna o arquivo específico
                target_file = bronze_path / filename
                if not target_file.exists():
                    raise FileNotFoundError(f"Arquivo {filename} não encontrado no dataset")
                self.logger.info(f"Arquivo encontrado em: {target_file}")
                return target_file
            else:
                # Retorna o diretório com todos os arquivos
                files = list(bronze_path.glob('*.*'))
                self.logger.info(
                    f"Dataset extraído com sucesso para {bronze_path}. "
                    f"Total de arquivos: {len(files)}"
                )
                return bronze_path
        
        except Exception as e:
            self.logger.error(f"Erro durante a extração do dataset: {str(e)}")
            raise
            
if __name__ == "__main__":
    # Test the extractor
    extractor = KaggleExtractor()
    file_path = extractor.extract_dataset()