"""
DataLakeManager é responsável por gerenciar a estrutura do data lake e suas operações.
"""

from pathlib import Path
import os
from datetime import datetime
from src.infrastructure.config import CONFIG
from src.infrastructure.logging_config import setup_logger, LOG_EMOJIS

class DataLakeManager:
    """
    Gerencia a estrutura e operações do data lake.
    Implementa o padrão medallion (bronze, silver, gold).
    """
    
    def __init__(self, base_path: str = None):
        """
        Inicializa o DataLakeManager.
        
        Args:
            base_path: Caminho base para o data lake. Se não fornecido, usa o caminho definido em CONFIG['DATA_PATH'].
        """
        self.logger = setup_logger('DataLakeManager')
        
        self.base_path = Path(base_path) if base_path else Path(CONFIG['DATA_PATH'])
        
        # Define os diretórios das camadas usando configurações centrais
        self.bronze_dir = self.base_path / CONFIG['BRONZE_PATH']
        self.silver_dir = self.base_path / CONFIG['SILVER_PATH']
        self.gold_dir = self.base_path / CONFIG['GOLD_PATH']
        
        # Cria a estrutura base
        self._create_base_structure()
        
    def _create_base_structure(self):
        """Cria a estrutura base do data lake."""
        self.logger.info("%s Inicializando estrutura do data lake em: %s", LOG_EMOJIS['START'], self.base_path)
        
        # Cria diretórios base
        os.makedirs(self.bronze_dir, exist_ok=True)
        os.makedirs(self.silver_dir, exist_ok=True)
        os.makedirs(self.gold_dir, exist_ok=True)
        
        self.logger.info("%s Estrutura base do data lake criada com sucesso", LOG_EMOJIS['SUCCESS'])
    
    def get_bronze_path(self, source: str, dataset: str, date: datetime = None) -> Path:
        """
        Retorna o caminho para a camada bronze.
        
        Args:
            source: Nome da fonte de dados (ex: kaggle, api, etc)
            dataset: Nome do dataset
            date: Data de processamento (opcional)
        
        Returns:
            Path completo para o diretório na camada bronze
        """
        path = self.bronze_dir / source / dataset
        if date:
            path = path / date.strftime("%Y%m%d")
        return path
    
    def get_silver_path(self, dataset: str, date: datetime = None) -> Path:
        """
        Retorna o caminho para a camada silver.
        
        Args:
            dataset: Nome do dataset
            date: Data de processamento (opcional)
            
        Returns:
            Path completo para o diretório na camada silver
        """
        path = self.silver_dir / dataset
        if date:
            path = path / date.strftime("%Y%m%d")
        return path
    
    def get_gold_path(self, dataset: str, date: datetime = None) -> Path:
        """
        Retorna o caminho para a camada gold.
        
        Args:
            dataset: Nome do dataset
            date: Data de processamento (opcional)
            
        Returns:
            Path completo para o diretório na camada gold
        """
        path = self.gold_dir / dataset
        if date:
            path = path / date.strftime("%Y%m%d")
        return path
    
    def create_processing_dirs(self, source: str, dataset: str, date: datetime) -> tuple[Path, Path, Path]:
        """
        Cria os diretórios necessários para o processamento de um dataset.
        
        Args:
            source: Nome da fonte de dados
            dataset: Nome do dataset
            date: Data de processamento
            
        Returns:
            Tupla com os paths para as camadas (bronze, silver, gold)
        """        
        
        # Cria diretórios com data
        bronze_path = self.get_bronze_path(source, dataset, date)
        silver_path = self.get_silver_path(dataset, date)
        gold_path = self.get_gold_path(dataset, date)
        
        self.logger.info("%s Criando diretórios de processamento para %s:", LOG_EMOJIS['FILE'], dataset)
        self.logger.info("%s Bronze: %s", LOG_EMOJIS['FILE'], bronze_path)
        self.logger.info("%s Silver: %s", LOG_EMOJIS['FILE'], silver_path)
        self.logger.info("%s Gold: %s", LOG_EMOJIS['FILE'], gold_path)
        
        os.makedirs(bronze_path, exist_ok=True)
        os.makedirs(silver_path, exist_ok=True)
        os.makedirs(gold_path, exist_ok=True)
        
        return bronze_path, silver_path, gold_path
