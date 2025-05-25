"""
DataLakeManager é responsável por gerenciar a estrutura do data lake e suas operações.
"""

from pathlib import Path
import os
import logging
from datetime import datetime

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

class DataLakeManager:
    """
    Gerencia a estrutura e operações do data lake.
    Implementa o padrão medallion (bronze, silver, gold).
    """
    
    def __init__(self, base_path: str = None):
        """
        Inicializa o DataLakeManager.
        
        Args:
            base_path: Caminho base para o data lake. Se não fornecido,
                      usa o diretório 'data' no root do projeto.
        """
        self.logger = setup_logger('DataLakeManager')
        
        if base_path:
            self.base_path = Path(base_path)
        else:
            # Encontra o diretório root do projeto (onde está o README.md)
            current_dir = Path(__file__).resolve().parent
            while current_dir.name != 'BD3-2025.1' and current_dir.parent != current_dir:
                current_dir = current_dir.parent
            self.base_path = current_dir / 'data'
        
        # Define os diretórios das camadas
        self.bronze_dir = self.base_path / 'bronze'
        self.silver_dir = self.base_path / 'silver'
        self.gold_dir = self.base_path / 'gold'
        
        # Cria a estrutura base
        self._create_base_structure()
        
    def _create_base_structure(self):
        """Cria a estrutura base do data lake."""
        self.logger.info(f"Inicializando estrutura do data lake em: {self.base_path}")
        
        # Cria diretórios base
        os.makedirs(self.bronze_dir, exist_ok=True)
        os.makedirs(self.silver_dir, exist_ok=True)
        os.makedirs(self.gold_dir, exist_ok=True)
        
        self.logger.info("Estrutura base do data lake criada com sucesso")
    
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
        
        os.makedirs(bronze_path, exist_ok=True)
        os.makedirs(silver_path, exist_ok=True)
        os.makedirs(gold_path, exist_ok=True)
        
        return bronze_path, silver_path, gold_path
