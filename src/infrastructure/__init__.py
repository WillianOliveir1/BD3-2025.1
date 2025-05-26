"""
Módulo de infraestrutura para o sistema de análise de preços de combustíveis.

Componentes principais:
- DataLakeManager: Gerencia estrutura e operações do Data Lake
- SparkManager: Gerencia sessões Spark
"""

from .data_lake_manager import DataLakeManager
from .spark_manager import SparkManager
from .environment_manager import EnvironmentManager

__all__ = ['DataLakeManager', 'SparkManager', 'EnvironmentManager']
